package connector

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/invopop/jsonschema"
	go_ora "github.com/sijms/go-ora/v2"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
)

var featureFlagDefaults = map[string]common.FlagDefault{
	"retain_existing_data_on_backfill": common.FlagDisabled,
	"truncate_after_backfill":          common.FlagEnabled,
}

const defaultPort = "1521"

type AuthType string

const UserPassword AuthType = "UserPassword"

type userPasswordConfig struct {
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=1"`
}

type credentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	userPasswordConfig
}

func (credentialsConfig) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(UserPassword),
		schemagen.OneOfSubSchema("Password", userPasswordConfig{}, string(UserPassword)),
	)
}

func (c *credentialsConfig) Validate() error {
	switch c.AuthType {
	case UserPassword:
		if c.Password == "" {
			return errors.New("missing 'password'")
		}
		return nil
	}
	return fmt.Errorf("unknown 'auth_type' %q", c.AuthType)
}

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type sslMode string

const (
	sslDisable    sslMode = "disable"
	sslRequire    sslMode = "require"
	sslVerifyFull sslMode = "verify-full"
)

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
}

type config struct {
	Address     string            `json:"address" jsonschema:"title=Address,description=Host and port of the database (in the form of host[:port]). Port 1521 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	User        string            `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Credentials credentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=2"`
	Database    string            `json:"database" jsonschema:"title=Database,description=Service name of the database (pluggable database) to materialize to." jsonschema_extras:"order=3"`
	Schema      string            `json:"schema,omitempty" jsonschema:"title=Schema,description=Schema for bound collection tables (unless overridden within the binding resource configuration). Defaults to the schema of the user." jsonschema_extras:"order=4"`
	SSLMode     sslMode           `json:"sslmode" jsonschema:"title=SSL Mode,description=How to secure the connection. 'verify-full' connects over TLS (TCPS) and verifies the server certificate and host name. 'require' connects over TLS without verifying them. 'disable' connects over plain TCP and relies on an SSH tunnel or Oracle native network encryption for security.,enum=verify-full,enum=require,enum=disable,default=verify-full" jsonschema_extras:"order=5,nonsensitive=true"`
	HardDelete  bool              `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=6,nonsensitive=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	if err := c.Credentials.Validate(); err != nil {
		return err
	}
	switch c.SSLMode {
	case sslDisable, sslRequire, sslVerifyFull:
	case "":
		return errors.New("missing 'sslmode'")
	default:
		return fmt.Errorf("invalid 'sslmode' %q", c.SSLMode)
	}
	return nil
}

func (c config) DefaultNamespace() string {
	return c.Schema
}

func (c config) FeatureFlags() (string, map[string]common.FlagDefault) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c config) tunnelEnabled() bool {
	return c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != ""
}

// toURI returns the go-ora connection string.
func (c config) toURI() string {
	var address = c.Address
	if c.tunnelEnabled() {
		address = "localhost:" + defaultPort
	}
	if !strings.Contains(address, ":") {
		address = address + ":" + defaultPort
	}

	var params = url.Values{}
	params.Set("PROGRAM", "Estuary Flow Materialization")
	switch c.SSLMode {
	case sslRequire:
		params.Set("SSL", "true")
		params.Set("SSL VERIFY", "false")
	case sslVerifyFull:
		params.Set("SSL", "true")
		params.Set("SSL VERIFY", "true")
	}

	var uri = url.URL{
		Scheme:   "oracle",
		Host:     address,
		User:     url.UserPassword(c.User, c.Credentials.Password),
		Path:     "/" + c.Database,
		RawQuery: params.Encode(),
	}
	return uri.String()
}

// sessionSettings make values parse and render the same way regardless of
// the server's and the user's NLS defaults. DDL_LOCK_TIMEOUT lets Apply's DDL
// wait for row locks of a transaction still in flight.
const sessionSettings = `ALTER SESSION SET TIME_ZONE = 'UTC' NLS_NUMERIC_CHARACTERS = '.,' DDL_LOCK_TIMEOUT = 60`

type sessionConnector struct {
	driver.Connector
}

func (c sessionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.(driver.ExecerContext).ExecContext(ctx, sessionSettings, nil); err != nil {
		conn.Close()
		return nil, fmt.Errorf("configuring session: %w", err)
	}
	return conn, nil
}

func (c config) openDB() *stdsql.DB {
	var connector = go_ora.NewConnector(c.toURI())
	if tlsConfig := c.tlsConfig(); tlsConfig != nil {
		connector.(*go_ora.OracleConnector).WithTLSConfig(tlsConfig)
	}
	return stdsql.OpenDB(sessionConnector{connector})
}

// tlsConfig verifies the server certificate against the configured host, not
// the address dialed, which is localhost through an SSH tunnel. The driver
// sets ServerName to the dialed address, so the check is our own.
func (c config) tlsConfig() *tls.Config {
	switch c.SSLMode {
	case sslRequire:
		return &tls.Config{InsecureSkipVerify: true}
	case sslVerifyFull:
		var host = c.Address
		if h, _, err := net.SplitHostPort(c.Address); err == nil {
			host = h
		}
		return &tls.Config{
			InsecureSkipVerify: true,
			VerifyConnection: func(cs tls.ConnectionState) error {
				if len(cs.PeerCertificates) == 0 {
					return errors.New("the server presented no certificate")
				}
				var intermediates = x509.NewCertPool()
				for _, cert := range cs.PeerCertificates[1:] {
					intermediates.AddCert(cert)
				}
				_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{DNSName: host, Intermediates: intermediates})
				return err
			},
		}
	}
	return nil
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table" jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)" jsonschema_extras:"x-schema-name=true"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false." jsonschema_extras:"x-delta-updates=true,nonsensitive=true"`
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("missing table")
	}
	return nil
}

func (c tableConfig) WithDefaults(cfg config) tableConfig {
	if c.Schema == "" {
		c.Schema = cfg.Schema
	}
	return c
}

func (c tableConfig) Parameters() ([]string, bool, error) {
	var path []string
	if c.Schema != "" {
		path = append(path, c.Schema)
	}
	path = append(path, c.Table)
	return path, c.Delta, nil
}

func NewDriver() *sql.Driver[config, tableConfig] {
	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-oracle",
		StartTunnel: func(ctx context.Context, cfg config) error {
			if !cfg.tunnelEnabled() {
				return nil
			}
			host, port, err := net.SplitHostPort(cfg.Address)
			if err != nil {
				host, port = cfg.Address, defaultPort
			}
			var sshConfig = &networkTunnel.SshConfig{
				SshEndpoint: cfg.NetworkTunnel.SshForwarding.SshEndpoint,
				PrivateKey:  []byte(cfg.NetworkTunnel.SshForwarding.PrivateKey),
				ForwardHost: host,
				ForwardPort: port,
				LocalPort:   defaultPort,
			}
			if err := sshConfig.CreateTunnel().Start(); err != nil {
				return fmt.Errorf("error starting network tunnel: %w", err)
			}
			return nil
		},
		NewEndpoint: func(ctx context.Context, cfg config, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			log.WithFields(log.Fields{
				"database": cfg.Database,
				"address":  cfg.Address,
				"user":     cfg.User,
			}).Info("opening database")

			var db = cfg.openDB()
			defer db.Close()

			// A failure to get a connection is a failure to log in, so it's
			// reported as such rather than as the first query's error.
			conn, err := db.Conn(ctx)
			if err != nil {
				return nil, cerrors.NewUserError(err, "unable to connect to database: "+connectError(err, cfg).Error())
			}
			conn.Close()

			var schema = cfg.Schema
			if schema == "" {
				if err := db.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual").Scan(&schema); err != nil {
					return nil, fmt.Errorf("querying the current schema: %w", err)
				}
			} else {
				schema = translateIdentifier(schema)
			}

			var dialect = newOracleDialect(schema)
			var templates = renderTemplates(dialect)

			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             dialect,
				MetaCheckpoints:     nil,
				NewClient:           newClient,
				CreateTableTemplate: templates.createTargetTable,
				NewTransactor:       newTransactor,
				ConcurrentApply:     false,
			}, nil
		},
		PreReqs: preReqs,
	}
}

const (
	// Bounds of the rows of one array-bound statement, which the driver
	// buffers in full.
	storeBatchRows  = 10_000
	storeBatchBytes = 32 << 20
	// Bounds of the keys of one Load query.
	loadBatchKeys  = 10_000
	loadBatchBytes = 4 << 20
)

type binding struct {
	target sql.Table
	// stage is the identifier of the table's stage table.
	stage          string
	createStageSQL string
	insertSQL      string
	loadSQL        string
	// order maps each column's INSERT bind position, after the token and
	// operation, to its index in Table.ConvertAll's output, and lobStart is the
	// first bind position of a LOB column.
	order    []int
	lobStart int

	// truncateSQL deletes the rows published before a boundary. It is empty
	// when truncateReason states why the table cannot be truncated.
	truncateSQL    string
	truncateReason error

	// The transaction being staged, if any.
	staged *stateItem
}

type transactor struct {
	cfg       config
	db        *stdsql.DB
	bindings  []*binding
	be        *m.BindingEvents
	loadConn  *stdsql.Conn
	storeConn *stdsql.Conn

	rangeKey string
	// The shard whose key range begins at 0 applies every shard's staged
	// rows; the others only stage.
	primary bool
	// The primary's staged transactions not yet applied, from every shard.
	state connectorState
	// The tokens of the entries of the state read at Open, which a previous
	// instance may have applied already.
	recovered map[string]bool

	truncateAfterBackfill bool
	// truncations maps a binding index to the boundary of a completed
	// backfill, for staging by Store.
	truncations map[int]time.Time
}

var _ m.Transactor = (*transactor)(nil)

func newTransactor(
	ctx context.Context,
	materializationName string,
	featureFlags map[string]bool,
	ep *sql.Endpoint[config],
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
	is *boilerplate.InfoSchema,
	be *m.BindingEvents,
) (m.Transactor, error) {
	var t = &transactor{
		cfg:                   ep.Config,
		be:                    be,
		rangeKey:              rangeKeyOf(fence.KeyBegin, fence.KeyEnd),
		primary:               fence.KeyBegin == 0,
		state:                 make(connectorState),
		truncateAfterBackfill: sql.TruncateAfterBackfill(featureFlags),
	}

	var err error
	t.db = ep.Config.openDB()
	if t.loadConn, err = t.db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("load db.Conn: %w", err)
	}
	if t.storeConn, err = t.db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("store db.Conn: %w", err)
	}

	for _, target := range bindings {
		var b = &binding{
			target: target,
			stage:  ep.Dialect.Identifier(stagePath(target.Path)...),
		}
		b.createStageSQL = createStageSQL(&target, b.stage)
		b.insertSQL = stageInsertSQL(&target, b.stage)
		var cols []*sql.Column
		cols, b.order = storeColumns(&target)
		b.lobStart = 2 + len(cols)
		for i, c := range cols {
			if isLOB(c) {
				b.lobStart = 2 + i
				break
			}
		}
		if target.Document != nil {
			b.loadSQL = loadSQL(&target)
		}
		if col, err := target.PublishedAtColumn(); err != nil {
			b.truncateReason = err
		} else {
			b.truncateReason = addTruncation(b, col, is)
		}
		t.bindings = append(t.bindings, b)
	}

	if t.primary {
		if t.state, err = parseState(open.StateJson); err != nil {
			return nil, err
		}
		t.recovered = make(map[string]bool)
		for _, bucket := range t.state {
			for _, e := range bucket {
				t.recovered[e.Token] = true
			}
		}
		if err := t.prepareStages(ctx); err != nil {
			return nil, err
		}
	}

	return t, nil
}

// Staged rows this old that no pending entry has are abandoned: those of a
// zombie's Store, of a crash between staging a transaction and recording it,
// and of a binding's state key from before a backfill. A younger row may be
// pending in a newer state than this instance has, if it's a zombie.
const abandonedAge = 24 * time.Hour

// prepareStages creates the stage tables that don't exist, and deletes their
// abandoned rows.
func (t *transactor) prepareStages(ctx context.Context) error {
	for _, b := range t.bindings {
		if _, err := t.db.ExecContext(ctx, b.createStageSQL); err != nil {
			return fmt.Errorf("creating stage table %s: %w", b.stage, err)
		}
		var args = []any{tokenBefore(time.Now().Add(-abandonedAge))}
		for _, e := range t.state[b.target.StateKey] {
			args = append(args, e.Token)
		}
		res, err := t.db.ExecContext(ctx, pruneStageSQL(b.stage, len(args)-1), args...)
		if err != nil {
			return fmt.Errorf("deleting abandoned rows of stage table %s: %w", b.stage, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("deleting abandoned rows of stage table %s (rows affected): %w", b.stage, err)
		} else if n > 0 {
			log.WithFields(log.Fields{
				"table": b.stage,
				"rows":  n,
			}).Info("deleted abandoned staged rows")
		}
	}
	return nil
}

// addTruncation prepares b to delete the rows whose col is earlier than a
// boundary, compared in the type that col has in the table. It returns why b
// cannot be truncated, as a clause suited for an operator.
func addTruncation(b *binding, col *sql.Column, is *boilerplate.InfoSchema) error {
	var res = is.GetResource(b.target.Path)
	if res == nil {
		return fmt.Errorf("table %s was not found in the database", b.target.Identifier)
	}
	var existing = res.GetField(col.Field)
	if existing == nil {
		return fmt.Errorf("column %s was not found in the table", col.Identifier)
	}
	var ddl = strings.ToUpper(existing.Type)
	if !strings.HasPrefix(ddl, "TIMESTAMP") {
		return fmt.Errorf("column %s is not a TIMESTAMP", col.Identifier)
	}
	b.truncateSQL = truncateSQL(b.target.Identifier, col.Identifier, ddl)
	return nil
}

// truncateSQL deletes the rows whose column of type ddl is earlier than the
// boundary bound as :1. Stored values reach the column by conversion from the
// bound TIMESTAMP WITH TIME ZONE, so the boundary takes the same conversion
// and rounds the same way: a row published at or after it is never deleted.
func truncateSQL(table, column, ddl string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s < CAST(TO_TIMESTAMP_TZ(:1, '%s') AS %s)", table, column, timestampFormat, ddl)
}

func (t *transactor) RecoverCheckpoint(_ context.Context, _ pf.MaterializationSpec, _ pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

// UnmarshalState is a no-op: newTransactor reads the state from Open, to
// prune the stage tables before any shard stages rows.
func (t *transactor) UnmarshalState(json.RawMessage) error { return nil }

// mergePeerStatePatches folds the other shards' StartedCommit patches of the
// just-committed transaction into the primary's state. StartCommit records
// the primary's own entries, so their echo is skipped.
func (t *transactor) mergePeerStatePatches(patches []json.RawMessage) error {
	for _, patch := range patches {
		if bytes.Equal(bytes.TrimSpace(patch), []byte("null")) {
			// The runtime's encoding of a peer's full (non-merge-patch) state
			// replacement, which no shard here ever produces.
			return fmt.Errorf("unexpected state reset patch in aggregated shard state")
		}
		state, err := parseState(patch)
		if err != nil {
			return fmt.Errorf("parsing aggregated state patch: %w", err)
		}
		for sk, bucket := range state {
			for rk, e := range bucket {
				if rk == t.rangeKey {
					continue
				}
				// The previous transaction's entry is applied before the next
				// one's patch arrives, so a collision would lose staged rows.
				if t.state[sk][rk] != nil {
					return fmt.Errorf("shard %s staged a new transaction for %s while its previous one is still pending", rk, sk)
				}
				t.state.add(sk, rk, e)
			}
		}
	}
	return nil
}

// Flush records completed backfills on the primary only, since every shard
// receives the same completions and only the primary applies staged rows.
func (t *transactor) Flush(_ context.Context, completes map[int]time.Time) error {
	if !t.primary {
		return nil
	}
	t.truncations = make(map[int]time.Time, len(completes))
	for binding, boundary := range completes {
		var b = t.bindings[binding]
		if b.target.DeltaUpdates {
			continue
		} else if !t.truncateAfterBackfill {
			log.WithFields(log.Fields{
				"table":    b.target.Identifier,
				"boundary": boundary,
			}).Info("rows published before the backfill were not deleted because feature flags disable it")
			continue
		}
		if b.truncateSQL == "" {
			t.be.Warn(b.target.Identifier, fmt.Sprintf("Rows published before the backfill of table %s were not deleted because %s.", b.target.Identifier, b.truncateReason))
			continue
		}
		t.truncations[binding] = boundary
	}
	return nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()
	var started bool

	query := func(binding int, keys []byte) error {
		if !started {
			// Load results may only be sent once the prior commit is
			// acknowledged, and the query may run before the iterator drains.
			it.WaitForAcknowledged()
			t.be.StartedEvaluatingLoads()
			started = true
		}
		var b = t.bindings[binding]

		rows, err := t.loadConn.QueryContext(ctx, b.loadSQL, go_ora.Clob{String: string(keys), Valid: true})
		if err != nil {
			return fmt.Errorf("querying Load documents of %s: %w", b.target.Identifier, err)
		}
		defer rows.Close()

		for rows.Next() {
			var doc string
			if err := rows.Scan(&doc); err != nil {
				return fmt.Errorf("scanning Load document of %s: %w", b.target.Identifier, err)
			} else if err := loaded(binding, json.RawMessage(doc)); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("reading Load documents of %s: %w", b.target.Identifier, err)
		}
		return nil
	}

	// The primary applies the previous transaction's rows in its own
	// Acknowledge, which the other shards' acknowledgements don't wait for.
	// They hold their queries until the iterator ends at Flush, which follows
	// every shard's acknowledgement.
	var spill *loadSpill
	if !t.primary {
		spill = &loadSpill{}
		defer spill.close()
	}

	type pending struct {
		buf  []byte
		keys int
	}
	var batches = make(map[int]*pending)

	send := func(binding int, p *pending) error {
		p.buf = append(p.buf, ']')
		var err error
		if spill != nil {
			err = spill.add(binding, p.buf)
		} else {
			err = query(binding, p.buf)
		}
		p.buf, p.keys = p.buf[:0], 0
		return err
	}

	for it.Next() {
		var b = t.bindings[it.Binding]
		var p, ok = batches[it.Binding]
		if !ok {
			p = &pending{}
			batches[it.Binding] = p
		}

		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}
		if p.keys == 0 {
			p.buf = append(p.buf, '[')
		} else {
			p.buf = append(p.buf, ',')
		}
		if p.buf, err = appendLoadKey(p.buf, &b.target, converted); err != nil {
			return err
		}
		p.keys++

		if p.keys >= loadBatchKeys || len(p.buf) >= loadBatchBytes {
			if err := send(it.Binding, p); err != nil {
				return err
			}
		}
	}
	if err := it.Err(); err != nil {
		return err
	}

	for binding, p := range batches {
		if p.keys == 0 {
			continue
		} else if err := send(binding, p); err != nil {
			return err
		}
	}
	if spill != nil {
		if err := spill.replay(query); err != nil {
			return err
		}
	}
	if started {
		t.be.FinishedEvaluatingLoads()
	}
	return nil
}

// loadSpill holds the keys of Load queries in a temporary file until they can
// run.
type loadSpill struct {
	f *os.File
	w *bufio.Writer
}

func (s *loadSpill) add(binding int, keys []byte) error {
	if s.f == nil {
		var err error
		if s.f, err = os.CreateTemp("", "load-keys-"); err != nil {
			return fmt.Errorf("creating Load keys file: %w", err)
		}
		s.w = bufio.NewWriter(s.f)
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(binding))
	binary.LittleEndian.PutUint32(header[4:], uint32(len(keys)))
	if _, err := s.w.Write(header[:]); err != nil {
		return fmt.Errorf("writing Load keys file: %w", err)
	} else if _, err := s.w.Write(keys); err != nil {
		return fmt.Errorf("writing Load keys file: %w", err)
	}
	return nil
}

// replay calls fn with each query's binding and keys, in the order added.
func (s *loadSpill) replay(fn func(binding int, keys []byte) error) error {
	if s.f == nil {
		return nil
	} else if err := s.w.Flush(); err != nil {
		return fmt.Errorf("writing Load keys file: %w", err)
	} else if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewinding Load keys file: %w", err)
	}
	var r = bufio.NewReader(s.f)
	var header [8]byte
	var keys []byte
	for {
		if _, err := io.ReadFull(r, header[:]); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("reading Load keys file: %w", err)
		}
		var n = int(binary.LittleEndian.Uint32(header[4:]))
		if cap(keys) < n {
			keys = make([]byte, n)
		}
		keys = keys[:n]
		if _, err := io.ReadFull(r, keys); err != nil {
			return fmt.Errorf("reading Load keys file: %w", err)
		} else if err := fn(int(binary.LittleEndian.Uint32(header[:4])), keys); err != nil {
			return err
		}
	}
}

func (s *loadSpill) close() {
	if s.f != nil {
		s.f.Close()
		os.Remove(s.f.Name())
	}
}

// storeBatch accumulates one binding's rows for one array-bound insert into
// its stage table, with one array per bind position.
type storeBatch struct {
	columns     [][]any
	rows, bytes int
}

func newStoreBatch(b *binding) *storeBatch {
	return &storeBatch{columns: make([][]any, 2+len(b.order))}
}

func valueSize(v any) int {
	switch v := v.(type) {
	case string:
		return len(v)
	case []byte:
		return len(v)
	default:
		return 8
	}
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var ctx = it.Context()
	var round = it.Round
	var truncations = t.truncations
	t.truncations = nil
	var lastBinding = -1
	var batch *storeBatch

	// The rows are staged in one transaction, so that a Store which fails
	// leaves none behind. Its undo is small, since it only inserts.
	txn, err := t.storeConn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("DB.BeginTx: %w", err)
	}
	var defuser = sql.NewTxDefuser(txn)
	defer defuser.MaybeRollback()

	flush := func() error {
		if batch == nil || batch.rows == 0 {
			return nil
		}
		var b = t.bindings[lastBinding]
		if _, err := insertRows(ctx, txn, b.insertSQL, batch.columns, b.lobStart); err != nil {
			return fmt.Errorf("staging rows of %s: %w", b.target.Identifier, err)
		}
		batch = newStoreBatch(b)
		return nil
	}

	for it.Next(t.cfg.HardDelete) {
		if it.Binding != lastBinding {
			if err := flush(); err != nil {
				return nil, err
			}
			lastBinding = it.Binding
			batch = newStoreBatch(t.bindings[it.Binding])
		}
		var b = t.bindings[it.Binding]
		if b.staged == nil {
			b.staged = &stateItem{Token: newToken(), Round: round}
		}

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		var op string
		switch {
		case t.cfg.HardDelete && it.Delete:
			op = opDelete
			b.staged.Deletes++
		case it.Exists:
			op = opUpdate
			b.staged.Updates++
		default:
			op = opInsert
			b.staged.Inserts++
		}
		batch.columns[0] = append(batch.columns[0], b.staged.Token)
		batch.columns[1] = append(batch.columns[1], op)
		for pos, idx := range b.order {
			var v = converted[idx]
			if op == opDelete && idx >= len(b.target.Keys) {
				v = nil
			}
			batch.columns[2+pos] = append(batch.columns[2+pos], v)
			batch.bytes += valueSize(v)
		}
		batch.rows++

		if batch.rows >= storeBatchRows || batch.bytes >= storeBatchBytes {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}

	defuser.Defuse()
	return func(context.Context, *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		defer txn.Rollback()
		if err := txn.Commit(); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("committing staged rows: %w", err))
		}

		// The primary records its own entries here: Acknowledge skips their
		// echo in the state patches.
		var update = make(connectorState)
		for i, b := range t.bindings {
			var e = b.staged
			b.staged = nil
			if boundary, ok := truncations[i]; ok {
				if e == nil {
					e = &stateItem{Token: newToken(), Round: round}
				}
				e.TruncateBefore = &boundary
			}
			if e == nil {
				continue
			}
			update.add(b.target.StateKey, t.rangeKey, e)
			if t.primary {
				t.state.add(b.target.StateKey, t.rangeKey, e)
			}
		}
		if len(update) == 0 {
			return nil, nil
		}

		patch, err := json.Marshal(update)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("marshalling staged transactions: %w", err))
		}
		return &pf.ConnectorState{UpdatedJson: patch, MergePatch: true}, nil
	}, nil
}

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	if !t.primary {
		return nil, nil
	} else if err := t.mergePeerStatePatches(statePatches); err != nil {
		return nil, err
	}

	// Entries of a state key with no binding are left pending. Clearing an
	// entry from the state is a null at its place.
	var shouldProcess = m.StateKeyFilter(stateKeys)
	var pending = make(map[*binding]map[string]*stateItem)
	var clear = make(connectorState)
	for _, b := range t.bindings {
		var sk = b.target.StateKey
		if !shouldProcess(sk) || len(t.state[sk]) == 0 {
			continue
		}
		pending[b] = t.state[sk]
		for rk := range t.state[sk] {
			clear.add(sk, rk, nil)
		}
	}
	if len(pending) == 0 {
		return nil, nil
	}

	if err := t.apply(ctx, pending); err != nil {
		return nil, err
	}
	for sk, bucket := range clear {
		for rk := range bucket {
			delete(t.state[sk], rk)
		}
		if len(t.state[sk]) == 0 {
			delete(t.state, sk)
		}
	}

	patch, err := json.Marshal(clear)
	if err != nil {
		return nil, fmt.Errorf("marshalling state clearing patch: %w", err)
	}
	return &pf.ConnectorState{UpdatedJson: patch, MergePatch: true}, nil
}

// apply applies the pending entries of each binding in one transaction.
//
// Staged rows are deleted only by the transaction that applies them, so
// applying an entry whose rows are gone does nothing, and one entry can be
// applied any number of times. Each binding's stage table is locked first,
// which makes any other instance applying the same entries wait, then find
// the rows gone. Rows are missing only for entries recovered from Open's
// state; otherwise they were deleted by something else, which is an error.
func (t *transactor) apply(ctx context.Context, pending map[*binding]map[string]*stateItem) error {
	txn, err := t.storeConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer txn.Rollback()

	exec := func(query string, args ...any) (int64, error) {
		res, err := txn.ExecContext(ctx, query, args...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}

	type report struct {
		round int
		path  []string
		stats m.RowStats
	}
	var reports []report

	for _, b := range t.bindings {
		var entries = pending[b]
		if entries == nil {
			continue
		}
		t.be.StartedResourceCommit(b.target.Path)

		var tokens []any
		var staged stateItem
		var round, hasRound = 0, false
		var allRecovered = true
		for rk, e := range entries {
			tokens = append(tokens, e.Token)
			allRecovered = allRecovered && t.recovered[e.Token]
			staged.Inserts += e.Inserts
			staged.Updates += e.Updates
			staged.Deletes += e.Deletes
			if e.TruncateBefore != nil && (staged.TruncateBefore == nil || e.TruncateBefore.After(*staged.TruncateBefore)) {
				staged.TruncateBefore = e.TruncateBefore
			}
			if rk == t.rangeKey || !hasRound {
				round, hasRound = e.Round, true
			}
		}

		var stats = m.ExactRowStats(0, 0, 0)
		if staged.Inserts+staged.Updates+staged.Deletes > 0 {
			var replaced, deleted, inserted int64
			if _, err := txn.ExecContext(ctx, "LOCK TABLE "+b.stage+" IN EXCLUSIVE MODE"); err != nil {
				return fmt.Errorf("locking stage table %s: %w", b.stage, err)
			}
			if staged.Updates > 0 {
				if replaced, err = exec(applyDeleteSQL(&b.target, b.stage, opUpdate, len(tokens)), tokens...); err != nil {
					return fmt.Errorf("deleting the replaced rows of %s: %w", b.target.Identifier, err)
				}
			}
			if staged.Deletes > 0 {
				if deleted, err = exec(applyDeleteSQL(&b.target, b.stage, opDelete, len(tokens)), tokens...); err != nil {
					return fmt.Errorf("deleting rows of %s: %w", b.target.Identifier, err)
				}
			}
			if staged.Inserts+staged.Updates > 0 {
				if inserted, err = exec(applyInsertSQL(&b.target, b.stage, len(tokens)), tokens...); err != nil {
					return fmt.Errorf("inserting rows of %s: %w", b.target.Identifier, err)
				}
			}
			// All of the entries' rows are found, or none of them because a
			// previous instance applied the recovered entries.
			var want = staged.Inserts + staged.Updates + staged.Deletes
			if cleared, err := exec(clearStageSQL(b.stage, len(tokens)), tokens...); err != nil {
				return fmt.Errorf("deleting applied rows of stage table %s: %w", b.stage, err)
			} else if cleared != want && !(cleared == 0 && allRecovered) {
				return fmt.Errorf("stage table %s holds %d of the %d rows staged for %s", b.stage, cleared, want, b.target.Identifier)
			}
			stats = m.ExactRowStats(inserted-replaced, replaced, deleted)
		}

		// The truncation follows the stores, so it removes only the rows they
		// did not rewrite.
		if boundary := staged.TruncateBefore; boundary == nil {
			// Pass.
		} else if b.truncateSQL == "" {
			t.be.Warn(b.target.Identifier, fmt.Sprintf("Rows published before the backfill of table %s were not deleted because %s.", b.target.Identifier, b.truncateReason))
		} else if n, err := exec(b.truncateSQL, boundary.UTC().Format(goTimestampFormat)); err != nil {
			return fmt.Errorf("truncating %s after backfill: %w", b.target.Identifier, err)
		} else {
			stats = stats.WithTruncated(n)
			log.WithFields(log.Fields{
				"table":    b.target.Identifier,
				"boundary": *boundary,
				"deleted":  n,
			}).Info("truncated rows published before the backfill")
		}

		reports = append(reports, report{round, b.target.Path, stats})
		t.be.FinishedResourceCommit(b.target.Path)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing staged rows: %w", err)
	}
	for _, r := range reports {
		t.be.ReportRowStats(r.round, r.path, r.stats)
	}
	return nil
}

// lobSizeClass classes a value of a LOB column by the bind type it may take.
// A value binds as a LONG, sent after all other binds, when it's over 4000
// bytes, or over 32767 where the server supports 32K strings
// (MAX_STRING_SIZE=EXTENDED). The driver and Oracle each use one of those two
// limits, so values of one class are on the same side of either.
func lobSizeClass(v any) byte {
	var n int
	switch v := v.(type) {
	case string:
		n = len(v)
	case []byte:
		n = len(v)
	}
	switch {
	case n <= 4000:
		return 0
	case n <= 32767:
		return 1
	default:
		return 2
	}
}

// insertRows inserts the rows of columns, which holds one array per bind
// position, with the LOB columns from position lobStart. It runs one array
// insert per group of rows whose LOB values are of the same size classes.
//
// The driver types an array bind from its last non-NULL element, and Oracle
// from its largest. Where they disagree on whether a bind is a LONG, the
// driver sends the bind's values in a different position than Oracle reads
// them from, and Oracle stores them into the wrong columns without error.
func insertRows(ctx context.Context, txn *stdsql.Tx, insertSQL string, columns [][]any, lobStart int) (int64, error) {
	var rows = len(columns[0])
	var groups = make(map[string][]int)
	var order []string
	var key = make([]byte, len(columns)-lobStart)
	for r := 0; r < rows; r++ {
		for c := lobStart; c < len(columns); c++ {
			key[c-lobStart] = lobSizeClass(columns[c][r])
		}
		if _, ok := groups[string(key)]; !ok {
			order = append(order, string(key))
		}
		groups[string(key)] = append(groups[string(key)], r)
	}

	var total int64
	for _, k := range order {
		var args = columns
		if len(order) > 1 {
			args = make([][]any, len(columns))
			for c := range columns {
				for _, r := range groups[k] {
					args[c] = append(args[c], columns[c][r])
				}
			}
		}
		var want = int64(len(groups[k]))
		res, err := txn.ExecContext(ctx, insertSQL, asArgs(args)...)
		if err != nil {
			return 0, err
		} else if n, err := res.RowsAffected(); err != nil {
			return 0, fmt.Errorf("rows affected: %w", err)
		} else if n != want {
			return 0, fmt.Errorf("inserted %d rows of %d", n, want)
		}
		total += want
	}
	return total, nil
}

func asArgs(arrays [][]any) []any {
	var out = make([]any, len(arrays))
	for i := range arrays {
		out[i] = arrays[i]
	}
	return out
}

func (t *transactor) Destroy() {
	t.loadConn.Close()
	t.storeConn.Close()
	t.db.Close()
}
