package connector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"text/template"
	"time"
	"unicode/utf8"

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
	Schema      string            `json:"schema,omitempty" jsonschema:"title=Schema,description=Schema for bound collection tables (unless overridden within the binding resource configuration) as well as the flow_checkpoints_v1 table. Defaults to the schema of the user." jsonschema_extras:"order=4"`
	SSLMode     sslMode           `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=How to secure the connection. 'verify-full' connects over TLS (TCPS) and verifies the server certificate and host name. 'require' connects over TLS without verifying them. 'disable' connects over plain TCP.,enum=verify-full,enum=require,enum=disable,default=verify-full" jsonschema_extras:"order=5,nonsensitive=true"`
	HardDelete  bool              `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=6,nonsensitive=true"`

	NetworkTunnel *networkTunnel.TunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`

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
	// The driver's own parse error quotes the connection string, password
	// included, so an address it can't parse is rejected here.
	if _, err := url.Parse(c.toURI()); err != nil {
		return fmt.Errorf("invalid 'address' %q: expected host or host:port", c.Address)
	}
	if err := c.Credentials.Validate(); err != nil {
		return err
	}
	// Without known hosts the tunnel accepts any SSH server.
	if t := c.NetworkTunnel; t.InUse() && strings.TrimSpace(t.SSHForwarding.KnownHosts) == "" && !t.SSHForwarding.SkipHostKeyVerification {
		return errors.New("missing 'knownHosts' of the network tunnel: set SSH Known Hosts to the output of ssh-keyscan for the SSH server")
	}
	switch c.SSLMode {
	case "", sslDisable, sslRequire, sslVerifyFull:
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

// hostPort is the address, with the default port if it has none.
func (c config) hostPort() string {
	if _, _, err := net.SplitHostPort(c.Address); err == nil {
		return c.Address
	}
	return net.JoinHostPort(strings.Trim(c.Address, "[]"), defaultPort)
}

// toURI returns the go-ora connection string.
func (c config) toURI() string {
	var address = c.hostPort()
	if c.NetworkTunnel.InUse() {
		address = "localhost:" + defaultPort
	}

	var params = url.Values{}
	params.Set("PROGRAM", "Estuary Flow Materialization")
	// What is verified is up to tlsConfig.
	if c.SSLMode != sslDisable {
		params.Set("SSL", "true")
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

// sessionSettings make values parse, render and compare the same way
// regardless of the server's and the user's NLS defaults: a case-insensitive
// NLS_COMP would let a Load key match another key's row. DDL_LOCK_TIMEOUT lets
// Apply's DDL wait for row locks of a transaction still in flight.
const sessionSettings = `ALTER SESSION SET TIME_ZONE = 'UTC' NLS_NUMERIC_CHARACTERS = '.,' NLS_COMP = BINARY NLS_SORT = BINARY DDL_LOCK_TIMEOUT = 60`

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

// tlsConfig is verify-full unless another SSL mode is set.
func (c config) tlsConfig() *tls.Config {
	switch c.SSLMode {
	case sslDisable:
		return nil
	case sslRequire:
		return &tls.Config{InsecureSkipVerify: true}
	}
	if !c.NetworkTunnel.InUse() {
		return &tls.Config{}
	}

	host, _, _ := net.SplitHostPort(c.hostPort())
	return &tls.Config{
		// When using NetworkTunnel, because the dialed address is `localhost`
		// we disable the built-in check and instead do the check ourselves against
		// the configured hostname instead of the dialed `localhost`
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
			if !cfg.NetworkTunnel.InUse() {
				return nil
			}
			if _, err := cfg.NetworkTunnel.Start(ctx, cfg.hostPort(), defaultPort); err != nil {
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

			// This first query also connects, so its failure is reported as
			// one to connect.
			var maxStringBytes int
			if err := db.QueryRowContext(ctx, maxStringBytesQuery).Scan(&maxStringBytes); err != nil {
				return nil, cerrors.NewUserError(err, "unable to connect to database: "+connectError(err, cfg).Error())
			}

			var schema = cfg.Schema
			if schema == "" {
				if err := db.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual").Scan(&schema); err != nil {
					return nil, fmt.Errorf("querying the current schema: %w", err)
				}
			} else {
				schema = translateIdentifier(schema)
			}

			var metaBase sql.TablePath
			if cfg.Schema != "" {
				metaBase = append(metaBase, cfg.Schema)
			}

			var dialect = newOracleDialect(schema, maxStringBytes)
			var templates = renderTemplates(dialect)

			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             dialect,
				MetaCheckpoints:     sql.FlowCheckpointsTable(metaBase),
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
	// The driver buffers array-bound statements fully, so
	// we bound the number of rows to avoid RAM overuse.
	storeBatchRows  = 10_000
	storeBatchBytes = 32 << 20
	// Bounds of the keys of one Load query.
	loadBatchKeys  = 10_000
	loadBatchBytes = 4 << 20
)

type binding struct {
	target    sql.Table
	insertSQL string
	updateSQL string
	deleteSQL string
	// loadInsertSQL fills the binding's load table with keys, loadSQL selects
	// their documents, and loadClearSQL empties the table again.
	loadInsertSQL string
	loadSQL       string
	loadClearSQL  string

	// truncateSQL deletes the rows published before a boundary, or with soft
	// deletes marks them deleted. It is empty when truncateReason states why
	// the table cannot be truncated.
	truncateSQL    string
	truncateReason error

	// varcharChars holds how many characters each column, in Table.Columns
	// order, holds when it's a VARCHAR2 that isn't part of the key, and 0
	// otherwise.
	varcharChars []int
	existing     *boilerplate.ExistingResource
}

type transactor struct {
	cfg      config
	db       *stdsql.DB
	bindings []*binding
	be       *m.BindingEvents

	load struct {
		conn *stdsql.Conn
	}
	store struct {
		conn  *stdsql.Conn
		fence sql.Fence
	}

	fenceSQL string
	dialect  sql.Dialect
	// maxStringBytes is the result of maxStringBytesQuery.
	maxStringBytes int

	truncateAfterBackfill bool
	// truncations maps a binding index to the boundary of a completed
	// backfill, for deletion in this transaction's commit.
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
	if err := boilerplate.RequireSingleShard(open); err != nil {
		return nil, err
	}

	var t = &transactor{
		cfg:                   ep.Config,
		dialect:               ep.Dialect,
		be:                    be,
		truncateAfterBackfill: sql.TruncateAfterBackfill(featureFlags),
	}
	t.store.fence = fence

	var templates = renderTemplates(ep.Dialect)
	var fenceUpdate strings.Builder
	if err := templates.updateFence.Execute(&fenceUpdate, fence); err != nil {
		return nil, fmt.Errorf("evaluating fence template: %w", err)
	}
	t.fenceSQL = fenceUpdate.String()

	var err error
	t.db = ep.Config.openDB()
	if t.load.conn, err = t.db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("load db.Conn: %w", err)
	}
	if t.store.conn, err = t.db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("store db.Conn: %w", err)
	}
	if err := t.store.conn.QueryRowContext(ctx, maxStringBytesQuery).Scan(&t.maxStringBytes); err != nil {
		return nil, fmt.Errorf("querying the most bytes a VARCHAR2 holds: %w", err)
	}

	for _, target := range bindings {
		if err := t.addBinding(ctx, target, templates, is); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", target.Path, err)
		}
	}

	return t, nil
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, templates templates, is *boilerplate.InfoSchema) error {
	var b = &binding{
		target:       target,
		loadClearSQL: "TRUNCATE TABLE " + loadTableName(target.Binding),
		existing:     is.GetResource(target.Path),
	}
	if b.existing == nil {
		return fmt.Errorf("table %s was not found in the database", target.Identifier)
	}
	for i, col := range target.Columns() {
		var chars int
		if f := b.existing.GetField(col.Field); f != nil && f.Type == "VARCHAR2" && i >= len(target.Keys) {
			chars = f.CharacterMaxLength
		}
		b.varcharChars = append(b.varcharChars, chars)
	}

	var createLoadTable string
	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.insertSQL, templates.storeInsert},
		{&b.updateSQL, templates.storeUpdate},
		{&b.deleteSQL, templates.deleteQuery},
		{&createLoadTable, templates.createLoadTable},
		{&b.loadInsertSQL, templates.loadInsert},
		{&b.loadSQL, templates.loadQuery},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	// Only standard bindings are loaded.
	if !target.DeltaUpdates {
		if _, err := t.load.conn.ExecContext(ctx, createLoadTable); err != nil {
			return fmt.Errorf("creating the temporary load table: %w", err)
		}
	}

	if col, err := target.PublishedAtColumn(); err != nil {
		b.truncateReason = err
	} else {
		b.truncateReason = addTruncation(b, col, t.cfg.HardDelete)
	}

	t.bindings = append(t.bindings, b)
	return nil
}

// addTruncation prepares b to remove the rows whose published column is
// earlier than a boundary: to delete them, or with soft deletes to set their
// _meta/op to 'd'.
func addTruncation(b *binding, published *sql.Column, hardDelete bool) error {
	// With castToString the column holds text.
	if published.BareDDL != timestampDDL {
		return fmt.Errorf("column %s is not a TIMESTAMP", published.Identifier)
	} else if hardDelete {
		b.truncateSQL = truncateSQL(b.target.Identifier, published.Identifier, "")
		return nil
	}
	for _, col := range b.target.Values {
		if col.Ptr == "/_meta/op" {
			b.truncateSQL = truncateSQL(b.target.Identifier, published.Identifier, col.Identifier)
			return nil
		}
	}
	return fmt.Errorf("the binding excludes the _meta/op field, which soft deletes need to mark them deleted")
}

// truncateSQL deletes the rows whose published column is earlier than the
// boundary bound as :1, which is parsed like a stored value, or sets their op
// column to 'd' if op is set.
func truncateSQL(table, published, op string) string {
	var where = fmt.Sprintf("%s < TO_TIMESTAMP_TZ(:1, '%s')", published, timestampFormat)
	if op == "" {
		return fmt.Sprintf("DELETE FROM %s WHERE %s", table, where)
	}
	return fmt.Sprintf("UPDATE %s SET %s = 'd' WHERE %s", table, op, where)
}

func (t *transactor) RecoverCheckpoint(_ context.Context, _ pf.MaterializationSpec, _ pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return t.store.fence.Checkpoint, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error { return nil }

func (t *transactor) Flush(_ context.Context, completes map[int]time.Time) error {
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

func (t *transactor) Acknowledge(context.Context, []json.RawMessage, []string) (*pf.ConnectorState, error) {
	return nil, nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	// Keys go into each binding's load table as they arrive, in batches.
	type pending struct {
		rows  [][]any
		bytes int
	}
	var batches = make(map[int]*pending)
	insert := func(binding int, p *pending) error {
		var b = t.bindings[binding]
		if _, err := t.load.conn.ExecContext(ctx, b.loadInsertSQL, arrayArgs(p.rows)...); err != nil {
			return fmt.Errorf("inserting Load keys of %s: %w", b.target.Identifier, err)
		}
		p.rows, p.bytes = p.rows[:0], 0
		return nil
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
		p.rows = append(p.rows, converted)
		for _, v := range converted {
			p.bytes += valueSize(v)
		}
		if len(p.rows) >= loadBatchKeys || p.bytes >= loadBatchBytes {
			if err := insert(it.Binding, p); err != nil {
				return err
			}
		}
	}
	if err := it.Err(); err != nil {
		return err
	} else if len(batches) == 0 {
		return nil
	}

	// The iterator drained only once the prior commit was acknowledged, so
	// the documents can be loaded now.
	t.be.StartedEvaluatingLoads()
	for binding, p := range batches {
		var b = t.bindings[binding]
		if len(p.rows) != 0 {
			if err := insert(binding, p); err != nil {
				return err
			}
		}

		rows, err := t.load.conn.QueryContext(ctx, b.loadSQL)
		if err != nil {
			return fmt.Errorf("querying Load documents of %s: %w", b.target.Identifier, err)
		}
		// Next closes rows when it returns false, before the next query.
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

		if _, err := t.load.conn.ExecContext(ctx, b.loadClearSQL); err != nil {
			return fmt.Errorf("clearing Load keys of %s: %w", b.target.Identifier, err)
		}
	}
	t.be.FinishedEvaluatingLoads()
	return nil
}

// storeBatch accumulates one binding's array-bound statements.
type storeBatch struct {
	// inserts, updates and deletes hold a row of bind values for each
	// execution of their statement.
	inserts, updates, deletes [][]any
	rows, bytes               int
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
	txn, err := t.store.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("DB.BeginTx: %w", err)
	}
	// We use a defuser since the transaction needs to survive `Store`, so a
	// basic `defer txn.Rollback()` would not allow us to have that. The
	// defuser allows us to "defuse" the rollback at the end of Store, so that
	// StartCommit then can take over and actually commit the transaction.
	var defuser = sql.NewTxDefuser(txn)
	defer defuser.MaybeRollback()

	var round = it.Round
	var truncations = t.truncations
	var stats = make(map[int]m.RowStats)
	var lastBinding = -1
	var batch *storeBatch

	flush := func() error {
		if batch == nil || batch.rows == 0 {
			return nil
		}
		var b = t.bindings[lastBinding]
		var s = stats[lastBinding]

		deleted, err := writeRows(ctx, txn, b.deleteSQL, batch.deletes, t.maxStringBytes)
		if err != nil {
			return fmt.Errorf("deleting rows of %s: %w", b.target.Identifier, err)
		}
		updated, err := writeRows(ctx, txn, b.updateSQL, batch.updates, t.maxStringBytes)
		if err != nil {
			return fmt.Errorf("updating rows of %s: %w", b.target.Identifier, err)
		}
		inserted, err := writeRows(ctx, txn, b.insertSQL, batch.inserts, t.maxStringBytes)
		if err != nil {
			return fmt.Errorf("inserting rows of %s: %w", b.target.Identifier, err)
		}

		stats[lastBinding] = m.ExactRowStats(s.Inserted+inserted, s.Updated+updated, s.Deleted+deleted)
		batch = &storeBatch{}
		return nil
	}

	for it.Next(t.cfg.HardDelete) {
		if it.Binding != lastBinding {
			if err := flush(); err != nil {
				return nil, err
			}
			lastBinding = it.Binding
			batch = &storeBatch{}
			if _, ok := stats[it.Binding]; !ok {
				stats[it.Binding] = m.ExactRowStats(0, 0, 0)
			}
		}
		var b = t.bindings[it.Binding]

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}
		if long := b.overflowingColumns(converted, t.maxStringBytes); len(long) != 0 {
			// DDL can't change the table while this transaction has rows in
			// it, so the transaction is rolled back and retried once the
			// columns are CLOBs.
			txn.Rollback()
			return nil, t.widenToCLOB(ctx, b, long)
		}

		var keys = len(b.target.Keys)
		if t.cfg.HardDelete && it.Delete {
			batch.deletes = append(batch.deletes, converted[:keys])
		} else if it.Exists {
			// The UPDATE binds the values to set, and then the keys.
			batch.updates = append(batch.updates, slices.Concat(converted[keys:], converted[:keys]))
		} else {
			batch.inserts = append(batch.inserts, converted)
		}
		for _, v := range converted {
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
	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		defer txn.Rollback()

		for i, b := range t.bindings {
			var s, stored = stats[i]
			var boundary, truncate = truncations[i]
			if !stored && !truncate {
				continue
			} else if !stored {
				s = m.ExactRowStats(0, 0, 0)
			}
			t.be.StartedResourceCommit(b.target.Path)

			// The truncation follows the binding's stores, so it removes only
			// the rows they did not rewrite.
			if truncate {
				res, err := txn.ExecContext(ctx, b.truncateSQL, boundary.UTC().Format(goTimestampFormat))
				if err != nil {
					return nil, m.FinishedOperation(fmt.Errorf("truncating %s after backfill: %w", b.target.Identifier, err))
				}
				n, err := res.RowsAffected()
				if err != nil {
					return nil, m.FinishedOperation(fmt.Errorf("truncating %s after backfill (rows affected): %w", b.target.Identifier, err))
				}
				s = s.WithTruncated(n)
				log.WithFields(log.Fields{
					"table":    b.target.Identifier,
					"boundary": boundary,
					"deleted":  n,
				}).Info("truncated rows published before the backfill")
			}
			t.be.ReportRowStats(round, b.target.Path, s)
			t.be.FinishedResourceCommit(b.target.Path)
		}

		checkpoint, err := runtimeCheckpoint.Marshal()
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}
		if res, err := txn.ExecContext(ctx, t.fenceSQL, go_ora.Clob{String: base64.StdEncoding.EncodeToString(checkpoint), Valid: true}); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("updating flow checkpoint: %w", err))
		} else if n, err := res.RowsAffected(); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("updating flow checkpoint (rows affected): %w", err))
		} else if n != 1 {
			return nil, m.FinishedOperation(errors.New("this instance was fenced off by another"))
		}

		if err := txn.Commit(); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("committing Store transaction: %w", err))
		}
		return nil, nil
	}, nil
}

// overflowingColumns lists the columns of b's table that are a VARCHAR2
// but the current row has a value larger than their capacity
func (b *binding) overflowingColumns(row []any, maxStringBytes int) []int {
	var out []int
	for i, chars := range b.varcharChars {
		if s, ok := row[i].(string); ok && chars != 0 &&
			(len(s) > maxStringBytes || len(s) > chars && utf8.RuneCountInString(s) > chars) {
			out = append(out, i)
		}
	}
	return out
}

// widenToCLOB converts columns of b's table from VARCHAR2 to CLOB, and returns
// the error that has the runtime retry the transaction. The store transaction
// must be rolled back first, since its row locks would block the DDL.
func (t *transactor) widenToCLOB(ctx context.Context, b *binding, columns []int) error {
	var all = b.target.Columns()
	var migrations []sql.ColumnTypeMigration
	var names []string
	for _, i := range columns {
		var col = *all[i]
		col.MappedType = sql.MappedType{DDL: "CLOB", BareDDL: "CLOB", NullableDDL: "CLOB", TargetType: sql.StringDDL("CLOB")}
		var spec = t.dialect.MigratableTypes.FindMigrationSpec(*b.existing.GetField(col.Field), col.MappedType)
		if spec == nil {
			return fmt.Errorf("column %s of table %s can't be converted to CLOB", col.Identifier, b.target.Identifier)
		}
		migrations = append(migrations, sql.ColumnTypeMigration{
			Column:               col,
			MigrationSpec:        *spec,
			OriginalColumnExists: true,
			// An earlier conversion that was interrupted carries on.
			ProgressColumnExists: b.existing.GetField(col.Field+sql.ColumnMigrationTemporarySuffix) != nil,
		})
		names = append(names, col.Identifier)
	}

	stmts, err := sql.StdColumnTypeMigrations(ctx, t.dialect, b.target, migrations, columnMigrationSteps...)
	if err != nil {
		return fmt.Errorf("rendering the conversion of %s to CLOB: %w", b.target.Identifier, err)
	}
	for _, stmt := range stmts {
		if _, err := t.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("converting columns of %s to CLOB: executing %q: %w", b.target.Identifier, stmt, err)
		}
	}
	return fmt.Errorf("columns %s of table %s were converted from VARCHAR2 to CLOB to hold longer values, and the transaction will be retried",
		strings.Join(names, ", "), b.target.Identifier)
}

// maxStringBytesQuery returns the most bytes a VARCHAR2 holds, which is also
// the longest string or []byte the driver binds inline rather than as a LONG:
// 4000, or 32767 when the server's MAX_STRING_SIZE is EXTENDED. RPAD cuts its
// result to that length.
const maxStringBytesQuery = "SELECT LENGTHB(RPAD('x', 32767, 'x')) FROM dual"

// writeRows runs query once for each row of bind values, and each run must
// affect exactly one row of the table.
func writeRows(ctx context.Context, txn *stdsql.Tx, query string, rows [][]any, maxInlineBytes int) (int64, error) {
	// Rows share an array-bound statement when the same columns of each are
	// LONG binds. With a limit of 4000 bytes, the rows
	//
	//	{"1", "a", <2 KB doc>}   key "..."
	//	{"2", "b", <9 KB doc>}   key "..L"
	//	{"3", "c", <3 KB doc>}   key "..."
	//
	// run as two statements: one for rows 1 and 3, and one for row 2.
	var statements = make(map[string][][]any)
	for _, row := range rows {
		var key = longColumns(row, maxInlineBytes)
		statements[key] = append(statements[key], row)
	}
	for _, statementRows := range statements {
		res, err := txn.ExecContext(ctx, query, arrayArgs(statementRows)...)
		if err != nil {
			return 0, err
		} else if n, err := res.RowsAffected(); err != nil {
			return 0, fmt.Errorf("rows affected: %w", err)
		} else if n != int64(len(statementRows)) {
			return 0, fmt.Errorf("affected %d rows of %d", n, len(statementRows))
		}
	}
	return int64(len(rows)), nil
}

// longColumns marks the values of row that are LONG binds, as in "..L.".
func longColumns(row []any, maxInlineBytes int) string {
	var key = []byte(strings.Repeat(".", len(row)))
	for i, v := range row {
		if valueSize(v) > maxInlineBytes {
			key[i] = 'L'
		}
	}
	return string(key)
}

// arrayArgs binds rows of values as arrays of columns. Unlike connectors that
// bind one row of values per statement, an Oracle array bind gives each
// placeholder an array holding its value in every row, and Oracle runs the
// statement once per array index in a single round trip. For example, the rows
//
//	{"1", "a", "{...}"}
//	{"2", "b", "{...}"}
//	{"3", "c", "{...}"}
//
// of INSERT INTO t (id, name, doc) VALUES (TO_NUMBER(:1), :2, :3) bind as
//
//	:1 = ["1", "2", "3"]
//	:2 = ["a", "b", "c"]
//	:3 = ["{...}", "{...}", "{...}"]
func arrayArgs(rows [][]any) []any {
	var columns = make([][]any, len(rows[0]))
	for _, row := range rows {
		for i, v := range row {
			columns[i] = append(columns[i], v)
		}
	}
	var args = make([]any, len(columns))
	for i := range columns {
		args[i] = columns[i]
	}
	return args
}

func (t *transactor) Destroy() {
	t.load.conn.Close()
	t.store.conn.Close()
	t.db.Close()
}
