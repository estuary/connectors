package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"slices"
	"strings"
	"text/template"
	"time"

	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/util"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	mysql "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
)

const (
	// we try to keep the size of the batch up to 2^25 bytes (32MiB), however this is an
	// approximate. we need to allow for documents up to ~50MiB, and the
	// implementation adds rows to a batch until the threshold is reached, and the batch is
	// drained after that. so the actual memory usage of a batch can potentially
	// grow to 32MiB + 50MiB = 82 MiB in worst case scenario
	batchSizeThreshold = 33554432
)

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

// config represents the endpoint configuration for mysql.
type config struct {
	Address  string         `json:"address" jsonschema:"title=Address,description=Host and port of the database (in the form of host[:port]). Port 3306 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string         `json:"database" jsonschema:"title=Database,description=Name of the logical database to materialize to." jsonschema_extras:"order=3"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	SSLMode string `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Overrides SSL connection behavior by setting the 'sslmode' parameter.,enum=disabled,enum=preferred,enum=required,enum=verify_ca,enum=verify_identity"`

	SSLServerCA   string `json:"ssl_server_ca,omitempty" jsonschema:"title=SSL Server CA,description=Optional server certificate authority to use when connecting with custom SSL mode." jsonschema_extras:"secret=true,multiline=true"`
	SSLClientCert string `json:"ssl_client_cert,omitempty" jsonschema:"title=SSL Client Certificate,description=Optional client certificate to use when connecting with custom SSL mode." jsonschema_extras:"secret=true,multiline=true"`
	SSLClientKey  string `json:"ssl_client_key,omitempty" jsonschema:"title=SSL Client Key,description=Optional client key to use when connecting with custom SSL mode." jsonschema_extras:"secret=true,multiline=true"`
}

// Validate the configuration.
func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if err := util.CheckEndpointSpaces("address", c.Address); err != nil {
		return err
	}

	if c.Advanced.SSLMode != "" {
		if !slices.Contains([]string{"disabled", "preferred", "required", "verify_ca", "verify_identity"}, c.Advanced.SSLMode) {
			return fmt.Errorf("invalid 'sslmode' configuration: unknown setting %q", c.Advanced.SSLMode)
		}
	}

	if (c.Advanced.SSLMode == "verify_ca" || c.Advanced.SSLMode == "verify_identity") && c.Advanced.SSLServerCA == "" {
		return fmt.Errorf("ssl_server_ca is required when using `verify_ca` and `verify_identity` modes")
	}

	return nil
}

const customSSLConfigName = "custom"

func registerCustomSSL(c *config) error {
	// Use the provided Server CA to create a root cert pool
	var rootCertPool = x509.NewCertPool()
	var rawServerCert = []byte(c.Advanced.SSLServerCA)
	if ok := rootCertPool.AppendCertsFromPEM(rawServerCert); !ok {
		return fmt.Errorf("failed to append PEM, this usually means the PEM is not correctly formatted")
	}

	// By default, Go's tls implementation verifies both the validity of the
	// server certificate against the CA, and the hostname

	// In case of `verify_ca`, we want to verify the server certificate
	// against the given Server CA, but we do not want to verify the identity of
	// the server (the hostname). This is useful when connecting to an IP address
	// So we mark the request as `insecure`, and provide our own implementation
	// for verifying the server cert against the provided server CA
	var insecure = c.Advanced.SSLMode == "verify_ca"
	var customVerify func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error

	if c.Advanced.SSLMode == "verify_ca" {
		customVerify = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			opts := x509.VerifyOptions{
				Roots: rootCertPool,
			}
			for _, rawCert := range rawCerts {
				cert, _ := x509.ParseCertificate(rawCert)
				if _, err := cert.Verify(opts); err != nil {
					return err
				}
			}
			return nil
		}
	}

	var customClientCerts []tls.Certificate
	if c.Advanced.SSLClientCert != "" {
		var rawClientCert = []byte(c.Advanced.SSLClientCert)
		var rawClientKey = []byte(c.Advanced.SSLClientKey)
		certs, err := tls.X509KeyPair(rawClientCert, rawClientKey)
		if err != nil {
			return fmt.Errorf("creating certificate from provided input: %w", err)
		}

		customClientCerts = []tls.Certificate{certs}
	}

	mysql.RegisterTLSConfig(customSSLConfigName, &tls.Config{
		RootCAs:               rootCertPool,
		Certificates:          customClientCerts,
		InsecureSkipVerify:    insecure,
		VerifyPeerCertificate: customVerify,
	})

	return nil
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {
	var address = c.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:3306
	// to address through the bastion server, so we use the tunnel's address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != "" {
		address = "localhost:3306"
	}

	// If the user did not specify a port (or no network tunnel is being used), default to port
	// 3306. mysql ends up doing this anyway, but we do it here to make it more explicit and stable in
	// case that underlying behavior changes in the future.
	if !strings.Contains(address, ":") {
		address = address + ":3306"
	}

	var uri = url.URL{
		Scheme: "mysql",
		Path:   "/" + c.Database,
		Host:   fmt.Sprintf("tcp(%s)", address),
		User:   url.UserPassword(c.User, c.Password),
	}
	var params = make(url.Values)
	if c.Advanced.SSLMode != "" {
		// see https://pkg.go.dev/github.com/go-sql-driver/mysql#section-readme
		var tlsConfigMap = map[string]string{
			"required":        "skip-verify",
			"disabled":        "false",
			"preferred":       "preferred",
			"verify_ca":       "custom",
			"verify_identity": "custom",
		}

		var tlsMode = tlsConfigMap[c.Advanced.SSLMode]

		params.Set("tls", tlsMode)
	}
	params.Set("time_zone", "'+00:00'")
	if len(params) > 0 {
		uri.RawQuery = params.Encode()
	}

	// MySQL driver expects a uri without the scheme
	return strings.TrimPrefix(uri.String(), "mysql://")
}

type tableConfig struct {
	Table         string `json:"table" jsonschema:"title=Table,description=Name of the database table" jsonschema_extras:"x-collection-name=true"`
	AdditionalSql string `json:"additional_table_create_sql,omitempty" jsonschema:"title=Additional Table Create SQL,description=Additional SQL statement(s) to be run in the same transaction that creates the table." jsonschema_extras:"multiline=true"`
	Delta         bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false."`
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{}
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.Table}
}

func (c tableConfig) GetAdditionalSql() string {
	return c.AdditionalSql
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newMysqlDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-mysql",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("parsing endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"database": cfg.Database,
				"address":  cfg.Address,
				"user":     cfg.User,
			}).Info("opening database")

			var metaBase sql.TablePath
			var metaSpecs, metaCheckpoints = sql.MetaTables(metaBase)

			// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
			if cfg.NetworkTunnel != nil && cfg.NetworkTunnel.SshForwarding != nil && cfg.NetworkTunnel.SshForwarding.SshEndpoint != "" {
				host, port, err := net.SplitHostPort(cfg.Address)
				if err != nil {
					return nil, fmt.Errorf("splitting address to host and port: %w", err)
				}

				var sshConfig = &networkTunnel.SshConfig{
					SshEndpoint: cfg.NetworkTunnel.SshForwarding.SshEndpoint,
					PrivateKey:  []byte(cfg.NetworkTunnel.SshForwarding.PrivateKey),
					ForwardHost: host,
					ForwardPort: port,
					LocalPort:   "3306",
				}
				var tunnel = sshConfig.CreateTunnel()

				// FIXME/question: do we need to shut down the tunnel manually if it is a child process?
				// at the moment tunnel.Stop is not being called anywhere, but if the connector shuts down, the child process also shuts down.
				if err := tunnel.Start(); err != nil {
					return nil, fmt.Errorf("error starting network tunnel: %w", err)
				}
			}

			if cfg.Advanced.SSLMode == "verify_ca" || cfg.Advanced.SSLMode == "verify_identity" {
				if err := registerCustomSSL(cfg); err != nil {
					return nil, fmt.Errorf("error registering custom ssl: %w", err)
				}
			}

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             mysqlDialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				Client:              client{uri: cfg.ToURI()},
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				CheckPrerequisites:  prereqs,
				Tenant:              tenant,
			}, nil
		},
	}
}

func prereqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	errs := &sql.PrereqErr{}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then.
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if db, err := stdsql.Open("mysql", cfg.ToURI()); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var mysqlErr *mysql.MySQLError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &mysqlErr) {
			// See MySQL error reference: https://dev.mysql.com/doc/mysql-errors/5.7/en/error-reference-introduction.html
			switch mysqlErr.Number {
			case 1045:
				err = fmt.Errorf("incorrect username or password (%d): %s", mysqlErr.Number, mysqlErr.Message)
			case 1049:
				err = fmt.Errorf("database %q cannot be accessed, it might not exist or you do not have permission to access it (%d): %s", cfg.Database, mysqlErr.Number, mysqlErr.Message)
			case 1044:
				err = fmt.Errorf("database %q cannot be accessed, it might not exist or you do not have permission to access it (%d): %s", cfg.Database, mysqlErr.Number, mysqlErr.Message)
			}
		} else if errors.As(err, &netConnErr) {
			if netConnErr.IsNotFound {
				err = fmt.Errorf("host at address %q cannot be found", cfg.Address)
			}
		} else if errors.As(err, &netOpErr) {
			if netOpErr.Timeout() {
				err = fmt.Errorf("connection to host at address %q timed out (incorrect host or port?)", cfg.Address)
			}
		}

		errs.Err(err)
	} else if conn, err := db.Conn(ctx); err != nil {
		errs.Err(fmt.Errorf("could not create a connection to database %q at %q: %w", cfg.Database, cfg.Address, err))
	} else {
		var row = conn.QueryRowContext(ctx, "SELECT @@GLOBAL.local_infile;")
		var localInFileEnabled bool

		if err := row.Scan(&localInFileEnabled); err != nil {
			errs.Err(fmt.Errorf("could not read `local_infile` global variable: %w", err))
		} else if !localInFileEnabled {
			errs.Err(fmt.Errorf("`local_infile` global variable must be enabled on your mysql server. You can enable this using `SET GLOBAL local_infile = true`"))
		} else {
			db.Close()
		}
	}

	return errs
}

// client implements the sql.Client interface.
type client struct {
	uri string
}

func (c client) AddColumnToTable(ctx context.Context, dryRun bool, tableIdentifier string, columnIdentifier string, columnDDL string) (string, error) {
	var query string

	err := c.withDB(func(db *stdsql.DB) error {
		query = fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s %s;",
			tableIdentifier,
			columnIdentifier,
			columnDDL,
		)

		if !dryRun {
			if err := c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, []string{query}) }); err != nil {
				var mysqlErr *mysql.MySQLError

				if errors.As(err, &mysqlErr) {
					// See MySQL error reference: https://dev.mysql.com/doc/mysql-errors/5.7/en/error-reference-introduction.html
					switch mysqlErr.Number {
					case 1060:
						// 1060: Duplicate column name, means the column already exists, we
						// just skip
						return nil
					}
				}

				return err
			}
		}

		return nil
	})

	if err != nil {
		return "", err
	}

	return query, nil
}

func (c client) DropNotNullForColumn(ctx context.Context, dryRun bool, table sql.Table, column sql.Column) (string, error) {
	var projection = column.Projection
	projection.Inference.Exists = pf.Inference_MAY
	var mapped, err = mysqlDialect.TypeMapper.MapType(&projection)
	if err != nil {
		return "", fmt.Errorf("drop not null: mapping type of %s failed: %w", column.Identifier, err)
	}

	query := fmt.Sprintf(
		"ALTER TABLE %s MODIFY %s %s;",
		table.Identifier,
		column.Identifier,
		mapped.DDL,
	)

	if !dryRun {
		if err := c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, []string{query}) }); err != nil {
			return "", err
		}
	}

	return query, nil
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	err = c.withDB(func(db *stdsql.DB) error {
		specB64, version, err = sql.StdFetchSpecAndVersion(ctx, db, specs, materialization)
		return err
	})
	return
}

// ExecStatements is used for the DDL statements of ApplyUpsert and ApplyDelete.
// Mysql does not support transactional DDL statements
func (c client) ExecStatements(ctx context.Context, statements []string) error {
	return c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements) })
}

func (c client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	var err = c.withDB(func(db *stdsql.DB) error {
		var err error
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence, base64.StdEncoding.DecodeString)
		return err
	})
	return fence, err
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	var db, err = stdsql.Open("mysql", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

type transactor struct {
	// Variables exclusively used by Load.
	load struct {
		conn     *stdsql.Conn
		unionSQL string
	}
	// Variables exclusively used by Store.
	store struct {
		conn  *stdsql.Conn
		fence sql.Fence
	}
	bindings []*binding
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
) (_ pm.Transactor, err error) {
	var d = &transactor{}
	d.store.fence = fence

	var cfg = ep.Config.(*config)
	// Establish connections.
	if db, err := stdsql.Open("mysql", cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("load sql.Open: %w", err)
	} else if d.load.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("load db.Conn: %w", err)
	}
	if db, err := stdsql.Open("mysql", cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("store sql.Open: %w", err)
	} else if d.store.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("store db.Conn: %w", err)
	}

	db, err := stdsql.Open("mysql", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("newTransactor sql.Open: %w", err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("newTransactor db.Conn: %w", err)
	}
	defer conn.Close()

	tableVarchars, err := getVarcharDetails(ctx, cfg.Database, conn)
	if err != nil {
		return nil, fmt.Errorf("getting existing varchar column lengths: %w", err)
	}

	for _, binding := range bindings {
		if err = d.addBinding(ctx, binding, tableVarchars[binding.Identifier]); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	// Build a query which unions the results of each load subquery.
	var subqueries []string
	for _, b := range d.bindings {
		subqueries = append(subqueries, b.loadQuerySQL)
	}
	d.load.unionSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

	return d, nil
}

// varcharColumnMeta contains metadata about mysql varchar columns. Currently this is just the
// maximum length of the field as reported from the database, populated upon connector startup.
type varcharColumnMeta struct {
	identifier string
	maxLength  int
}

type binding struct {
	target sql.Table

	varcharColumnMetas []varcharColumnMeta
	tempVarcharMetas   []varcharColumnMeta

	tempTableName string
	tempTruncate  string

	createLoadTableSQL   string
	createUpdateTableSQL string
	loadLoadSQL          string
	storeLoadSQL         string
	loadQuerySQL         string

	updateLoadSQL     string
	updateReplaceSQL  string
	updateTruncateSQL string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, varchars map[string]int) error {
	var b = &binding{target: target}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, tplCreateLoadTable},
		{&b.createUpdateTableSQL, tplCreateUpdateTable},
		{&b.loadLoadSQL, tplLoadLoad},
		{&b.loadQuerySQL, tplLoadQuery},
		{&b.storeLoadSQL, tplStoreLoad},
		{&b.updateLoadSQL, tplUpdateLoad},
		{&b.updateReplaceSQL, tplUpdateReplace},
		{&b.updateTruncateSQL, tplUpdateTruncate},
		{&b.tempTableName, tplTempTableName},
		{&b.tempTruncate, tplTempTruncate},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	// Retain column metadata information for this binding as a snapshot of the table configuration
	// when the connector started, indexed in the same order as values will be received from the
	// runtime for Store requests. Only VARCHAR columns will have non-zero-valued varcharColumnMeta.
	allColumns := target.Columns()
	columnMetas := make([]varcharColumnMeta, len(allColumns))
	if varchars != nil { // There may not be any varchar columns for this binding
		for idx, col := range allColumns {
			// If this column is not found in varchars, it must not have been a VARCHAR column.
			if maxLength, ok := varchars[col.Identifier]; ok {
				columnMetas[idx] = varcharColumnMeta{
					identifier: col.Identifier,
					maxLength:  maxLength,
				}

				log.WithFields(log.Fields{
					"table":            b.target.Identifier,
					"column":           col.Identifier,
					"varcharMaxLength": maxLength,
					"collection":       b.target.Source.String(),
					"field":            col.Field,
				}).Debug("matched string collection field to table VARCHAR column")
			}
		}
	}
	b.varcharColumnMetas = columnMetas

	t.bindings = append(t.bindings, b)

	// Create a binding-scoped temporary table for staged keys to load.
	if _, err := t.load.conn.ExecContext(ctx, b.createLoadTableSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.createLoadTableSQL, err)
	}

	if _, err := t.store.conn.ExecContext(ctx, b.createUpdateTableSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.createUpdateTableSQL, err)
	}

	tempColumnMetas := make([]varcharColumnMeta, len(allColumns))
	for idx, key := range target.Keys {
		columnType, err := mysqlDialect.TypeMapper.MapType(&key.Projection)
		if err != nil {
			return fmt.Errorf("temp column metas: %w", err)
		}

		if strings.Contains(columnType.DDL, "VARCHAR(256)") {
			tempColumnMetas[idx] = varcharColumnMeta{
				identifier: key.Identifier,
				maxLength:  256,
			}
		}
	}

	b.tempVarcharMetas = tempColumnMetas

	return nil
}

func rowToCSVRecord(row []any) ([]string, error) {
	var record = make([]string, len(row))
	for i, v := range row {
		switch value := v.(type) {
		case []byte:
			record[i] = string(value)
		case string:
			record[i] = value
		case nil:
			// See https://dev.mysql.com/doc/refman/8.0/en/problems-with-null.html
			record[i] = "NULL"
		case bool:
			if !value {
				record[i] = "0"
			} else {
				record[i] = "1"
			}
		default:
			b, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("encoding value as json: %w", err)
			}
			record[i] = string(b)
		}
	}

	return record, nil
}

type batchMeta struct {
	// use w to write csv records for a batch
	w *csv.Writer
	// use buff to check how much data has been written so far
	buff *bytes.Buffer
}

func (batch batchMeta) Write(converted []any) error {
	record, err := rowToCSVRecord(converted)
	if err != nil {
		return fmt.Errorf("error encoding row to CSV: %w", err)
	}

	batch.w.Write(record)
	batch.w.Flush()
	if err := batch.w.Error(); err != nil {
		return fmt.Errorf("writing csv to buffer: %w", err)
	}

	return nil
}

func setupBatch(ctx context.Context, readerSuffix string) batchMeta {
	var buff bytes.Buffer
	var writer = csv.NewWriter(&buff)

	var readerName = fmt.Sprintf("batch_data_%s", readerSuffix)

	mysql.RegisterReaderHandler(readerName,
		func() io.Reader {
			return &buff
		},
	)

	return batchMeta{
		w:    writer,
		buff: &buff,
	}
}

func drainBatch(ctx context.Context, txn *stdsql.Tx, query string, batch batchMeta) error {
	batch.w.Flush()

	if err := batch.w.Error(); err != nil {
		return fmt.Errorf("flushing csv writes: %w", err)
	}

	if _, err := txn.ExecContext(ctx, query); err != nil {
		return err
	}

	return nil
}

func drainUpdateBatch(ctx context.Context, txn *stdsql.Tx, b *binding, batch batchMeta) error {
	if err := drainBatch(ctx, txn, b.updateLoadSQL, batch); err != nil {
		return fmt.Errorf("store batch update on %q: %w", b.target.Identifier, err)
	}

	if _, err := txn.ExecContext(ctx, b.updateReplaceSQL); err != nil {
		return fmt.Errorf("store batch update replace on %q: %w", b.target.Identifier, err)
	}

	if _, err := txn.ExecContext(ctx, b.updateTruncateSQL); err != nil {
		return fmt.Errorf("store batch update truncate on %q: %w", b.target.Identifier, err)
	}

	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	// Use a read-only "load" transaction, which will automatically
	// truncate the temporary key staging tables on commit.
	var txn, err = d.load.conn.BeginTx(ctx, &stdsql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer txn.Rollback()

	var batches = make(map[int]batchMeta)
	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else {
			// See if we need to increase any VARCHAR column lengths
			for idx, c := range converted {
				varcharMeta := b.tempVarcharMetas[idx]
				if varcharMeta.identifier != "" {
					l := len(c.(string))

					if l > varcharMeta.maxLength {
						log.WithFields(log.Fields{
							"table":               b.target.Identifier,
							"column":              varcharMeta.identifier,
							"currentColumnLength": varcharMeta.maxLength,
							"stringValueLength":   l,
							"query":               fmt.Sprintf(varcharTableAlter, b.tempTableName, varcharMeta.identifier, l),
						}).Info("column will be altered to VARCHAR(stringLength) to accommodate large string value")
						b.tempVarcharMetas[idx].maxLength = l

						if _, err := d.load.conn.ExecContext(ctx, fmt.Sprintf(varcharTableAlter, b.tempTableName, varcharMeta.identifier, l)); err != nil {
							return fmt.Errorf("altering size for column %s of table %s: %w", varcharMeta.identifier, b.tempTableName, err)
						}
					}
				}
			}

			if _, ok := batches[it.Binding]; !ok {
				batches[it.Binding] = setupBatch(ctx, fmt.Sprintf("load_%d", it.Binding))
			}

			if err := batches[it.Binding].Write(converted); err != nil {
				return fmt.Errorf("load writing data to batch on %q: %w", b.target.Identifier, err)
			}

			if batches[it.Binding].buff.Len() > batchSizeThreshold {
				if err := drainBatch(ctx, txn, b.loadLoadSQL, batches[it.Binding]); err != nil {
					return fmt.Errorf("load batch insert on %q: %w", b.target.Identifier, err)
				}
			}
		}
	}

	for bindingIndex, batch := range batches {
		if batch.buff.Len() < 1 {
			continue
		}
		var b = d.bindings[bindingIndex]

		if err := drainBatch(ctx, txn, b.loadLoadSQL, batches[it.Binding]); err != nil {
			return fmt.Errorf("load batch insert on %q: %w", b.target.Identifier, err)
		}
	}

	if it.Err() != nil {
		return it.Err()
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := txn.QueryContext(ctx, d.load.unionSQL)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document json.RawMessage

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(binding, json.RawMessage(document)); err != nil {
			return err
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	for _, b := range d.bindings {
		if _, err = txn.ExecContext(ctx, b.tempTruncate); err != nil {
			return fmt.Errorf("truncating load table: %w", err)
		}
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("commiting Load transaction: %w", err)
	}

	return nil
}

func (d *transactor) Store(it *pm.StoreIterator) (_ pm.StartCommitFunc, err error) {
	ctx := it.Context()
	txn, err := d.store.conn.BeginTx(ctx, &stdsql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer func() {
		if err != nil {
			txn.Rollback()
		}
	}()

	var inserts = make(map[int]batchMeta)
	var updates = make(map[int]batchMeta)

	// The StoreIterator iterates over documents ordered by their binding, so we
	// can keep track of the last binding that we have seen, and if we have moved
	// on from a binding, we can drain its leftover batches to avoid unnecessary
	// memory use
	var lastBinding = -1

	for it.Next() {
		if lastBinding == -1 {
			lastBinding = it.Binding
		}

		// The last binding is fully processed for this RPC now, we can drain its
		// remaining batches
		if lastBinding != it.Binding {
			var insert = inserts[lastBinding]
			var b = d.bindings[lastBinding]
			if insert.buff.Len() > 0 {
				if err := drainBatch(ctx, txn, b.storeLoadSQL, insert); err != nil {
					return nil, fmt.Errorf("store batch insert on %q: %w", b.target.Identifier, err)
				}
			}

			var update = updates[lastBinding]
			if update.buff.Len() > 0 {
				if err := drainUpdateBatch(ctx, txn, b, updates[lastBinding]); err != nil {
					return nil, fmt.Errorf("store batch update on %q: %w", b.target.Identifier, err)
				}
			}

			lastBinding = it.Binding
		}

		var b = d.bindings[it.Binding]

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		// See if we need to increase any VARCHAR column lengths
		for idx, c := range converted {
			varcharMeta := b.varcharColumnMetas[idx]
			if varcharMeta.identifier != "" {
				l := len(c.(string))

				if l > varcharMeta.maxLength {
					log.WithFields(log.Fields{
						"table":               b.target.Identifier,
						"column":              varcharMeta.identifier,
						"currentColumnLength": varcharMeta.maxLength,
						"stringValueLength":   l,
					}).Info("column will be altered to VARCHAR(stringLength) to accommodate large string value")
					b.varcharColumnMetas[idx].maxLength = l

					if _, err := d.store.conn.ExecContext(ctx, fmt.Sprintf(varcharTableAlter, b.target.Identifier, varcharMeta.identifier, l)); err != nil {
						return nil, fmt.Errorf("altering size for column %s of table %s: %w", varcharMeta.identifier, b.target.Identifier, err)
					}
				}
			}
		}

		if _, ok := inserts[it.Binding]; !ok {
			inserts[it.Binding] = setupBatch(ctx, fmt.Sprintf("store_%d", it.Binding))
			updates[it.Binding] = setupBatch(ctx, fmt.Sprintf("update_%d", it.Binding))
		}

		if it.Exists {
			if err := updates[it.Binding].Write(converted); err != nil {
				return nil, fmt.Errorf("store writing data to batch on %q: %w", b.target.Identifier, err)
			}

			if updates[it.Binding].buff.Len() > batchSizeThreshold {
				if err := drainUpdateBatch(ctx, txn, b, updates[it.Binding]); err != nil {
					return nil, fmt.Errorf("store batch update on %q: %w", b.target.Identifier, err)
				}
			}
		} else {
			if err := inserts[it.Binding].Write(converted); err != nil {
				return nil, fmt.Errorf("store writing data to batch on %q: %w", b.target.Identifier, err)
			}

			if inserts[it.Binding].buff.Len() > batchSizeThreshold {
				if err := drainBatch(ctx, txn, b.storeLoadSQL, inserts[it.Binding]); err != nil {
					return nil, fmt.Errorf("store batch insert on %q: %w", b.target.Identifier, err)
				}
			}
		}
	}

	for bindingIndex, insert := range inserts {
		if insert.buff.Len() < 1 {
			continue
		}

		var b = d.bindings[bindingIndex]
		if err := drainBatch(ctx, txn, b.storeLoadSQL, insert); err != nil {
			return nil, fmt.Errorf("store batch insert on %q: %w", b.target.Identifier, err)
		}
	}

	for bindingIndex, update := range updates {
		if update.buff.Len() < 1 {
			continue
		}

		var b = d.bindings[bindingIndex]
		if err := drainUpdateBatch(ctx, txn, b, update); err != nil {
			return nil, fmt.Errorf("store batch update on %q: %w", b.target.Identifier, err)
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		return nil, pf.RunAsyncOperation(func() error {
			defer txn.Rollback()

			var err error
			if d.store.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
				return fmt.Errorf("marshalling checkpoint: %w", err)
			}

			var fenceUpdate strings.Builder
			if err := tplUpdateFence.Execute(&fenceUpdate, d.store.fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}

			if results, err := txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
				return fmt.Errorf("updating flow checkpoint: %w", err)
			} else if rowsAffected, err := results.RowsAffected(); err != nil {
				return fmt.Errorf("updating flow checkpoint (rows affected): %w", err)
			} else if rowsAffected < 1 {
				return fmt.Errorf("this instance was fenced off by another")
			}

			if err := txn.Commit(); err != nil {
				return fmt.Errorf("committing Store transaction: %w", err)
			}

			return nil
		})
	}, nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close()
	d.store.conn.Close()
}

func main() {
	boilerplate.RunMain(newMysqlDriver())
}
