package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"text/template"
	"time"

	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	mysql "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
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
	Timezone string         `json:"timezone,omitempty" jsonschema:"title=Timezone,description=Timezone to use when materializing datetime columns. Should normally be left blank to use the database's 'time_zone' system variable. Only required if the 'time_zone' system variable cannot be read. Must be a valid IANA time zone name or +HH:MM offset. Takes precedence over the 'time_zone' system variable if both are set." jsonschema_extras:"order=4"`
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

	if c.Timezone != "" {
		if _, err := sql.ParseTimezone(c.Timezone); err != nil {
			return err
		}
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
	// 3306
	if !strings.Contains(address, ":") {
		address = address + ":3306"
	}

	mysqlCfg := mysql.NewConfig()
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = address
	mysqlCfg.User = c.User
	mysqlCfg.Passwd = c.Password
	mysqlCfg.DBName = c.Database

	if c.Advanced.SSLMode != "" {
		// see https://pkg.go.dev/github.com/go-sql-driver/mysql#section-readme
		var tlsConfigMap = map[string]string{
			"required":        "skip-verify",
			"disabled":        "false",
			"preferred":       "preferred",
			"verify_ca":       "custom",
			"verify_identity": "custom",
		}

		mysqlCfg.TLSConfig = tlsConfigMap[c.Advanced.SSLMode]
	}

	return mysqlCfg.FormatDSN()
}

type tableConfig struct {
	Table string `json:"table" jsonschema:"title=Table,description=Name of the database table" jsonschema_extras:"x-collection-name=true"`
	Delta bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false."`
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
	return []string{translateFlowIdentifier(c.Table)}
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

			db, err := stdsql.Open("mysql", cfg.ToURI())
			if err != nil {
				return nil, fmt.Errorf("opening database: %w", err)
			}

			conn, err := db.Conn(ctx)
			if err != nil {
				return nil, fmt.Errorf("could not create a connection to database %q at %q: %w", cfg.Database, cfg.Address, err)
			}

			var tzLocation *time.Location
			if cfg.Timezone != "" {
				// The user-entered timezone value is verified to parse without error in (*Config).Validate,
				// so this parsing is not expected to fail.
				var err error
				tzLocation, err = sql.ParseTimezone(cfg.Timezone)
				if err != nil {
					return nil, fmt.Errorf("invalid config timezone: %w", err)
				}
				log.WithFields(log.Fields{
					"tzName": cfg.Timezone,
					"loc":    tzLocation.String(),
				}).Debug("using datetime location from config")
			} else {
				// Infer the location in which captured DATETIME values will be interpreted from the system
				// variable 'time_zone' if it is set on the database.
				var tzName string
				if tzName, err = queryTimeZone(ctx, conn); err == nil {
					if tzLocation, err = sql.ParseTimezone(tzName); err == nil {
						log.WithFields(log.Fields{
							"tzName": tzName,
							"loc":    tzLocation.String(),
						}).Debug("using datetime location queried from database")
					}
				}

				if tzLocation == nil {
					log.WithField("error", err).Error("could not determine database timezone")
					return nil, fmt.Errorf("unable to determine database timezone and no timezone in materialization configuration. A timezone is required for mysql materializations to avoid ambiguity about date-time and time fields")
				}
			}

			var dialect = mysqlDialect(tzLocation, cfg.Database)
			var templates = renderTemplates(dialect)

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             dialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				NewClient:           newClient,
				CreateTableTemplate: templates.createTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       prepareNewTransactor(dialect, templates),
				Tenant:              tenant,
				ConcurrentApply:     false,
			}, nil
		},
	}
}

var errDatabaseTimezoneUnknown = errors.New("system variable 'time_zone' or timezone from capture configuration must contain a valid IANA time zone name or +HH:MM offset")

func queryTimeZone(ctx context.Context, conn *stdsql.Conn) (string, error) {
	var row = conn.QueryRowContext(ctx, `SELECT @@GLOBAL.time_zone;`)
	var tzName string
	if err := row.Scan(&tzName); err != nil {
		return "", fmt.Errorf("error reading 'time_zone' system variable: %w", err)
	}

	log.WithField("time_zone", tzName).Debug("queried time_zone system variable")
	if tzName == "SYSTEM" {
		return "", errDatabaseTimezoneUnknown
	}

	return tzName, nil
}

type transactor struct {
	dialect   sql.Dialect
	templates templates
	// Variables exclusively used by Load.
	load struct {
		conn     *stdsql.Conn
		unionSQL string
		infile   *infile
	}
	// Variables exclusively used by Store.
	store struct {
		conn         *stdsql.Conn
		fence        sql.Fence
		insertInfile *infile
		updateInfile *infile
	}
	bindings []*binding
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func prepareNewTransactor(
	dialect sql.Dialect,
	templates templates,
) func(context.Context, *sql.Endpoint, sql.Fence, []sql.Table, pm.Request_Open) (m.Transactor, error) {
	return func(
		ctx context.Context,
		ep *sql.Endpoint,
		fence sql.Fence,
		bindings []sql.Table,
		open pm.Request_Open,
	) (_ m.Transactor, err error) {
		var d = &transactor{dialect: dialect, templates: templates}
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

		resourcePaths := make([][]string, 0, len(open.Materialization.Bindings))
		for _, b := range open.Materialization.Bindings {
			resourcePaths = append(resourcePaths, b.ResourcePath)
		}
		is, err := sql.StdFetchInfoSchema(ctx, db, ep.Dialect, "def", cfg.Database, resourcePaths)
		if err != nil {
			return nil, err
		}

		for _, binding := range bindings {
			if err = d.addBinding(ctx, binding, is); err != nil {
				return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
			}
		}

		d.load.infile = newInfile(loadInfileName)
		d.store.insertInfile = newInfile(insertInfileName)
		d.store.updateInfile = newInfile(updateInfileName)

		// Build a query which unions the results of each load subquery.
		var subqueries []string
		for _, b := range d.bindings {
			subqueries = append(subqueries, b.loadQuerySQL)
		}
		d.load.unionSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

		return d, nil
	}
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
	storeInsertSQL       string
	loadQuerySQL         string
	storeUpdateSQL       string
	updateReplaceSQL     string
	updateTruncateSQL    string

	mustMerge bool
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, is *boilerplate.InfoSchema) error {
	var b = &binding{target: target}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, t.templates.createLoadTable},
		{&b.createUpdateTableSQL, t.templates.createUpdateTable},
		{&b.loadLoadSQL, t.templates.loadLoad},
		{&b.loadQuerySQL, t.templates.loadQuery},
		{&b.storeInsertSQL, t.templates.insertLoad},
		{&b.storeUpdateSQL, t.templates.updateLoad},
		{&b.updateReplaceSQL, t.templates.updateReplace},
		{&b.updateTruncateSQL, t.templates.updateTruncate},
		{&b.tempTableName, t.templates.tempTableName},
		{&b.tempTruncate, t.templates.tempTruncate},
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
	for idx, col := range allColumns {
		existing, err := is.GetField(target.Path, col.Field)
		if err != nil {
			return fmt.Errorf("getting existing column metadata: %w", err)
		}

		if existing.Type == "varchar" {
			columnMetas[idx] = varcharColumnMeta{
				identifier: col.Identifier,
				maxLength:  existing.CharacterMaxLength,
			}

			log.WithFields(log.Fields{
				"table":            b.target.Identifier,
				"column":           col.Identifier,
				"varcharMaxLength": existing.CharacterMaxLength,
				"collection":       b.target.Source.String(),
				"field":            col.Field,
			}).Debug("matched string collection field to table VARCHAR column")
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
		columnType, err := t.dialect.TypeMapper.MapType(&key.Projection)
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

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	var txn, err = d.load.conn.BeginTx(ctx, &stdsql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer txn.Rollback()

	var lastBinding = -1
	for it.Next() {
		if lastBinding == -1 {
			lastBinding = it.Binding
		}

		if lastBinding != it.Binding {
			var b = d.bindings[lastBinding]
			// Drain the prior binding as naturally-ordered key groupings are cycled through.
			if err := d.load.infile.drain(ctx, txn, b.loadLoadSQL); err != nil {
				return fmt.Errorf("load infile drain on %q: %w", b.target.Identifier, err)
			}
			lastBinding = it.Binding
		}

		var b = d.bindings[it.Binding]

		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}

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

		if err := d.load.infile.write(ctx, converted, txn, b.loadLoadSQL); err != nil {
			return fmt.Errorf("load writing to infile: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	// Drain the final binding if we processed any loads.
	if lastBinding != -1 {
		var b = d.bindings[lastBinding]
		if err := d.load.infile.drain(ctx, txn, b.loadLoadSQL); err != nil {
			return fmt.Errorf("load infile drain on %q: %w", b.target.Identifier, err)
		}
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

func (d *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
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

	drainBinding := func(b *binding) error {
		if err := d.store.insertInfile.drain(ctx, txn, b.storeInsertSQL); err != nil {
			return fmt.Errorf("store writing to insert infile for %q: %w", b.target.Identifier, err)
		} else if err := d.store.updateInfile.drain(ctx, txn, b.storeUpdateSQL); err != nil {
			return fmt.Errorf("store writing to update infile for %q: %w", b.target.Identifier, err)
		}
		return nil
	}

	// The StoreIterator iterates over documents ordered by their binding, so we
	// can keep track of the last binding that we have seen, and if we have moved
	// on from a binding, we can drain its leftover batches to avoid unnecessary
	// memory use
	var lastBinding = -1

	for it.Next() {
		if lastBinding == -1 {
			lastBinding = it.Binding
		}

		if lastBinding != it.Binding {
			// The last binding is fully processed for this RPC now, we can drain its remaining
			// data.
			if err := drainBinding(d.bindings[lastBinding]); err != nil {
				return nil, err
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

		var inf *infile
		var drainQuery string

		if it.Exists {
			b.mustMerge = true
			inf = d.store.updateInfile
			drainQuery = b.storeUpdateSQL
		} else {
			inf = d.store.insertInfile
			drainQuery = b.storeInsertSQL
		}

		if err := inf.write(ctx, converted, txn, drainQuery); err != nil {
			return nil, fmt.Errorf("store writing to infile for %q: %w", b.target.Identifier, err)
		}
	}

	// Drain the final binding.
	if err := drainBinding(d.bindings[lastBinding]); err != nil {
		return nil, err
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		return nil, m.RunAsyncOperation(func() error {
			defer txn.Rollback()

			for _, b := range d.bindings {
				if !b.mustMerge {
					// Keys that don't already exist are inserted directly into the target table.
					continue
				}

				// Merge data from the temporary staging table into the target table, replacing keys
				// that already exist.
				if _, err := txn.ExecContext(ctx, b.updateReplaceSQL); err != nil {
					return fmt.Errorf("running REPLACE INTO for %q: %w", b.target.Identifier, err)
				} else if _, err := txn.ExecContext(ctx, b.updateTruncateSQL); err != nil {
					return fmt.Errorf("truncating update table for %q: %w", b.target.Identifier, err)
				}

				// Reset for the next round.
				b.mustMerge = false
			}

			var err error
			if d.store.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
				return fmt.Errorf("marshalling checkpoint: %w", err)
			}

			var fenceUpdate strings.Builder
			if err := d.templates.updateFence.Execute(&fenceUpdate, d.store.fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}

			if results, err := txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
				return fmt.Errorf("updating flow checkpoint: %w", err)
			} else if rowsAffected, err := results.RowsAffected(); err != nil {
				return fmt.Errorf("updating flow checkpoint (rows affected): %w", err)
			} else if rowsAffected < 1 {
				return fmt.Errorf("This instance was fenced off by another")
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
