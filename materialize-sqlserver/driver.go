package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"text/template"

	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	mssqldb "github.com/microsoft/go-mssqldb"
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

// config represents the endpoint configuration for sql server.
type config struct {
	Address  string `json:"address" jsonschema:"title=Address,description=Host and port of the database (in the form of host[:port]). Port 1433 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string `json:"database" jsonschema:"title=Database,description=Name of the logical database to materialize to." jsonschema_extras:"order=3"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
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

	return nil
}

const defaultPort = "1433"

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {
	var address = c.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:1433
	// to address through the bastion server, so we use the tunnel's address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != "" {
		address = "localhost:" + defaultPort
	}

	// If the user did not specify a port (or no network tunnel is being used), default to port
	// 1433
	if !strings.Contains(address, ":") {
		address = address + ":" + defaultPort
	}

	var params = make(url.Values)
	params.Add("app name", "Flow Materialization Connector")
	params.Add("encrypt", "true")
	params.Add("TrustServerCertificate", "true")
	params.Add("database", c.Database)

	var uri = url.URL{
		Scheme:   "sqlserver",
		Host:     address,
		User:     url.UserPassword(c.User, c.Password),
		RawQuery: params.Encode(),
	}

	return uri.String()
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
	return []string{c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newSqlServerDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-sqlserver",
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
					LocalPort:   defaultPort,
				}
				var tunnel = sshConfig.CreateTunnel()

				// FIXME/question: do we need to shut down the tunnel manually if it is a child process?
				// at the moment tunnel.Stop is not being called anywhere, but if the connector shuts down, the child process also shuts down.
				if err := tunnel.Start(); err != nil {
					return nil, fmt.Errorf("error starting network tunnel: %w", err)
				}
			}

			db, err := stdsql.Open("sqlserver", cfg.ToURI())
			if err != nil {
				return nil, fmt.Errorf("opening db: %w", err)
			}
			defer db.Close()

			collation, err := getCollation(ctx, db)
			if err != nil {
				return nil, fmt.Errorf("check collations: %w", err)
			}

			// We don't (yet) allow setting a schema for the endpoint or resources, so we need to get the
			// default schema for the configured user.
			var schema string
			if err := db.QueryRowContext(ctx, "select schema_name()").Scan(&schema); err != nil {
				return nil, fmt.Errorf("querying schema for current user: %w", err)
			}

			var dialect = sqlServerDialect(collation, schema)
			var templates = renderTemplates(dialect)

			return &sql.Endpoint{
				Config:               cfg,
				Dialect:              dialect,
				MetaSpecs:            &metaSpecs,
				MetaCheckpoints:      &metaCheckpoints,
				NewClient:            newClient,
				CreateTableTemplate:  templates["createTargetTable"],
				ReplaceTableTemplate: templates["replaceTargetTable"],
				NewResource:          newTableConfig,
				NewTransactor:        prepareNewTransactor(templates),
				Tenant:               tenant,
				ConcurrentApply:      false,
			}, nil
		},
	}
}

type transactor struct {
	templates map[string]*template.Template
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

func prepareNewTransactor(
	templates map[string]*template.Template,
) func(context.Context, *sql.Endpoint, sql.Fence, []sql.Table, pm.Request_Open) (pm.Transactor, error) {
	return func(
		ctx context.Context,
		ep *sql.Endpoint,
		fence sql.Fence,
		bindings []sql.Table,
		open pm.Request_Open,
	) (_ pm.Transactor, err error) {
		var d = &transactor{templates: templates}
		d.store.fence = fence

		var cfg = ep.Config.(*config)
		// Establish connections.
		if db, err := stdsql.Open("sqlserver", cfg.ToURI()); err != nil {
			return nil, fmt.Errorf("load sql.Open: %w", err)
		} else if d.load.conn, err = db.Conn(ctx); err != nil {
			return nil, fmt.Errorf("load db.Conn: %w", err)
		}
		if db, err := stdsql.Open("sqlserver", cfg.ToURI()); err != nil {
			return nil, fmt.Errorf("store sql.Open: %w", err)
		} else if d.store.conn, err = db.Conn(ctx); err != nil {
			return nil, fmt.Errorf("store db.Conn: %w", err)
		}

		for _, binding := range bindings {
			if err = d.addBinding(ctx, binding); err != nil {
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
}

type binding struct {
	target sql.Table

	// a binding needs to be merged if there are updates to existing documents
	// otherwise we just do a direct copy by moving all data from temporary table
	// into the target table. Note that in case of delta updates, "needsMerge"
	// will always be false
	needsMerge bool

	createLoadTableSQL string
	loadQuerySQL       string
	loadInsertSQL      string
	tempLoadTableName  string
	tempLoadTruncate   string

	createStoreTableSQL string
	tempStoreTableName  string
	tempStoreTruncate   string
	mergeInto           string
	directCopy          string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var b = &binding{target: target}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, t.templates["createLoadTable"]},
		{&b.createStoreTableSQL, t.templates["createStoreTable"]},
		{&b.loadInsertSQL, t.templates["loadInsert"]},
		{&b.loadQuerySQL, t.templates["loadQuery"]},
		{&b.tempLoadTruncate, t.templates["tempLoadTruncate"]},
		{&b.tempStoreTruncate, t.templates["tempStoreTruncate"]},
		{&b.tempStoreTableName, t.templates["tempStoreTableName"]},
		{&b.tempLoadTableName, t.templates["tempLoadTableName"]},
		{&b.mergeInto, t.templates["mergeInto"]},
		{&b.directCopy, t.templates["directCopy"]},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	t.bindings = append(t.bindings, b)

	// Create a binding-scoped temporary table for staged keys to load.
	if _, err := t.load.conn.ExecContext(ctx, b.createLoadTableSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.createLoadTableSQL, err)
	}

	// Create a binding-scoped temporary table for store documents to be merged
	// into target table
	if _, err := t.store.conn.ExecContext(ctx, b.createStoreTableSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.createStoreTableSQL, err)
	}

	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	var txn, err = d.load.conn.BeginTx(ctx, &stdsql.TxOptions{})
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer txn.Rollback()

	// The LoadIterator iterates over documents in a natural order, the keys are
	// not ordered by their binding, but naturally keys from the same binding are
	// usually grouped together. We take advantage of that by inserting these
	// consecutive keys of the same binding as a bulk insert to minimise roundtrips
	var lastBinding = -1
	var batch *stdsql.Stmt
	for it.Next() {
		var b = d.bindings[it.Binding]

		if it.Binding != lastBinding {
			// Submit previous `batch`
			if batch != nil {
				if _, err := batch.ExecContext(ctx); err != nil {
					return fmt.Errorf("load: batch insert on %q: %w", b.target.Identifier, err)
				}
			}

			lastBinding = it.Binding

			var colNames = []string{}
			for _, col := range b.target.Keys {
				// Column names passed here must not be quoted, so we use Field instead
				// of Identifier
				colNames = append(colNames, col.Field)
			}

			var err error
			batch, err = txn.PrepareContext(ctx, mssqldb.CopyIn(b.tempLoadTableName, mssqldb.BulkOptions{}, colNames...))
			if err != nil {
				return fmt.Errorf("load: preparing bulk insert statement on %q: %w", b.tempLoadTableName, err)
			}
		}

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if _, err := batch.ExecContext(ctx, converted...); err != nil {
			return fmt.Errorf("load: writing data to batch on %q: %w", b.tempLoadTableName, err)
		}
	}

	if batch != nil {
		if _, err := batch.ExecContext(ctx); err != nil {
			return fmt.Errorf("load batch insert on %q: %w", d.bindings[lastBinding].target.Identifier, err)
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
		var document string

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(binding, json.RawMessage([]byte(document))); err != nil {
			return err
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	for _, b := range d.bindings {
		if _, err = txn.ExecContext(ctx, b.tempLoadTruncate); err != nil {
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

	// The mssql driver uses prepared statements to drive bulk inserts. A bulk
	// insert is initiated by preparing a `mssqldb.CopyIn` statement. Afterwards,
	// executions of this prepared statement that have arguments, are considered rows
	// to be added as part of the bulk insert, whereas executions without
	// arguments mark the end of a bulk insert operation
	var batches = make(map[int]*stdsql.Stmt)

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
			var batch = batches[lastBinding]
			var b = d.bindings[lastBinding]

			if _, err := batch.ExecContext(ctx); err != nil {
				return nil, fmt.Errorf("store batch insert on %q: %w", b.target.Identifier, err)
			}

			lastBinding = it.Binding
		}

		var b = d.bindings[it.Binding]

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		if _, ok := batches[it.Binding]; !ok {
			var colNames = []string{}
			for _, col := range b.target.Columns() {
				// Column names passed here must not be quoted, so we use Field instead
				// of Identifier
				colNames = append(colNames, col.Field)
			}

			var err error
			batches[it.Binding], err = txn.PrepareContext(ctx, mssqldb.CopyIn(b.tempStoreTableName, mssqldb.BulkOptions{}, colNames...))
			if err != nil {
				return nil, fmt.Errorf("load: preparing bulk insert statement on %q: %w", b.tempStoreTableName, err)
			}
		}

		if _, err := batches[it.Binding].ExecContext(ctx, converted...); err != nil {
			return nil, fmt.Errorf("store writing data to batch on %q: %w", b.tempStoreTableName, err)
		}

		if it.Exists {
			b.needsMerge = true
		}
	}

	if _, err := batches[lastBinding].ExecContext(ctx); err != nil {
		return nil, fmt.Errorf("store batch insert on %q: %w", d.bindings[lastBinding].tempStoreTableName, err)
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		return nil, pf.RunAsyncOperation(func() error {
			defer txn.Rollback()

			for _, b := range d.bindings {
				if b.needsMerge {
					log.WithField("table", b.target.Identifier).Info("store: starting merging data into table")
					if _, err := txn.ExecContext(ctx, b.mergeInto); err != nil {
						return fmt.Errorf("store batch merge on %q: %w", b.target.Identifier, err)
					}
					log.WithField("table", b.target.Identifier).Info("store: finishing merging data into table")
				} else {
					log.WithField("table", b.target.Identifier).Info("store: starting direct copying data into table")
					if _, err := txn.ExecContext(ctx, b.directCopy); err != nil {
						return fmt.Errorf("store batch direct insert on %q: %w", b.target.Identifier, err)
					}
					log.WithField("table", b.target.Identifier).Info("store: finishing direct copying data into table")
				}

				if _, err = txn.ExecContext(ctx, b.tempStoreTruncate); err != nil {
					return fmt.Errorf("truncating store table: %w", err)
				}

				// reset the value for next transaction
				b.needsMerge = false
			}

			var err error
			if d.store.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
				return fmt.Errorf("marshalling checkpoint: %w", err)
			}

			var fenceUpdate strings.Builder
			if err := d.templates["updateFence"].Execute(&fenceUpdate, d.store.fence); err != nil {
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
	boilerplate.RunMain(newSqlServerDriver())
}
