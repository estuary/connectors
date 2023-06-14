package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"text/template"
	"time"

	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
)

const (
	// These are coarse limits on the amount of memory that will be used to buffer insert/update
	// batches for load and store operations. In the future it may be interesting to investigate
	// using the Postgres copy protocol in the future for bulk loading data tables instead of with
	// DML statements.
	storeBatchSizeLimit = 4096                    // Assuming store records average ~2kB then 4k * 2kB = 8MB
	loadBatchSizeLimit  = storeBatchSizeLimit * 5 // Load records are keys-only so allow for more in a batch
)

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

// config represents the endpoint configuration for postgres.
type config struct {
	Address  string `json:"address" jsonschema:"title=Address,description=Host and port of the database (in the form of host[:port]). Port 5432 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string `json:"database,omitempty" jsonschema:"title=Database,description=Name of the logical database to materialize to." jsonschema_extras:"order=3"`
	Schema   string `json:"schema,omitempty" jsonschema:"title=Database Schema,default=public,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables" jsonschema_extras:"order=4"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

// Validate the configuration.
func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {
	var address = c.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432
	// to address through the bastion server, so we use the tunnel's address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != "" {
		address = "localhost:5432"
	}

	// If the user did not specify a port (or no network tunnel is being used), default to port
	// 5432. pgx ends up doing this anyway, but we do it here to make it more explicit and stable in
	// case that underlying behavior changes in the future.
	if !strings.Contains(address, ":") {
		address = address + ":5432"
	}

	var uri = url.URL{
		Scheme: "postgres",
		Host:   address,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}

	return uri.String()
}

type tableConfig struct {
	Table         string `json:"table" jsonschema:"title=Table,description=Name of the database table" jsonschema_extras:"x-collection-name=true"`
	Schema        string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)"`
	AdditionalSql string `json:"additional_table_create_sql,omitempty" jsonschema:"title=Additional Table Create SQL,description=Additional SQL statement(s) to be run in the same transaction that creates the table." jsonschema_extras:"multiline=true"`
	Delta         bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false."`
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{
		// Default to an explicit endpoint configuration schema, if set.
		// This will be over-written by a present `schema` property within `raw`.
		Schema: ep.Config.(*config).Schema,
	}
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	return nil
}

func (c tableConfig) Path() sql.TablePath {
	if c.Schema != "" {
		return []string{c.Schema, c.Table}
	}
	return []string{c.Table}
}

func (c tableConfig) GetAdditionalSql() string {
	return c.AdditionalSql
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newPostgresDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-postgresql",
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
			if cfg.Schema != "" {
				metaBase = append(metaBase, cfg.Schema)
			}
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
					LocalPort:   "5432",
				}
				var tunnel = sshConfig.CreateTunnel()

				// FIXME/question: do we need to shut down the tunnel manually if it is a child process?
				// at the moment tunnel.Stop is not being called anywhere, but if the connector shuts down, the child process also shuts down.
				if err := tunnel.Start(); err != nil {
					return nil, fmt.Errorf("error starting network tunnel: %w", err)
				}
			}

			return &sql.Endpoint{
				Config:                      cfg,
				Dialect:                     pgDialect,
				MetaSpecs:                   &metaSpecs,
				MetaCheckpoints:             &metaCheckpoints,
				Client:                      client{uri: cfg.ToURI()},
				CreateTableTemplate:         tplCreateTargetTable,
				AlterColumnNullableTemplate: tplAlterColumnNullable,
				AlterTableAddColumnTemplate: tplAlterTableAddColumn,
				NewResource:                 newTableConfig,
				NewTransactor:               newTransactor,
				CheckPrerequisites:          prereqs,
				Tenant:                      tenant,
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

	if db, err := stdsql.Open("pgx", cfg.ToURI()); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var pgErr *pgconn.PgError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "28P01":
				err = fmt.Errorf("incorrect username or password")
			case "3D000":
				err = fmt.Errorf("database %q does not exist", cfg.Database)
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
	} else {
		db.Close()
	}

	return errs
}

// client implements the sql.Client interface.
type client struct {
	uri string
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	err = c.withDB(func(db *stdsql.DB) error {
		specB64, version, err = sql.StdFetchSpecAndVersion(ctx, db, specs, materialization)
		return err
	})
	return
}

// ExecStatements is used for the DDL statements of ApplyUpsert and ApplyDelete. Postgres supports
// transactional DDL statements, so the statements are wrapped in a transaction.
func (c client) ExecStatements(ctx context.Context, statements []string) error {
	statements = append(append([]string{"begin;"}, statements...), "commit;")
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
	var db, err = stdsql.Open("pgx", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

type transactor struct {
	// Variables exclusively used by Load.
	load struct {
		conn     *pgx.Conn
		unionSQL string
	}
	// Variables exclusively used by Store.
	store struct {
		conn  *pgx.Conn
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
	if d.load.conn, err = pgx.Connect(ctx, cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("load pgx.Connect: %w", err)
	}
	if d.store.conn, err = pgx.Connect(ctx, cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("store pgx.Connect: %w", err)
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

type binding struct {
	target             sql.Table
	createLoadTableSQL string
	loadInsertSQL      string
	storeUpdateSQL     string
	storeInsertSQL     string
	loadQuerySQL       string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var b = &binding{target: target}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, tplCreateLoadTable},
		{&b.loadInsertSQL, tplLoadInsert},
		{&b.storeInsertSQL, tplStoreInsert},
		{&b.storeUpdateSQL, tplStoreUpdate},
		{&b.loadQuerySQL, tplLoadQuery},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	t.bindings = append(t.bindings, b)

	// Create a binding-scoped temporary table for staged keys to load.
	if _, err := t.load.conn.Exec(ctx, b.createLoadTableSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.createLoadTableSQL, err)
	}

	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	// Use a read-only "load" transaction, which will automatically
	// truncate the temporary key staging tables on commit.
	var txn, err = d.load.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer txn.Rollback(ctx)

	var batch pgx.Batch
	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else {
			batch.Queue(b.loadInsertSQL, converted...)
		}

		if batch.Len() >= loadBatchSizeLimit {
			if err := sendBatch(ctx, txn, &batch); err != nil {
				return fmt.Errorf("sending load batch: %w", err)
			}
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	// Send any remaining keys for this load.
	if batch.Len() > 0 {
		if err := sendBatch(ctx, txn, &batch); err != nil {
			return fmt.Errorf("sending final load batch: %w", err)
		}
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := txn.Query(ctx, d.load.unionSQL)
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
	} else if err = txn.Commit(ctx); err != nil {
		return fmt.Errorf("commiting Load transaction: %w", err)
	}

	return nil
}

func (d *transactor) Store(it *pm.StoreIterator) (_ pm.StartCommitFunc, err error) {
	ctx := it.Context()
	txn, err := d.store.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer func() {
		if err != nil {
			txn.Rollback(ctx)
		}
	}()

	var batch pgx.Batch
	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if it.Exists {
			batch.Queue(b.storeUpdateSQL, converted...)
		} else {
			batch.Queue(b.storeInsertSQL, converted...)
		}

		if batch.Len() >= storeBatchSizeLimit {
			if err := sendBatch(ctx, txn, &batch); err != nil {
				return nil, fmt.Errorf("sending store batch: %w", err)
			}
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		return nil, pf.RunAsyncOperation(func() error {
			defer txn.Rollback(ctx)

			var err error
			if d.store.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
				return fmt.Errorf("marshalling checkpoint: %w", err)
			}

			var fenceUpdate strings.Builder
			if err := tplUpdateFence.Execute(&fenceUpdate, d.store.fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}

			// Add the update to the fence as the last statement in the batch.
			batch.Queue(fenceUpdate.String())

			results := txn.SendBatch(ctx, &batch)

			// Execute all remaining doc inserts & updates.
			for i := 0; i < batch.Len()-1; i++ {
				if _, err := results.Exec(); err != nil {
					return fmt.Errorf("store at index %d: %w", i, err)
				}
			}

			// The fence update is always the last operation in the batch.
			if _, err := results.Exec(); err != nil {
				return fmt.Errorf("updating flow checkpoint: %w", err)
			} else if err = results.Close(); err != nil {
				return fmt.Errorf("results.Close(): %w", err)
			}

			if err := txn.Commit(ctx); err != nil {
				return fmt.Errorf("committing Store transaction: %w", err)
			}

			return nil
		})
	}, nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close(context.Background())
	d.store.conn.Close(context.Background())
}

func main() {
	boilerplate.RunMain(newPostgresDriver())
}

// Send a single batch of queries with the given transaction, discarding any results. The batch is
// zero'd upon completion.
func sendBatch(ctx context.Context, txn pgx.Tx, batch *pgx.Batch) error {
	results := txn.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("exec at index %d: %w", i, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("closing batch: %w", err)
	}

	var newBatch pgx.Batch
	*batch = newBatch

	return nil
}
