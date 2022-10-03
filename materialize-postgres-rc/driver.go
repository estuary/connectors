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
	"time"

	networkTunnel "github.com/estuary/connectors/go-network-tunnel"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	log "github.com/sirupsen/logrus"
)

type tunnelConfig struct {
	SshEndpoint string `json:"ssh_endpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling, in the form of ssh://user@hostname[:port]"`
	PrivateKey  string `json:"private_key" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

// config represents the endpoint configuration for postgres.
type config struct {
	Address  string `json:"address" jsonschema:"title=Address,description=Host and port of the database."`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as."`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true"`
	Database string `json:"database,omitempty" jsonschema:"title=Database,description=Name of the logical database to materialize to."`
	Schema   string `json:"schema,omitempty" jsonschema:"title=Database Schema,default=public,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables"`

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
	if c.NetworkTunnel != nil && c.NetworkTunnel.SshEndpoint != "" {
		address = "localhost:5432"
	}
	var uri = url.URL{
		Scheme: "postgres",
		Host:   address,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}
	// Compatibility with pgBouncer. See:
	// https://github.com/jackc/pgx/issues/650
	uri.RawQuery = "statement_cache_mode=describe"
	return uri.String()
}

type tableConfig struct {
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)"`
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Defaults is false."`
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

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newPostgresDriver() pm.DriverServer {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-postgresql",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage) (*sql.Endpoint, error) {
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

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             pgDialect,
				MetaSpecs:           metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				Client:              client{uri: cfg.ToURI()},
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
			}, nil
		},
	}
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

func (c client) ExecStatements(ctx context.Context, statements []string) error {
	return c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements) })
}

func (c client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	var err = c.withDB(func(db *stdsql.DB) error {
		// Pick a path randomly to ensure both are exercised and correct.
		if time.Now().Unix()%2 == 0 {

			// Option 1: Install using template.
			var query strings.Builder
			if err := tplInstallFence.Execute(&query, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			return db.QueryRow(query.String()).Scan(&fence.Fence, &fence.Checkpoint)
		}

		// Option 2: Install using StdInstallFence.
		var err error
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence)
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
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		batch pgBatch
		conn  *pgx.Conn
		fence sql.Fence
	}
	bindings []*binding

	tunnel networkTunnel.SshTunnel
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
) (_ pm.Transactor, err error) {
	var d = &transactor{}
	d.store.fence = fence

	// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
	var cfg = ep.Config.(config)
	if cfg.NetworkTunnel != nil && cfg.NetworkTunnel.SshEndpoint != "" {
		host, port, err := net.SplitHostPort(cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("splitting address to host and port: %w", err)
		}

		var sshConfig = &networkTunnel.SshConfig{
			SshEndpoint: cfg.NetworkTunnel.SshEndpoint,
			PrivateKey:  []byte(cfg.NetworkTunnel.PrivateKey),
			ForwardHost: host,
			ForwardPort: port,
			LocalPort:   "5432",
		}
		d.tunnel = sshConfig.CreateTunnel()

		// TODO: better handle synchronisation of network-tunnel
		go func() {
			out, err := d.tunnel.Start()

			if err != nil {
				log.WithField("error", err).WithField("output", string(out)).Error("network tunnel error")
			} else {
				log.WithField("error", string(out)).Debug("network tunnel output")
			}
		}()
	}

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
	execLoadInsertSQL  string
	execStoreInsertSQL string
	execStoreUpdateSQL string
	loadQuerySQL       string
	prepLoadInsertSQL  string
	prepStoreInsertSQL string
	prepStoreUpdateSQL string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var b = &binding{target: target}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, tplCreateLoadTable},
		{&b.execLoadInsertSQL, tplExecLoadInsert},
		{&b.execStoreInsertSQL, tplExecStoreInsert},
		{&b.execStoreUpdateSQL, tplExecStoreUpdate},
		{&b.loadQuerySQL, tplLoadQuery},
		{&b.prepLoadInsertSQL, tplPrepLoadInsert},
		{&b.prepStoreInsertSQL, tplPrepStoreInsert},
		{&b.prepStoreUpdateSQL, tplPrepStoreUpdate},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	t.bindings = append(t.bindings, b)
	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, _, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	// We send SQL commands in batches to amortize network round-trips.
	// All operations of Load run as a single transaction.
	var batch = newPGBatch()
	batch.queue(nil, "begin;")

	for _, b := range d.bindings {
		batch.queue(nil, b.createLoadTableSQL)
		batch.queue(nil, b.prepLoadInsertSQL)
	}

	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else {
			batch.queueParams(nil, b.execLoadInsertSQL, converted...)
		}

		if len(batch.buf) > 1024*1024 {
			if err := batch.roundTrip(ctx, d.load.conn.PgConn()); err != nil {
				return err
			}
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	var loadFn = func(rr *pgconn.ResultReader) error {
		var fields = rr.FieldDescriptions()
		for rr.NextRow() {
			var binding int
			var document json.RawMessage

			if err := scanRow(d.load.conn.ConnInfo(), fields, rr.Values(), &binding, &document); err != nil {
				return fmt.Errorf("scanning Load document: %w", err)
			} else if err = loaded(binding, json.RawMessage(document)); err != nil {
				return err
			}
		}
		return nil
	}

	// Issue a union join of the target tables and their (now staged) load keys.
	batch.queue(loadFn, d.load.unionSQL)
	// We're done with our prepared statements - clean them up.
	batch.queue(nil, "deallocate prepare all;")
	// Commit to drop temporary load table.
	batch.queue(nil, "commit;")

	return batch.roundTrip(ctx, d.load.conn.PgConn())
}

func (d *transactor) Prepare(_ context.Context, prepare pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	d.store.batch = newPGBatch()
	d.store.batch.queue(nil, "begin;")

	for _, b := range d.bindings {
		d.store.batch.queue(nil, b.prepStoreInsertSQL)
		d.store.batch.queue(nil, b.prepStoreUpdateSQL)
	}

	d.store.fence.Checkpoint = prepare.FlowCheckpoint
	var fenceUpdate strings.Builder
	if err := tplUpdateFence.Execute(&fenceUpdate, d.store.fence); err != nil {
		return pf.DriverCheckpoint{}, fmt.Errorf("evaluating fence template: %w", err)
	}
	d.store.batch.queue(nil, fenceUpdate.String())

	return pf.DriverCheckpoint{}, nil
}

func (d *transactor) Store(it *pm.StoreIterator) error {
	var ctx = it.Context()

	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return fmt.Errorf("converting store parameters: %w", err)
		} else if it.Exists {
			d.store.batch.queueParams(nil, b.execStoreUpdateSQL, converted...)
		} else {
			d.store.batch.queueParams(nil, b.execStoreInsertSQL, converted...)
		}

		if len(d.store.batch.buf) > 1024*1024 {
			if err := d.store.batch.roundTrip(ctx, d.store.conn.PgConn()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *transactor) Commit(ctx context.Context) error {
	d.store.batch.queue(nil, "deallocate prepare all;") // Cleanup prepared statements.
	d.store.batch.queue(nil, "commit;")
	return d.store.batch.roundTrip(ctx, d.store.conn.PgConn())
}

func (d *transactor) Acknowledge(context.Context) error {
	return nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close(context.Background())
	d.store.conn.Close(context.Background())
}

func main() {
	boilerplate.RunMain(newPostgresDriver())
}
