package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"text/template"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/go/dbt"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var featureFlagDefaults = map[string]bool{}

const (
	// As a very rough approximation, this will limit the amount of memory used for accumulating
	// batches of keys to load or documents to store based on the size of their packed tuples. As an
	// example, if documents average 2kb then a 10mb batch size will allow for ~5000 documents per
	// batch.
	batchBytesLimit = 10 * 1024 * 1024 // Bytes

	// Limit the maximum number of documents in a buffered batch as well since
	// very small documents (particularly keys to load) have a much higher ratio
	// of "overhead" to actual data size, and the batchBytesLimit alone may
	// result in excessive memory use.
	batchSizeLimit = 5000
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
	Address    string `json:"address" jsonschema:"title=Address,description=Host and port of the database (in the form of host[:port]). Port 5432 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	User       string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password   string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database   string `json:"database,omitempty" jsonschema:"title=Database,description=Name of the logical database to materialize to." jsonschema_extras:"order=3"`
	Schema     string `json:"schema,omitempty" jsonschema:"title=Database Schema,default=public,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables" jsonschema_extras:"order=4"`
	HardDelete bool   `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=5"`

	DBTJobTrigger dbt.JobConfig `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt Job when new data is available"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	SSLMode      string `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Overrides SSL connection behavior by setting the 'sslmode' parameter.,enum=disable,enum=allow,enum=prefer,enum=require,enum=verify-ca,enum=verify-full"`
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

// Validate the configuration.
func (c config) Validate() error {
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

	if c.Advanced.SSLMode != "" {
		if !slices.Contains([]string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}, c.Advanced.SSLMode) {
			return fmt.Errorf("invalid 'sslmode' configuration: unknown setting %q", c.Advanced.SSLMode)
		}
	}

	if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	// Connection poolers cause all sorts of problems with the materialization's
	// use of temporary tables and prepared statements, so the most common
	// addresses that use connection poolers are not allowed.
	if strings.HasSuffix(c.Address, ".pooler.supabase.com:6543") || strings.HasSuffix(c.Address, ".pooler.supabase.com") {
		return cerrors.NewUserError(nil, fmt.Sprintf("address must be a direct connection: address %q is using the Supabase connection pooler, consult go.estuary.dev/supabase-direct-address for details", c.Address))
	}

	return nil
}

// ToURI converts the Config to a DSN string.
func (c config) ToURI() string {
	var address = c.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432
	// to address through the bastion server, so we use the tunnel's address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != "" {
		address = "localhost:5432"
	}

	address = ensurePort(address)

	var uri = url.URL{
		Scheme: "postgres",
		Host:   address,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}
	var params = make(url.Values)
	if c.Advanced.SSLMode != "" {
		params.Set("sslmode", c.Advanced.SSLMode)
	}
	if len(params) > 0 {
		uri.RawQuery = params.Encode()
	}

	return uri.String()
}

func ensurePort(addr string) string {
	// If the user did not specify a port, default to 5432 since that is almost always what is
	// desired. pgx ends up doing this anyway, and we also need it for cases where a network tunnel
	// is being used and the port is not set.
	if !strings.Contains(addr, ":") {
		addr = addr + ":5432"
	}

	return addr
}

type tableConfig struct {
	Table         string `json:"table" jsonschema:"title=Table,description=Name of the database table" jsonschema_extras:"x-collection-name=true"`
	Schema        string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)" jsonschema_extras:"x-schema-name=true"`
	AdditionalSql string `json:"additional_table_create_sql,omitempty" jsonschema:"title=Additional Table Create SQL,description=Additional SQL statement(s) to be run in the same transaction that creates the table." jsonschema_extras:"multiline=true"`
	Delta         bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false." jsonschema_extras:"x-delta-updates=true"`
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	if r.Table == "" {
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
		path = []string{c.Schema, normalizeColumn(c.Table)}
	} else {
		path = []string{normalizeColumn(c.Table)}
	}
	return path, c.Delta, nil
}

func newPostgresDriver() *sql.Driver[config, tableConfig] {
	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-cratedb",
		StartTunnel: func(ctx context.Context, cfg config) error {

			// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
			if cfg.NetworkTunnel != nil && cfg.NetworkTunnel.SshForwarding != nil && cfg.NetworkTunnel.SshForwarding.SshEndpoint != "" {
				host, port, err := net.SplitHostPort(ensurePort(cfg.Address))
				if err != nil {
					return fmt.Errorf("splitting address to host and port: %w", err)
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
					return err
				}
			}

			return nil
		},
		NewEndpoint: func(ctx context.Context, cfg config, tenant string, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			log.WithFields(log.Fields{
				"database": cfg.Database,
				"address":  cfg.Address,
				"user":     cfg.User,
			}).Info("opening database")

			var metaBase sql.TablePath
			if cfg.Schema != "" {
				metaBase = append(metaBase, cfg.Schema)
			}

			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             crateDialect,
				MetaCheckpoints:     sql.FlowCheckpointsTable(metaBase),
				NewClient:           newClient,
				CreateTableTemplate: tplCreateTargetTable,
				NewTransactor:       newTransactor,
				Tenant:              tenant,
				ConcurrentApply:     false,
				Options: boilerplate.MaterializeOptions{
					DBTJobTrigger: &cfg.DBTJobTrigger,
				},
			}, nil
		},
		PreReqs: preReqs,
	}
}

type transactor struct {
	cfg *config

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
	be       *boilerplate.BindingEvents
}

func newTransactor(
	ctx context.Context,
	featureFlags map[string]bool,
	ep *sql.Endpoint[config],
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
	is *boilerplate.InfoSchema,
	be *boilerplate.BindingEvents,
) (m.Transactor, error) {
	var cfg = ep.Config

	var d = &transactor{cfg: &cfg, be: be}
	d.store.fence = fence

	// Establish connections.
	var err error
	if d.load.conn, err = pgx.Connect(ctx, cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("load pgx.Connect: %w", err)
	}

	if d.store.conn, err = pgx.Connect(ctx, cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("store pgx.Connect: %w", err)
	}

	// Override statement_timeout with a session-level setting to never timeout
	// statements.
	if _, err := d.load.conn.Exec(ctx, "set statement_timeout = 0;"); err != nil {
		return nil, fmt.Errorf("load set statement_timeout: %w", err)
	} else if _, err := d.store.conn.Exec(ctx, "set statement_timeout = 0;"); err != nil {
		return nil, fmt.Errorf("store set statement_timeout: %w", err)
	}

	for _, binding := range bindings {
		// Make sure that the binding does not exist before creating it.
		if err = d.removeBinding(ctx, binding); err != nil {
			return nil, fmt.Errorf("remove binding: %w", err)
		}

		if err = d.addBinding(ctx, binding, is); err != nil {
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
	target         sql.Table
	loadInsertSQL  string
	storeUpdateSQL string
	storeInsertSQL string
	deleteQuerySQL string
	loadQuerySQL   string
}

func (t *transactor) removeBinding(ctx context.Context, target sql.Table) error {
	var b = &binding{target: target}
	var w strings.Builder

	if err := tplDropLoadTable.Execute(&w, &b.target); err != nil {
		return fmt.Errorf("executing dropLoadTable template: %w", err)
	} else if _, err := t.load.conn.Exec(ctx, w.String()); err != nil {
		return fmt.Errorf("Exec(%s): %w", w.String(), err)
	}
	return nil
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, is *boilerplate.InfoSchema) error {
	var b = &binding{target: target}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.loadInsertSQL, tplLoadInsert},
		{&b.storeInsertSQL, tplStoreInsert},
		{&b.storeUpdateSQL, tplStoreUpdate},
		{&b.deleteQuerySQL, tplDeleteQuery},
		{&b.loadQuerySQL, tplLoadQuery},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	t.bindings = append(t.bindings, b)

	var w strings.Builder

	if err := tplCreateLoadTable.Execute(&w, &b.target); err != nil {
		return fmt.Errorf("executing createLoadTable template: %w", err)
	} else if _, err := t.load.conn.Exec(ctx, w.String()); err != nil {
		return fmt.Errorf("Exec(%s): %w", w.String(), err)
	}
	return nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	// Use a read-only "load" transaction, which will automatically
	// truncate the temporary key staging tables on commit.
	var txn, err = d.load.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer txn.Rollback(ctx)

	var batch pgx.Batch
	batchBytes := 0
	for it.Next() {
		// This assumes that the length of the packed key is at least proportional to the amount of
		// memory it will occupy when populated buffered in a pgx.Batch.
		batchBytes += len(it.PackedKey)

		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else {
			batch.Queue(b.loadInsertSQL, converted...)
		}

		if batchBytes >= batchBytesLimit || batch.Len() > batchSizeLimit {
			if err := sendBatch(ctx, txn, &batch); err != nil {
				return fmt.Errorf("sending load batch: %w", err)
			}
			batchBytes = 0
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
	d.be.StartedEvaluatingLoads()
	rows, err := txn.Query(ctx, d.load.unionSQL)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()
	d.be.FinishedEvaluatingLoads()

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

func (d *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
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
	batchBytes := 0
	for it.Next() {
		var b = d.bindings[it.Binding]

		if it.Delete && d.cfg.HardDelete {
			if it.Exists {
				if converted, err := b.target.ConvertKey(it.Key); err != nil {
					return nil, fmt.Errorf("converting delete keys: %w", err)
				} else if it.Exists {
					batch.Queue(b.deleteQuerySQL, converted...)
				}

				batchBytes += len(it.PackedKey)
			} else {
				// Ignore items which do not exist and are already deleted
				continue
			}
		} else {
			// Similar to the accounting in (*transactor).Store, this assumes that lengths of packed
			// tuples & the document JSON are proportional to the size of the item in the batch.
			batchBytes += len(it.PackedKey) + len(it.PackedValues) + len(it.RawJSON)

			if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
				return nil, fmt.Errorf("converting store parameters: %w", err)
			} else if it.Exists {
				batch.Queue(b.storeUpdateSQL, converted...)
			} else {
				batch.Queue(b.storeInsertSQL, converted...)
			}
		}

		if batchBytes >= batchBytesLimit || batch.Len() > batchSizeLimit {
			if err := sendBatch(ctx, txn, &batch); err != nil {
				return nil, fmt.Errorf("sending store batch: %w", err)
			}
			batchBytes = 0
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		return nil, m.RunAsyncOperation(func() error {
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
	for _, b := range d.bindings {
		err := d.removeBinding(context.Background(), b.target)
		if err != nil {
			return
		}
	}

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
