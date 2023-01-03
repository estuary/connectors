package main

import (
	"bufio"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	sf "github.com/snowflakedb/gosnowflake"
)

// config represents the endpoint configuration for snowflake.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type config struct {
	Host      string `json:"host" jsonschema:"title=Host URL,description=The Snowflake Host used for the connection. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol)." jsonschema_extras:"order=0"`
	Account   string `json:"account" jsonschema:"title=Account,description=The Snowflake account identifier." jsonschema_extras:"order=1"`
	User      string `json:"user" jsonschema:"title=User,description=The Snowflake user login name." jsonschema_extras:"order=2"`
	Password  string `json:"password" jsonschema:"title=Password,description=The password for the provided user." jsonschema_extras:"secret=true,order=3"`
	Database  string `json:"database" jsonschema:"title=Database,description=The SQL database to connect to." jsonschema_extras:"order=4"`
	Schema    string `json:"schema" jsonschema:"title=Schema,description=The SQL schema to use." jsonschema_extras:"order=5"`
	Warehouse string `json:"warehouse,omitempty" jsonschema:"title=Warehouse,description=The Snowflake virtual warehouse used to execute queries." jsonschema_extras:"order=6"`
	Role      string `json:"role,omitempty" jsonschema:"title=Role,description=The user role used to perform actions." jsonschema_extras:"order=7"`
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {
	// Build a DSN connection string.
	var configCopy = c.asSnowflakeConfig()
	// client_session_keep_alive causes the driver to issue a periodic keepalive request.
	// Without this, the authentication token will expire after 4 hours of inactivity.
	// The Params map will not have been initialized if the endpoint config didn't specify
	// it, so we check and initialize here if needed.
	if configCopy.Params == nil {
		configCopy.Params = make(map[string]*string)
	}
	configCopy.Params["client_session_keep_alive"] = &trueString
	dsn, err := sf.DSN(&configCopy)
	if err != nil {
		panic(fmt.Errorf("building snowflake dsn: %w", err))
	}

	return dsn
}

func (c *config) asSnowflakeConfig() sf.Config {
	var maxStatementCount string = "0"
	return sf.Config{
		Account:   c.Account,
		Host:      c.Host,
		User:      c.User,
		Password:  c.Password,
		Database:  c.Database,
		Schema:    c.Schema,
		Warehouse: c.Warehouse,
		Role:      c.Role,
		Params: map[string]*string{
			// By default Snowflake expects the number of statements to be provided
			// with every request. By setting this parameter to zero we are allowing a
			// variable number of statements to be executed in a single request
			"MULTI_STATEMENT_COUNT": &maxStatementCount,
		},
	}
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"account", c.Account},
		{"host", c.Host},
		{"user", c.User},
		{"password", c.Password},
		{"database", c.Database},
		{"schema", c.Schema},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

type tableConfig struct {
	base *config

	Table string `json:"table" jsonschema_extras:"x-collection-name=true"`
	Delta bool   `json:"delta_updates,omitempty"`
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{}
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func (c tableConfig) GetAdditionalSql() string {
	return ""
}

// The Snowflake driver Params map uses string pointers as values, which is what this is used for.
var trueString = "true"

// newSnowflakeDriver creates a new Driver for Snowflake.
func newSnowflakeDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-snowflake",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage) (*sql.Endpoint, error) {
			var parsed = new(config)
			if err := pf.UnmarshalStrict(raw, parsed); err != nil {
				return nil, fmt.Errorf("parsing Snowflake configuration: %w", err)
			}

			var dsn = parsed.ToURI()

			log.WithFields(log.Fields{
				"host":     parsed.Host,
				"user":     parsed.User,
				"database": parsed.Database,
				"schema":   parsed.Schema,
			}).Info("opening Snowflake")

			db, err := stdsql.Open("snowflake", dsn)
			if err == nil {
				err = db.PingContext(ctx)
			}

			if err != nil {
				return nil, fmt.Errorf("opening Snowflake database: %w", err)
			}

			var metaBase sql.TablePath
			var metaSpecs, metaCheckpoints = sql.MetaTables(metaBase)

			return &sql.Endpoint{
				Config:              parsed,
				Dialect:             snowflakeDialect,
				MetaSpecs:           metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				Client:              client{uri: dsn},
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
		var err error
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence)
		return err
	})
	return fence, err
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	var db, err = stdsql.Open("snowflake", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

type transactor struct {
	cfg     *config
	dialect *sql.Dialect

	// Variables exclusively used by Load.
	load struct {
		conn *stdsql.Conn
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		conn  *stdsql.Conn
		fence *sql.Fence
	}
	bindings []*binding
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
) (_ pm.Transactor, err error) {
	var d = &transactor{
		cfg: ep.Config.(*config),
	}
	d.store.fence = &fence

	var cfg = ep.Config.(*config)

	// Establish connections.
	if db, err := stdsql.Open("snowflake", cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("load stdsql.Open: %w", err)
	} else if d.load.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("load db.Conn: %w", err)
	}
	if db, err := stdsql.Open("snowflake", cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("store stdsql.Open: %w", err)
	} else if d.store.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("store db.Conn: %w", err)
	}

	// Create stage for file-based transfers.
	if _, err = d.load.conn.ExecContext(ctx, createStageSQL); err != nil {
		return nil, fmt.Errorf("creating transfer stage : %w", err)
	}

	for _, binding := range bindings {
		if err = d.addBinding(ctx, binding); err != nil {
			return nil, fmt.Errorf("%v: %w", binding, err)
		}
	}

	return d, nil
}

type binding struct {
	target sql.Table
	// Variables exclusively used by Load.
	load struct {
		loadQuery string
		stage     *scratchFile
		hasKeys   bool
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		stage     *scratchFile
		mergeInto string
		copyInto  string
		mustMerge bool
		hasDocs   bool
	}
}

type TableWithUUID struct {
	Table      *sql.Table
	RandomUUID uuid.UUID
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var d = new(binding)
	var err error
	d.target = target

	// Create local scratch files used for loads and stores.
	if d.load.stage, err = newScratchFile(os.TempDir()); err != nil {
		return fmt.Errorf("newScratchFile: %w", err)
	}
	if d.store.stage, err = newScratchFile(os.TempDir()); err != nil {
		return fmt.Errorf("newScratchFile: %w", err)
	}

	if d.load.loadQuery, err = RenderTableWithRandomUUIDTemplate(target, d.load.stage.uuid, tplLoadQuery); err != nil {
		return fmt.Errorf("loadQuery template: %w", err)
	}
	if d.store.copyInto, err = RenderTableWithRandomUUIDTemplate(target, d.store.stage.uuid, tplCopyInto); err != nil {
		return fmt.Errorf("copyInto template: %w", err)
	}
	if d.store.mergeInto, err = RenderTableWithRandomUUIDTemplate(target, d.store.stage.uuid, tplMergeInto); err != nil {
		return fmt.Errorf("mergeInto template: %w", err)
	}

	t.bindings = append(t.bindings, d)
	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, _, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err = b.load.stage.Encode(converted); err != nil {
			return fmt.Errorf("encoding Load key to scratch file: %w", err)
		}
		b.load.hasKeys = true
	}
	if it.Err() != nil {
		return it.Err()
	}

	var subqueries []string
	// PUT staged keys to Snowflake in preparation for querying.
	for _, b := range d.bindings {
		if !b.load.hasKeys {
			// Pass.
		} else if err := b.load.stage.put(ctx, d.load.conn, d.cfg); err != nil {
			return fmt.Errorf("load.stage(): %w", err)
		} else {
			subqueries = append(subqueries, b.load.loadQuery)
			b.load.hasKeys = false // Reset for next transaction.
		}
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}
	var loadAllSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

	// Issue a join of the target table and (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := d.load.conn.QueryContext(ctx, loadAllSQL)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document stdsql.RawBytes

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(binding, json.RawMessage(document)); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	return nil
}

func (d *transactor) Prepare(_ context.Context, prepare pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	d.store.fence.Checkpoint = prepare.FlowCheckpoint
	return pf.DriverCheckpoint{}, nil
}

func (d *transactor) Store(it *pm.StoreIterator) error {
	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return fmt.Errorf("converting Store: %w", err)
		} else if err = b.store.stage.Encode(converted); err != nil {
			return fmt.Errorf("encoding Store to scratch file: %w", err)
		}

		if it.Exists {
			b.store.mustMerge = true
		}
		b.store.hasDocs = true
	}
	return nil
}

func (d *transactor) Commit(ctx context.Context) error {
	for _, b := range d.bindings {
		if b.store.hasDocs {
			// PUT staged keys to Snowflake in preparation for querying.
			if err := b.store.stage.put(ctx, d.store.conn, d.cfg); err != nil {
				return fmt.Errorf("load.stage(): %w", err)
			}
		}
	}

	// Start a transaction for our Store phase.
	var txn, err = d.store.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("conn.BeginTx: %w", err)
	}
	defer txn.Rollback()

	// First we must validate the fence has not been modified.
	var fenceUpdate strings.Builder
	if err := tplUpdateFence.Execute(&fenceUpdate, d.store.fence); err != nil {
		return fmt.Errorf("evaluating fence template: %w", err)
	}
	if _, err := txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
		err = fmt.Errorf("txn.Exec: %w", err)

		// Give recommendation to user for resolving timeout issues
		if strings.Contains(err.Error(), "timeout") {
			return fmt.Errorf("fence.Update: %w  (ensure LOCK_TIMEOUT and STATEMENT_TIMEOUT_IN_SECONDS are at least ten minutes)", err)
		}

		return err
	}

	for _, b := range d.bindings {
		if !b.store.hasDocs {
			// No table update required
		} else if !b.store.mustMerge {
			// We can issue a faster COPY INTO the target table.
			if _, err = d.store.conn.ExecContext(ctx, b.store.copyInto); err != nil {
				return fmt.Errorf("copying Store documents: %w", err)
			}
		} else {
			// We must MERGE into the target table.
			if _, err = d.store.conn.ExecContext(ctx, b.store.mergeInto); err != nil {
				return fmt.Errorf("merging Store documents: %w", err)
			}
		}

		// Reset for next transaction.
		b.store.hasDocs = false
		b.store.mustMerge = false
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("txn.Commit: %w", err)
	}

	return nil
}

// Acknowledge is a no-op.
func (d *transactor) Acknowledge(context.Context) error {
	return nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close()
	d.store.conn.Close()

	for _, b := range d.bindings {
		b.load.stage.destroy()
		b.store.stage.destroy()
	}
}

type scratchFile struct {
	uuid uuid.UUID
	file *os.File
	bw   *bufio.Writer
	*json.Encoder
}

func (f *scratchFile) destroy() {
	// TODO remove from Snowflake.
	os.Remove(f.file.Name())
	f.file.Close()
}

func newScratchFile(tempdir string) (*scratchFile, error) {
	var uuid, err = uuid.NewRandom()
	if err != nil {
		panic(err)
	}

	var path = filepath.Join(tempdir, uuid.String())
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating scratch %q: %w", path, err)
	}
	var bw = bufio.NewWriter(file)
	var enc = json.NewEncoder(bw)

	return &scratchFile{
		uuid:    uuid,
		file:    file,
		bw:      bw,
		Encoder: enc,
	}, nil
}

func (f *scratchFile) put(ctx context.Context, conn *stdsql.Conn, cfg *config) error {
	if err := f.bw.Flush(); err != nil {
		return fmt.Errorf("scratch.Flush: %w", err)
	}

	var query = fmt.Sprintf(
		`PUT file://%s @flow_v1 AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=NONE OVERWRITE=TRUE ;`,
		f.file.Name(),
	)

	var rows, err = conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("PUT to stage: %w", err)
	} else {
		rows.Close()
	}

	if err := f.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate after stage: %w", err)
	} else if _, err = f.file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek after truncate: %w", err)
	}
	f.bw.Reset(f.file)

	return nil
}

func main() { boilerplate.RunMain(newSnowflakeDriver()) }
