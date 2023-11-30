package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	sf "github.com/snowflakedb/gosnowflake"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
	"sync"
)

type tableConfig struct {
	Table  string `json:"table" jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)"`
	Delta  bool   `json:"delta_updates,omitempty"`

	// If the endpoint schema is the same as the resource schema, the resource path will be only the
	// table name. This is to provide compatibility for materializations that were created prior to
	// the resource-level schema setting existing, which always had a resource path of only the
	// table name.
	endpointSchema string
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{
		// Default to the explicit endpoint configuration schema. This may be over-written by a
		// present `schema` property within `raw` for the resource.
		Schema:         ep.Config.(*config).Schema,
		endpointSchema: ep.Config.(*config).Schema,
	}
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}

func schemasEqual(s1 string, s2 string) bool {
	// Both are unquoted: Do a case insensitive match, since Snowflake uppercases everything that
	// isn't quoted. For example, "Public" is the same as "public", which is the same as "PUBLIC"
	// etc.
	if isSimpleIdentifier(s1) && isSimpleIdentifier(s2) {
		return strings.EqualFold(s1, s2)
	}

	return s1 == s2
}

func (c tableConfig) Path() sql.TablePath {
	if c.Schema == "" || schemasEqual(c.Schema, c.endpointSchema) {
		return []string{c.Table}
	}
	return []string{c.Schema, c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

// The Snowflake driver Params map uses string pointers as values, which is what this is used for.
var trueString = "true"

// newSnowflakeDriver creates a new Driver for Snowflake.
func newSnowflakeDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-snowflake",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var parsed = new(config)
			if err := pf.UnmarshalStrict(raw, parsed); err != nil {
				return nil, fmt.Errorf("parsing Snowflake configuration: %w", err)
			}

			var dsn = parsed.ToURI(tenant)

			log.WithFields(log.Fields{
				"host":     parsed.Host,
				"user":     parsed.User,
				"database": parsed.Database,
				"schema":   parsed.Schema,
				"tenant":   tenant,
			}).Info("opening Snowflake")

			var metaBase sql.TablePath
			var metaSpecs, metaCheckpoints = sql.MetaTables(metaBase)

			var dialect = snowflakeDialect(parsed.Schema)
			var templates = renderTemplates(dialect)

			return &sql.Endpoint{
				Config:              parsed,
				Dialect:             dialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				Client:              client{uri: dsn, dialect: dialect},
				CreateTableTemplate: templates["createTargetTable"],
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				Tenant:              tenant,
			}, nil
		},
	}
}

type client struct {
	uri     string
	dialect sql.Dialect
}

func (c client) Apply(ctx context.Context, ep *sql.Endpoint, actions sql.ApplyActions, updateSpec sql.MetaSpecsUpdate, dryRun bool) (string, error) {
	db, err := stdsql.Open("snowflake", c.uri)
	if err != nil {
		return "", err
	}
	defer db.Close()

	// Currently the "catalog" is always the database value from the endpoint configuration in all
	// capital letters. It is possible to connect to Snowflake databases that aren't in all caps by
	// quoting the database name. We don't do that currently and it's hard to say if we ever will
	// need to, although that means we can't connect to databases that aren't in the Snowflake
	// default ALL CAPS format. The practical implications are that if somebody puts in a database
	// like "database", we'll actually connect to the database "DATABASE", and so we can't rely on
	// the endpoint configuration value entirely and will query it here to be future-proof.
	var catalog string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_DATABASE()").Scan(&catalog); err != nil {
		return "", fmt.Errorf("querying for connected database: %w", err)
	}

	resolved, err := sql.ResolveActions(ctx, db, actions, c.dialect, catalog)
	if err != nil {
		return "", fmt.Errorf("resolving apply actions: %w", err)
	}

	statements := []string{}
	for _, tc := range resolved.CreateTables {
		statements = append(statements, tc.TableCreateSql)
	}

	for _, ta := range resolved.AlterTables {
		var alterColumnStmt strings.Builder
		if err := renderTemplates(c.dialect)["alterTableColumns"].Execute(&alterColumnStmt, ta); err != nil {
			return "", fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		statements = append(statements, alterColumnStmt.String())
	}

	// The spec will get updated last, after all the other actions are complete, but include it in
	// the description of actions.
	action := strings.Join(append(statements, updateSpec.QueryString), "\n")
	if dryRun {
		return action, nil
	}

	// Execute statements in parallel for efficiency. Each statement acts on a single table, and
	// everything that needs to be done for a given table is contained in that statement.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, s := range statements {
		s := s
		group.Go(func() error {
			if _, err := db.ExecContext(groupCtx, s); err != nil {
				return fmt.Errorf("executing apply statement: %w", err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return "", err
	}

	// Once all the table actions are done, we can update the stored spec.
	if _, err := db.ExecContext(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...); err != nil {
		return "", fmt.Errorf("executing spec update statement: %w", err)
	}

	return action, nil
}

func (c client) PreReqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	errs := &sql.PrereqErr{}

	if db, err := stdsql.Open("snowflake", cfg.ToURI(ep.Tenant)); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(ctx); err != nil {
		var sfError *sf.SnowflakeError
		if errors.As(err, &sfError) {
			switch sfError.Number {
			case 260008:
				// This is the error if the host URL has an incorrect account identifier. The error
				// message from the Snowflake driver will accurately report that the account name is
				// incorrect, but would be confusing for a user because we have a separate "Account"
				// input field. We want to be specific here and report that it is the account
				// identifier in the host URL.
				err = fmt.Errorf("incorrect account identifier %q in host URL", strings.TrimSuffix(cfg.Host, ".snowflakecomputing.com"))
			case 390100:
				err = fmt.Errorf("incorrect username or password")
			case 390201:
				// This means "doesn't exist or not authorized", and we don't have a way to
				// distinguish between that for the database, schema, or warehouse. The snowflake
				// error message in these cases is fairly decent fortunately.
			case 390189:
				err = fmt.Errorf("role %q does not exist", cfg.Role)
			}
		}

		errs.Err(err)
	} else {
		// Check for an active warehouse for the connection. If there is no default warehouse for
		// the user and the configuration did not set a warehouse, this may be `null`, and the user
		// needs to configure a specific warehouse to use.
		var currentWarehouse *string
		if err := db.QueryRowContext(ctx, "SELECT CURRENT_WAREHOUSE();").Scan(&currentWarehouse); err != nil {
			errs.Err(fmt.Errorf("checking for active warehouse: %w", err))
		} else {
			if currentWarehouse == nil {
				errs.Err(fmt.Errorf("no warehouse configured and default warehouse not set for user '%s': must set a value for 'Warehouse' in the endpoint configuration", cfg.User))
			}
		}

		db.Close()
	}

	return errs
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
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence, base64.StdEncoding.DecodeString)
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
	cfg *config
	db  *stdsql.DB
	// Variables exclusively used by Load.
	load struct {
		conn *stdsql.Conn
	}
	// Variables exclusively used by Store.
	store struct {
		conn  *stdsql.Conn
		fence *sql.Fence
		round int
	}
	templates   map[string]*template.Template
	bindings    []*binding
	updateDelay time.Duration
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
) (_ pm.Transactor, err error) {
	var cfg = ep.Config.(*config)

	dialect := snowflakeDialect(cfg.Schema)

	db, err := stdsql.Open("snowflake", cfg.ToURI(ep.Tenant))
	if err != nil {
		return nil, fmt.Errorf("newTransactor stdsql.Open: %w", err)
	}

	var d = &transactor{
		cfg:       cfg,
		templates: renderTemplates(dialect),
		db:        db,
	}

	if d.updateDelay, err = sql.ParseDelay(cfg.Advanced.UpdateDelay); err != nil {
		return nil, err
	}

	d.store.fence = &fence

	// Establish connections.
	if db, err := stdsql.Open("snowflake", cfg.ToURI(ep.Tenant)); err != nil {
		return nil, fmt.Errorf("load stdsql.Open: %w", err)
	} else if d.load.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("load db.Conn: %w", err)
	}
	if db, err := stdsql.Open("snowflake", cfg.ToURI(ep.Tenant)); err != nil {
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
	target   sql.Table
	snowPipe bool
	// Variables exclusively used by Load.
	load struct {
		loadQuery string
		stage     *stagedFile
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		stage     *stagedFile
		mergeInto string
		copyInto  string
		mustMerge bool
	}
}

type TableWithUUID struct {
	Table      *sql.Table
	RandomUUID string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var d = new(binding)
	var err error
	d.target = target

	d.load.stage = newStagedFile(os.TempDir())
	d.store.stage = newStagedFile(os.TempDir())

	if d.load.loadQuery, err = RenderTableWithRandomUUIDTemplate(target, d.load.stage.uuid, t.templates["loadQuery"]); err != nil {
		return fmt.Errorf("loadQuery template: %w", err)
	}
	if d.store.copyInto, err = RenderTableWithRandomUUIDTemplate(target, d.store.stage.uuid, t.templates["copyInto"]); err != nil {
		return fmt.Errorf("copyInto template: %w", err)
	}
	if d.store.mergeInto, err = RenderTableWithRandomUUIDTemplate(target, d.store.stage.uuid, t.templates["mergeInto"]); err != nil {
		return fmt.Errorf("mergeInto template: %w", err)
	}

	// If this is a delta updates binding and we are using JWT auth type, this binding
	// can use snowpipe
	d.snowPipe = target.DeltaUpdates && t.cfg.Credentials.AuthType == JWT

	if d.snowPipe {
		if createPipe, err := RenderTableWithRandomUUIDTemplate(target, d.store.stage.uuid, t.templates["createPipe"]); err != nil {
			return fmt.Errorf("createPipe template: %w", err)
		} else if _, err := t.db.ExecContext(sf.WithStreamDownloader(ctx), createPipe); err != nil {
			return fmt.Errorf("creating pipe for table %q: %w", target.Path, err)
		}
	}

	t.bindings = append(t.bindings, d)
	return nil
}

const MaxConcurrentLoads = 5

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	log.Info("load: starting encoding and uploading of files")
	for it.Next() {
		var b = d.bindings[it.Binding]

		if err := b.load.stage.start(ctx, d.db); err != nil {
			return err
		} else if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err = b.load.stage.encodeRow(converted); err != nil {
			return fmt.Errorf("encoding Load key to scratch file: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	var subqueries = make(map[int]string)
	for i, b := range d.bindings {
		if !b.load.stage.started {
			// Pass.
		} else if err := b.load.stage.flush(); err != nil {
			return fmt.Errorf("load.stage(): %w", err)
		} else {
			subqueries[i] = b.load.loadQuery
		}
	}

	log.Info("load: finished encoding and uploading of files")

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(MaxConcurrentLoads)
	// Used to ensure we have no data-interleaving when calling |loaded|
	var mutex sync.Mutex

	// In order for the concurrent requests below to be actually run concurrently we need
	// a separate connection for each

	for iLoop, queryLoop := range subqueries {
		var query = queryLoop
		var i = iLoop
		group.Go(func() error {
			var b = d.bindings[i]

			log.WithField("table", b.target.Identifier).Info("load: starting querying documents")
			// Issue a join of the target table and (now staged) load keys,
			// and send results to the |loaded| callback.
			rows, err := d.db.QueryContext(sf.WithStreamDownloader(groupCtx), query)
			if err != nil {
				return fmt.Errorf("querying Load documents: %w", err)
			}
			defer rows.Close()

			var binding int
			var document stdsql.RawBytes

			log.WithField("table", b.target.Identifier).Info("load: finished querying documents")

			mutex.Lock()
			defer mutex.Unlock()
			for rows.Next() {
				if err = rows.Scan(&binding, &document); err != nil {
					return fmt.Errorf("scanning Load document: %w", err)
				} else if err = loaded(binding, json.RawMessage(document)); err != nil {
					return fmt.Errorf("sending loaded document for table %q: %w", d.bindings[binding].target.Identifier, err)
				}
			}

			if err = rows.Err(); err != nil {
				return fmt.Errorf("querying Loads: %w", err)
			}

			log.WithField("table", b.target.Identifier).Info("load: flushed documents to runtime")

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	log.Info("load: finished loading")

	return nil
}

func (d *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	d.store.round++

	log.Info("store: starting encoding and uploading of files")
	for it.Next() {
		var b = d.bindings[it.Binding]

		if err := b.store.stage.start(it.Context(), d.db); err != nil {
			return nil, err
		} else if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return nil, fmt.Errorf("converting Store: %w", err)
		} else if err = b.store.stage.encodeRow(converted); err != nil {
			return nil, fmt.Errorf("encoding Store to scratch file: %w", err)
		}

		if it.Exists {
			b.store.mustMerge = true
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, _ <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		log.Info("store: starting commit phase")
		var err error
		if d.store.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		return nil, sql.CommitWithDelay(ctx, d.store.round, d.updateDelay, it.Total, d.commit)
	}, nil
}

func (d *transactor) commit(ctx context.Context) error {
	var txn, err = d.store.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store.conn.BeginTx: %w", err)
	}
	defer txn.Rollback()

	// First we must validate the fence has not been modified.
	var fenceUpdate strings.Builder
	if err := d.templates["updateFence"].Execute(&fenceUpdate, d.store.fence); err != nil {
		return fmt.Errorf("evaluating fence template: %w", err)
	} else if _, err = txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
		err = fmt.Errorf("txn.Exec: %w", err)

		// Give recommendation to user for resolving timeout issues
		if strings.Contains(err.Error(), "timeout") {
			return fmt.Errorf("fence.Update: %w  (ensure LOCK_TIMEOUT and STATEMENT_TIMEOUT_IN_SECONDS are at least ten minutes)", err)
		}

		return err
	}

	log.Info("store: starting copying of files into tables")
	for _, b := range d.bindings {
		if !b.store.stage.started {
			// No table update required
		} else if err := b.store.stage.flush(); err != nil {
			return err
		} else if !b.store.mustMerge {
			log.WithField("table", b.target.Identifier).Info("store: starting direct copying data into table")
			// We can issue a faster COPY INTO the target table.
			if _, err = txn.ExecContext(ctx, b.store.copyInto); err != nil {
				return fmt.Errorf("copying Store documents into table %q: %w", b.target.Identifier, err)
			}
			log.WithField("table", b.target.Identifier).Info("store: finishing direct copying data into table")
		} else {
			log.WithField("table", b.target.Identifier).Info("store: starting merging data into table")
			// We must MERGE into the target table.
			if _, err = txn.ExecContext(ctx, b.store.mergeInto); err != nil {
				return fmt.Errorf("merging Store documents into table %q: %w", b.target.Identifier, err)
			}
			log.WithField("table", b.target.Identifier).Info("store: finishing merging data into table")
		}

		// Reset for next transaction.
		b.store.mustMerge = false
	}

	log.Info("store: finished encoding and uploading of files")

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("txn.Commit: %w", err)
	}
	log.Info("store: finished commit")

	return nil
}

func (d *transactor) Destroy() {
	d.db.Close()
	d.load.conn.Close()
	d.store.conn.Close()
}

func main() {
	// gosnowflake also uses logrus for logging and the logs it produces may be confusing when
	// intermixed with our connector logs. We disable the gosnowflake logger here and log as needed
	// when handling errors from the sql driver.
	sf.GetLogger().SetOutput(io.Discard)
	boilerplate.RunMain(newSnowflakeDriver())
}
