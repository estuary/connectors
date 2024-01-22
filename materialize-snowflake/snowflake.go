package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"

	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	sf "github.com/snowflakedb/gosnowflake"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
)

// config represents the endpoint configuration for snowflake.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type config struct {
	Host      string `json:"host" jsonschema:"title=Host URL,description=The Snowflake Host used for the connection. Must include the account identifier and end in .snowflakecomputing.com. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol)." jsonschema_extras:"order=0"`
	Account   string `json:"account" jsonschema:"title=Account,description=The Snowflake account identifier." jsonschema_extras:"order=1"`
	User      string `json:"user" jsonschema:"title=User,description=The Snowflake user login name." jsonschema_extras:"order=2"`
	Password  string `json:"password" jsonschema:"title=Password,description=The password for the provided user." jsonschema_extras:"secret=true,order=3"`
	Database  string `json:"database" jsonschema:"title=Database,description=The SQL database to connect to." jsonschema_extras:"order=4"`
	Schema    string `json:"schema" jsonschema:"title=Schema,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=5"`
	Warehouse string `json:"warehouse,omitempty" jsonschema:"title=Warehouse,description=The Snowflake virtual warehouse used to execute queries. Uses the default warehouse for the Snowflake user if left blank." jsonschema_extras:"order=6"`
	Role      string `json:"role,omitempty" jsonschema:"title=Role,description=The user role used to perform actions." jsonschema_extras:"order=7"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	UpdateDelay string `json:"updateDelay,omitempty" jsonschema:"title=Update Delay,description=Potentially reduce active warehouse time by increasing the delay between updates. Defaults to 30 minutes if unset.,enum=0s,enum=15m,enum=30m,enum=1h,enum=2h,enum=4h"`
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI(tenant string) string {
	// Build a DSN connection string.
	var configCopy = c.asSnowflakeConfig(tenant)
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

func (c *config) asSnowflakeConfig(tenant string) sf.Config {
	var maxStatementCount string = "0"
	var json string = "json"
	return sf.Config{
		Account:     c.Account,
		Host:        c.Host,
		User:        c.User,
		Password:    c.Password,
		Database:    c.Database,
		Schema:      c.Schema,
		Warehouse:   c.Warehouse,
		Role:        c.Role,
		Application: fmt.Sprintf("%s_EstuaryFlow", tenant),
		Params: map[string]*string{
			// By default Snowflake expects the number of statements to be provided
			// with every request. By setting this parameter to zero we are allowing a
			// variable number of statements to be executed in a single request
			"MULTI_STATEMENT_COUNT":  &maxStatementCount,
			"GO_QUERY_RESULT_FORMAT": &json,
		},
	}
}

var hostRe = regexp.MustCompile(`(?i)^.+.snowflakecomputing\.com$`)

func validHost(h string) error {
	hasProtocol := strings.Contains(h, "://")
	missingDomain := !hostRe.MatchString(h)

	if hasProtocol && missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com and not include a protocol)", h)
	} else if hasProtocol {
		return fmt.Errorf("invalid host %q (must not include a protocol)", h)
	} else if missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com)", h)
	}

	return nil
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

	if _, err := sql.ParseDelay(c.Advanced.UpdateDelay); err != nil {
		return err
	}

	return validHost(c.Host)
}

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

			log.WithFields(log.Fields{
				"host":     parsed.Host,
				"user":     parsed.User,
				"database": parsed.Database,
				"schema":   parsed.Schema,
				"tenant":   tenant,
			}).Info("opening Snowflake")

			var metaBase sql.TablePath
			var metaSpecs, _ = sql.MetaTables(metaBase)

			var dialect = snowflakeDialect(parsed.Schema)
			var templates = renderTemplates(dialect)

			return &sql.Endpoint{
				Config:    parsed,
				Dialect:   dialect,
				MetaSpecs: &metaSpecs,
				// Snowflake does not use the checkpoint table, instead we use the recovery log
				// as the authoritative checkpoint and idempotent apply pattern
				MetaCheckpoints:      nil,
				NewClient:            newClient,
				CreateTableTemplate:  templates["createTargetTable"],
				ReplaceTableTemplate: templates["replaceTargetTable"],
				NewResource:          newTableConfig,
				NewTransactor:        newTransactor,
				Tenant:               tenant,
				ConcurrentApply:      true,
			}, nil
		},
	}
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
	cp          checkpoint
}

func (d *transactor) UnmarshalState(state json.RawMessage) error {
	var cp checkpoint
	if err := json.Unmarshal(state, &cp); err != nil {
		return err
	}
	d.cp = cp

	return nil
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
) (_ m.Transactor, err error) {
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
	target sql.Table
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
	Table *sql.Table
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var d = new(binding)
	var err error
	d.target = target

	d.load.stage = newStagedFile(os.TempDir())
	d.store.stage = newStagedFile(os.TempDir())

	if d.load.loadQuery, err = sql.RenderTableTemplate(target, t.templates["loadQuery"]); err != nil {
		return fmt.Errorf("loadQuery template: %w", err)
	}
	if d.store.copyInto, err = sql.RenderTableTemplate(target, t.templates["copyInto"]); err != nil {
		return fmt.Errorf("copyInto template: %w", err)
	}
	if d.store.mergeInto, err = sql.RenderTableTemplate(target, t.templates["mergeInto"]); err != nil {
		return fmt.Errorf("mergeInto template: %w", err)
	}

	t.bindings = append(t.bindings, d)
	return nil
}

const MaxConcurrentLoads = 5

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
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
	var toDelete []string
	for i, b := range d.bindings {
		if !b.load.stage.started {
			// Pass.
		} else if dir, err := b.load.stage.flush(); err != nil {
			return fmt.Errorf("load.stage(): %w", err)
		} else {
			subqueries[i] = renderWithDir(b.load.loadQuery, dir)
			toDelete = append(toDelete, dir)
		}
	}
	defer d.deleteFiles(ctx, toDelete)

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

// TODO: use stateKey for the checkpoint
type checkpoint struct {
	// Map of table name to query list
	Queries map[string]string

	// List of files to cleanup after committing the queries of a table
	ToDelete map[string]string
}

func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	d.store.round++

	log.Info("store: starting encoding and uploading of files")
	for it.Next() {
		var b = d.bindings[it.Binding]

		if it.Exists {
			b.store.mustMerge = true
		}

		if err := b.store.stage.start(it.Context(), d.db); err != nil {
			return nil, err
		} else if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return nil, fmt.Errorf("converting Store: %w", err)
		} else if err = b.store.stage.encodeRow(converted); err != nil {
			return nil, fmt.Errorf("encoding Store to scratch file: %w", err)
		}
	}

	// Upload the staged files and build a list of merge and copy into queries that need to be run
	// to effectively commit the files into destination tables. These queries are stored
	// in the checkpoint so that if the connector is restarted in middle of a commit
	// it can run the same queries on the next startup. This is the pattern for
	// recovery log being authoritative and the connector idempotently applies a commit
	// These are keyed on the binding table name so that in case of a recovery being necessary
	// we don't run queries belonging to bindings that have been removed
	var queries = make(map[string]string)
	// all files uploaded across bindings
	var toDelete = make(map[string]string)

	log.Info("store: starting copying of files into tables")
	for _, b := range d.bindings {
		if !b.store.stage.started {
			continue
		}

		dir, err := b.store.stage.flush()
		if err != nil {
			return nil, err
		}

		if b.store.mustMerge {
			queries[b.target.Identifier] = renderWithDir(b.store.mergeInto, dir)
		} else {
			queries[b.target.Identifier] = renderWithDir(b.store.copyInto, dir)
		}

		toDelete[b.target.Identifier] = dir
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var cp = checkpoint{Queries: queries, ToDelete: toDelete}
		d.cp = cp

		var checkpointJSON, err = json.Marshal(cp)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON, MergePatch: true}, nil
	}, nil
}

func renderWithDir(tpl string, dir string) string {
	return fmt.Sprintf(tpl, dir)
}

// Acknowledge merges data from temporary table to main table
func (d *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	var asyncCtx = sf.WithAsyncMode(ctx)
	log.Info("store: starting committing changes")

	// Run the queries using AsyncMode, which means that `ExecContext` will not block
	// until the query is successful, rather we will store the results of these queries
	// in a map so that we can then call `RowsAffected` on them, blocking until
	// the queries are actually executed and done
	var results = make(map[string]stdsql.Result)
	for table, query := range d.cp.Queries {
		// during recovery we skip queries that belong to tables which do not have a binding anymore
		// since these tables might be deleted already
		if !d.hasTableBinding(table) {
			continue
		}

		log.WithField("table", table).Info("store: starting query")
		if result, err := d.store.conn.ExecContext(asyncCtx, query); err != nil {
			return nil, fmt.Errorf("query %q failed: %w", query, err)
		} else {
			results[table] = result
		}
	}

	for table, r := range results {
		if _, err := r.RowsAffected(); err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}
		log.WithField("table", table).Info("store: finished query")
	}

	for table, toDelete := range d.cp.ToDelete {
		// during recovery we skip queries that belong to tables which do not have a binding anymore
		// since these tables might be deleted already
		if !d.hasTableBinding(table) {
			continue
		}

		d.deleteFiles(ctx, []string{toDelete})
	}

	log.Info("store: finished committing changes")

	// After having applied the checkpoint, we try to clean up the checkpoint in the ack response
	// so that a restart of the connector does not need to run the same queries again
	// Note that this is an best-effort "attempt" and there is no guarantee that this checkpoint update
	// can actually be committed
	// Important to note that in this case we do not reset the checkpoint for all bindings, but only the ones
	// that have been committed in this transaction. The reason is that it may be the case that a binding
	// which has been disabled right after a failed attempt to run its queries, must be able to recover by enabling
	// the binding and running the queries that are pending for its last transaction.
	var checkpointClear = checkpoint{Queries: make(map[string]string), ToDelete: make(map[string]string)}
	for _, b := range d.bindings {
		checkpointClear.Queries[b.target.Identifier] = ""
		checkpointClear.ToDelete[b.target.Identifier] = ""
	}
	var checkpointJSON, err = json.Marshal(checkpointClear)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint clearing json: %w", err)
	}
	return &pf.ConnectorState{UpdatedJson: json.RawMessage(checkpointJSON), MergePatch: true}, nil
}

func (d *transactor) hasTableBinding(tableIdentifier string) bool {
	for _, b := range d.bindings {
		if b.target.Identifier == tableIdentifier {
			return true
		}
	}

	return false
}

func (d *transactor) deleteFiles(ctx context.Context, files []string) {
	for _, f := range files {
		if _, err := d.store.conn.ExecContext(ctx, fmt.Sprintf("REMOVE %s", f)); err != nil {
			log.WithFields(log.Fields{
				"file": f,
				"err":  err,
			}).Debug("deleteFiles failed")
		}
	}
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
