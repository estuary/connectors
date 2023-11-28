package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	_ "github.com/databricks/databricks-sql-go"
	driverctx "github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
)

const defaultPort = "443"
const volumeName = "flow_staging"

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the table" jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema" jsonschema:"title=Schema,description=Schema where the table resides,default=default"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false."`
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{Schema: ep.Config.(*config).SchemaName}
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	var forbiddenChars = ". /"
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	if strings.ContainsAny(r.Table, forbiddenChars) {
		return fmt.Errorf("table name %q contains one of the forbidden characters %q", r.Table, forbiddenChars)
	}

	if r.Schema == "" {
		return fmt.Errorf("missing schema")
	}
	if strings.ContainsAny(r.Schema, forbiddenChars) {
		return fmt.Errorf("schema name %q contains one of the forbidden characters %q", r.Schema, forbiddenChars)
	}

	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.Schema, c.Table}
}

func (c tableConfig) GetAdditionalSql() string {
	return ""
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newDatabricksDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-databricks",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("parsing endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"address": cfg.Address,
				"path":    cfg.HTTPPath,
				"catalog": cfg.CatalogName,
			}).Info("connecting to databricks")

			var metaBase sql.TablePath = []string{cfg.SchemaName}
			var metaSpecs, _ = sql.MetaTables(metaBase)

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             databricksDialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     nil,
				Client:              client{uri: cfg.ToURI()},
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				Tenant:              tenant,
			}, nil
		},
	}
}

type client struct {
	uri string
}

func (c client) Apply(ctx context.Context, ep *sql.Endpoint, actions sql.ApplyActions, updateSpec sql.MetaSpecsUpdate, dryRun bool) (string, error) {
	cfg := ep.Config.(*config)

	db, err := stdsql.Open("databricks", c.uri)
	if err != nil {
		return "", err
	}
	defer db.Close()

	resolved, err := sql.ResolveActions(ctx, db, actions, databricksDialect, cfg.CatalogName)
	if err != nil {
		return "", fmt.Errorf("resolving apply actions: %w", err)
	}

	// Build up the list of actions for logging. These won't be executed directly, since Databricks
	// apparently doesn't support multi-statement queries or dropping nullability for multiple
	// columns in a single statement, and throws errors on concurrent table updates.
	actionList := []string{}
	for _, tc := range resolved.CreateTables {
		actionList = append(actionList, tc.TableCreateSql)
	}
	for _, ta := range resolved.AlterTables {
		if len(ta.AddColumns) > 0 {
			var addColumnsStmt strings.Builder
			if err := tplAlterTableColumns.Execute(&addColumnsStmt, ta); err != nil {
				return "", fmt.Errorf("rendering alter table columns statement: %w", err)
			}
			actionList = append(actionList, addColumnsStmt.String())
		}
		for _, dn := range ta.DropNotNulls {
			actionList = append(actionList, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
				ta.Identifier,
				dn.Identifier,
			))
		}
	}
	action := strings.Join(append(actionList, updateSpec.QueryString), "\n")
	if dryRun {
		return action, nil
	}

	// Execute actions for each table involved separately. Concurrent actions can't be done on the
	// same table without throwing errors, so each goroutine handles the actions only for that
	// table, looping over them if needed until they are done. This is going to look pretty similar
	// to building the actions list, except this time we are actually running the queries.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, tc := range resolved.CreateTables {
		tc := tc
		group.Go(func() error {
			if _, err := db.ExecContext(groupCtx, tc.TableCreateSql); err != nil {
				return fmt.Errorf("executing table create statement: %w", err)
			}
			return nil
		})
	}

	for _, ta := range resolved.AlterTables {
		ta := ta

		group.Go(func() error {
			if len(ta.AddColumns) > 0 {
				var addColumnsStmt strings.Builder
				if err := tplAlterTableColumns.Execute(&addColumnsStmt, ta); err != nil {
					return fmt.Errorf("rendering alter table columns statement: %w", err)
				}
				if _, err := db.ExecContext(groupCtx, addColumnsStmt.String()); err != nil {
					return fmt.Errorf("executing table add columns statement: %w", err)
				}
			}
			for _, dn := range ta.DropNotNulls {
				q := fmt.Sprintf(
					"ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
					ta.Identifier,
					dn.Identifier,
				)
				if _, err := db.ExecContext(groupCtx, q); err != nil {
					return fmt.Errorf("executing table drop not null statement: %w", err)
				}
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return "", err
	}

	// Once all the table actions are done, we can update the stored spec.
	if _, err := db.ExecContext(ctx, updateSpec.QueryString); err != nil {
		return "", fmt.Errorf("executing spec update statement: %w", err)
	}

	return action, nil
}

func (c client) PreReqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	errs := &sql.PrereqErr{}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then. Note that this should be long enough to allow
	// for an automatically shut down instance to be started up again
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if db, err := stdsql.Open("databricks", c.uri); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var execErr dbsqlerr.DBExecutionError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &execErr) {
			// See https://pkg.go.dev/github.com/databricks/databricks-sql-go/errors#pkg-constants
			// and https://docs.databricks.com/en/error-messages/index.html
			switch execErr.SqlState() {
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

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	err = c.withDB(func(db *stdsql.DB) error {
		// Fail-fast: surface a connection issue.
		if err = db.PingContext(ctx); err != nil {
			return fmt.Errorf("connecting to DB: %w", err)
		}
		err = db.QueryRowContext(
			ctx,
			fmt.Sprintf(
				"SELECT version, spec FROM %s WHERE materialization = %s;",
				specs.Identifier,
				databricksDialect.Literal(materialization.String()),
			),
		).Scan(&version, &specB64)

		return err
	})
	return
}

// ExecStatements is used for the DDL statements of ApplyUpsert and ApplyDelete.
func (c client) ExecStatements(ctx context.Context, statements []string) error {
	return c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements) })
}

func (c client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.Fence{}, nil
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	var db, err = stdsql.Open("databricks", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

type transactor struct {
	cfg *config

	wsClient *databricks.WorkspaceClient

	localStagingPath string

	// Variables exclusively used by Load.
	load struct {
		conn     *stdsql.Conn
		unionSQL string
	}
	// Variables exclusively used by Store.
	store struct {
		conn *stdsql.Conn
	}
	bindings []*binding
}

func (t transactor) Context(ctx context.Context) context.Context {
	return driverctx.NewContextWithStagingInfo(ctx, []string{t.localStagingPath})
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
) (_ pm.Transactor, err error) {
	var cfg = ep.Config.(*config)

	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:       cfg.Credentials.PersonalAccessToken,
		Credentials: dbConfig.PatCredentials{}, // enforce PAT auth
	})
	if err != nil {
		return nil, fmt.Errorf("initialising workspace client: %w", err)
	}

	var d = &transactor{cfg: cfg, wsClient: wsClient}

	// Establish connections.
	if db, err := stdsql.Open("databricks", cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("load sql.Open: %w", err)
	} else if d.load.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("load db.Conn: %w", err)
	}
	if db, err := stdsql.Open("databricks", cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("store sql.Open: %w", err)
	} else if d.store.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("store db.Conn: %w", err)
	}

	for _, binding := range bindings {
		if err = d.addBinding(ctx, binding, open.Range); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	// Create volume for storing staged files
	if _, err := d.store.conn.ExecContext(ctx, fmt.Sprintf("CREATE VOLUME IF NOT EXISTS %s.%s;", cfg.SchemaName, volumeName)); err != nil {
		return nil, fmt.Errorf("Exec(CREATE VOLUME IF NOT EXISTS %s;): %w", volumeName, err)
	}

	var cp checkpoint
	if open.StateJson != nil {
		if err := json.Unmarshal(open.StateJson, &cp); err != nil {
			return nil, fmt.Errorf("parsing driver config: %w", err)
		}
	}

	if err = d.applyCheckpoint(ctx, cp, true); err != nil {
		return nil, fmt.Errorf("applying recovered checkpoint: %w", err)
	}

	// Build a query which unions the results of each load subquery.
	// TODO: we can build this query per-transaction so that tables that do not
	// need to be loaded (such as delta updates tables) are not part of the query
	// furthermore we can run these load queries concurrently for each binding to
	// speed things up
	var subqueries []string
	for _, b := range d.bindings {
		subqueries = append(subqueries, b.loadQuerySQL)
	}
	d.load.unionSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

	if tempDir, err := os.MkdirTemp("", "staging"); err != nil {
		return nil, err
	} else {
		d.localStagingPath = tempDir
	}

	return d, nil
}

type binding struct {
	target sql.Table

	// path to where we store staging files
	rootStagingPath string

	loadFile  *stagedFile
	storeFile *stagedFile

	// a binding needs to be merged if there are updates to existing documents
	// otherwise we just do a direct copy by moving all data from temporary table
	// into the target table. Note that in case of delta updates, "needsMerge"
	// will always be false
	needsMerge bool

	createLoadTableSQL string
	truncateLoadSQL    string
	dropLoadSQL        string
	loadQuerySQL       string

	createStoreTableSQL string
	truncateStoreSQL    string
	dropStoreSQL        string

	mergeInto string

	copyIntoDirect string
	copyIntoLoad   string
	copyIntoStore  string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, _range *pf.RangeSpec) error {
	var b = &binding{target: target}

	b.rootStagingPath = fmt.Sprintf("/Volumes/%s/%s/%s/flow_temp_tables", t.cfg.CatalogName, target.Path[0], volumeName)
	b.loadFile = newStagedFile(t.wsClient.Files, b.rootStagingPath, target.KeyNames())
	b.storeFile = newStagedFile(t.wsClient.Files, b.rootStagingPath, target.ColumnNames())

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, tplCreateLoadTable},
		{&b.createStoreTableSQL, tplCreateStoreTable},
		{&b.copyIntoDirect, tplCopyIntoDirect},
		{&b.copyIntoStore, tplCopyIntoStore},
		{&b.copyIntoLoad, tplCopyIntoLoad},
		{&b.loadQuerySQL, tplLoadQuery},
		{&b.truncateLoadSQL, tplTruncateLoad},
		{&b.truncateStoreSQL, tplTruncateStore},
		{&b.dropLoadSQL, tplDropLoad},
		{&b.dropStoreSQL, tplDropStore},
		{&b.mergeInto, tplMergeInto},
	} {
		var err error
		var shardRange = fmt.Sprintf("%08x_%08x", _range.KeyBegin, _range.RClockBegin)
		if *m.sql, err = RenderTable(target, b.rootStagingPath, shardRange, m.tpl); err != nil {
			return err
		}
	}

	t.bindings = append(t.bindings, b)

	// Drop existing temp tables
	if _, err := t.load.conn.ExecContext(ctx, b.dropLoadSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.dropLoadSQL, err)
	}
	if _, err := t.store.conn.ExecContext(ctx, b.dropStoreSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.dropStoreSQL, err)
	}

	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = d.Context(it.Context())

	for _, b := range d.bindings {
		// Create a binding-scoped temporary table for staged keys to load.
		if _, err := d.load.conn.ExecContext(ctx, b.createLoadTableSQL); err != nil {
			return fmt.Errorf("Exec(%s): %w", b.createLoadTableSQL, err)
		}
	}

	log.Info("load: starting upload and copying of files")
	for it.Next() {
		var b = d.bindings[it.Binding]

		b.loadFile.start(ctx)

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err := b.loadFile.encodeRow(converted); err != nil {
			return fmt.Errorf("encoding row for load: %w", err)
		}
	}

	// Copy the staged files in parallel
	// TODO: we may want to limit the concurrency here, but we don't yet know a good
	// value for tuning this
	group, groupCtx := errgroup.WithContext(ctx)
	for idx, b := range d.bindings {
		if !b.loadFile.started {
			continue
		}

		// This is due to a bug in Golang, see https://go.dev/blog/loopvar-preview
		var bindingCopy = b
		var idxCopy = idx
		group.Go(func() error {
			toCopy, toDelete, err := bindingCopy.loadFile.flush()
			if err != nil {
				return fmt.Errorf("flushing load file for binding[%d]: %w", idxCopy, err)
			}
			defer d.deleteFiles(groupCtx, toDelete)

			// COPY INTO temporary load table from staged files
			if _, err := d.load.conn.ExecContext(groupCtx, renderWithFiles(bindingCopy.copyIntoLoad, toCopy...)); err != nil {
				return fmt.Errorf("load: writing keys: %w", err)
			}

			return nil
		})
	}

	if it.Err() != nil {
		return it.Err()
	}

	if err := group.Wait(); err != nil {
		return err
	}

	log.Info("load: finished upload and copying of files")

	log.Info("load: starting join query")
	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := d.load.conn.QueryContext(ctx, d.load.unionSQL)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document string

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if binding > -1 {
			if err = loaded(binding, json.RawMessage([]byte(document))); err != nil {
				return err
			}
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}
	log.Info("load: finished join query")

	for _, b := range d.bindings {
		if _, err = d.load.conn.ExecContext(ctx, b.dropLoadSQL); err != nil {
			return fmt.Errorf("dropping load table: %w", err)
		}
	}

	return nil
}

type checkpoint struct {
	Queries []string

	// List of files to cleanup after committing the queries
	ToDelete []string
}

func (d *transactor) deleteFiles(ctx context.Context, files []string) {
	for _, f := range files {
		if err := d.wsClient.Files.DeleteByFilePath(ctx, f); err != nil {
			log.WithFields(log.Fields{
				"file": f,
				"err":  err,
			}).Debug("deleteFiles failed")
		}
	}
}

func (d *transactor) Store(it *pm.StoreIterator) (_ pm.StartCommitFunc, err error) {
	ctx := it.Context()

	log.Info("store: starting file upload and copies")
	for it.Next() {
		var b = d.bindings[it.Binding]
		b.storeFile.start(ctx)

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		if err := b.storeFile.encodeRow(converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		}

		if it.Exists {
			b.needsMerge = true
		}
	}

	// Upload the staged files and build a list of merge and truncate queries that need to be run
	// to effectively commit the files into destination tables. These queries are stored
	// in the checkpoint so that if the connector is restarted in middle of a commit
	// it can run the same queries on the next startup. This is the pattern for
	// recovery log being authoritative and the connector idempotently applies a commit
	var queries []string
	// all files uploaded across bindings
	var toDelete []string
	// mutex to ensure safety of updating these variables
	m := sync.Mutex{}

	// we run these processes in parallel
	group, groupCtx := errgroup.WithContext(ctx)
	for idx, b := range d.bindings {
		if !b.storeFile.started {
			continue
		}

		var bindingCopy = b
		var idxCopy = idx
		group.Go(func() error {
			toCopy, toDeleteBinding, err := bindingCopy.storeFile.flush()
			if err != nil {
				return fmt.Errorf("flushing store file for binding[%d]: %w", idxCopy, err)
			}
			m.Lock()
			toDelete = append(toDelete, toDeleteBinding...)
			m.Unlock()

			// In case of delta updates or if there are no existing keys being stored
			// we directly copy from staged files into the target table. Note that this is retriable
			// given that COPY INTO is idempotent by default: files that have already been loaded into a table will
			// not be loaded again
			// see https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
			if bindingCopy.target.DeltaUpdates || !bindingCopy.needsMerge {
				m.Lock()
				queries = append(queries, renderWithFiles(bindingCopy.copyIntoDirect, toCopy...))
				m.Unlock()
			} else {
				// Create a binding-scoped temporary table for store documents to be merged
				// into target table
				if _, err := d.store.conn.ExecContext(groupCtx, bindingCopy.createStoreTableSQL); err != nil {
					return fmt.Errorf("Exec(%s): %w", bindingCopy.createStoreTableSQL, err)
				}

				// COPY INTO temporary load table from staged files
				if _, err := d.store.conn.ExecContext(groupCtx, renderWithFiles(bindingCopy.copyIntoStore, toCopy...)); err != nil {
					return fmt.Errorf("store: copying into to temporary table: %w", err)
				}

				m.Lock()
				queries = append(queries, renderWithFiles(bindingCopy.mergeInto, toCopy...), bindingCopy.dropStoreSQL)
				m.Unlock()
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	log.Info("store: finished file upload and copies")

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		var cp = checkpoint{Queries: queries, ToDelete: toDelete}

		var checkpointJSON, err = json.Marshal(cp)
		if err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}
		var commitOp = pf.RunAsyncOperation(func() error {
			select {
			case <-runtimeAckCh:
				return d.applyCheckpoint(ctx, cp, false)
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, commitOp
	}, nil
}

func renderWithFiles(tpl string, files ...string) string {
	var s = strings.Join(files, "','")
	s = "'" + s + "'"

	return fmt.Sprintf(tpl, s)
}

// applyCheckpoint merges data from temporary table to main table
// TODO: run these queries concurrently for improved performance
func (d *transactor) applyCheckpoint(ctx context.Context, cp checkpoint, recovery bool) error {
	log.Info("store: starting committing changes")
	for _, q := range cp.Queries {
		if _, err := d.store.conn.ExecContext(ctx, q); err != nil {
			// When doing a recovery apply, it may be the case that some tables & files have already been deleted after being applied
			// it is okay to skip them in this case
			if recovery {
				if strings.Contains(err.Error(), "PATH_NOT_FOUND") || strings.Contains(err.Error(), "Path does not exist") || strings.Contains(err.Error(), "Table doesn't exist") || strings.Contains(err.Error(), "TABLE_OR_VIEW_NOT_FOUND") {
					continue
				}
			}
			return fmt.Errorf("query %q failed: %w", q, err)
		}
	}
	log.Info("store: finished committing changes")

	// Cleanup files and tables
	d.deleteFiles(ctx, cp.ToDelete)

	return nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close()
	d.store.conn.Close()
}

func main() {
	boilerplate.RunMain(newDatabricksDriver())
}
