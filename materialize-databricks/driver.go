package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/logger"
	driverctx "github.com/databricks/databricks-sql-go/driverctx"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"

	_ "github.com/databricks/databricks-sql-go"
)

const defaultPort = "443"
const volumeName = "flow_staging"

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the table" jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Schema,description=Schema where the table resides"`
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
				Config:               cfg,
				Dialect:              databricksDialect,
				MetaSpecs:            &metaSpecs,
				MetaCheckpoints:      nil,
				NewClient:            newClient,
				CreateTableTemplate:  tplCreateTargetTable,
				ReplaceTableTemplate: tplReplaceTargetTable,
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

	cp checkpoint
	// is this checkpoint a recovered checkpoint?
	cpRecovery bool

	wsClient *databricks.WorkspaceClient

	localStagingPath string

	// Variables exclusively used by Load.
	load struct {
		conn     *stdsql.Conn
		unionSQL string
	}
	// Variables exclusively used by Store.
	store struct {
		conn  *stdsql.Conn
		round int
		// number of documents stored in current transaction
		// set to StoreIterator.Total
		stored int
	}
	bindings []*binding

	updateDelay time.Duration
}

func (t transactor) Context(ctx context.Context) context.Context {
	return driverctx.NewContextWithStagingInfo(ctx, []string{t.localStagingPath})
}

func (d *transactor) UnmarshalState(state json.RawMessage) error {
	if err := json.Unmarshal(state, &d.cp); err != nil {
		return err
	}
	d.cpRecovery = true

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

	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:               fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:              cfg.Credentials.PersonalAccessToken,
		Credentials:        dbConfig.PatCredentials{}, // enforce PAT auth
		HTTPTimeoutSeconds: 5 * 60,                    // This is necessary for file uploads as they can sometimes take longer than the default 60s
	})
	if err != nil {
		return nil, fmt.Errorf("initialising workspace client: %w", err)
	}

	var d = &transactor{cfg: cfg, wsClient: wsClient}

	if d.updateDelay, err = sql.ParseDelay(cfg.Advanced.UpdateDelay); err != nil {
		return nil, err
	}

	// If the warehouse has auto-stop configured to be longer than 15 minutes, disable update delay
	var httpPathSplit = strings.Split(cfg.HTTPPath, "/")
	var warehouseId = httpPathSplit[len(httpPathSplit)-1]
	if res, err := wsClient.Warehouses.GetById(ctx, warehouseId); err != nil {
		return nil, fmt.Errorf("get warehouse %q details: %w", warehouseId, err)
	} else {
		if res.AutoStopMins >= 15 && d.updateDelay.Minutes() > 0 {
			log.Info(fmt.Sprintf("Auto-stop is configured to be %d minutes for this warehouse, disabling update delay. To save costs you can reduce the auto-stop idle configuration and tune the update delay config of this connector. See docs for more information: https://go.estuary.dev/materialize-databricks", res.AutoStopMins))

			d.updateDelay, _ = time.ParseDuration("0s")
		}
	}

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

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
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

func (d *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
	d.store.round++
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
	mtx := sync.Mutex{}

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
			mtx.Lock()
			toDelete = append(toDelete, toDeleteBinding...)
			mtx.Unlock()

			// In case of delta updates or if there are no existing keys being stored
			// we directly copy from staged files into the target table. Note that this is retriable
			// given that COPY INTO is idempotent by default: files that have already been loaded into a table will
			// not be loaded again
			// see https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
			if bindingCopy.target.DeltaUpdates || !bindingCopy.needsMerge {
				mtx.Lock()
				queries = append(queries, renderWithFiles(bindingCopy.copyIntoDirect, toCopy...))
				mtx.Unlock()
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

				mtx.Lock()
				queries = append(queries, renderWithFiles(bindingCopy.mergeInto, toCopy...), bindingCopy.dropStoreSQL)
				mtx.Unlock()
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	log.Info("store: finished file upload and copies")

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var cp = checkpoint{Queries: queries, ToDelete: toDelete}
		d.cp = cp

		var checkpointJSON, err = json.Marshal(cp)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		d.store.stored = it.Total
		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
	}, nil
}

func renderWithFiles(tpl string, files ...string) string {
	var s = strings.Join(files, "','")
	s = "'" + s + "'"

	return fmt.Sprintf(tpl, s)
}

// Acknowledge merges data from temporary table to main table
// TODO: run these queries concurrently for improved performance
func (d *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	return sql.AcknowledgeWithDelay(ctx, d.store.round, d.updateDelay, d.store.stored, func(ctx context.Context) (*pf.ConnectorState, error) {
		log.Info("store: starting committing changes")
		for _, q := range d.cp.Queries {
			if _, err := d.store.conn.ExecContext(ctx, q); err != nil {
				// When doing a recovery apply, it may be the case that some tables & files have already been deleted after being applied
				// it is okay to skip them in this case
				if d.cpRecovery {
					if strings.Contains(err.Error(), "PATH_NOT_FOUND") || strings.Contains(err.Error(), "Path does not exist") || strings.Contains(err.Error(), "Table doesn't exist") || strings.Contains(err.Error(), "TABLE_OR_VIEW_NOT_FOUND") {
						continue
					}
				}
				return nil, fmt.Errorf("query %q failed: %w", q, err)
			}
		}
		log.Info("store: finished committing changes")

		// Cleanup files and tables
		d.deleteFiles(ctx, d.cp.ToDelete)

		d.cpRecovery = false

		return nil, nil
	})
}

func (d *transactor) Destroy() {
	d.load.conn.Close()
	d.store.conn.Close()
}

func main() {
	logger.DefaultLogger = &NoOpLogger{}
	if err := dbsqllog.SetLogLevel("disabled"); err != nil {
		panic(err)
	}

	boilerplate.RunMain(newDatabricksDriver())
}
