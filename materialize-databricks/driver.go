package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/logger"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"

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
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}

	if r.Schema == "" {
		return fmt.Errorf("missing schema")
	}

	var forbiddenChars = ". /"
	if strings.ContainsAny(r.Schema, forbiddenChars) {
		return fmt.Errorf("schema name %q contains one of the forbidden characters %q", r.Schema, forbiddenChars)
	}

	return nil
}

// Databricks does not allow these characters in table names, as well as some other obscure ASCII
// control characters. Ref: https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html
var tableSanitizerRegex = regexp.MustCompile(`[\. \/]`)

func (c tableConfig) Path() sql.TablePath {
	return []string{c.Schema, tableSanitizerRegex.ReplaceAllString(c.Table, "_")}
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
				NewClient:           newClient,
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				Tenant:              tenant,
				ConcurrentApply:     true,
			}, nil
		},
	}
}

var _ m.DelayedCommitter = (*transactor)(nil)

type transactor struct {
	cfg *config

	cp checkpoint
	// is this checkpoint a recovered checkpoint?
	cpRecovery bool
	// Guard for concurrent access for cp.
	mu sync.Mutex

	wsClient *databricks.WorkspaceClient

	localStagingPath string

	bindings []*binding

	updateDelay time.Duration
}

func (d *transactor) UnmarshalState(state json.RawMessage) error {
	// A connector running on the "old" state may not have emitted an ack yet. We don't support
	// migrating states so hopefully this is nothing but an empty checkpoint.
	var oldCp oldCheckpoint
	if err := json.Unmarshal(state, &oldCp); err != nil {
		log.WithField("oldCheckpoint", oldCp).WithError(err).Warn("failed to unmarshal state into oldCheckpoint")
	} else if len(oldCp.Queries) > 0 || len(oldCp.ToDelete) > 0 {
		return fmt.Errorf("UnmarshalState application logic error: oldCp was not empty: %#v", oldCp)
	}

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

	if d.updateDelay, err = m.ParseDelay(cfg.Advanced.UpdateDelay); err != nil {
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

	db, err := stdsql.Open("databricks", d.cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	defer db.Close()

	for _, binding := range bindings {
		if err = d.addBinding(ctx, binding, open.Range); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	// Create volume for storing staged files
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE VOLUME IF NOT EXISTS `%s`.`%s`;", cfg.SchemaName, volumeName)); err != nil {
		return nil, fmt.Errorf("Exec(CREATE VOLUME IF NOT EXISTS %s;): %w", volumeName, err)
	}

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

	mergeInto string

	copyIntoDirect string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, _range *pf.RangeSpec) error {
	var b = &binding{target: target}

	b.rootStagingPath = fmt.Sprintf("/Volumes/%s/%s/%s/flow_temp_tables", t.cfg.CatalogName, target.Path[0], volumeName)

	translatedFieldNames := func(in []string) []string {
		out := make([]string, 0, len(in))
		for _, f := range in {
			out = append(out, translateFlowField(f))
		}
		return out
	}

	b.loadFile = newStagedFile(t.cfg, b.rootStagingPath, translatedFieldNames(target.KeyNames()))
	b.storeFile = newStagedFile(t.cfg, b.rootStagingPath, translatedFieldNames(target.ColumnNames()))

	t.bindings = append(t.bindings, b)

	return nil
}

func (t *transactor) AckDelay() time.Duration {
	return t.updateDelay
}

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

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
	var queries []string
	var toDelete []string
	for idx, b := range d.bindings {
		if !b.loadFile.started {
			continue
		}

		toLoad, err := b.loadFile.flush()
		if err != nil {
			return fmt.Errorf("flushing load file for binding[%d]: %w", idx, err)
		}
		var fullPaths = pathsWithRoot(b.rootStagingPath, toLoad)

		if loadQuery, err := RenderTableWithFiles(b.target, fullPaths, b.rootStagingPath, tplLoadQuery); err != nil {
			return fmt.Errorf("loadQuery template: %w", err)
		} else {
			queries = append(queries, loadQuery)
			toDelete = append(toDelete, fullPaths...)
		}
	}
	defer d.deleteFiles(ctx, toDelete)

	if it.Err() != nil {
		return it.Err()
	}

	log.Info("load: finished upload and copying of files")

	log.Info("load: starting join query")

	if len(queries) > 0 {
		db, err := stdsql.Open("databricks", d.cfg.ToURI())
		if err != nil {
			return fmt.Errorf("sql.Open: %w", err)
		}
		defer db.Close()

		// Issue a union join of the target tables and their (now staged) load keys,
		// and send results to the |loaded| callback.
		var unionQuery = strings.Join(queries, "\nUNION ALL\n")
		rows, err := db.QueryContext(ctx, unionQuery)
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
	}
	log.Info("load: finished join query")

	return nil
}

type checkpointItem struct {
	Query    string
	ToDelete []string
}

type checkpoint map[string]*checkpointItem

// TODO: Remove once all tasks have migrated to using the new checkpoint.
type oldCheckpoint struct {
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
	ctx := it.Context()

	log.Info("store: starting file upload and copies")
	for it.Next() {
		var b = d.bindings[it.Binding]

		if err := b.storeFile.start(ctx); err != nil {
			return nil, err
		} else if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err := b.storeFile.encodeRow(converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		}

		if it.Exists {
			b.needsMerge = true
		}
	}

	// Upload the staged files and build a list of merge and truncate queries that need to be run to
	// effectively commit the files into destination tables. These queries are stored in the
	// checkpoint so that if the connector is restarted in middle of a commit it can run the same
	// queries on the next startup. This is the pattern for recovery log being authoritative and the
	// connector idempotently applies a commit. These are keyed on the binding stateKey so that in
	// case of a recovery being necessary we don't run queries belonging to bindings that have been
	// removed.
	for idx, b := range d.bindings {
		if !b.storeFile.started {
			continue
		}

		toCopy, err := b.storeFile.flush()
		if err != nil {
			return nil, fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
		}
		var fullPaths = pathsWithRoot(b.rootStagingPath, toCopy)

		var query string
		// In case of delta updates or if there are no existing keys being stored
		// we directly copy from staged files into the target table. Note that this is retriable
		// given that COPY INTO is idempotent by default: files that have already been loaded into a table will
		// not be loaded again
		// see https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
		if b.target.DeltaUpdates || !b.needsMerge {
			if query, err = RenderTableWithFiles(b.target, toCopy, b.rootStagingPath, tplCopyIntoDirect); err != nil {
				return nil, fmt.Errorf("copyIntoDirect template: %w", err)
			}
		} else {
			if query, err = RenderTableWithFiles(b.target, fullPaths, b.rootStagingPath, tplMergeInto); err != nil {
				return nil, fmt.Errorf("mergeInto template: %w", err)
			}
		}

		d.cp[b.target.StateKey] = &checkpointItem{
			Query:    query,
			ToDelete: fullPaths,
		}
	}

	log.Info("store: finished file upload and copies")

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var checkpointJSON, err = json.Marshal(d.cp)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
	}, nil
}

// Acknowledge merges data from temporary table to main table
// TODO: run these queries concurrently for improved performance
func (d *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	log.Info("store: starting committing changes")

	var db *stdsql.DB
	if len(d.cp) > 0 {
		var err error
		db, err = stdsql.Open("databricks", d.cfg.ToURI())
		if err != nil {
			return nil, fmt.Errorf("sql.Open: %w", err)
		}
		defer db.Close()
	}

	for stateKey, item := range d.cp {
		// we skip queries that belong to tables which do not have a binding anymore
		// since these tables might be deleted already
		if !d.hasStateKey(stateKey) {
			continue
		}

		if _, err := db.ExecContext(ctx, item.Query); err != nil {
			// When doing a recovery apply, it may be the case that some tables & files have already been deleted after being applied
			// it is okay to skip them in this case
			if d.cpRecovery {
				if strings.Contains(err.Error(), "PATH_NOT_FOUND") || strings.Contains(err.Error(), "Path does not exist") || strings.Contains(err.Error(), "Table doesn't exist") || strings.Contains(err.Error(), "TABLE_OR_VIEW_NOT_FOUND") {
					continue
				}
			}
			return nil, fmt.Errorf("query %q failed: %w", item.Query, err)
		}

		// Cleanup files.
		d.deleteFiles(ctx, item.ToDelete)
	}

	d.cpRecovery = false

	// After having applied the checkpoint, we try to clean up the checkpoint in the ack response
	// so that a restart of the connector does not need to run the same queries again
	// Note that this is an best-effort "attempt" and there is no guarantee that this checkpoint update
	// can actually be committed
	// Important to note that in this case we do not reset the checkpoint for all bindings, but only the ones
	// that have been committed in this transaction. The reason is that it may be the case that a binding
	// which has been disabled right after a failed attempt to run its queries, must be able to recover by enabling
	// the binding and running the queries that are pending for its last transaction.
	var checkpointClear = make(checkpoint)
	for _, b := range d.bindings {
		checkpointClear[b.target.StateKey] = nil
		delete(d.cp, b.target.StateKey)
	}
	var checkpointJSON, err = json.Marshal(checkpointClear)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint clearing json: %w", err)
	}

	log.Info("store: finished committing changes")
	return &pf.ConnectorState{UpdatedJson: json.RawMessage(checkpointJSON), MergePatch: true}, nil
}

func (d *transactor) hasStateKey(stateKey string) bool {
	for _, b := range d.bindings {
		if b.target.StateKey == stateKey {
			return true
		}
	}
	return false
}

func pathsWithRoot(root string, paths []string) []string {
	var newPaths = make([]string, len(paths))
	for i, p := range paths {
		newPaths[i] = filepath.Join(root, p)
	}

	return newPaths
}

func (d *transactor) Destroy() {
}

func main() {
	logger.DefaultLogger = &NoOpLogger{}
	if err := dbsqllog.SetLogLevel("disabled"); err != nil {
		panic(err)
	}

	boilerplate.RunMain(newDatabricksDriver())
}
