package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
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
				MetaCheckpoints:     nil,
				NewClient:           newClient,
				CreateTableTemplate: templates.createTargetTable,
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
	cfg        *config
	db         *stdsql.DB
	pipeClient *PipeClient
	// Variables exclusively used by Load.
	load struct {
		conn *stdsql.Conn
	}
	// Variables exclusively used by Store.
	store struct {
		conn  *stdsql.Conn
		fence *sql.Fence
	}
	templates   templates
	bindings    []*binding
	updateDelay time.Duration
	cp          checkpoint
}

func (t *transactor) AckDelay() time.Duration {
	return t.updateDelay
}

func (d *transactor) UnmarshalState(state json.RawMessage) error {
	if err := json.Unmarshal(state, &d.cp); err != nil {
		return err
	}
	// TODO: remove after migration
	for _, item := range d.cp {
		if len(item.StagedDir) == 0 {
			log.WithField("state", string(state)).Info("found old checkpoint format, ignoring the checkpoint and starting clean")
			d.cp = make(checkpoint)
			break
		}
	}

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

	var pipeClient *PipeClient

	if cfg.Credentials.AuthType == JWT {
		pipeClient, err = NewPipeClient(cfg, ep.Tenant)
		if err != nil {
			return nil, fmt.Errorf("NewPipeCLient: %w", err)
		}
	}

	var d = &transactor{
		cfg:        cfg,
		templates:  renderTemplates(dialect),
		db:         db,
		pipeClient: pipeClient,
	}

	if d.updateDelay, err = m.ParseDelay(cfg.Advanced.UpdateDelay); err != nil {
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
	pipeName string
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

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var d = new(binding)
	d.target = target

	d.load.stage = newStagedFile(os.TempDir())
	d.store.stage = newStagedFile(os.TempDir())

	// If this is a delta updates binding and we are using JWT auth type, this binding
	// can use snowpipe
	if target.DeltaUpdates && t.cfg.Credentials.AuthType == JWT {
		if pipeName, err := sql.RenderTableTemplate(target, t.templates.pipeName); err != nil {
			return fmt.Errorf("pipeName template: %w", err)
		} else {
			d.pipeName = fmt.Sprintf("%s.%s.%s", t.cfg.Database, t.cfg.Schema, strings.ToUpper(strings.Trim(pipeName, "`")))
		}

		if createPipe, err := sql.RenderTableTemplate(target, t.templates.pipeName); err != nil {
			return fmt.Errorf("createPipe template: %w", err)
		} else if _, err := t.db.ExecContext(ctx, createPipe); err != nil {
			return fmt.Errorf("creating pipe for table %q: %w", target.Path, err)
		} else {
			log.WithField("q", createPipe).Info("creating pipe")
		}
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
		} else if subqueries[i], err = RenderTableAndFileTemplate(tableAndFile{Table: b.target, File: dir}, d.templates.loadQuery); err != nil {
			return fmt.Errorf("loadQuery template: %w", err)
		} else {
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

type checkpointItem struct {
	Table     string
	Query     string
	StagedDir string
	PipeName  string
	PipeFiles []fileRecord
}

type checkpoint = map[string]*checkpointItem

func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
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
	for _, b := range d.bindings {
		if !b.store.stage.started {
			continue
		}

		dir, err := b.store.stage.flush()
		if err != nil {
			return nil, err
		}

		if b.store.mustMerge {
			if mergeIntoQuery, err := RenderTableAndFileTemplate(tableAndFile{Table: b.target, File: dir}, d.templates.mergeInto); err != nil {
				return nil, fmt.Errorf("mergeInto template: %w", err)
			} else {
				d.cp[b.target.StateKey] = &checkpointItem{
					Table:     b.target.Identifier,
					Query:     mergeIntoQuery,
					StagedDir: dir,
				}
			}
		} else if b.pipeName != "" {
			d.cp[b.target.StateKey] = &checkpointItem{
				Table:     b.target.Identifier,
				StagedDir: dir,
				PipeFiles: b.store.stage.uploaded,
				PipeName:  b.pipeName,
			}
		} else {
			if copyIntoQuery, err := RenderTableAndFileTemplate(tableAndFile{Table: b.target, File: dir}, d.templates.copyInto); err != nil {
				return nil, fmt.Errorf("copyInto template: %w", err)
			} else {
				d.cp[b.target.StateKey] = &checkpointItem{
					Table:     b.target.Identifier,
					Query:     copyIntoQuery,
					StagedDir: dir,
				}
			}
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var checkpointJSON, err = json.Marshal(d.cp)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON, MergePatch: true}, nil
	}, nil
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
	for stateKey, item := range d.cp {
		// we skip queries that belong to tables which do not have a binding anymore
		// since these tables might be deleted already
		if !d.hasStateKey(stateKey) {
			continue
		}

		if len(item.Query) > 0 {
			log.WithField("table", item.Table).Info("store: starting query")
			if result, err := d.store.conn.ExecContext(asyncCtx, item.Query); err != nil {
				return nil, fmt.Errorf("query %q failed: %w", item.Query, err)
			} else {
				results[stateKey] = result
			}
		} else if len(item.PipeFiles) > 0 {
			var fileRequests = make([]FileRequest, len(item.PipeFiles))
			for i, f := range item.PipeFiles {
				fileRequests[i] = FileRequest{
					Path: "/" + f.Path,
					Size: f.Size,
				}
			}

			if resp, err := d.pipeClient.InsertFiles(item.PipeName, fileRequests); err != nil {
				return nil, fmt.Errorf("snowpipe insertFiles: %w", err)
			} else if report, err := d.pipeClient.InsertReport(item.PipeName, ""); err != nil {
				return nil, fmt.Errorf("snowpipe insertReports: %w", err)
			} else {
				// TODO: make me DEBUG
				log.WithField("response", resp).Info(fmt.Sprintf("insertFiles sucesssful %+v", report))
			}
		}
	}

	for stateKey, r := range results {
		var item = d.cp[stateKey]
		if _, err := r.RowsAffected(); err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}
		log.WithField("table", item.Table).Info("store: finished query")

		d.deleteFiles(ctx, []string{item.StagedDir})
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
	var checkpointClear = make(checkpoint)
	for _, b := range d.bindings {
		checkpointClear[b.target.StateKey] = nil
		delete(d.cp, b.target.StateKey)
	}
	var checkpointJSON, err = json.Marshal(checkpointClear)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint clearing json: %w", err)
	}

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
	d.load.conn.Close()
	d.store.conn.Close()
	d.db.Close()
}

func main() {
	// gosnowflake also uses logrus for logging and the logs it produces may be confusing when
	// intermixed with our connector logs. We disable the gosnowflake logger here and log as needed
	// when handling errors from the sql driver.
	sf.GetLogger().SetOutput(io.Discard)
	boilerplate.RunMain(newSnowflakeDriver())
}
