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
	Schema string `json:"schema,omitempty" jsonschema:"title=Schema,description=Schema where the table resides" jsonschema_extras:"x-schema-name=true"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false." jsonschema_extras:"x-delta-updates=true"`
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
		StartTunnel:      func(ctx context.Context, conf any) error { return nil },
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
		PreReqs: preReqs,
	}
}

type transactor struct {
	cfg              *config
	cp               checkpoint
	cpRecovery       bool // is this checkpoint a recovered checkpoint?
	wsClient         *databricks.WorkspaceClient
	localStagingPath string
	bindings         []*binding
	be               *boilerplate.BindingEvents
}

func (d *transactor) UnmarshalState(state json.RawMessage) error {
	if err := pf.UnmarshalStrict(state, &d.cp); err != nil {
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
	is *boilerplate.InfoSchema,
	be *boilerplate.BindingEvents,
) (_ m.Transactor, _ *boilerplate.MaterializeOptions, err error) {
	var cfg = ep.Config.(*config)

	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:               fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:              cfg.Credentials.PersonalAccessToken,
		Credentials:        dbConfig.PatCredentials{}, // enforce PAT auth
		HTTPTimeoutSeconds: 5 * 60,                    // This is necessary for file uploads as they can sometimes take longer than the default 60s
	})
	if err != nil {
		return nil, nil, fmt.Errorf("initialising workspace client: %w", err)
	}

	var d = &transactor{cfg: cfg, wsClient: wsClient, be: be}

	var httpPathSplit = strings.Split(cfg.HTTPPath, "/")
	var warehouseId = httpPathSplit[len(httpPathSplit)-1]

	db, err := stdsql.Open("databricks", d.cfg.ToURI())
	if err != nil {
		return nil, nil, fmt.Errorf("sql.Open: %w", err)
	}
	defer db.Close()

	schemas := map[string]struct{}{cfg.SchemaName: {}}
	for _, binding := range bindings {
		if err = d.addBinding(binding); err != nil {
			return nil, nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}

		schemas[binding.Path[0]] = struct{}{}
	}

	// Create a volume for storing staged files for all schemas in the scope of the materialization.
	for sch := range schemas {
		log.WithFields(log.Fields{
			"schema":     sch,
			"volumeName": volumeName,
		}).Debug("creating volume for schema if it doesn't already exist")
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE VOLUME IF NOT EXISTS `%s`.`%s`;", sch, volumeName)); err != nil {
			return nil, nil, fmt.Errorf("Exec(CREATE VOLUME IF NOT EXISTS `%s`.`%s`;): %w", sch, volumeName, err)
		}
	}

	if tempDir, err := os.MkdirTemp("", "staging"); err != nil {
		return nil, nil, err
	} else {
		d.localStagingPath = tempDir
	}

	opts := &boilerplate.MaterializeOptions{
		ExtendedLogging: true,
		AckSchedule: &boilerplate.AckScheduleOption{
			Config: cfg.Schedule,
			Jitter: []byte(warehouseId),
		},
		DBTJobTrigger: &cfg.DBTJobTrigger,
	}

	return d, opts, nil
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
}

func (t *transactor) addBinding(target sql.Table) error {
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

type minMax struct {
	Min any
	Max any
}

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	var bindingMinMaxKeys = make(map[string]minMax)
	for it.Next() {
		var b = d.bindings[it.Binding]

		if err := b.loadFile.start(ctx); err != nil {
			return fmt.Errorf("starting load file: %w", err)
		}

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err := b.loadFile.encodeRow(converted); err != nil {
			return fmt.Errorf("encoding row for load: %w", err)
		} else {
			bindingName := b.target.Identifier
			if bmx, ok := bindingMinMaxKeys[bindingName]; !ok {
				bindingMinMaxKeys[bindingName] = minMax{
					Min: converted[0],
					Max: converted[0],
				}
			} else {
				switch v := converted[0].(type) {
				case int64:
					if v < bmx.Min.(int64) {
						bmx.Min = v
					}
					if v > bmx.Max.(int64) {
						bmx.Max = v
					}
				case uint64:
					if v < bmx.Min.(uint64) {
						bmx.Min = v
					}
					if v > bmx.Max.(uint64) {
						bmx.Max = v
					}
				case float64:
					if v < bmx.Min.(float64) {
						bmx.Min = v
					}
					if v > bmx.Max.(float64) {
						bmx.Max = v
					}
				case string:
					if v < bmx.Min.(string) {
						bmx.Min = v
					}
					if v > bmx.Max.(string) {
						bmx.Max = v
					}
				}
			}
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
	//defer d.deleteFiles(ctx, toDelete)

	if it.Err() != nil {
		return it.Err()
	}

	if len(queries) > 0 {
		db, err := stdsql.Open("databricks", d.cfg.ToURI())
		if err != nil {
			return fmt.Errorf("sql.Open: %w", err)
		}
		defer db.Close()

		// Issue a union join of the target tables and their (now staged) load keys,
		// and send results to the |loaded| callback.
		d.be.StartedEvaluatingLoads()
		var unionQuery = strings.Join(queries, "\nUNION ALL\n")
		rows, err := db.QueryContext(ctx, unionQuery)
		if err != nil {
			return fmt.Errorf("querying Load documents: %w", err)
		}
		defer rows.Close()
		d.be.FinishedEvaluatingLoads()

		var loadedCounts = make(map[string]int)
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

			bindingName := d.bindings[binding].target.Identifier
			loadedCounts[bindingName] = loadedCounts[bindingName] + 1
		}
		log.WithFields(log.Fields{"loadQuery": unionQuery, "loaded": loadedCounts, "minMaxKeys": bindingMinMaxKeys, "fullPaths": toDelete}).Info("load")

		if err = rows.Err(); err != nil {
			return fmt.Errorf("querying Loads: %w", err)
		}
	}

	return nil
}

type checkpointItem struct {
	Query    string   `json:",omitempty"` // deprecated, kept for backward compatibility
	Queries  []string `json:",omitempty"`
	ToDelete []string
}

type checkpoint map[string]*checkpointItem

func (c *checkpoint) Validate() error {
	return nil
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

const queryBatchSize = 128

func (d *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
	ctx := it.Context()

	for it.Next() {
		var b = d.bindings[it.Binding]

		var flowDocument = it.RawJSON
		if d.cfg.HardDelete && it.Delete {
			if it.Exists {
				flowDocument = json.RawMessage(`"delete"`)
			} else {
				// Ignore items which do not exist and are already deleted
				continue
			}
		}

		if err := b.storeFile.start(ctx); err != nil {
			return nil, err
		} else if converted, err := b.target.ConvertAll(it.Key, it.Values, flowDocument); err != nil {
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

		var queries []string
		// In case of delta updates or if there are no existing keys being stored
		// we directly copy from staged files into the target table. Note that this is retriable
		// given that COPY INTO is idempotent by default: files that have already been loaded into a table will
		// not be loaded again
		// see https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
		if b.target.DeltaUpdates || !b.needsMerge {
			// TODO: switch to slices.Chunk once we switch to go1.23
			for i := 0; i < len(toCopy); i += queryBatchSize {
				end := i + queryBatchSize
				if end > len(toCopy) {
					end = len(toCopy)
				}

				if query, err := RenderTableWithFiles(b.target, toCopy[i:end], b.rootStagingPath, tplCopyIntoDirect); err != nil {
					return nil, fmt.Errorf("copyIntoDirect template: %w", err)
				} else {
					queries = append(queries, query)
				}
			}
		} else {
			for i := 0; i < len(toCopy); i += queryBatchSize {
				end := i + queryBatchSize
				if end > len(toCopy) {
					end = len(toCopy)
				}
				if query, err := RenderTableWithFiles(b.target, fullPaths[i:end], b.rootStagingPath, tplMergeInto); err != nil {
					return nil, fmt.Errorf("mergeInto template: %w", err)
				} else {
					queries = append(queries, query)
				}
			}
		}

		d.cp[b.target.StateKey] = &checkpointItem{
			Queries:  queries,
			ToDelete: fullPaths,
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var checkpointJSON, err = json.Marshal(d.cp)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
	}, nil
}

func (d *transactor) checkForDuplicates(ctx context.Context, db *stdsql.DB, stateKey string, item *checkpointItem, logMessage string) error {
	var binding = d.bindingForStateKey(stateKey)
	var keyStrings []string
	for _, k := range binding.target.Keys {
		keyStrings = append(keyStrings, k.Identifier)
	}
	var duplicatesQuery = fmt.Sprintf(`select %s from %s group by %s having count(*) > 1`, strings.Join(keyStrings, ","), binding.target.Identifier, strings.Join(keyStrings, ","))
	rows, err := db.QueryContext(ctx, duplicatesQuery)
	if err != nil {
		return fmt.Errorf("finding %q duplicates: %w", duplicatesQuery, err)
	}
	defer rows.Close()
	var duplicates [][]any
	for rows.Next() {
		var row = make([]any, len(binding.target.Keys))

		var rowReferences = make([]any, len(row))
		for i := 0; i < len(row); i++ {
			rowReferences[i] = &row[i]
		}

		if err := rows.Scan(rowReferences...); err != nil {
			return fmt.Errorf("scanning duplicates: %w", err)
		}

		duplicates = append(duplicates, row)
	}

	if len(duplicates) > 0 {
		log.WithFields(log.Fields{"query": item.Queries, "fullPaths": item.ToDelete, "duplicates": duplicates, "recovery": d.cpRecovery}).Info(logMessage)
	}

	return nil
}

// Acknowledge merges data from temporary table to main table
// TODO: run these queries concurrently for improved performance
func (d *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
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
		path := d.pathForStateKey(stateKey)
		// we skip queries that belong to tables which do not have a binding anymore
		// since these tables might be deleted already
		if len(path) == 0 {
			continue
		}

		if err := d.checkForDuplicates(ctx, db, stateKey, item, "before running query"); err != nil {
			return nil, err
		}

		var queries = item.Queries
		if item.Query != "" {
			if len(queries) != 0 {
				return nil, fmt.Errorf("checkpoint has both query and queries, this is unexpected")
			}
			queries = []string{item.Query}
		}
		d.be.StartedResourceCommit(path)
		for _, query := range queries {
			if _, err := db.ExecContext(ctx, query); err != nil {
				// When doing a recovery apply, it may be the case that some tables & files have already been deleted after being applied
				// it is okay to skip them in this case
				if d.cpRecovery {
					if strings.Contains(err.Error(), "PATH_NOT_FOUND") || strings.Contains(err.Error(), "Path does not exist") || strings.Contains(err.Error(), "Table doesn't exist") || strings.Contains(err.Error(), "TABLE_OR_VIEW_NOT_FOUND") {
						continue
					}
				}
				return nil, fmt.Errorf("query %q failed: %w", query, err)
			}

		}

		if err := d.checkForDuplicates(ctx, db, stateKey, item, "after running query"); err != nil {
			return nil, err
		}
		d.be.FinishedResourceCommit(path)

		// Cleanup files.
		//d.deleteFiles(ctx, item.ToDelete)
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

	return &pf.ConnectorState{UpdatedJson: json.RawMessage(checkpointJSON), MergePatch: true}, nil
}

func (d *transactor) bindingForStateKey(stateKey string) *binding {
	for _, b := range d.bindings {
		if b.target.StateKey == stateKey {
			return b
		}
	}
	return nil
}

func (d *transactor) pathForStateKey(stateKey string) []string {
	for _, b := range d.bindings {
		if b.target.StateKey == stateKey {
			return b.target.Path
		}
	}
	return nil
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
