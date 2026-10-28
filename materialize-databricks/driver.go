package connector

import (
	"bytes"
	"context"
	"crypto/sha256"
	stdsql "database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/logger"
	"github.com/databricks/databricks-sdk-go/service/files"
	"github.com/databricks/databricks-sdk-go/useragent"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"

	_ "github.com/databricks/databricks-sql-go"
)

const defaultPort = "443"
const volumeName = "flow_staging"

// deleteConcurrency is how many Files API deletes a cleanup runs at once.
const deleteConcurrency = 16

// maxConcurrentCommits is how many bindings commit at once.
const maxConcurrentCommits = 5

// maxConcurrentLoads is how many bindings' load queries run at once.
const maxConcurrentLoads = 5

// maxBoundRanges is the most ranges a date-time key's merge bound splits into.
const maxBoundRanges = 8

const sweepAge = 24 * time.Hour

type tableConfig struct {
	Table         string `json:"table" jsonschema:"title=Table,description=Name of the table" jsonschema_extras:"x-collection-name=true"`
	Schema        string `json:"schema,omitempty" jsonschema:"title=Schema,description=Schema where the table resides" jsonschema_extras:"x-schema-name=true"`
	Delta         bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false." jsonschema_extras:"x-delta-updates=true,nonsensitive=true"`
	AdditionalSql string `json:"additional_table_create_sql,omitempty" jsonschema:"title=Additional Table Create SQL,description=Additional SQL statement(s) to be run after table is created." jsonschema_extras:"multiline=true"`
}

func (c tableConfig) WithDefaults(cfg config) tableConfig {
	if c.Schema == "" {
		c.Schema = cfg.SchemaName
	}
	return c
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
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

func (c tableConfig) Parameters() ([]string, bool, error) {
	return []string{c.Schema, tableSanitizerRegex.ReplaceAllString(c.Table, "_")}, c.Delta, nil
}

// NewDriver builds the Databricks materialization driver, and is the only entry
// point into this package. The driver's operations assume an order of
// initialization that it establishes itself — the endpoint configuration is
// validated before a client is built, for instance — so callers are given the
// assembled driver rather than its pieces.
func NewDriver() *sql.Driver[config, tableConfig] {
	useragent.WithProduct(productGlobalDescription.name, productGlobalDescription.version)

	// The Databricks SQL driver logs copiously at INFO, which is noise in the
	// connector's own logs. Both settings it needs are package-level globals of
	// the SDK, so this is done here to apply to every user of the driver rather
	// than only when running as a connector.
	if log.GetLevel() != log.DebugLevel {
		logger.DefaultLogger = &NoOpLogger{}
		if err := dbsqllog.SetLogLevel("disabled"); err != nil {
			panic(err)
		}
	}

	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-databricks",
		StartTunnel:      func(ctx context.Context, cfg config) error { return nil },
		NewEndpoint: func(ctx context.Context, cfg config, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			log.WithFields(log.Fields{
				"address": cfg.Address,
				"path":    cfg.HTTPPath,
				"catalog": cfg.CatalogName,
			}).Info("connecting to databricks")

			var httpPathSplit = strings.Split(cfg.HTTPPath, "/")
			var warehouseId = httpPathSplit[len(httpPathSplit)-1]

			dialect := createDatabricksDialect(featureFlags)

			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             dialect,
				MetaCheckpoints:     nil,
				NewClient:           newClient,
				CreateTableTemplate: renderTemplates(dialect).createTargetTable,
				NewTransactor:       newTransactor,
				ConcurrentApply:     true,
				NoFlowDocument:      cfg.Advanced.NoFlowDocument,
				KeyChangeInPlace:    true,
				Options: m.MaterializeOptions{
					ExtendedLogging: true,
					AckSchedule: &m.AckScheduleOption{
						Config: cfg.Schedule,
						Jitter: []byte(warehouseId),
					},
					DBTJobTrigger: &cfg.DBTJobTrigger,
				},
			}, nil
		},
		PreReqs: preReqs,
	}
}

var _ m.Transactor = (*transactor)(nil)

type transactor struct {
	runtimeCheckpoint m.RuntimeCheckpoint
	cfg               config
	// Every shard's pending entries; non-primary shards hold only their own.
	cp connectorState
	// Pending entries in the layouts of earlier connector versions: range-first
	// buckets, and flat items under legacyRangeKey. Only the primary shard
	// tracks and executes these.
	// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
	peerShardsCheckpoints rangeCheckpoints
	cpRecovery            bool // is this checkpoint a recovered checkpoint?
	primary               bool // does this shard's range begin at key 0?
	rangeKey              string
	wsClient              *databricks.WorkspaceClient
	files                 files.FilesInterface
	localStagingPath      string
	bindings              []*binding
	be                    *m.BindingEvents
	ep                    *sql.Endpoint[config]
	templates             templates
	materializationName   string
	stagingPrefix         string
	cleanups              sync.WaitGroup
	sweepOnce             sync.Once
	profiler              *queryProfiler
	// twoPhaseLoads uses two queries to prune files: one that finds files
	// where the rows matching the keys exist on, and another to read
	// flow_document from those specific files.
	twoPhaseLoads bool
}

func (d *transactor) RecoverCheckpoint(_ context.Context, _ pf.MaterializationSpec, _ pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return d.runtimeCheckpoint, nil
}

func (d *transactor) UnmarshalState(state json.RawMessage) error {
	if !d.primary {
		// Non-primary shards recover nothing: the primary replays the entire
		// consolidated state document. If a non-primary shard retained and
		// later re-emitted recovered entries, the primary would re-execute
		// them after their staged files were already deleted, outside of the
		// recovery tolerance for missing files.
		return nil
	}

	if err := d.absorbState(state, false); err != nil {
		return err
	}
	d.cpRecovery = true

	return nil
}

// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
func (d *transactor) legacyBucket(rangeKey string) checkpoint {
	if d.peerShardsCheckpoints[rangeKey] == nil {
		d.peerShardsCheckpoints[rangeKey] = make(checkpoint)
	}
	return d.peerShardsCheckpoints[rangeKey]
}

// absorbState folds a state document into the pending entries. Earlier
// connector versions wrote {rangeKey: {stateKey: item}}, and before that flat
// {stateKey: item}, which can share a state key with a current bucket: the
// bucket's non-range keys are then the flat item's fields. skipOwnRange drops
// this shard's own entries, which the runtime echoes back in peer patches.
func (d *transactor) absorbState(doc json.RawMessage, skipOwnRange bool) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(doc, &raw); err != nil {
		return err
	}

	for key, val := range raw {
		var bucket map[string]json.RawMessage
		if err := json.Unmarshal(val, &bucket); err != nil {
			return fmt.Errorf("parsing checkpoint entry %q: %w", key, err)
		}

		// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
		if rangeKeyRe.MatchString(key) {
			for stateKey, itemRaw := range bucket {
				if item, err := parseCheckpointItem(itemRaw); err != nil {
					return fmt.Errorf("parsing checkpoint entry %q of %q: %w", stateKey, key, err)
				} else {
					d.legacyBucket(key)[stateKey] = item
				}
			}
			continue
		}

		var flat = make(map[string]json.RawMessage)
		for rangeKey, itemRaw := range bucket {
			// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
			if !rangeKeyRe.MatchString(rangeKey) {
				flat[rangeKey] = itemRaw
			} else if skipOwnRange && rangeKey == d.rangeKey {
				continue
			} else if item, err := parseCheckpointItem(itemRaw); err != nil {
				return fmt.Errorf("parsing checkpoint entry %q of %q: %w", rangeKey, key, err)
			} else {
				d.cp.add(key, rangeKey, item)
			}
		}
		// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
		if len(flat) > 0 {
			var flatRaw, _ = json.Marshal(flat)
			if item, err := parseCheckpointItem(flatRaw); err != nil {
				return fmt.Errorf("parsing checkpoint entry %q: %w", key, err)
			} else {
				d.legacyBucket(legacyRangeKey)[key] = item
			}
		}
	}

	return nil
}

// mergePeerStatePatches folds the aggregated StartedCommit state patches of
// all task shards into the primary's bookkeeping, so that Acknowledge
// executes the queries staged by every shard of the just-committed
// transaction. Under the v1 runtime `patches` is always empty.
func (d *transactor) mergePeerStatePatches(patches []json.RawMessage) error {
	if !d.primary {
		return nil
	}

	for _, patch := range patches {
		if isJSONNull(patch) {
			// The runtime encodes a full-replace (non-merge-patch) state
			// update as a literal null reset patch followed by the new state
			// document. No shard emits full replacements, so a reset means a
			// peer (e.g. one running an older connector image) just clobbered
			// the consolidated state.
			return fmt.Errorf("unexpected state reset patch in aggregated shard state")
		}

		if err := d.absorbState(patch, true); err != nil {
			return fmt.Errorf("parsing aggregated state patch: %w", err)
		}
	}

	return nil
}

func newTransactor(
	ctx context.Context,
	materializationName string,
	featureFlags map[string]bool,
	ep *sql.Endpoint[config],
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
	is *boilerplate.InfoSchema,
	be *m.BindingEvents,
) (m.Transactor, error) {
	var cfg = ep.Config

	wsConfig := cfg.workspaceConfig()
	wsConfig.HTTPTimeoutSeconds = 5 * 60 // This is necessary for file uploads as they can sometimes take longer than the default 60s
	wsClient, err := databricks.NewWorkspaceClient(wsConfig)
	if err != nil {
		return nil, fmt.Errorf("initialising workspace client: %w", err)
	}

	var keyBegin, keyEnd uint32 = 0, math.MaxUint32
	if open.Range != nil {
		keyBegin, keyEnd = open.Range.KeyBegin, open.Range.KeyEnd
	}

	var d = &transactor{
		runtimeCheckpoint:     fence.Checkpoint,
		cfg:                   cfg,
		cp:                    make(connectorState),
		peerShardsCheckpoints: make(rangeCheckpoints),
		primary:               keyBegin == 0,
		rangeKey:              fmt.Sprintf("%08x-%08x", keyBegin, keyEnd),
		wsClient:              wsClient,
		files:                 wsClient.Files,
		be:                    be,
		ep:                    ep,
		templates:             renderTemplates(ep.Dialect),
		stagingPrefix:         stagingPrefix(materializationName),
		twoPhaseLoads:         featureFlags["two_phase_loads"],
	}
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	schemas := map[string]struct{}{cfg.SchemaName: {}}
	for _, binding := range bindings {
		if err = d.addBinding(binding); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
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
			// Concurrent shards of a scaled-out task can race Unity Catalog's
			// IF NOT EXISTS check.
			if strings.Contains(strings.ToLower(err.Error()), "already exists") {
				log.WithFields(log.Fields{"schema": sch}).Debug("volume was concurrently created")
			} else {
				return nil, fmt.Errorf("Exec(CREATE VOLUME IF NOT EXISTS `%s`.`%s`;): %w", sch, volumeName, err)
			}
		}
	}

	if tempDir, err := os.MkdirTemp("", "staging"); err != nil {
		return nil, err
	} else {
		d.localStagingPath = tempDir
	}
	d.profiler = newQueryProfiler(wsClient.QueryHistory)

	return d, nil
}

func (t *transactor) openDB() (*stdsql.DB, error) {
	if db, err := stdsql.Open("databricks", t.cfg.ToURI(t.materializationName)); err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	} else {
		return db, nil
	}
}

type binding struct {
	target            sql.Table
	nullFieldsToStrip []string

	// path to where we store staging files
	rootStagingPath string

	loadFile  *stagedFile
	storeFile *stagedFile
	// schema of load and store files rendered as a DDL and
	// passed to Databricks `read_files`
	loadSchema  string
	storeSchema string

	// stagingTable is the Delta table of this transaction's store rows,
	// relative to rootStagingPath. It's set only when the rows are merged.
	stagingTable string

	// a binding needs to be merged if there are updates to existing documents
	// otherwise we just do a direct copy by moving all data from temporary table
	// into the target table. Note that in case of delta updates, "needsMerge"
	// will always be false
	needsMerge bool

	loadMergeBounds  *sql.MergeBoundsBuilder
	storeMergeBounds *sql.MergeBoundsBuilder
}

// isTimestampColumn reports whether a date-time key is a TIMESTAMP column,
// which parses any RFC3339 form of a bound literal to the same instant. A
// STRING column, which datetime_keys_as_string selects, compares values as
// text, where equal instants in different forms don't sort together.
func isTimestampColumn(col sql.Column) bool {
	return col.BareDDL == "TIMESTAMP"
}

func (t *transactor) addBinding(target sql.Table) error {
	var b = &binding{target: target}

	if t.cfg.Advanced.NoFlowDocument {
		b.nullFieldsToStrip = target.NullableFieldsToStrip()
	}

	b.rootStagingPath = fmt.Sprintf("/Volumes/%s/%s/%s/flow_temp_tables", t.cfg.CatalogName, target.Path[0], volumeName)

	translatedFieldNames := func(in []string) []string {
		out := make([]string, 0, len(in))
		for _, f := range in {
			out = append(out, translateFlowField(f))
		}
		return out
	}

	b.loadFile = newStagedFile(t.cfg, b.rootStagingPath, t.stagingPrefix, translatedFieldNames(target.KeyNames()), t.files)
	b.storeFile = newStagedFile(t.cfg, b.rootStagingPath, t.stagingPrefix, append(translatedFieldNames(target.ColumnNames()), "_flow_delete"), t.files)
	b.loadSchema = stagedSchemaDDL(target.KeyPtrs(), false)
	b.storeSchema = stagedSchemaDDL(target.Columns(), true)
	b.loadMergeBounds = sql.NewMergeBoundsBuilder(target.Keys, t.ep.Dialect.Literal, sql.WithExactDatetimeBounds(isTimestampColumn), sql.WithDatetimeRanges(maxBoundRanges))
	b.storeMergeBounds = sql.NewMergeBoundsBuilder(target.Keys, t.ep.Dialect.Literal, sql.WithExactDatetimeBounds(isTimestampColumn), sql.WithDatetimeRanges(maxBoundRanges))

	t.bindings = append(t.bindings, b)

	return nil
}

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()
	db, err := d.openDB()
	if err != nil {
		return err
	}
	defer db.Close()

	var keyCounts = make([]int, len(d.bindings))
	for it.Next() {
		var b = d.bindings[it.Binding]
		keyCounts[it.Binding]++

		if err := b.loadFile.start(ctx, db); err != nil {
			return fmt.Errorf("starting load file: %w", err)
		}

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err := b.loadFile.writeRow(converted); err != nil {
			return fmt.Errorf("writing row for load: %w", err)
		} else {
			b.loadMergeBounds.NextKey(converted)
		}
	}

	var loadTemplate = d.templates.loadQuery
	if d.cfg.Advanced.NoFlowDocument {
		loadTemplate = d.templates.loadQueryNoFlowDocument
	}

	var loads []bindingLoad
	var toDelete []string
	for idx, b := range d.bindings {
		if !b.loadFile.started {
			continue
		}

		if err := b.loadFile.wait(); err != nil {
			return fmt.Errorf("flushing load file for binding[%d]: %w", idx, err)
		}

		var bounds = b.loadMergeBounds.Build()
		loads = append(loads, bindingLoad{
			data: &tableWithFiles{Table: &b.target, Directories: []string{b.loadFile.remoteDir()}, Schema: b.loadSchema, Bounds: bounds},
			fields: log.Fields{
				"resourcePath": b.target.Path,
				"keys":         keyCounts[idx],
				"bounds":       profileBounds(bounds),
			},
		})
		toDelete = append(toDelete, b.loadFile.remoteDir())
	}
	defer d.cleanUp(ctx, toDelete, nil)

	if it.Err() != nil {
		return it.Err()
	}

	if len(loads) == 0 {
		return nil
	}

	// Each binding's join of its target table and (now staged) load keys runs
	// as its own query, so the warehouse can run them at once. One goroutine
	// sends their documents, since Loaded responses are written in sequence.
	var loadCtx, cancel = context.WithCancel(ctx)
	defer cancel()
	var docs = make(chan loadedDoc)
	var sendErr = make(chan error, 1)
	go func() {
		var err = d.sendLoaded(docs, loaded)
		if err != nil {
			cancel()
		}
		sendErr <- err
	}()

	d.be.StartedEvaluatingLoads()
	var evaluating atomic.Int32
	evaluating.Store(int32(len(loads)))

	var group, groupCtx = errgroup.WithContext(loadCtx)
	group.SetLimit(maxConcurrentLoads)
	for _, load := range loads {
		group.Go(func() (err error) {
			var finishEvaluating = sync.OnceFunc(func() {
				if evaluating.Add(-1) == 0 {
					d.be.FinishedEvaluatingLoads()
				}
			})
			defer finishEvaluating()

			if d.twoPhaseLoads {
				if matched, err := d.matchLoadFiles(groupCtx, db, load); err != nil {
					return err
				} else if !matched {
					return nil
				}
			}
			query, err := renderTemplate(loadTemplate, load.data)
			if err != nil {
				return fmt.Errorf("loadQuery template: %w", err)
			}

			var queryCtx, trace = d.profiler.trace(groupCtx, "load", load.fields)
			var executeTook time.Duration
			var docCount, docBytes int
			defer func() {
				trace.finish(err, log.Fields{"executeTook": executeTook.String(), "docs": docCount, "docBytes": docBytes})
			}()

			rows, err := db.QueryContext(queryCtx, query)
			executeTook = time.Since(trace.started)
			finishEvaluating()
			if err != nil {
				return fmt.Errorf("querying Load documents: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var doc loadedDoc
				if err := rows.Scan(&doc.binding, &doc.document); err != nil {
					return fmt.Errorf("scanning Load document: %w", err)
				} else if doc.binding < 0 {
					continue
				}
				docCount, docBytes = docCount+1, docBytes+len(doc.document)
				select {
				case docs <- doc:
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("querying Loads: %w", err)
			}
			return nil
		})
	}

	var queryErr = group.Wait()
	close(docs)
	// A failure to send cancels the queries, so it's the cause of theirs.
	if err := <-sendErr; err != nil {
		return err
	}
	return queryErr
}

// bindingLoad is the load query parameters of a binding, with the fields of
// its query profile.
type bindingLoad struct {
	data   *tableWithFiles
	fields log.Fields
}

// maxMatchedFiles is the maximum number of files a two-phase load filters on.
// A load that matches more, will omit the filter altogether, reading all the
// files. With usually 256 MB to 1 GB per table file in Databricks, 10,000
// files is about 2.5 to 10 TB of table. Each file path is about 280 bytes so
// the longest list is about 2.9 MB that takes about 3s to compile.
const maxMatchedFiles = 10000

// matchLoadFiles is the first phase of a two-phase load. It pins the load to
// the table's current version, so that both phases read the same files, and
// restricts it to the files holding rows that match its keys, found by reading
// only the key columns. It reports whether any rows matched.
func (d *transactor) matchLoadFiles(ctx context.Context, db *stdsql.DB, load bindingLoad) (bool, error) {
	version, err := tableVersion(ctx, db, load.data.Table.Identifier)
	if err != nil {
		return false, err
	}
	load.data.Version = strconv.FormatInt(version, 10)
	load.fields["version"] = version

	query, err := renderTemplate(d.templates.loadMatchFiles, load.data)
	if err != nil {
		return false, fmt.Errorf("loadMatchFiles template: %w", err)
	}
	var queryCtx, trace = d.profiler.trace(ctx, "loadMatch", load.fields)
	files, err := queryStrings(queryCtx, db, query)
	trace.finish(err, log.Fields{"matchedFiles": len(files)})
	if err != nil {
		return false, fmt.Errorf("matching Load files: %w", err)
	}

	load.fields["matchedFiles"] = len(files)
	if len(files) <= maxMatchedFiles {
		load.data.MatchedFiles = files
	}
	return len(files) > 0, nil
}

// tableVersion is the latest version of a Delta table.
func tableVersion(ctx context.Context, db *stdsql.DB, identifier string) (int64, error) {
	var latest struct {
		Version int64 `db:"version"`
	}
	if err := sqlx.NewDb(db, "databricks").Unsafe().GetContext(ctx, &latest, "DESCRIBE HISTORY "+identifier+" LIMIT 1"); err != nil {
		return 0, fmt.Errorf("describing history of %s: %w", identifier, err)
	}
	return latest.Version, nil
}

// queryStrings runs a query and returns the values of its first column.
func queryStrings(ctx context.Context, db *stdsql.DB, query string) ([]string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, rows.Err()
}

// loadedDoc is a document read by a binding's load query.
type loadedDoc struct {
	binding  int
	document string
}

// sendLoaded sends each of docs as a Loaded response.
func (d *transactor) sendLoaded(docs <-chan loadedDoc, loaded func(int, json.RawMessage) error) error {
	for doc := range docs {
		var raw = json.RawMessage(doc.document)
		if b := d.bindings[doc.binding]; len(b.nullFieldsToStrip) > 0 {
			var err error
			if raw, err = sql.StripNullFields(raw, b.nullFieldsToStrip); err != nil {
				return fmt.Errorf("stripping null fields: %w", err)
			}
		}
		if err := loaded(doc.binding, raw); err != nil {
			return err
		}
	}
	return nil
}

// checkpointItem is one binding's staged-but-not-yet-committed work: the files
// uploaded to the staging volume and everything needed to commit them into the
// target table.
type checkpointItem struct {
	// TODO: Remove these deprecated state keys
	Query   string   `json:",omitempty"` // deprecated, kept for backward compatibility
	Queries []string `json:",omitempty"` // deprecated, kept for backward compatibility
	// ToDelete and StagedFiles are the files of an entry written before staged
	// files had a directory per transaction: full paths, and names relative to
	// the binding's staging root.
	// TODO: remove in about January 2027, with the root-level file path.
	ToDelete    []string
	StagedFiles []string `json:",omitempty"`

	// Directory is the transaction's staging directory, relative to the
	// binding's staging root.
	Directory string `json:",omitempty"`
	// StagingTable is the Delta table holding the rows of Directory, relative
	// to the binding's staging root.
	StagingTable string `json:",omitempty"`
	// Bounds are the rendered merge-bound literals of the observed key range,
	// positional with the target table's key columns.
	Bounds []mergeBoundLiterals `json:",omitempty"`
	// NeedsMerge marks entries whose files must MERGE into the target table;
	// otherwise a direct COPY INTO suffices.
	NeedsMerge bool `json:",omitempty"`

	// round is the transaction round this session staged the entry in, for
	// attributing its commit's row stats. Recovered and peer entries are
	// reported outside of round pairing, so theirs is left zero.
	round int
}

// mergeBoundLiterals is the serialized form of a key column's sql.MergeBound:
// the rendered literals of the minimum and maximum key values observed for a
// transaction. Empty literals mean the column carries no bound, as is the case
// for boolean keys.
type mergeBoundLiterals struct {
	Lower string `json:",omitempty"`
	Upper string `json:",omitempty"`
	// Ranges are the sql.MergeBound's ranges, if its values split at gaps.
	Ranges []mergeRangeLiterals `json:",omitempty"`
}

type mergeRangeLiterals struct {
	Lower string
	Upper string
}

func boundsLiterals(bounds []sql.MergeBound) []mergeBoundLiterals {
	var out = make([]mergeBoundLiterals, len(bounds))
	for i, b := range bounds {
		out[i] = mergeBoundLiterals{Lower: b.LiteralLower, Upper: b.LiteralUpper}
		for _, r := range b.Ranges {
			out[i].Ranges = append(out[i].Ranges, mergeRangeLiterals{Lower: r.LiteralLower, Upper: r.LiteralUpper})
		}
	}
	return out
}

type checkpoint map[string]*checkpointItem

func (c *checkpoint) Validate() error {
	return nil
}

// connectorState is the persisted layout: entries by binding state key, then
// by the range key ("%08x-%08x" of key_begin-key_end) of the shard that staged
// them. Range keys are disjoint across shards, so their StartedCommit merge
// patches commute when the runtime consolidates connector state.
type connectorState map[string]checkpoint

func (s connectorState) add(stateKey, rangeKey string, item *checkpointItem) {
	if s[stateKey] == nil {
		s[stateKey] = make(checkpoint)
	}
	s[stateKey][rangeKey] = item
}

// rangeCheckpoints is the range-first layout of earlier connector versions.
// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
type rangeCheckpoints map[string]checkpoint

// legacyRangeKey is the in-memory bucket of flat per-stateKey entries written
// before range scoping. A current bucket may since have been merged onto the
// same state key, so these clear by nulling the item's fields, not the key.
// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
const legacyRangeKey = ""

// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
var legacyItemClear = map[string]any{
	"Query": nil, "Queries": nil, "ToDelete": nil,
	"StagedFiles": nil, "Bounds": nil, "NeedsMerge": nil,
}

var rangeKeyRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{8}$`)

func unmarshalStrict(data json.RawMessage, v any) error {
	var dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func isJSONNull(data json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(data), []byte("null"))
}

func parseCheckpointItem(data json.RawMessage) (*checkpointItem, error) {
	var item checkpointItem
	if err := unmarshalStrict(data, &item); err != nil {
		return nil, err
	}
	return &item, nil
}

// deleteDirectory deletes a staging directory and its contents. The Files API
// deletes one file per request and only deletes empty directories, so the
// tree is listed first, then its files are deleted concurrently, then its
// directories from the deepest up.
func (d *transactor) deleteDirectory(ctx context.Context, dir string) error {
	var paths, dirs []string
	for stack := []string{dir}; len(stack) > 0; {
		var cur = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		dirs = append(dirs, cur)

		entries, err := d.files.ListDirectoryContentsAll(ctx, files.ListDirectoryContentsRequest{DirectoryPath: cur})
		if err != nil {
			log.WithFields(log.Fields{"dir": cur, "err": err}).Debug("listing staging directory failed")
			return err
		}
		for _, entry := range entries {
			if entry.IsDirectory {
				stack = append(stack, entry.Path)
			} else {
				paths = append(paths, entry.Path)
			}
		}
	}

	var group, groupCtx = errgroup.WithContext(ctx)
	group.SetLimit(deleteConcurrency)
	for _, path := range paths {
		group.Go(func() error {
			if err := d.files.DeleteByFilePath(groupCtx, path); err != nil {
				log.WithFields(log.Fields{"file": path, "err": err}).Debug("deleting staged file failed")
				return err
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	for _, cur := range slices.Backward(dirs) {
		if err := d.files.DeleteDirectoryByDirectoryPath(ctx, cur); err != nil {
			log.WithFields(log.Fields{"dir": cur, "err": err}).Debug("deleting staging directory failed")
			return err
		}
	}
	return nil
}

// cleanUp deletes staging directories and files in the background. A later
// sweepStaging deletes anything it leaves behind.
func (d *transactor) cleanUp(ctx context.Context, dirs, files []string) {
	ctx = context.WithoutCancel(ctx)
	d.cleanups.Add(1)
	go func() {
		defer d.cleanups.Done()
		for _, dir := range dirs {
			_ = d.deleteDirectory(ctx, dir)
		}
		d.deleteFiles(ctx, files)
	}()
}

// sweepStaging deletes this task's staging directories that cleanups didn't
// finish: those no pending entry references that are older than sweepAge.
// Younger ones may belong to other shards' transactions still being staged.
func (d *transactor) sweepStaging(ctx context.Context) {
	var referenced = make(map[string]bool)
	var mark = func(cp checkpoint) {
		for _, item := range cp {
			if item != nil {
				referenced[item.Directory], referenced[item.StagingTable] = true, true
			}
		}
	}
	for _, cp := range d.cp {
		mark(cp)
	}
	for _, cp := range d.peerShardsCheckpoints {
		mark(cp)
	}

	var roots = make(map[string]bool)
	for _, b := range d.bindings {
		roots[b.rootStagingPath] = true
	}
	ctx = context.WithoutCancel(ctx)
	go func() {
		for root := range roots {
			entries, err := d.files.ListDirectoryContentsAll(ctx, files.ListDirectoryContentsRequest{DirectoryPath: root})
			if err != nil {
				log.WithFields(log.Fields{"dir": root, "err": err}).Debug("listing staging root failed")
				continue
			}
			var stale []string
			for _, entry := range entries {
				if entry.IsDirectory && !referenced[entry.Name] && stagedBefore(d.stagingPrefix, entry.Name, time.Now().Add(-sweepAge)) {
					stale = append(stale, entry.Path)
				}
			}
			if len(stale) != 0 {
				log.WithFields(log.Fields{"dir": root, "count": len(stale)}).Info("deleting stale staging directories")
				d.cleanUp(ctx, stale, nil)
			}
		}
	}()
}

// stagedBefore reports whether name is a transaction directory or staging
// table of the prefix created before cutoff.
func stagedBefore(prefix, name string, cutoff time.Time) bool {
	var rest, ok = strings.CutPrefix(name, prefix+"_")
	if !ok || len(rest) < 36 {
		return false
	}
	id, err := uuid.Parse(rest[:36])
	if err != nil || id.Version() != 7 {
		return false
	}
	var sec, nsec = id.Time().UnixTime()
	return time.Unix(sec, nsec).Before(cutoff)
}

// stagingPrefix identifies a task's staging directories, since other tasks
// may share the staging volume.
func stagingPrefix(materializationName string) string {
	var sum = sha256.Sum256([]byte(materializationName))
	return hex.EncodeToString(sum[:6])
}

func (d *transactor) deleteFiles(ctx context.Context, files []string) {
	for _, f := range files {
		if err := d.files.DeleteByFilePath(ctx, f); err != nil {
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
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var stagingTableCopies = make(map[int]chan error)
	var copyIntoStaging = func(idx int) {
		var done = make(chan error, 1)
		stagingTableCopies[idx] = done
		go func() { done <- d.copyIntoStagingTable(ctx, db, d.bindings[idx]) }()
	}

	var lastBinding = -1
	// Skip deleted, non-existent documents iff HardDelete is enabled.
	for it.Next(d.cfg.HardDelete) {
		var b = d.bindings[it.Binding]

		// Store requests are ordered by binding, so the previous binding has all of its rows. Its
		// files are uploaded and copied into its staging table while later bindings are written.
		if lastBinding != -1 && lastBinding != it.Binding {
			copyIntoStaging(lastBinding)
		}
		if _, ok := stagingTableCopies[it.Binding]; ok {
			return nil, fmt.Errorf("store requests for binding[%d] are not contiguous", it.Binding)
		}
		lastBinding = it.Binding

		flowDelete := d.cfg.HardDelete && it.Delete
		if err := b.storeFile.start(ctx, db); err != nil {
			return nil, err
		} else if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err := b.storeFile.writeRow(append(converted, flowDelete)); err != nil {
			return nil, fmt.Errorf("writing row for store: %w", err)
		} else {
			b.storeMergeBounds.NextKey(converted[:len(b.target.Keys)])
		}

		if it.Exists {
			b.needsMerge = true
		}
	}
	if lastBinding != -1 {
		copyIntoStaging(lastBinding)
	}

	// Upload the staged files and record in the checkpoint everything needed to commit them into
	// the destination tables: if the connector is restarted in the middle of a commit it can run
	// the same commit on the next startup. This is the pattern for recovery log being
	// authoritative and the connector idempotently applies a commit. These are keyed on the
	// binding stateKey so that in case of a recovery being necessary we don't run queries
	// belonging to bindings that have been removed.
	for idx, b := range d.bindings {
		var done, ok = stagingTableCopies[idx]
		if !ok {
			continue
		} else if err := <-done; err != nil {
			return nil, fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
		}

		// In case of delta updates or if there are no existing keys being stored
		// we directly copy from staged files into the target table. Note that this is retriable
		// given that COPY INTO is idempotent by default: files that have already been loaded into a table will
		// not be loaded again
		// see https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
		d.cp.add(b.target.StateKey, d.rangeKey, &checkpointItem{
			Directory:    b.storeFile.txnDir,
			StagingTable: b.stagingTable,
			Bounds:       boundsLiterals(b.storeMergeBounds.Build()),
			NeedsMerge:   !b.target.DeltaUpdates && b.needsMerge,
			round:        it.Round,
		})
		b.needsMerge, b.stagingTable = false, "" // reset for next round
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		if state, err := d.startCommitState(); err != nil {
			return nil, m.FinishedOperation(err)
		} else {
			return state, nil
		}
	}, nil
}

// copyIntoStagingTable waits for the binding's store files to upload and, if
// they need a MERGE, copies them into a Delta table. The MERGE reads that table rather than
// the JSON files, so Databricks doesn't materialize the files during MERGE time. The table lives
// in the staging volume by path and isn't registered in the catalog.
func (d *transactor) copyIntoStagingTable(ctx context.Context, db *stdsql.DB, b *binding) error {
	if err := b.storeFile.wait(); err != nil {
		return err
	} else if b.target.DeltaUpdates || !b.needsMerge {
		return nil
	}

	var name = b.storeFile.txnDir + "_delta"
	var data = &stagingCopy{Target: &b.target, Identifier: stagingIdentifier(b, name), Directory: b.storeFile.remoteDir()}
	for _, tpl := range []*template.Template{d.templates.createStagingTable, d.templates.copyIntoStaging} {
		query, err := renderTemplate(tpl, data)
		if err != nil {
			return fmt.Errorf("%s template: %w", tpl.Name(), err)
		}
		var queryCtx, trace = d.profiler.trace(ctx, tpl.Name(), log.Fields{"resourcePath": b.target.Path})
		_, err = db.ExecContext(queryCtx, query)
		trace.finish(err, nil)
		if err != nil {
			return fmt.Errorf("%s: %w", tpl.Name(), err)
		}
	}
	b.stagingTable = name
	return nil
}

func stagingIdentifier(b *binding, name string) string {
	return "delta.`" + strings.ReplaceAll(filepath.Join(b.rootStagingPath, name), "`", "``") + "`"
}

func (d *transactor) startCommitState() (*pf.ConnectorState, error) {
	// Emit only this shard's own entries as a merge patch: range keys are
	// disjoint across shards, so concurrent patches never clobber when the
	// runtime consolidates connector state. A single-shard task uses the same
	// format, with one entry covering the full key range.
	var own = make(connectorState)
	for stateKey, bucket := range d.cp {
		if item := bucket[d.rangeKey]; item != nil {
			own.add(stateKey, d.rangeKey, item)
		}
	}

	var patch, err = json.Marshal(own)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint patch json: %w", err)
	}

	return &pf.ConnectorState{UpdatedJson: patch, MergePatch: true}, nil
}

func (d *transactor) Flush(context.Context, map[int]time.Time) error {
	return nil
}

// Acknowledge merges data from temporary table to main table
func (d *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	if err := d.mergePeerStatePatches(statePatches); err != nil {
		return nil, err
	}

	shouldProcess := m.StateKeyFilter(stateKeys)

	if !d.primary {
		// Non-primary shards only stage files: their committed entries are
		// executed by the primary, which observed them via the aggregated
		// state patches. Drop them from local bookkeeping, mirroring the
		// clearing the primary emits.
		for _, b := range d.bindings {
			if shouldProcess(b.target.StateKey) {
				delete(d.cp, b.target.StateKey)
			}
		}
		d.cpRecovery = false
		return nil, nil
	}

	// Opening is lazy (no connection is dialed until a query runs), so an
	// Acknowledge with nothing to execute costs nothing here.
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	state, err := d.acknowledgeApply(ctx, db, shouldProcess)
	if err == nil {
		// Recovered entries are committed by now, so staging directories that no
		// pending entry references can be swept.
		d.sweepOnce.Do(func() { d.sweepStaging(ctx) })
	}
	return state, err
}

// acknowledgeApply executes pending checkpoint entries — this shard's own, and as the primary
// those of peers and prior sessions — and returns the state update clearing them, or nil if
// nothing was executed.
//
// Grouped by state key, because one query over every shard's staged files takes a fraction of
// the time those per-shard queries take back to back.
func (d *transactor) acknowledgeApply(ctx context.Context, db *stdsql.DB, shouldProcess func(string) bool) (*pf.ConnectorState, error) {
	// The clearing state update, nulling each executed entry at the state-
	// document path it occupies. Clearing is a best-effort attempt to spare a
	// restart from running the same queries again; there is no guarantee this
	// checkpoint update can actually be committed.
	var clear = make(map[string]any)
	var clearAt = func(key string) map[string]any {
		if clear[key] == nil {
			clear[key] = make(map[string]any)
		}
		return clear[key].(map[string]any)
	}
	// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
	var peerRangeKeys = slices.Sorted(maps.Keys(d.peerShardsCheckpoints))

	// Everything else pending — entries of other state keys, and entries whose
	// table no longer has a binding (it might be deleted already) — is left
	// untouched, remaining pending in the persisted state.
	//
	// Bindings commit concurrently, since each operates on its own table.
	var group, groupCtx = errgroup.WithContext(ctx)
	group.SetLimit(maxConcurrentCommits)
	var committed []string
	for _, b := range d.bindings {
		var sk = b.target.StateKey
		if !shouldProcess(sk) {
			continue
		}

		// The binding's pending entries: every shard's, then those written by
		// earlier connector versions.
		var items []*checkpointItem
		for _, rk := range slices.Sorted(maps.Keys(d.cp[sk])) {
			items = append(items, d.cp[sk][rk])
		}
		// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
		for _, rk := range peerRangeKeys {
			if item := d.peerShardsCheckpoints[rk][sk]; item != nil {
				items = append(items, item)
			}
		}
		if len(items) == 0 {
			continue
		}

		committed = append(committed, sk)
		group.Go(func() error {
			return d.commitBindingCheckpointItems(groupCtx, db, b, items)
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	for _, sk := range committed {
		for rk := range d.cp[sk] {
			clearAt(sk)[rk] = nil
		}
		delete(d.cp, sk)
		// TODO(#5176): remove in about January 2027, after checkpoints have all migrated
		for rk, cp := range d.peerShardsCheckpoints {
			if cp[sk] != nil {
				delete(cp, sk)
				if rk == legacyRangeKey {
					maps.Copy(clearAt(sk), legacyItemClear)
				} else {
					clearAt(rk)[sk] = nil
				}
			}
			if len(cp) == 0 {
				delete(d.peerShardsCheckpoints, rk)
			}
		}
	}

	d.cpRecovery = false

	if len(clear) == 0 {
		return nil, nil
	}

	checkpointJSON, err := json.Marshal(clear)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint clearing json: %w", err)
	}

	return &pf.ConnectorState{UpdatedJson: json.RawMessage(checkpointJSON), MergePatch: true}, nil
}

// commitBindingCheckpointItems commits one binding's entries. Entries with structured staging
// metadata coalesce into a single set of queries; those written by older connector versions
// have only their pre-rendered queries, so they run individually.
//
// Acknowledge fans-in by the runtime: the runtime waits for all Acknowledged responses from all
// shards before a new transaction begins, and files are already cleaned up once all queries have
// run. Thus a missing file means the whole query has already run, and during recovery that is
// acceptable: we may have run the query but not have been able to clear the checkpoint.
func (d *transactor) commitBindingCheckpointItems(ctx context.Context, db *stdsql.DB, b *binding, items []*checkpointItem) error {
	var coalesce []*checkpointItem
	d.be.StartedResourceCommit(b.target.Path)
	// items[0] is this shard's own entry when it has one.
	var report = func(stats m.RowStats) {
		d.be.ReportRowStats(items[0].round, b.target.Path, stats)
	}
	for _, item := range items {
		if item.Directory != "" || len(item.StagedFiles) > 0 {
			coalesce = append(coalesce, item)
			continue
		}

		var queries = item.Queries
		if item.Query != "" {
			if len(queries) != 0 {
				return fmt.Errorf("checkpoint has both query and queries, this is unexpected")
			}
			queries = []string{item.Query}
		}
		if err := d.execQueries(ctx, db, log.Fields{"resourcePath": b.target.Path}, queries, d.cpRecovery, report); err != nil {
			return err
		}
	}

	if len(coalesce) > 0 {
		var needsMerge bool
		for _, item := range coalesce {
			needsMerge = needsMerge || item.NeedsMerge
		}

		var bounds = combineBounds(b.target.Keys, coalesce)
		queries, err := d.renderCommitQueries(b, coalesce, bounds, needsMerge)
		if err != nil {
			return err
		}
		var profile = log.Fields{"resourcePath": b.target.Path}
		if needsMerge {
			profile["bounds"] = profileBounds(bounds)
		}
		if err := d.execQueries(ctx, db, profile, queries, d.cpRecovery, report); err != nil {
			return err
		}
	}
	d.be.FinishedResourceCommit(b.target.Path)

	var dirs, files []string
	for _, item := range items {
		if item.StagingTable != "" {
			dirs = append(dirs, filepath.Join(b.rootStagingPath, item.StagingTable))
		}
		if item.Directory != "" {
			dirs = append(dirs, filepath.Join(b.rootStagingPath, item.Directory))
		} else {
			files = append(files, item.ToDelete...)
		}
	}
	d.cleanUp(ctx, dirs, files)

	return nil
}

// execQueries runs the given queries in order, passing each one's row stats
// to report, and profiles them with the given fields. tolerateMissing is set
// when recovering entries which may already have been applied by a previous
// session whose state clearing didn't commit: their staged files and staging
// tables (and possibly their target table) were already deleted, or partly
// deleted, and it is okay to skip them in this case.
func (d *transactor) execQueries(ctx context.Context, db *stdsql.DB, profile log.Fields, queries []string, tolerateMissing bool, report func(m.RowStats)) error {
	for _, query := range queries {
		if stats, err := d.execQuery(ctx, db, profile, query); err != nil {
			if tolerateMissing && isMissingError(err) {
				continue
			}
			return fmt.Errorf("query %q failed: %w", query, err)
		} else {
			report(stats)
		}
	}
	return nil
}

// missingErrorMessages identify a commit query error caused by its staged
// files, staging table or target table having been deleted, in whole or in
// part.
var missingErrorMessages = []string{
	"PATH_NOT_FOUND",
	"Path does not exist",
	"CF_PATH_DOES_NOT_EXIST_FOR_READ_FILES",
	"COPY_INTO_SOURCE_SCHEMA_INFERENCE_FAILED",
	"Table doesn't exist",
	"TABLE_OR_VIEW_NOT_FOUND",
	"DELTA_TABLE_NOT_FOUND",
	"DELTA_TRUNCATED_TRANSACTION_LOG",
	"DELTA_MISSING_TRANSACTION_LOG",
	"DELTA_FILE_NOT_FOUND",
	"FAILED_READ_FILE",
}

func isMissingError(err error) bool {
	var text = err.Error()
	return slices.ContainsFunc(missingErrorMessages, func(msg string) bool {
		return strings.Contains(text, msg)
	})
}

// retriableErrorClasses are the Databricks error classes of a commit query
// which succeeds when re-run once the conflicting activity has finished.
//
// COPY_INTO_DUPLICATED_FILES_COPY_NOT_ALLOWED: another COPY INTO into the same
// table committed some of the same files, which usually means a previous
// primary shard was killed mid-Acknowledge. We retry until that orphaned query
// has finished, at which point our query skips the already-copied files.
var retriableErrorClasses = []string{
	"COPY_INTO_DUPLICATED_FILES_COPY_NOT_ALLOWED",
}

const maxQueryRetries = 5

// queryRetryDelay is the wait before retry number attempt+1: 15s, 30s, 1m,
// 2m, 4m.
var queryRetryDelay = func(attempt int) time.Duration {
	return time.Duration(1<<attempt) * 15 * time.Second
}

func isRetriableError(err error) bool {
	var text = err.Error()
	return slices.ContainsFunc(retriableErrorClasses, func(class string) bool {
		return strings.Contains(text, class)
	})
}

// execQuery runs a commit query, profiled with the given fields, and returns
// the row counts of its result row.
func (d *transactor) execQuery(ctx context.Context, db *stdsql.DB, profile log.Fields, query string) (m.RowStats, error) {
	for attempt := 0; ; attempt++ {
		var queryCtx, trace = d.profiler.trace(ctx, statementKind(query), profile)
		var rows, err = db.QueryContext(queryCtx, query)
		trace.finish(err, nil)
		if err == nil {
			return scanRowStats(rows), nil
		} else if !isRetriableError(err) || attempt >= maxQueryRetries {
			return m.RowStats{}, err
		}

		var delay = queryRetryDelay(attempt)
		log.WithFields(log.Fields{
			"attempt": attempt + 1,
			"delay":   delay.String(),
			"err":     err,
		}).Warn("query failed with a retriable error; retrying")

		select {
		case <-ctx.Done():
			return m.RowStats{}, ctx.Err()
		case <-time.After(delay):
		}
	}
}

// scanRowStats reads the result row of MERGE INTO (num_affected_rows,
// num_updated_rows, num_deleted_rows, num_inserted_rows) or COPY INTO
// (num_affected_rows, num_inserted_rows, num_skipped_corrupt_files). The
// query has already succeeded, so any other result yields the zero stats.
func scanRowStats(rows *stdsql.Rows) m.RowStats {
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil || !slices.Contains(cols, "num_affected_rows") || !rows.Next() {
		return m.RowStats{}
	}
	var vals = make([]int64, len(cols))
	var dest = make([]any, len(cols))
	for i := range vals {
		dest[i] = &vals[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return m.RowStats{}
	}
	var count = func(col string) int64 {
		if i := slices.Index(cols, col); i >= 0 {
			return vals[i]
		}
		return 0
	}
	return m.ExactRowStats(count("num_inserted_rows"), count("num_updated_rows"), count("num_deleted_rows")).
		WithTotal(count("num_affected_rows"))
}

// renderCommitQueries renders the queries which commit the staged files of
// checkpoint entries into the binding's target table: MERGE when any of the
// staged rows update existing documents, and a direct COPY INTO otherwise.
// Entries with a staging directory are read whole; the root-level files of
// entries written before staging directories existed are read one by one,
// chunked to bound the size of any single query.
// TODO: remove the root-level file path in about January 2027, after every
// task has started on this version and drained its pending checkpoint.
func (d *transactor) renderCommitQueries(b *binding, items []*checkpointItem, bounds []sql.MergeBound, needsMerge bool) ([]string, error) {
	var dirs, tables, rootFiles []string
	for _, item := range items {
		if item.StagingTable != "" && needsMerge {
			tables = append(tables, stagingIdentifier(b, item.StagingTable))
		} else if item.Directory != "" {
			dirs = append(dirs, filepath.Join(b.rootStagingPath, item.Directory))
		} else {
			rootFiles = append(rootFiles, item.StagedFiles...)
		}
	}

	var queries []string
	if !needsMerge {
		for chunk := range slices.Chunk(rootFiles, queryBatchSize) {
			if query, err := RenderTableWithFiles(b.target, chunk, b.rootStagingPath, d.templates.copyIntoDirect, bounds); err != nil {
				return nil, fmt.Errorf("copyIntoDirect template: %w", err)
			} else {
				queries = append(queries, query)
			}
		}
		for _, dir := range dirs {
			if query, err := RenderTableWithFiles(b.target, nil, dir, d.templates.copyIntoDirect, bounds); err != nil {
				return nil, fmt.Errorf("copyIntoDirect template: %w", err)
			} else {
				queries = append(queries, query)
			}
		}
		return queries, nil
	}

	for chunk := range slices.Chunk(rootFiles, queryBatchSize) {
		if query, err := RenderTableWithStaged(b.target, nil, pathsWithRoot(b.rootStagingPath, chunk), b.storeSchema, d.templates.mergeInto, bounds); err != nil {
			return nil, fmt.Errorf("mergeInto template: %w", err)
		} else {
			queries = append(queries, query)
		}
	}
	if len(dirs) > 0 || len(tables) > 0 {
		if query, err := renderTemplate(d.templates.mergeInto, &tableWithFiles{Table: &b.target, Directories: dirs, Tables: tables, Schema: b.storeSchema, Bounds: bounds}); err != nil {
			return nil, fmt.Errorf("mergeInto template: %w", err)
		} else {
			queries = append(queries, query)
		}
	}
	return queries, nil
}

// combineBounds unions the per-key-column merge bounds of coalesced checkpoint
// entries. A column keeps a bound only when every entry carries one, and
// differing literals combine with LEAST/GREATEST cast to the column's type:
// those functions compare in their operands' type, and comparing e.g.
// timestamp literals as strings can pick the wrong extremum. When any entry's
// bound splits into ranges, the combined bound is the union of every entry's
// ranges, taking an unsplit bound as a single range.
func combineBounds(keys []sql.Column, items []*checkpointItem) []sql.MergeBound {
	var out = make([]sql.MergeBound, len(keys))
	for i, key := range keys {
		out[i] = sql.MergeBound{Column: key}

		var valid, split = true, false
		var lowers, uppers []string
		var ranges []sql.MergeRange
		var addRange = func(lower, upper string) {
			var r = sql.MergeRange{LiteralLower: lower, LiteralUpper: upper}
			if !slices.Contains(ranges, r) {
				ranges = append(ranges, r)
			}
		}
		for _, item := range items {
			if len(item.Bounds) != len(keys) || item.Bounds[i].Lower == "" || item.Bounds[i].Upper == "" {
				valid = false
				break
			}
			if !slices.Contains(lowers, item.Bounds[i].Lower) {
				lowers = append(lowers, item.Bounds[i].Lower)
			}
			if !slices.Contains(uppers, item.Bounds[i].Upper) {
				uppers = append(uppers, item.Bounds[i].Upper)
			}
			if len(item.Bounds[i].Ranges) == 0 {
				addRange(item.Bounds[i].Lower, item.Bounds[i].Upper)
			}
			for _, r := range item.Bounds[i].Ranges {
				split = true
				addRange(r.Lower, r.Upper)
			}
		}
		if !valid {
			continue
		}

		out[i].LiteralLower = extremumExpr("LEAST", lowers, key)
		out[i].LiteralUpper = extremumExpr("GREATEST", uppers, key)
		if split {
			out[i].Ranges = ranges
		}
	}
	return out
}

// extremumExpr renders the SQL expression selecting fn (LEAST or GREATEST) of
// the given literals, or the literal itself when they all agree.
func extremumExpr(fn string, literals []string, key sql.Column) string {
	if len(literals) == 1 {
		return literals[0]
	}
	var cast = make([]string, len(literals))
	for i, l := range literals {
		cast[i] = fmt.Sprintf("%s::%s", l, key.BareDDL)
	}
	return fmt.Sprintf("%s(%s)", fn, strings.Join(cast, ", "))
}

func pathsWithRoot(root string, paths []string) []string {
	var newPaths = make([]string, len(paths))
	for i, p := range paths {
		newPaths[i] = filepath.Join(root, p)
	}

	return newPaths
}

func (d *transactor) Destroy() {
	d.profiler.stop()
}
