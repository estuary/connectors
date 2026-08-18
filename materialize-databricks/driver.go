package connector

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/logger"
	"github.com/databricks/databricks-sdk-go/useragent"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	m "github.com/estuary/connectors/go/materialize"
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
	cp                checkpoint
	// Pending entries of other shards, of sessions predating the range-scoped
	// checkpoint format (legacyRangeKey), and of stale ranges from previous
	// shard topologies. Only the primary shard tracks and executes these.
	peerShardsCheckpoints rangeCheckpoints
	cpRecovery            bool // is this checkpoint a recovered checkpoint?
	primary               bool // does this shard's range begin at key 0?
	rangeKey              string
	wsClient              *databricks.WorkspaceClient
	localStagingPath      string
	bindings              []*binding
	be                    *m.BindingEvents
	ep                    *sql.Endpoint[config]
	templates             templates
	materializationName   string
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

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(state, &raw); err != nil {
		return err
	}

	for key, val := range raw {
		if bucket, ok := parseRangeBucket(key, val); ok {
			if key == d.rangeKey {
				d.cp = bucket
			} else {
				d.peerShardsCheckpoints[key] = bucket
			}
		} else if item, err := parseCheckpointItem(val); err != nil {
			return fmt.Errorf("parsing checkpoint entry %q: %w", key, err)
		} else {
			d.legacyBucket()[key] = item
		}
	}
	d.cpRecovery = true

	return nil
}

func (d *transactor) legacyBucket() checkpoint {
	if d.peerShardsCheckpoints[legacyRangeKey] == nil {
		d.peerShardsCheckpoints[legacyRangeKey] = make(checkpoint)
	}
	return d.peerShardsCheckpoints[legacyRangeKey]
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
			// document. No shard emits full replacements in the range-scoped
			// checkpoint format, so a reset means a peer (e.g. one running an
			// older connector image) just clobbered the consolidated state.
			return fmt.Errorf("unexpected state reset patch in aggregated shard state")
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(patch, &raw); err != nil {
			return fmt.Errorf("parsing aggregated state patch: %w", err)
		}

		for key, val := range raw {
			if key == d.rangeKey {
				// Our own contribution, echoed back by the runtime. It's
				// already tracked in d.cp.
				continue
			}

			if !rangeKeyRe.MatchString(key) {
				// Top-level stateKeys are never emitted by range-scoped peers,
				// but route them as legacy entries rather than dropping them.
				if item, err := parseCheckpointItem(val); err != nil {
					return fmt.Errorf("parsing aggregated state patch entry %q: %w", key, err)
				} else {
					d.legacyBucket()[key] = item
				}
				continue
			}

			var bucket map[string]json.RawMessage
			if err := json.Unmarshal(val, &bucket); err != nil {
				return fmt.Errorf("parsing aggregated state patch bucket %q: %w", key, err)
			}
			for stateKey, itemRaw := range bucket {
				if item, err := parseCheckpointItem(itemRaw); err != nil {
					return fmt.Errorf("parsing aggregated state patch entry %q of %q: %w", stateKey, key, err)
				} else {
					if d.peerShardsCheckpoints[key] == nil {
						d.peerShardsCheckpoints[key] = make(checkpoint)
					}
					d.peerShardsCheckpoints[key][stateKey] = item
				}
			}
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

	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:               fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:              cfg.Credentials.PersonalAccessToken,
		Credentials:        dbConfig.PatCredentials{}, // enforce PAT auth
		HTTPTimeoutSeconds: 5 * 60,                    // This is necessary for file uploads as they can sometimes take longer than the default 60s
	})
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
		cp:                    make(checkpoint),
		peerShardsCheckpoints: make(rangeCheckpoints),
		primary:               keyBegin == 0,
		rangeKey:              fmt.Sprintf("%08x-%08x", keyBegin, keyEnd),
		wsClient:              wsClient,
		be:                    be,
		ep:                    ep,
		templates:             renderTemplates(ep.Dialect),
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

	// a binding needs to be merged if there are updates to existing documents
	// otherwise we just do a direct copy by moving all data from temporary table
	// into the target table. Note that in case of delta updates, "needsMerge"
	// will always be false
	needsMerge bool

	loadMergeBounds  *sql.MergeBoundsBuilder
	storeMergeBounds *sql.MergeBoundsBuilder
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

	b.loadFile = newStagedFile(t.cfg, b.rootStagingPath, translatedFieldNames(target.KeyNames()))
	b.storeFile = newStagedFile(t.cfg, b.rootStagingPath, append(translatedFieldNames(target.ColumnNames()), "_flow_delete"))
	b.loadMergeBounds = sql.NewMergeBoundsBuilder(target.Keys, t.ep.Dialect.Literal)
	b.storeMergeBounds = sql.NewMergeBoundsBuilder(target.Keys, t.ep.Dialect.Literal)

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

	for it.Next() {
		var b = d.bindings[it.Binding]

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

		var loadTemplate = d.templates.loadQuery
		if d.cfg.Advanced.NoFlowDocument {
			loadTemplate = d.templates.loadQueryNoFlowDocument
		}

		if loadQuery, err := RenderTableWithFiles(b.target, fullPaths, b.rootStagingPath, loadTemplate, b.loadMergeBounds.Build()); err != nil {
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

	if len(queries) > 0 {
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

		for rows.Next() {
			var binding int
			var document string

			if err = rows.Scan(&binding, &document); err != nil {
				return fmt.Errorf("scanning Load document: %w", err)
			} else if binding > -1 {
				doc := json.RawMessage([]byte(document))
				if b := d.bindings[binding]; len(b.nullFieldsToStrip) > 0 {
					if doc, err = sql.StripNullFields(doc, b.nullFieldsToStrip); err != nil {
						return fmt.Errorf("stripping null fields: %w", err)
					}
				}
				if err = loaded(binding, doc); err != nil {
					return err
				}
			}
		}

		if err = rows.Err(); err != nil {
			return fmt.Errorf("querying Loads: %w", err)
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
	// TODO: consolidate ToDelete into StagedFiles, which records the same
	// files relative to the binding's staging root. Kept for simplicity for
	// now, and still read from checkpoints written by older versions whose
	// entries carry only ToDelete.
	ToDelete []string

	// StagedFiles are the staged file names, relative to the binding's staging
	// root.
	StagedFiles []string `json:",omitempty"`
	// Bounds are the rendered merge-bound literals of the observed key range,
	// positional with the target table's key columns.
	Bounds []mergeBoundLiterals `json:",omitempty"`
	// NeedsMerge marks entries whose files must MERGE into the target table;
	// otherwise a direct COPY INTO suffices.
	NeedsMerge bool `json:",omitempty"`
}

// mergeBoundLiterals is the serialized form of a key column's sql.MergeBound:
// the rendered literals of the minimum and maximum key values observed for a
// transaction. Empty literals mean the column carries no bound, as is the case
// for boolean keys.
type mergeBoundLiterals struct {
	Lower string `json:",omitempty"`
	Upper string `json:",omitempty"`
}

func boundsLiterals(bounds []sql.MergeBound) []mergeBoundLiterals {
	var out = make([]mergeBoundLiterals, len(bounds))
	for i, b := range bounds {
		out[i] = mergeBoundLiterals{Lower: b.LiteralLower, Upper: b.LiteralUpper}
	}
	return out
}

type checkpoint map[string]*checkpointItem

func (c *checkpoint) Validate() error {
	return nil
}

// rangeCheckpoints maps shard range keys ("%08x-%08x" of key_begin-key_end)
// to that shard's per-stateKey checkpoint. Range keys are disjoint across the
// shards of a task, which is what lets each shard's StartedCommit merge patch
// commute with its peers' when the runtime consolidates connector state.
type rangeCheckpoints map[string]checkpoint

// legacyRangeKey is the in-memory bucket holding top-level per-stateKey
// entries written by connector versions predating the range-scoped checkpoint
// format. Its entries clear with top-level nulls rather than nested ones.
const legacyRangeKey = ""

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

// parseRangeBucket decodes val as a per-stateKey checkpoint when key names a
// shard key-range. A key matching the range pattern whose value doesn't
// decode as a bucket falls through to legacy entry parsing, which fails
// loudly. StateKeys cannot collide with range keys: they are URL-encoded
// resource paths carrying a ".vN" backfill counter suffix.
func parseRangeBucket(key string, val json.RawMessage) (checkpoint, bool) {
	if !rangeKeyRe.MatchString(key) {
		return nil, false
	}
	var bucket checkpoint
	if err := unmarshalStrict(val, &bucket); err != nil {
		return nil, false
	}
	return bucket, true
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
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Skip deleted, non-existent documents iff HardDelete is enabled.
	for it.Next(d.cfg.HardDelete) {
		var b = d.bindings[it.Binding]

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

	// Upload the staged files and record in the checkpoint everything needed to commit them into
	// the destination tables: if the connector is restarted in the middle of a commit it can run
	// the same commit on the next startup. This is the pattern for recovery log being
	// authoritative and the connector idempotently applies a commit. These are keyed on the
	// binding stateKey so that in case of a recovery being necessary we don't run queries
	// belonging to bindings that have been removed.
	for idx, b := range d.bindings {
		if !b.storeFile.started {
			continue
		}

		toCopy, err := b.storeFile.flush()
		if err != nil {
			return nil, fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
		}

		// In case of delta updates or if there are no existing keys being stored
		// we directly copy from staged files into the target table. Note that this is retriable
		// given that COPY INTO is idempotent by default: files that have already been loaded into a table will
		// not be loaded again
		// see https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
		d.cp[b.target.StateKey] = &checkpointItem{
			ToDelete:    pathsWithRoot(b.rootStagingPath, toCopy),
			StagedFiles: toCopy,
			Bounds:      boundsLiterals(b.storeMergeBounds.Build()),
			NeedsMerge:  !b.target.DeltaUpdates && b.needsMerge,
		}
		b.needsMerge = false // reset for next round
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		if state, err := d.startCommitState(); err != nil {
			return nil, m.FinishedOperation(err)
		} else {
			return state, nil
		}
	}, nil
}

func (d *transactor) startCommitState() (*pf.ConnectorState, error) {
	// Emit only this shard's range bucket as a merge patch: range keys are
	// disjoint across shards, so concurrent patches never clobber when the
	// runtime consolidates connector state. A single-shard task uses the same
	// format, with one bucket covering the full key range.
	var patch, err = json.Marshal(rangeCheckpoints{d.rangeKey: d.cp})
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint patch json: %w", err)
	}

	return &pf.ConnectorState{UpdatedJson: patch, MergePatch: true}, nil
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

	return d.acknowledgeApply(ctx, db, shouldProcess)
}

// acknowledgeApply executes pending checkpoint entries — this shard's own, and as the primary
// those of peers and prior sessions — and returns the state update clearing them, or nil if
// nothing was executed.
//
// Grouped by state key, because one query over every shard's staged files takes a fraction of
// the time those per-shard queries take back to back.
func (d *transactor) acknowledgeApply(ctx context.Context, db *stdsql.DB, shouldProcess func(string) bool) (*pf.ConnectorState, error) {
	// The clearing state update, nulling each executed entry at the state-
	// document path it occupies: nested under a range key, or top-level
	// (legacyRangeKey) for entries predating the range-scoped checkpoint
	// format. Clearing is a best-
	// effort attempt to spare a restart from running the same queries again —
	// there is no guarantee this checkpoint update can actually be committed.
	var clear = make(map[string]interface{})
	var clearEntry = func(rangeKey, stateKey string) {
		if rangeKey == legacyRangeKey {
			clear[stateKey] = nil
		} else {
			if clear[rangeKey] == nil {
				clear[rangeKey] = make(map[string]interface{})
			}
			clear[rangeKey].(map[string]interface{})[stateKey] = nil
		}
	}
	var peerRangeKeys = slices.Sorted(maps.Keys(d.peerShardsCheckpoints))

	// Everything else pending — entries of other state keys, and entries whose
	// table no longer has a binding (it might be deleted already) — is left
	// untouched, remaining pending in the persisted state.
	for _, b := range d.bindings {
		var sk = b.target.StateKey
		if !shouldProcess(sk) {
			continue
		}

		// The binding's pending entries: this shard's own, then those of
		// legacy sessions and peer shards.
		var items []*checkpointItem
		if item := d.cp[sk]; item != nil {
			items = append(items, item)
		}
		for _, rk := range peerRangeKeys {
			if item := d.peerShardsCheckpoints[rk][sk]; item != nil {
				items = append(items, item)
			}
		}
		if len(items) == 0 {
			continue
		}

		if err := d.executeStateKeyItems(ctx, db, b, items); err != nil {
			return nil, err
		}

		if d.cp[sk] != nil {
			delete(d.cp, sk)
			clearEntry(d.rangeKey, sk)
		}
		for rk, cp := range d.peerShardsCheckpoints {
			if cp[sk] != nil {
				delete(cp, sk)
				clearEntry(rk, sk)
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

// executeStateKeyItems commits one state key's entries. Entries with structured staging
// metadata coalesce into a single set of queries; those written by older connector versions
// have only their pre-rendered queries, so they run individually.
//
// Acknowledge fans-in by the runtime: the runtime waits for all Acknowledged responses from all
// shards before a new transaction begins, and files are already cleaned up once all queries have
// run. Thus a missing file means the whole query has already run, and during recovery that is
// acceptable: we may have run the query but not have been able to clear the checkpoint.
func (d *transactor) executeStateKeyItems(ctx context.Context, db *stdsql.DB, b *binding, items []*checkpointItem) error {
	var coalesce []*checkpointItem
	d.be.StartedResourceCommit(b.target.Path)
	for _, item := range items {
		if len(item.StagedFiles) > 0 {
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
		if err := d.execQueries(ctx, db, queries, d.cpRecovery); err != nil {
			return err
		}
	}

	if len(coalesce) > 0 {
		var files []string
		var needsMerge bool
		for _, item := range coalesce {
			files = append(files, item.StagedFiles...)
			needsMerge = needsMerge || item.NeedsMerge
		}

		queries, err := d.renderCommitQueries(b, files, combineBounds(b.target.Keys, coalesce), needsMerge)
		if err != nil {
			return err
		}
		if err := d.execQueries(ctx, db, queries, d.cpRecovery); err != nil {
			return err
		}
	}
	d.be.FinishedResourceCommit(b.target.Path)

	for _, item := range items {
		d.deleteFiles(ctx, item.ToDelete)
	}

	return nil
}

// execQueries runs the given queries in order. tolerateMissing is set when
// recovering entries which may already have been applied by a previous
// session whose state clearing didn't commit: their staged files (and
// possibly their target table) were already deleted, and it is okay to skip
// them in this case.
func (d *transactor) execQueries(ctx context.Context, db *stdsql.DB, queries []string, tolerateMissing bool) error {
	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			if tolerateMissing && (strings.Contains(err.Error(), "PATH_NOT_FOUND") || strings.Contains(err.Error(), "Path does not exist") || strings.Contains(err.Error(), "Table doesn't exist") || strings.Contains(err.Error(), "TABLE_OR_VIEW_NOT_FOUND")) {
				continue
			}
			return fmt.Errorf("query %q failed: %w", query, err)
		}
	}
	return nil
}

// renderCommitQueries renders the queries which commit a set of staged files
// (named relative to the binding's staging root) into the binding's target
// table: MERGE when any of the staged rows update existing documents, and a
// direct COPY INTO otherwise, chunked to bound the size of any single query.
func (d *transactor) renderCommitQueries(b *binding, files []string, bounds []sql.MergeBound, needsMerge bool) ([]string, error) {
	var queries []string
	for chunk := range slices.Chunk(files, queryBatchSize) {
		if needsMerge {
			if query, err := RenderTableWithFiles(b.target, pathsWithRoot(b.rootStagingPath, chunk), b.rootStagingPath, d.templates.mergeInto, bounds); err != nil {
				return nil, fmt.Errorf("mergeInto template: %w", err)
			} else {
				queries = append(queries, query)
			}
		} else {
			if query, err := RenderTableWithFiles(b.target, chunk, b.rootStagingPath, d.templates.copyIntoDirect, bounds); err != nil {
				return nil, fmt.Errorf("copyIntoDirect template: %w", err)
			} else {
				queries = append(queries, query)
			}
		}
	}
	return queries, nil
}

// combineBounds unions the per-key-column merge bounds of coalesced checkpoint
// entries. A column keeps a bound only when every entry carries one, and
// differing literals combine with LEAST/GREATEST cast to the column's type:
// those functions compare in their operands' type, and comparing e.g.
// timestamp literals as strings can pick the wrong extremum.
func combineBounds(keys []sql.Column, items []*checkpointItem) []sql.MergeBound {
	var out = make([]sql.MergeBound, len(keys))
	for i, key := range keys {
		out[i] = sql.MergeBound{Column: key}

		var valid = true
		var lowers, uppers []string
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
		}
		if !valid {
			continue
		}

		out[i].LiteralLower = extremumExpr("LEAST", lowers, key)
		out[i].LiteralUpper = extremumExpr("GREATEST", uppers, key)
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
}
