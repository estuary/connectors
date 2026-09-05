package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"text/template"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/blob"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

type checkpointItem struct {
	// Query contains the rendered query string.  Deprecated.
	Query string `json:",omitempty"`
	// TempTableName is the table referenced in string queries.  Deprecated.
	TempTableName string `json:",omitempty"`

	NeedsMerge bool                 `json:",omitempty"`
	Bounds     []mergeBoundLiterals `json:",omitempty"`
	SourceURIs []string             `json:",omitempty"`
	JobPrefix  string               `json:",omitempty"`

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
}

func boundsLiterals(bounds []sql.MergeBound) []mergeBoundLiterals {
	var out = make([]mergeBoundLiterals, len(bounds))
	for i, b := range bounds {
		out[i] = mergeBoundLiterals{Lower: b.LiteralLower, Upper: b.LiteralUpper}
	}
	return out
}

type checkpoint = map[string]*checkpointItem  // keyed by bare StateKey
type rangeCheckpoints = map[string]checkpoint // keyed by "%08x-%08x"

// legacyRangeKey holds entries written before range-scoping (bare StateKey).
const legacyRangeKey = ""

var rangeKeyRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{8}$`)

func isJSONNull(data json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(data), []byte("null"))
}

var _ m.Transactor = (*transactor)(nil)

type transactor struct {
	runtimeCheckpoint m.RuntimeCheckpoint
	cfg               config
	dialect           sql.Dialect
	templates         templates

	client     *client
	storeFiles *boilerplate.StagedFiles
	loadFiles  *boilerplate.StagedFiles

	bindings []*binding
	be       *m.BindingEvents
	cp       checkpoint
	// peerShardsCheckpoints holds pending entries of other shards, and of
	// sessions predating the range-scoped checkpoint format (legacyRangeKey).
	// Only the primary shard tracks and executes these.
	peerShardsCheckpoints rangeCheckpoints
	primaryShard          bool
	rangeKey              string

	objAndArrayAsJson       bool
	loggedStorageApiMessage bool
	skipCleanup             bool
}

func prepareNewTransactor(
	templates templates,
) func(context.Context, string, map[string]bool, *sql.Endpoint[config], sql.Fence, []sql.Table, pm.Request_Open, *boilerplate.InfoSchema, *m.BindingEvents) (m.Transactor, error) {
	return func(
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

		client, err := cfg.client(ctx, ep)
		if err != nil {
			return nil, err
		}

		credOption, err := cfg.CredentialsClientOption()
		if err != nil {
			return nil, err
		}

		bucket, err := blob.NewGCSBucket(ctx, cfg.Bucket, credOption)
		if err != nil {
			return nil, fmt.Errorf("creating GCS bucket: %w", err)
		}

		var keyBegin, keyEnd uint32 = 0, math.MaxUint32
		if open.Range != nil {
			keyBegin, keyEnd = open.Range.KeyBegin, open.Range.KeyEnd
		}

		primaryShard := true
		if boilerplate.IsMaterializationSpecRuntimeV2(open.Materialization) {
			primaryShard = keyBegin == 0
		}

		t := &transactor{
			runtimeCheckpoint:     fence.Checkpoint,
			cfg:                   cfg,
			dialect:               ep.Dialect,
			templates:             templates,
			objAndArrayAsJson:     featureFlags["objects_and_arrays_as_json"],
			skipCleanup:           featureFlags["skip_cleanup"],
			client:                client,
			be:                    be,
			cp:                    make(checkpoint, 0),
			peerShardsCheckpoints: make(rangeCheckpoints),
			primaryShard:          primaryShard,
			rangeKey:              fmt.Sprintf("%08x-%08x", keyBegin, keyEnd),
			loadFiles:             boilerplate.NewStagedFiles(stagedFileClient{}, bucket, writer.DefaultJsonFileSizeLimit, cfg.effectiveBucketPath(), false, false),
			storeFiles:            boilerplate.NewStagedFiles(stagedFileClient{}, bucket, writer.DefaultJsonFileSizeLimit, cfg.effectiveBucketPath(), true, false),
		}

		for _, binding := range bindings {
			// Lookup metadata for the table to build the schema for the external file that will be used
			// for loading data. Schema definitions from the actual table columns are used instead of
			// directly using the dialect's output for JSON schema type to provide some degree in
			// flexibility in changing the dialect and having it still work for existing tables. As long
			// as the JSON encoding of the values is the same they may be used for columns that would
			// have been created differently due to evolution of the dialect's column types.
			res := is.GetResource(binding.Path)
			if res == nil {
				return nil, fmt.Errorf("could not get metadata for table %s: verify that the table exists and that the connector service account user is authorized for it", binding.Identifier)
			}

			schema := res.Meta.(bigquery.Schema)

			log.WithFields(log.Fields{
				"table":      binding.Path,
				"collection": binding.Source.String(),
				"schemaJson": schema,
			}).Debug("bigquery schema for table")

			fieldSchemas := make(map[string]*bigquery.FieldSchema)
			for _, f := range schema {
				fieldSchemas[f.Name] = f
			}

			if err = t.addBinding(binding, fieldSchemas, &ep.Dialect); err != nil {
				return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
			}
		}

		return t, nil
	}
}

func (t *transactor) addBinding(target sql.Table, fieldSchemas map[string]*bigquery.FieldSchema, dialect *sql.Dialect) error {
	loadSchema, err := schemaForCols(target.KeyPtrs(), fieldSchemas)
	if err != nil {
		return err
	}

	storeSchema, err := schemaForCols(target.Columns(), fieldSchemas)
	if err != nil {
		return err
	}
	storeSchema = append(storeSchema, &bigquery.FieldSchema{
		Name: "_flow_delete",
		Type: bigquery.BooleanFieldType,
	})

	b := &binding{
		target:           target,
		loadSchema:       loadSchema,
		storeSchema:      storeSchema,
		loadMergeBounds:  sql.NewMergeBoundsBuilder(target.Keys, dialect.Literal),
		storeMergeBounds: sql.NewMergeBoundsBuilder(target.Keys, dialect.Literal),
	}

	if t.cfg.Advanced.NoFlowDocument {
		b.nullFieldsToStrip = target.NullableFieldsToStrip()
	}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.tempTableName, t.templates.tempTableName},
		{&b.storeInsertSQL, t.templates.storeInsert},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	loadFields := make([]string, 0, len(loadSchema))
	storeFields := make([]string, 0, len(storeSchema))
	for _, field := range loadSchema {
		loadFields = append(loadFields, field.Name)
	}
	for _, field := range storeSchema {
		storeFields = append(storeFields, field.Name)
	}
	t.loadFiles.AddBinding(target.Binding, loadFields)
	t.storeFiles.AddBinding(target.Binding, storeFields)

	t.bindings = append(t.bindings, b)
	return nil
}

func schemaForCols(cols []*sql.Column, fieldSchemas map[string]*bigquery.FieldSchema) ([]*bigquery.FieldSchema, error) {
	s := make([]*bigquery.FieldSchema, 0, len(cols))

	for idx, col := range cols {
		schema, ok := fieldSchemas[translateFlowIdentifier(col.Field)]
		if !ok {
			return nil, fmt.Errorf("could not find metadata for field '%s'", col.Field)
		}

		// Copy the field schema rather than reusing the destination table's, both to avoid
		// mutating the caller's view of the destination schema and so that modifications made
		// here never leak back to BigQuery via the temp/external table definition.
		tempSchema := *schema

		// Use a placeholder value instead of the actual field name for the external table schema.
		// This allows for materialized tables to use "Flexible column names", which is not yet
		// supported by external tables. A similar placeholder is used in the generated SQL queries
		// to match this.
		tempSchema.Name = fmt.Sprintf("c%d", idx)

		// Never carry the destination column's policy tags onto the temp/external table:
		// BigQuery enforces column-level security on the query-scoped external table itself,
		// and Fine-Grained Reader grants do not extend to it, so a tagged temp column fails
		// with a 403 that no customer permissioning can fix. See
		// https://github.com/estuary/connectors/issues/4833.
		tempSchema.PolicyTags = nil

		s = append(s, &tempSchema)
	}

	return s, nil
}

func (t *transactor) RecoverCheckpoint(_ context.Context, _ pf.MaterializationSpec, _ pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return t.runtimeCheckpoint, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error {
	// Non-primary shards do not recover state.  This avoids them incorporating
	// state from other shards into their own checkpoint data.
	if !t.primaryShard {
		return nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(state, &raw); err != nil {
		return fmt.Errorf("unmarshaling checkpoint state: %w", err)
	}

	for key, val := range raw {
		if rangeKeyRe.MatchString(key) {
			var bucket checkpoint
			if err := json.Unmarshal(val, &bucket); err != nil {
				return fmt.Errorf("unmarshaling checkpoint range %q: %w", key, err)
			}
			if key == t.rangeKey {
				t.cp = bucket
			} else {
				t.peerShardsCheckpoints[key] = bucket
			}
			continue
		}

		// A bare StateKey: an entry from before range-scoping was added.
		var item checkpointItem
		if err := json.Unmarshal(val, &item); err != nil {
			return fmt.Errorf("unmarshaling checkpoint entry %q: %w", key, err)
		}
		t.legacyBucket()[key] = &item
	}

	return nil
}

func (t *transactor) legacyBucket() checkpoint {
	if t.peerShardsCheckpoints[legacyRangeKey] == nil {
		t.peerShardsCheckpoints[legacyRangeKey] = make(checkpoint)
	}
	return t.peerShardsCheckpoints[legacyRangeKey]
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		var b = t.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting load key: %w", err)
		} else if err = t.loadFiles.WriteRow(ctx, it.Binding, converted); err != nil {
			return fmt.Errorf("writing normalized key to keyfile: %w", err)
		} else {
			b.loadMergeBounds.NextKey(converted)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	if !t.skipCleanup {
		defer t.loadFiles.CleanupCurrentTransaction(ctx)
	}

	// Build the queries of all documents across all bindings that were requested.
	var subqueries []string
	// This is the map of external table references we will populate.
	var edcTableDefs = make(map[string]bigquery.ExternalData)
	var externalData = make(map[string][]string)

	for idx, b := range t.bindings {
		if !t.loadFiles.Started(idx) {
			continue
		} else if uris, err := t.loadFiles.Flush(idx); err != nil {
			return fmt.Errorf("flushing load file: %w", err)
		} else {
			// Choose appropriate load query template based on configuration
			var loadTemplate = t.templates.loadQuery
			if t.cfg.Advanced.NoFlowDocument {
				loadTemplate = t.templates.loadQueryNoFlowDocument
			}

			if loadQuery, err := renderQueryTemplate(b.target, loadTemplate, b.loadMergeBounds.Build(), t.objAndArrayAsJson); err != nil {
				return fmt.Errorf("rendering load query template: %w", err)
			} else {
				subqueries = append(subqueries, loadQuery)
				edcTableDefs[b.tempTableName] = edc(uris, b.loadSchema)
				externalData[b.tempTableName] = uris
			}
		}
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}

	// Build the query across all tables.
	queryStr := strings.Join(subqueries, "\nUNION ALL\n") + ";"
	query := t.client.newQuery(queryStr)
	query.TableDefinitions = edcTableDefs // Tell bigquery where to get the external references in gcs.
	ll := log.WithFields(log.Fields{"query": queryStr, "external_data": externalData})

	t.be.StartedEvaluatingLoads()
	job, err := t.client.runQuery(ctx, query)
	if err != nil {
		ll.WithError(err).Error("client runQuery failed")
		return fmt.Errorf("load query: %w", err)
	}

	bqit, err := job.Read(ctx)
	if err != nil {
		ll.WithError(err).Error("job read failed")
		return fmt.Errorf("load job read: %w", err)
	}

	ll.Debug("load query executed")
	t.be.FinishedEvaluatingLoads()

	if !bqit.IsAccelerated() && !t.loggedStorageApiMessage {
		log.Warn("not using the storage read API for load queries, performance may not be optimal. see https://go.estuary.dev/materialize-bigquery for more information")
		t.loggedStorageApiMessage = true
	}

	for {
		var bd bindingDocument
		if err = bqit.Next(&bd); err == iterator.Done {
			break
		} else if err != nil {
			ll.WithError(err).Error("query results iterator failed")
			return fmt.Errorf("load row read: %w", err)
		}

		doc := bd.Document
		if b := t.bindings[bd.Binding]; len(b.nullFieldsToStrip) > 0 {
			var err error
			if doc, err = sql.StripNullFields(doc, b.nullFieldsToStrip); err != nil {
				return fmt.Errorf("stripping null fields: %w", err)
			}
		}
		if err = loaded(bd.Binding, doc); err != nil {
			return fmt.Errorf("load row loaded: %w", err)
		}
	}

	if t.skipCleanup {
		log.Warn("cleanup disabled; load files retained")
	} else if err := t.loadFiles.CleanupCurrentTransaction(ctx); err != nil {
		return fmt.Errorf("cleaning up load files: %w", err)
	}

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var ctx = it.Context()

	// Skip deleted, non-existent documents iff HardDelete is enabled.
	for it.Next(t.cfg.HardDelete) {
		var b = t.bindings[it.Binding]

		flowDelete := t.cfg.HardDelete && it.Delete
		if flowDelete && !it.Exists {
			// Ignore documents which do not exist and are being deleted.
			continue
		}

		if it.Exists {
			b.mustMerge = true
		}

		if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err = t.storeFiles.WriteRow(ctx, it.Binding, append(converted, flowDelete)); err != nil {
			return nil, fmt.Errorf("writing Store to scratch file: %w", err)
		} else {
			b.storeMergeBounds.NextKey(converted[:len(b.target.Keys)])
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	for idx, b := range t.bindings {
		if !t.storeFiles.Started(idx) {
			continue
		}

		uris, err := t.storeFiles.Flush(idx)
		if err != nil {
			return nil, fmt.Errorf("flushing store file for %s: %w", b.target.Path, err)
		}

		t.cp[b.target.StateKey] = &checkpointItem{
			Bounds:     boundsLiterals(b.storeMergeBounds.Build()),
			NeedsMerge: b.mustMerge,
			SourceURIs: uris,
			JobPrefix:  uuid.NewString(),
			round:      it.Round,
		}

		b.mustMerge = false
		b.storeMergeBounds.Reset()
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		// Nest under this shard's range key: range keys are disjoint across
		// shards, so concurrent patches never clobber when the runtime
		// consolidates connector state. A single-shard task uses the same
		// format, with one bucket covering the full key range.
		var checkpointJSON, err = json.Marshal(rangeCheckpoints{t.rangeKey: t.cp})
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON, MergePatch: true}, nil
	}, nil
}

// mergePeerStatePatches folds the aggregated StartedCommit state patches of
// all task shards into the primary's bookkeeping, so Acknowledge executes
// the queries staged by every shard of the just-committed transaction.
func (t *transactor) mergePeerStatePatches(patches []json.RawMessage) error {
	if !t.primaryShard {
		return nil
	}

	for _, patch := range patches {
		if isJSONNull(patch) {
			return fmt.Errorf("unexpected state reset patch in aggregated shard state")
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(patch, &raw); err != nil {
			return fmt.Errorf("parsing aggregated state patch: %w", err)
		}

		for key, val := range raw {
			if key == t.rangeKey {
				continue // our own contribution, echoed back by the runtime
			}

			if !rangeKeyRe.MatchString(key) {
				var item checkpointItem
				if err := json.Unmarshal(val, &item); err != nil {
					return fmt.Errorf("parsing aggregated state patch entry %q: %w", key, err)
				}
				t.legacyBucket()[key] = &item
				continue
			}

			var bucket checkpoint
			if err := json.Unmarshal(val, &bucket); err != nil {
				return fmt.Errorf("parsing aggregated state patch bucket %q: %w", key, err)
			}
			if t.peerShardsCheckpoints[key] == nil {
				t.peerShardsCheckpoints[key] = make(checkpoint)
			}
			for stateKey, item := range bucket {
				t.peerShardsCheckpoints[key][stateKey] = item
			}
		}
	}

	return nil
}

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	if err := t.mergePeerStatePatches(statePatches); err != nil {
		return nil, err
	}

	shouldProcess := m.StateKeyFilter(stateKeys)

	if !t.primaryShard {
		for _, b := range t.bindings {
			if shouldProcess(b.target.StateKey) {
				delete(t.cp, b.target.StateKey)
			}
		}
		return nil, nil
	}

	return t.acknowledgeApply(ctx, shouldProcess)
}

// pendingItem pairs a checkpoint entry with the range and state key it was
// staged under, so the clearing pass afterward knows which JSON path it
// occupies.
type pendingItem struct {
	rangeKey string
	stateKey string
	item     *checkpointItem
}

func (t *transactor) acknowledgeApply(ctx context.Context, shouldProcess func(string) bool) (*pf.ConnectorState, error) {
	group, groupCtx := errgroup.WithContext(ctx)
	// You can run up to 1,000 concurrent multi-statement queries, so we use a
	// generous concurrency limit here, while not leaving it completely
	// unlimited just to maintain some sense of decorum.
	group.SetLimit(100)

	var peerRangeKeys = slices.Sorted(maps.Keys(t.peerShardsCheckpoints))
	var drained []pendingItem

	for _, b := range t.bindings {
		sk := b.target.StateKey
		if !shouldProcess(sk) {
			// This state key's pending work was not requested to be
			// processed, so it remains staged in the persisted state.
			continue
		}

		// This binding's pending entries: its own, then those of legacy
		// sessions and peer shards.
		var items []pendingItem
		if item := t.cp[sk]; item != nil {
			items = append(items, pendingItem{t.rangeKey, sk, item})
		}
		for _, rk := range peerRangeKeys {
			if item := t.peerShardsCheckpoints[rk][sk]; item != nil {
				items = append(items, pendingItem{rk, sk, item})
			}
		}
		if len(items) == 0 {
			continue
		}
		drained = append(drained, items...)

		group.Go(func() error {
			t.be.StartedResourceCommit(b.target.Path)

			// Entries still using the deprecated Query field were already
			// fully rendered when staged, so they run individually. The rest
			// coalesce into a single query over their combined bounds and
			// source files.
			// items[0] is this shard's own entry when it has one.
			round := items[0].item.round
			needsMerge := false
			var coalesce []*checkpointItem
			var sourceURIs []string
			for _, e := range items {
				sourceURIs = append(sourceURIs, e.item.SourceURIs...)
				if e.item.Query == "" {
					coalesce = append(coalesce, e.item)
					if e.item.NeedsMerge {
						needsMerge = true
					}
					continue
				}
				stat, err := t.client.queryIdempotent(groupCtx, b.storeSchema, e.item.Query, e.item.JobPrefix, e.item.SourceURIs, e.item.TempTableName)
				if err != nil {
					return fmt.Errorf("acknowledge query for %q: %w", b.target.Path, err)
				}
				t.be.ReportRowStats(round, b.target.Path, dmlRowStats(stat))
				log.WithFields(log.Fields{
					"query":         e.item.Query,
					"external_data": map[string][]string{e.item.TempTableName: e.item.SourceURIs},
				}).Debug("acknowledge legacy query executed")
			}

			if len(coalesce) > 0 {
				query := b.storeInsertSQL
				if needsMerge {
					var err error
					bounds := combineBounds(b.target.Keys, coalesce)
					query, err = renderQueryTemplate(b.target, t.templates.storeUpdate, bounds, t.objAndArrayAsJson)
					if err != nil {
						return fmt.Errorf("rendering merge query template: %w", err)
					}
				}
				var coalescedURIs []string
				for _, item := range coalesce {
					coalescedURIs = append(coalescedURIs, item.SourceURIs...)
				}
				stat, err := t.client.queryIdempotent(groupCtx, b.storeSchema, query, coalesce[0].JobPrefix, coalescedURIs, b.tempTableName)
				if err != nil {
					return fmt.Errorf("acknowledge query for %q: %w", b.target.Path, err)
				}
				t.be.ReportRowStats(round, b.target.Path, dmlRowStats(stat))
				log.WithFields(log.Fields{
					"query":         query,
					"external_data": map[string][]string{b.tempTableName: coalescedURIs},
				}).Debug("acknowledge query executed")
			}

			if t.skipCleanup {
				log.Warn("cleanup disabled; staged files retained")
			} else if err := t.storeFiles.CleanupCheckpoint(ctx, sourceURIs); err != nil {
				// Queries will fail if the source files have been deleted, but
				// if queryIdempotent has returned without error then a job for
				// that query will never be attempted again.
				return fmt.Errorf("cleaning up staged files for %q: %w", b.target.Path, err)
			}

			t.be.FinishedResourceCommit(b.target.Path)
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	if len(drained) == 0 {
		return nil, nil
	}

	// Null each executed entry at the JSON path it occupies: nested under
	// its range key, or top-level for legacyRangeKey.
	var clear = make(map[string]interface{})
	for _, d := range drained {
		if d.rangeKey == legacyRangeKey {
			clear[d.stateKey] = nil
			delete(t.legacyBucket(), d.stateKey)
			continue
		}
		if clear[d.rangeKey] == nil {
			clear[d.rangeKey] = make(map[string]interface{})
		}
		clear[d.rangeKey].(map[string]interface{})[d.stateKey] = nil

		if d.rangeKey == t.rangeKey {
			delete(t.cp, d.stateKey)
		} else {
			delete(t.peerShardsCheckpoints[d.rangeKey], d.stateKey)
		}
	}

	checkpointJSON, err := json.Marshal(clear)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint clearing json: %w", err)
	}

	return &pf.ConnectorState{UpdatedJson: json.RawMessage(checkpointJSON), MergePatch: true}, nil
}

func (t *transactor) Destroy() {
	_ = t.client.bigqueryClient.Close()
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
		cast[i] = fmt.Sprintf("CAST(%s AS %s)", l, key.BareDDL)
	}
	return fmt.Sprintf("%s(%s)", fn, strings.Join(cast, ", "))
}
