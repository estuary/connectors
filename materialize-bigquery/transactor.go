package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"text/template"
	"time"

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
	// TruncateBefore is the boundary of a backfill that completed in the
	// transaction. The entry's apply deletes the target's rows published
	// before it, after the entry's stores. It is omitted when unset, so
	// that the entry stays readable by a connector build without it.
	TruncateBefore *time.Time `json:",omitempty"`

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

// loadResultsTablePrefix marks a table as holding the results of one
// transaction's load query.
const loadResultsTablePrefix = "flow_load_results_"

// loadResultsTableName returns a fresh, unique table name for one transaction's
// load query results. The materialization name and range key identify the
// owning shard to an operator inspecting the dataset.
func loadResultsTableName(materialization, rangeKey string) string {
	return loadResultsTablePrefix + translateFlowIdentifier(materialization) + "_" + rangeKey + "_" + uuid.NewString()
}

// loadResultsTableTTL bounds how long a load results table survives a crash
// that prevents its deletion.
const loadResultsTableTTL = 24 * time.Hour

// deleteLoadResultsTable deletes a load results table under a context that
// survives cancellation of the transaction, so that a shard shutting down
// mid-transaction still cleans up. A failure is only logged, because the
// table's expiration removes it regardless.
func deleteLoadResultsTable(ctx context.Context, table *bigquery.Table) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	if err := table.Delete(ctx); err != nil {
		log.WithFields(log.Fields{"results_table": table.TableID, "error": err}).Warn("deleting load results table failed; it will expire on its own")
	}
}

var _ m.Transactor = (*transactor)(nil)

type transactor struct {
	runtimeCheckpoint   m.RuntimeCheckpoint
	materializationName string
	cfg                 config
	dialect             sql.Dialect
	templates           templates

	client     *client
	storeFiles *boilerplate.StagedFiles
	loadFiles  *boilerplate.StagedFiles

	bindings []*binding
	be       *m.BindingEvents
	state    *boilerplate.ShardedState[*checkpointItem]
	// truncateAfterBackfill permits deleting the rows published before a
	// completed backfill.
	truncateAfterBackfill bool
	// truncations maps a binding index to the boundary of a backfill that
	// completed in the current transaction, for staging by Store.
	truncations map[int]time.Time

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

		var stateKeys []string
		for _, b := range bindings {
			stateKeys = append(stateKeys, b.StateKey)
		}

		t := &transactor{
			runtimeCheckpoint:     fence.Checkpoint,
			materializationName:   materializationName,
			cfg:                   cfg,
			dialect:               ep.Dialect,
			templates:             templates,
			objAndArrayAsJson:     featureFlags["objects_and_arrays_as_json"],
			skipCleanup:           featureFlags["skip_cleanup"],
			client:                client,
			be:                    be,
			state:                 boilerplate.NewShardedState[*checkpointItem](keyBegin, keyEnd, stateKeys),
			truncateAfterBackfill: sql.TruncateAfterBackfill(featureFlags),
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

			if err = t.addBinding(binding, fieldSchemasByName(schema), &ep.Dialect); err != nil {
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
		loadMergeBounds:  newMergeBoundsBuilder(target, dialect.Literal),
		storeMergeBounds: newMergeBoundsBuilder(target, dialect.Literal),
		timestampKeys:    timestampKeys(target),
	}

	if t.cfg.Advanced.NoFlowDocument {
		b.nullFieldsToStrip = target.NullablePropertiesToStrip()
	}

	if col, err := target.PublishedAtColumn(); err == nil {
		b.truncateSQL = fmt.Sprintf("DELETE FROM %s WHERE %s < %%s;", target.Identifier, col.Identifier)
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

// fieldSchemasByName indexes a table's field schemas by lowercased column
// name, because BigQuery column names are case-insensitive.
func fieldSchemasByName(schema bigquery.Schema) map[string]*bigquery.FieldSchema {
	out := make(map[string]*bigquery.FieldSchema, len(schema))
	for _, f := range schema {
		out[strings.ToLower(f.Name)] = f
	}
	return out
}

// schemaForCols returns the temp/external table schema for cols, looking each
// column up in fieldSchemas as built by fieldSchemasByName.
func schemaForCols(cols []*sql.Column, fieldSchemas map[string]*bigquery.FieldSchema) ([]*bigquery.FieldSchema, error) {
	s := make([]*bigquery.FieldSchema, 0, len(cols))

	for idx, col := range cols {
		schema, ok := fieldSchemas[strings.ToLower(translateFlowIdentifier(col.Field))]
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
	return t.state.Recover(state)
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		var b = t.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting load key: %w", err)
		} else if err = t.loadFiles.WriteRow(ctx, it.Binding, converted); err != nil {
			return fmt.Errorf("writing normalized key to keyfile: %w", err)
		} else if err := b.loadMergeBounds.NextKey(b.boundsKey(converted)); err != nil {
			return fmt.Errorf("tracking Load key bounds for resource %s: %w", b.target.Path, err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	if !t.skipCleanup {
		defer t.loadFiles.CleanupCurrentTransaction(ctx)
	}

	// Build the queries of all documents across all bindings that were requested.
	var subqueries []loadSubquery
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

			subqueries = append(subqueries, loadSubquery{table: b.target, tpl: loadTemplate, bounds: b.loadMergeBounds.Build()})
			edcTableDefs[b.tempTableName] = edc(uris, b.loadSchema)
			externalData[b.tempTableName] = uris
		}
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}

	// Build the query across all tables.
	queryStr, err := renderLoadQuery(subqueries, maxLoadQueryLength, t.objAndArrayAsJson)
	if err != nil {
		return fmt.Errorf("rendering load query template: %w", err)
	}
	query := t.client.newQuery(queryStr)
	query.TableDefinitions = edcTableDefs // Tell bigquery where to get the external references in gcs.

	// A query with no destination table caps its response at 10 GB, so the
	// results are written to a table created for this transaction.
	dst := t.client.bigqueryClient.DatasetInProject(t.cfg.ProjectID, t.cfg.Dataset).Table(loadResultsTableName(t.materializationName, t.state.RangeKey()))
	ll := log.WithFields(log.Fields{"query": queryStr, "external_data": externalData, "results_table": dst.TableID})
	if err := dst.Create(ctx, &bigquery.TableMetadata{ExpirationTime: time.Now().Add(loadResultsTableTTL)}); err != nil {
		return fmt.Errorf("creating load results table: %w", err)
	}
	defer deleteLoadResultsTable(ctx, dst)
	query.Dst = dst
	query.WriteDisposition = bigquery.WriteTruncate

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
		} else if err := b.storeMergeBounds.NextKey(b.boundsKey(converted[:len(b.target.Keys)])); err != nil {
			return nil, fmt.Errorf("tracking Store key bounds for resource %s: %w", b.target.Path, err)
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	var staged = make(map[string]*checkpointItem)
	for idx, b := range t.bindings {
		if !t.storeFiles.Started(idx) {
			continue
		}

		uris, err := t.storeFiles.Flush(idx)
		if err != nil {
			return nil, fmt.Errorf("flushing store file for %s: %w", b.target.Path, err)
		}

		staged[b.target.StateKey] = &checkpointItem{
			Bounds:     boundsLiterals(b.storeMergeBounds.Build()),
			NeedsMerge: b.mustMerge,
			SourceURIs: uris,
			JobPrefix:  uuid.NewString(),
			round:      it.Round,
		}

		b.mustMerge = false
		b.storeMergeBounds.Reset()
	}

	for binding, boundary := range t.truncations {
		var b = t.bindings[binding]
		if staged[b.target.StateKey] == nil {
			staged[b.target.StateKey] = &checkpointItem{JobPrefix: uuid.NewString(), round: it.Round}
		}
		staged[b.target.StateKey].TruncateBefore = &boundary
	}
	t.truncations = nil

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		state, err := t.state.StartCommit(staged)
		if err != nil {
			return nil, m.FinishedOperation(err)
		}
		return state, nil
	}, nil
}

// Flush records completed backfills on the primary only, since every shard
// receives the same completions and only the primary applies staged work.
func (t *transactor) Flush(_ context.Context, completes map[int]time.Time) error {
	t.truncations = make(map[int]time.Time, len(completes))
	if !t.state.Primary() {
		return nil
	}
	for binding, boundary := range completes {
		var b = t.bindings[binding]
		if b.target.DeltaUpdates {
			continue
		} else if !t.truncateAfterBackfill {
			log.WithFields(log.Fields{
				"table":    b.target.Identifier,
				"boundary": boundary,
			}).Info("rows published before the backfill were not deleted because feature flags disable it")
			continue
		}
		if b.truncateSQL == "" {
			var _, reason = b.target.PublishedAtColumn()
			t.be.Warn(b.target.Identifier, fmt.Sprintf("Rows published before the backfill of table %s were not deleted because %s.", b.target.Identifier, reason))
			continue
		}
		t.truncations[binding] = boundary
	}
	return nil
}

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return t.state.Acknowledge(statePatches, stateKeys, func(pending boilerplate.ShardedEntries[*checkpointItem]) error {
		return t.acknowledgeApply(ctx, pending)
	})
}

func (t *transactor) acknowledgeApply(ctx context.Context, pending boilerplate.ShardedEntries[*checkpointItem]) error {
	group, groupCtx := errgroup.WithContext(ctx)
	// You can run up to 1,000 concurrent multi-statement queries, so we use a
	// generous concurrency limit here, while not leaving it completely
	// unlimited just to maintain some sense of decorum.
	group.SetLimit(100)

	for _, b := range t.bindings {
		// This binding's pending entries: its own, then those of peer shards.
		var items = pending.Sorted(b.target.StateKey)
		if len(items) == 0 {
			continue
		}

		group.Go(func() error {
			t.be.StartedResourceCommit(b.target.Path)

			// Entries still using the deprecated Query field were already
			// fully rendered when staged, so they run individually. The rest
			// coalesce into a single query over their combined bounds and
			// source files.
			// items[0] is this shard's own entry when it has one.
			round := items[0].round
			needsMerge := false
			var coalesce []*checkpointItem
			var coalescedURIs []string
			var sourceURIs []string
			var truncateBefore *time.Time
			var truncateJobPrefix string
			for _, e := range items {
				sourceURIs = append(sourceURIs, e.SourceURIs...)
				if e.TruncateBefore != nil && (truncateBefore == nil || e.TruncateBefore.After(*truncateBefore)) {
					truncateBefore, truncateJobPrefix = e.TruncateBefore, e.JobPrefix
				}
				if e.Query == "" {
					coalesce = append(coalesce, e)
					coalescedURIs = append(coalescedURIs, e.SourceURIs...)
					if e.NeedsMerge {
						needsMerge = true
					}
					continue
				}
				stat, err := t.client.queryIdempotent(groupCtx, b.storeSchema, e.Query, e.JobPrefix, e.SourceURIs, e.TempTableName)
				if err != nil {
					return fmt.Errorf("acknowledge query for %q: %w", b.target.Path, err)
				}
				t.be.ReportRowStats(round, b.target.Path, dmlRowStats(stat))
				log.WithFields(log.Fields{
					"query":         e.Query,
					"external_data": map[string][]string{e.TempTableName: e.SourceURIs},
				}).Debug("acknowledge legacy query executed")
			}

			// A truncation-only entry stages no files.
			var stats = m.TotalRowStats(0)
			if len(coalescedURIs) > 0 {
				query := b.storeInsertSQL
				if needsMerge {
					var err error
					bounds := combineBounds(b.target.Keys, coalesce)
					query, err = renderQueryTemplate(b.target, t.templates.storeUpdate, bounds, t.objAndArrayAsJson)
					if err != nil {
						return fmt.Errorf("rendering merge query template: %w", err)
					}
				}
				stat, err := t.client.queryIdempotent(groupCtx, b.storeSchema, query, coalesce[0].JobPrefix, coalescedURIs, b.tempTableName)
				if err != nil {
					return fmt.Errorf("acknowledge query for %q: %w", b.target.Path, err)
				}
				stats = dmlRowStats(stat)
				log.WithFields(log.Fields{
					"query":         query,
					"external_data": map[string][]string{b.tempTableName: coalescedURIs},
				}).Debug("acknowledge query executed")
			}

			// The truncation runs after the stores, so that it keeps the rows
			// they just wrote, which were published after the boundary.
			if truncateBefore == nil {
				// Pass.
			} else if b.truncateSQL == "" {
				var _, reason = b.target.PublishedAtColumn()
				log.WithFields(log.Fields{
					"table":    b.target.Identifier,
					"boundary": *truncateBefore,
					"reason":   reason.Error(),
				}).Warn("skipping staged truncation because the binding no longer supports it")
			} else if stat, err := t.client.queryIdempotent(groupCtx, nil, fmt.Sprintf(b.truncateSQL, timestampLiteral(*truncateBefore)), truncateJobPrefix+"-truncate", nil, ""); err != nil {
				return fmt.Errorf("truncating %s after backfill: %w", b.target.Identifier, err)
			} else {
				var n = dmlRowStats(stat).Deleted
				stats = stats.WithTruncated(n)
				log.WithFields(log.Fields{
					"table":    b.target.Identifier,
					"boundary": *truncateBefore,
					"deleted":  n,
				}).Info("truncated rows published before the backfill")
			}
			if len(coalescedURIs) > 0 || stats.Truncated != nil {
				t.be.ReportRowStats(round, b.target.Path, stats)
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

	return group.Wait()
}

// timestampLiteral renders a TIMESTAMP literal floored to microseconds, the
// precision of the column it is compared against.
func timestampLiteral(ts time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s UTC'", ts.UTC().Format("2006-01-02 15:04:05.000000"))
}

func (t *transactor) Destroy() {
	_ = t.client.bigqueryClient.Close()
}

// maxLoadQueryLength is the longest load query, in bytes, which keeps the
// split ranges of its merge bounds. BigQuery rejects a query whose text is
// longer than 1,024,000 characters.
const maxLoadQueryLength = 1_000_000

// loadSubquery is one binding's part of a load query.
type loadSubquery struct {
	table  sql.Table
	tpl    *template.Template
	bounds []sql.MergeBound
}

// renderLoadQuery renders the UNION ALL of subqueries. A query longer than
// limit is rendered with each merge bound as its one enclosing range, which
// takes less text than its split ranges.
func renderLoadQuery(subqueries []loadSubquery, limit int, objAndArrayAsJson bool) (string, error) {
	var render = func(split bool) (string, error) {
		var parts = make([]string, 0, len(subqueries))
		for _, sq := range subqueries {
			var bounds = sq.bounds
			if !split {
				bounds = slices.Clone(bounds)
				for i := range bounds {
					bounds[i].Ranges = nil
				}
			}
			var part, err = renderQueryTemplate(sq.table, sq.tpl, bounds, objAndArrayAsJson)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		return strings.Join(parts, "\nUNION ALL\n") + ";", nil
	}

	if query, err := render(true); err != nil || len(query) <= limit {
		return query, err
	}
	return render(false)
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
		cast[i] = fmt.Sprintf("CAST(%s AS %s)", l, key.BareDDL)
	}
	return fmt.Sprintf("%s(%s)", fn, strings.Join(cast, ", "))
}
