package connector

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/go/blob"
	"github.com/estuary/connectors/go/common"
	realBoilerplate "github.com/estuary/connectors/materialize-boilerplate"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/estuary/connectors/materialize-iceberg/python"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/klauspost/compress/gzip"
	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/require"
)

// TestPartitionedTable answers whether a table whose partition spec was added
// behind the connector's back keeps working, and whether that spec buys the
// load anything. It creates a table through the connector, adds a bucket
// partition on the key with Spark SQL, and then runs the connector's own
// merge and load queries through the same PySpark entry points the EMR path
// uses, against both Spark sets of the compose stack.
//
// What it pins:
//
//   - merges into the partitioned table succeed, and every data file they
//     write carries the bucket partition value under the new spec;
//   - loads return the right documents;
//   - a schema update through the connector's apply path leaves the spec in
//     place;
//   - Spark attaches a dynamic partition pruning runtime filter to the
//     connector's load query on the partitioned table and not on an
//     unpartitioned twin. That filter is what Iceberg's SparkBatchQueryScan
//     turns into dropped file tasks at execution time, so its presence in the
//     plan is the evidence that the load prunes.
func TestPartitionedTable(t *testing.T) {
	forEachLocalConfig(t, func(t *testing.T, _ string, cfg config) {
		runPartitionedTable(t, cfg)
	})
}

// partitionBuckets is small so that fifty keys populate every bucket, which
// keeps the runtime filter from being trivially satisfied.
const partitionBuckets = 8

func runPartitionedTable(t *testing.T, cfg config) {
	h := newPartitionHarness(t, cfg, "acmeCo/tests/partitioned")
	suffix := fmt.Sprintf("_flow_test_%d", time.Now().Unix())

	partitionSpec := func(path []string) string {
		tbl, err := h.mat.catalog.GetTable(h.ctx, path[0], path[1])
		require.NoError(t, err)
		spec := tbl.Metadata.PartitionSpec()
		return spec.String()
	}

	// The control table stays unpartitioned so the plans can be compared.
	partitioned, partitionedPath := h.createTable("partitioned" + suffix)
	control, _ := h.createTable("control" + suffix)

	h.exec(fmt.Sprintf("ALTER TABLE %s ADD PARTITION FIELD bucket(%d, `id`)", fqn(partitionedPath), partitionBuckets))
	require.Contains(t, partitionSpec(partitionedPath), fmt.Sprintf("bucket[%d]", partitionBuckets), "the catalog must accept the spec change")

	// rows are the fifty documents merged into both tables, keyed k00..k49.
	var rows []partitionRow
	for i := range 50 {
		rows = append(rows, partitionRow{id: fmt.Sprintf("k%02d", i), n: int64(i)})
	}

	h.merge(partitioned, rows)
	h.merge(control, rows)

	// Every file the merge wrote belongs to the bucket spec, which is spec
	// 1 after the unpartitioned spec 0 the table was created with.
	files := h.query(fmt.Sprintf("SELECT spec_id, partition.id_bucket_%d FROM %s.files", partitionBuckets, fqn(partitionedPath)))
	require.NotEmpty(t, files)
	bucketsSeen := map[string]struct{}{}
	for _, f := range files {
		require.JSONEq(t, "1", string(f[0].(json.RawMessage)), "data file written under the unpartitioned spec")
		require.NotNil(t, f[1], "data file without a bucket partition value")
		bucketsSeen[string(f[1].(json.RawMessage))] = struct{}{}
	}
	require.Len(t, bucketsSeen, partitionBuckets, "fifty keys should populate every bucket")

	// A load of three keys returns exactly those documents.
	wantIDs := []string{"k03", "k17", "k42"}
	var gotIDs []string
	for id := range h.load(partitioned, wantIDs) {
		gotIDs = append(gotIDs, id)
	}
	require.ElementsMatch(t, wantIDs, gotIDs)

	// The same load query gets a dynamic partition pruning filter on the
	// partitioned table only. A filter Spark later decides not to plan
	// prints as dynamicpruningexpression(true), so the assertion requires
	// the IN form that carries the keys.
	pruned := regexp.MustCompile(`dynamicpruningexpression\([^)]*\bIN\b`)
	query, lb := h.loadQuery(partitioned, wantIDs)
	plan := strings.Join(h.runLoad("EXPLAIN "+query, lb), "\n")
	require.Regexp(t, pruned, plan, "no runtime filter on the partitioned table's load:\n%s", plan)
	for _, line := range strings.Split(plan, "\n") {
		if pruned.MatchString(line) {
			t.Logf("partitioned table scan: %s", strings.TrimSpace(line))
		}
	}

	controlQuery, controlLB := h.loadQuery(control, wantIDs)
	controlPlan := strings.Join(h.runLoad("EXPLAIN "+controlQuery, controlLB), "\n")
	require.NotContains(t, controlPlan, "dynamicpruningexpression(", "unexpected runtime filter on the unpartitioned table's load:\n%s", controlPlan)

	// A second merge updates every row, which is the copy-on-write rewrite
	// of bucketed files, and the table reads back with the new values.
	var updated []partitionRow
	for _, r := range rows {
		updated = append(updated, partitionRow{id: r.id, n: r.n + 100})
	}
	h.merge(partitioned, updated)
	got := h.query(fmt.Sprintf("SELECT `id`, `n` FROM %s ORDER BY `id`", fqn(partitionedPath)))
	require.Len(t, got, len(updated))
	for i, r := range got {
		require.JSONEq(t, fmt.Sprintf("%q", updated[i].id), string(r[0].(json.RawMessage)))
		require.JSONEq(t, fmt.Sprintf("%d", updated[i].n), string(r[1].(json.RawMessage)))
	}

	// Adding a column through the connector's apply path commits a new
	// schema, and the spec it did not ask for survives that commit.
	tbl, err := h.mat.catalog.GetTable(h.ctx, partitionedPath[0], partitionedPath[1])
	require.NoError(t, err)
	_, apply, err := h.mat.UpdateResource(h.ctx, partitionedPath, realBoilerplate.ExistingResource{Meta: tbl.Metadata}, realBoilerplate.BindingUpdate[config, resource, mapped]{
		Binding:        partitioned,
		NewProjections: []realBoilerplate.MappedProjection[mapped]{h.project("extra", "/extra", []string{"string"}, false)},
	})
	require.NoError(t, err)
	require.NoError(t, apply(h.ctx))
	require.Contains(t, partitionSpec(partitionedPath), fmt.Sprintf("bucket[%d]", partitionBuckets), "the spec must survive a schema update")
	t.Logf("partition spec after the schema update: %s", partitionSpec(partitionedPath))
}

// forEachLocalConfig runs fn once per Spark set of the compose stack.
func forEachLocalConfig(t *testing.T, fn func(t *testing.T, name string, cfg config)) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ensureDockerUp(t)

	for name, configPath := range localConfigTemplates {
		name := strings.TrimSuffix(strings.TrimPrefix(path.Base(name), "config."), ".yaml")
		t.Run(name, func(t *testing.T) {
			raw, err := boilerplate.ReadConfigFile(configPath)
			require.NoError(t, err)
			var cfg config
			require.NoError(t, json.Unmarshal(raw, &cfg))
			fn(t, name, cfg)
		})
	}
}

type partitionBinding = realBoilerplate.MappedBinding[config, resource, mapped]

// partitionRow is a document {"id": id, "n": n}.
type partitionRow struct {
	id string
	n  int64
}

// partitionHarness runs the connector's own merge and load queries against
// tables it creates through the connector, on the local Spark daemon.
type partitionHarness struct {
	t   *testing.T
	ctx context.Context
	cfg config
	mat *materialization
}

func newPartitionHarness(t *testing.T, cfg config, name string) *partitionHarness {
	ctx := context.Background()
	flags := common.ResolveFlags(cfg.Advanced.FeatureFlags, featureFlagDefaults, common.CreatedAt{})
	matIface, err := NewMaterializer(ctx, name, cfg, flags)
	require.NoError(t, err)
	mat := matIface.(*materialization)
	_, ok := mat.compute.(*sparkClient)
	require.True(t, ok, "this test drives the local spark daemon")

	// Setup stages the PySpark scripts and records their locations, which
	// runJob needs to pick its entry point.
	_, err = mat.Setup(ctx, nil)
	require.NoError(t, err)

	return &partitionHarness{t: t, ctx: ctx, cfg: cfg, mat: mat}
}

func (h *partitionHarness) project(field, ptr string, types []string, isKey bool) realBoilerplate.MappedProjection[mapped] {
	p := realBoilerplate.MapProjection(pf.Projection{
		Field:        field,
		Ptr:          ptr,
		IsPrimaryKey: isKey,
		Inference:    pf.Inference{Types: types, Exists: pf.Inference_MUST},
	}, fieldConfig{})
	mt, _ := h.mat.MapType(p, fieldConfig{})
	return realBoilerplate.MappedProjection[mapped]{Projection: p, Mapped: mt}
}

// createTable creates a table keyed on a string id with an integer n, and
// drops it when the test ends.
func (h *partitionHarness) createTable(table string) (partitionBinding, []string) {
	res := resource{Table: table}.WithDefaults(h.cfg)
	path, _, err := res.Parameters()
	require.NoError(h.t, err)
	doc := h.project("flow_document", "", []string{"object"}, false)
	mb := partitionBinding{
		MaterializationSpec_Binding: pf.MaterializationSpec_Binding{ResourcePath: path},
		Config:                      res,
		Keys:                        []realBoilerplate.MappedProjection[mapped]{h.project("id", "/id", []string{"string"}, true)},
		Values:                      []realBoilerplate.MappedProjection[mapped]{h.project("n", "/n", []string{"integer"}, false)},
		Document:                    &doc,
	}

	_, err = h.mat.CreateNamespace(h.ctx, path[0])
	require.NoError(h.t, err)
	_, apply, err := h.mat.CreateResource(h.ctx, mb)
	require.NoError(h.t, err)
	require.NoError(h.t, apply(h.ctx))
	h.t.Cleanup(func() {
		_, del, err := h.mat.DeleteResource(context.Background(), path)
		require.NoError(h.t, err)
		_ = del(context.Background())
	})
	return mb, path
}

func fqn(path []string) string {
	return fmt.Sprintf("`estuary`.%s.%s", quoteIdentifier(path[0]), quoteIdentifier(path[1]))
}

func (h *partitionHarness) exec(query string) {
	require.NoError(h.t, h.mat.compute.(*sparkClient).postDaemon(h.ctx, "exec", python.ExecInput{Query: query}, &daemonResult{}), query)
}

func (h *partitionHarness) query(query string) [][]any {
	_, rows, err := h.mat.compute.(rowQuerier).queryRows(h.ctx, query)
	require.NoError(h.t, err, query)
	return rows
}

func (h *partitionHarness) stage(fields []string, values [][]any) string {
	key := stagedFileClient{}.NewKey([]string{h.cfg.Compute.BucketPath, "partition-" + uuid.NewString(), "staged"})
	w := stagedFileClient{}.NewWriter(h.mat.bucket.NewWriter(h.ctx, key), fields)
	for _, v := range values {
		require.NoError(h.t, w.Write(v))
	}
	require.NoError(h.t, w.Close())
	return h.mat.bucket.URI(key)
}

func columnsOf(mb partitionBinding) (cols, keys []python.NestedField) {
	for _, p := range mb.SelectedProjections() {
		f := python.NestedField{Name: p.Field, Type: p.Mapped.type_.Type()}
		if p.IsPrimaryKey {
			keys = append(keys, f)
		}
		cols = append(cols, f)
	}
	return cols, keys
}

func boundsOf(mb partitionBinding, ids []string) []mergeBound {
	b := newMergeBoundsBuilder(mb.Keys)
	for _, id := range ids {
		b.nextKey([]any{id})
	}
	return b.build()
}

// merge runs the connector's merge query for the rows exactly as
// Acknowledge would, through the merge entry point. Rows must be sorted by
// id, as the connector sends them.
func (h *partitionHarness) merge(mb partitionBinding, rows []partitionRow) {
	cols, _ := columnsOf(mb)
	var values [][]any
	var ids []string
	for _, r := range rows {
		values = append(values, []any{r.id, r.n, fmt.Sprintf(`{"id":%q,"n":%d}`, r.id, r.n)})
		ids = append(ids, r.id)
	}
	var q strings.Builder
	require.NoError(h.t, h.mat.templates.mergeQuery.Execute(&q, templateInput{
		binding: binding{Idx: 0, Mapped: &mb},
		Bounds:  boundsOf(mb, ids),
	}))
	require.NoError(h.t, h.mat.compute.runJob(h.ctx, computeJob{
		Input: python.MergeInput{Bindings: []python.MergeBinding{{
			Binding:          0,
			Query:            q.String(),
			Columns:          cols,
			Files:            []string{h.stage([]string{"id", "n", "flow_document"}, values)},
			IdempotencyToken: uuid.NewString(),
		}}},
		EntryPointURI:    h.mat.pyFiles.merge,
		PyFilesCommonURI: h.mat.pyFiles.common,
		Name:             "merge",
		WorkingPrefix:    path.Join(h.cfg.Compute.BucketPath, uuid.NewString()),
	}))
}

// loadQuery renders the connector's load query for the given keys, with
// the staged key file the query's temporary view reads.
func (h *partitionHarness) loadQuery(mb partitionBinding, ids []string) (string, python.LoadBinding) {
	_, keys := columnsOf(mb)
	var values [][]any
	for _, id := range ids {
		values = append(values, []any{id})
	}
	var q strings.Builder
	require.NoError(h.t, h.mat.templates.loadQuery.Execute(&q, templateInput{
		binding: binding{Idx: 0, Mapped: &mb},
		Bounds:  boundsOf(mb, ids),
	}))
	return q.String(), python.LoadBinding{Binding: 0, Keys: keys, Files: []string{h.stage([]string{"id"}, values)}}
}

// runLoad runs a query through the load entry point and returns the
// lines of its output files, so it serves both a real load and an
// EXPLAIN of one.
func (h *partitionHarness) runLoad(query string, lb python.LoadBinding) []string {
	outputPrefix := path.Join(h.cfg.Compute.BucketPath, uuid.NewString())
	require.NoError(h.t, h.mat.compute.runJob(h.ctx, computeJob{
		Input: python.LoadInput{
			Query:          query,
			Bindings:       []python.LoadBinding{lb},
			OutputLocation: "s3://" + path.Join(h.cfg.Compute.Bucket, outputPrefix),
		},
		EntryPointURI:    h.mat.pyFiles.load,
		PyFilesCommonURI: h.mat.pyFiles.common,
		Name:             "load",
		WorkingPrefix:    outputPrefix,
	}))

	var lines []string
	for obj := range h.mat.bucket.List(h.ctx, blob.Query{Prefix: outputPrefix}) {
		if !strings.HasPrefix(path.Base(obj.Key), "part-") {
			continue
		}
		r, err := h.mat.bucket.NewReader(h.ctx, obj.Key)
		require.NoError(h.t, err)
		gzr, err := gzip.NewReader(r)
		require.NoError(h.t, err)
		scanner := bufio.NewScanner(gzr)
		scanner.Buffer(make([]byte, 0, 64*1024), math.MaxInt64)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		require.NoError(h.t, scanner.Err())
		require.NoError(h.t, r.Close())
	}
	return lines
}

// load runs the connector's load for the keys and returns n by id. It fails
// the test if the load returns any id more than once.
func (h *partitionHarness) load(mb partitionBinding, ids []string) map[string]int64 {
	got := map[string]int64{}
	for _, line := range h.runLoad(h.loadQuery(mb, ids)) {
		idx, doc, found := strings.Cut(line, "\x00")
		require.True(h.t, found, "load output line without a binding separator: %q", line)
		require.Equal(h.t, "0", idx)
		var parsed struct {
			ID string `json:"id"`
			N  int64  `json:"n"`
		}
		require.NoError(h.t, json.Unmarshal([]byte(doc), &parsed), doc)
		require.NotContains(h.t, got, parsed.ID, "load returned a key twice")
		got[parsed.ID] = parsed.N
	}
	return got
}
