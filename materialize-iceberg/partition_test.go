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
	ctx := t.Context()
	h, err := newPartitionHarness(ctx, cfg, "acmeCo/tests/partitioned")
	require.NoError(t, err)
	suffix := fmt.Sprintf("_flow_test_%d", time.Now().Unix())

	partitionSpec := func(path []string) string {
		tbl, err := h.mat.catalog.GetTable(ctx, path[0], path[1])
		require.NoError(t, err)
		spec := tbl.Metadata.PartitionSpec()
		return spec.String()
	}
	query := func(q string) [][]any {
		rows, err := h.query(ctx, q)
		require.NoError(t, err, q)
		return rows
	}
	merge := func(mb partitionBinding, rows []partitionRow) {
		require.NoError(t, h.merge(ctx, mb, rows))
	}
	explainLoad := func(mb partitionBinding, ids []string) string {
		q, lb, err := h.loadQuery(ctx, mb, ids)
		require.NoError(t, err)
		lines, err := h.runLoad(ctx, "EXPLAIN "+q, lb)
		require.NoError(t, err)
		return strings.Join(lines, "\n")
	}

	// The control table stays unpartitioned so the plans can be compared.
	partitioned, partitionedPath := createPartitionTable(t, h, "partitioned"+suffix)
	control, _ := createPartitionTable(t, h, "control"+suffix)

	require.NoError(t, h.exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD PARTITION FIELD bucket(%d, `id`)", fqn(partitionedPath), partitionBuckets)))
	require.Contains(t, partitionSpec(partitionedPath), fmt.Sprintf("bucket[%d]", partitionBuckets), "the catalog must accept the spec change")

	// rows are the fifty documents merged into both tables, keyed k00..k49.
	var rows []partitionRow
	for i := range 50 {
		rows = append(rows, partitionRow{id: fmt.Sprintf("k%02d", i), n: int64(i)})
	}

	merge(partitioned, rows)
	merge(control, rows)

	// Every file the merge wrote belongs to the bucket spec, which is spec
	// 1 after the unpartitioned spec 0 the table was created with.
	files := query(fmt.Sprintf("SELECT spec_id, partition.id_bucket_%d FROM %s.files", partitionBuckets, fqn(partitionedPath)))
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
	loaded, err := h.load(ctx, partitioned, wantIDs)
	require.NoError(t, err)
	var gotIDs []string
	for id := range loaded {
		gotIDs = append(gotIDs, id)
	}
	require.ElementsMatch(t, wantIDs, gotIDs)

	// The same load query gets a dynamic partition pruning filter on the
	// partitioned table only. A filter Spark later decides not to plan
	// prints as dynamicpruningexpression(true), so the assertion requires
	// the IN form that carries the keys.
	pruned := regexp.MustCompile(`dynamicpruningexpression\([^)]*\bIN\b`)
	plan := explainLoad(partitioned, wantIDs)
	require.Regexp(t, pruned, plan, "no runtime filter on the partitioned table's load:\n%s", plan)
	for _, line := range strings.Split(plan, "\n") {
		if pruned.MatchString(line) {
			t.Logf("partitioned table scan: %s", strings.TrimSpace(line))
		}
	}

	controlPlan := explainLoad(control, wantIDs)
	require.NotContains(t, controlPlan, "dynamicpruningexpression(", "unexpected runtime filter on the unpartitioned table's load:\n%s", controlPlan)

	// A second merge updates every row, which is the copy-on-write rewrite
	// of bucketed files, and the table reads back with the new values.
	var updated []partitionRow
	for _, r := range rows {
		updated = append(updated, partitionRow{id: r.id, n: r.n + 100})
	}
	merge(partitioned, updated)
	got := query(fmt.Sprintf("SELECT `id`, `n` FROM %s ORDER BY `id`", fqn(partitionedPath)))
	require.Len(t, got, len(updated))
	for i, r := range got {
		require.JSONEq(t, fmt.Sprintf("%q", updated[i].id), string(r[0].(json.RawMessage)))
		require.JSONEq(t, fmt.Sprintf("%d", updated[i].n), string(r[1].(json.RawMessage)))
	}

	// Adding a column through the connector's apply path commits a new
	// schema, and the spec it did not ask for survives that commit.
	tbl, err := h.mat.catalog.GetTable(ctx, partitionedPath[0], partitionedPath[1])
	require.NoError(t, err)
	_, apply, err := h.mat.UpdateResource(ctx, partitionedPath, realBoilerplate.ExistingResource{Meta: tbl.Metadata}, realBoilerplate.BindingUpdate[config, resource, mapped]{
		Binding:        partitioned,
		NewProjections: []realBoilerplate.MappedProjection[mapped]{h.project("extra", "/extra", []string{"string"}, false)},
	})
	require.NoError(t, err)
	require.NoError(t, apply(ctx))
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

// createPartitionTable creates a table through the harness and drops it when
// the test ends.
func createPartitionTable(t *testing.T, h *partitionHarness, table string) (partitionBinding, []string) {
	mb, path, err := h.createTable(t.Context(), table)
	require.NoError(t, err)
	t.Cleanup(func() {
		// The test's context is already canceled when cleanups run.
		ctx := context.Background()
		_, del, err := h.mat.DeleteResource(ctx, path)
		require.NoError(t, err)
		_ = del(ctx)
	})
	return mb, path
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
	cfg config
	mat *materialization
}

func newPartitionHarness(ctx context.Context, cfg config, name string) (*partitionHarness, error) {
	flags := common.ResolveFlags(cfg.Advanced.FeatureFlags, featureFlagDefaults, common.CreatedAt{})
	matIface, err := NewMaterializer(ctx, name, cfg, flags)
	if err != nil {
		return nil, err
	}
	mat := matIface.(*materialization)
	if _, ok := mat.compute.(*sparkClient); !ok {
		return nil, fmt.Errorf("this test drives the local spark daemon, got compute %T", mat.compute)
	}

	// Setup stages the PySpark scripts and records their locations, which
	// runJob needs to pick its entry point.
	if _, err := mat.Setup(ctx, nil); err != nil {
		return nil, err
	}

	return &partitionHarness{cfg: cfg, mat: mat}, nil
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

// createTable creates a table keyed on a string id with an integer n.
func (h *partitionHarness) createTable(ctx context.Context, table string) (partitionBinding, []string, error) {
	res := resource{Table: table}.WithDefaults(h.cfg)
	path, _, err := res.Parameters()
	if err != nil {
		return partitionBinding{}, nil, err
	}
	doc := h.project("flow_document", "", []string{"object"}, false)
	mb := partitionBinding{
		MaterializationSpec_Binding: pf.MaterializationSpec_Binding{ResourcePath: path},
		Config:                      res,
		Keys:                        []realBoilerplate.MappedProjection[mapped]{h.project("id", "/id", []string{"string"}, true)},
		Values:                      []realBoilerplate.MappedProjection[mapped]{h.project("n", "/n", []string{"integer"}, false)},
		Document:                    &doc,
	}

	if _, err := h.mat.CreateNamespace(ctx, path[0]); err != nil {
		return partitionBinding{}, nil, err
	}
	_, apply, err := h.mat.CreateResource(ctx, mb)
	if err != nil {
		return partitionBinding{}, nil, err
	}
	if err := apply(ctx); err != nil {
		return partitionBinding{}, nil, err
	}
	return mb, path, nil
}

func fqn(path []string) string {
	return fmt.Sprintf("`estuary`.%s.%s", quoteIdentifier(path[0]), quoteIdentifier(path[1]))
}

func (h *partitionHarness) exec(ctx context.Context, query string) error {
	if err := h.mat.compute.(*sparkClient).postDaemon(ctx, "exec", python.ExecInput{Query: query}, &daemonResult{}); err != nil {
		return fmt.Errorf("%s: %w", query, err)
	}
	return nil
}

func (h *partitionHarness) query(ctx context.Context, query string) ([][]any, error) {
	_, rows, err := h.mat.compute.(rowQuerier).queryRows(ctx, query)
	return rows, err
}

func (h *partitionHarness) stage(ctx context.Context, fields []string, values [][]any) (string, error) {
	key := stagedFileClient{}.NewKey([]string{h.cfg.Compute.BucketPath, "partition-" + uuid.NewString(), "staged"})
	w := stagedFileClient{}.NewWriter(h.mat.bucket.NewWriter(ctx, key), fields)
	for _, v := range values {
		if err := w.Write(v); err != nil {
			return "", err
		}
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	return h.mat.bucket.URI(key), nil
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
func (h *partitionHarness) merge(ctx context.Context, mb partitionBinding, rows []partitionRow) error {
	cols, _ := columnsOf(mb)
	var values [][]any
	var ids []string
	for _, r := range rows {
		values = append(values, []any{r.id, r.n, fmt.Sprintf(`{"id":%q,"n":%d}`, r.id, r.n)})
		ids = append(ids, r.id)
	}
	var q strings.Builder
	if err := h.mat.templates.mergeQuery.Execute(&q, templateInput{
		binding: binding{Idx: 0, Mapped: &mb},
		Bounds:  boundsOf(mb, ids),
	}); err != nil {
		return err
	}
	file, err := h.stage(ctx, []string{"id", "n", "flow_document"}, values)
	if err != nil {
		return err
	}
	return h.mat.compute.runJob(ctx, computeJob{
		Input: python.MergeInput{Bindings: []python.MergeBinding{{
			Binding:          0,
			Query:            q.String(),
			Columns:          cols,
			Files:            []string{file},
			IdempotencyToken: uuid.NewString(),
		}}},
		EntryPointURI:    h.mat.pyFiles.merge,
		PyFilesCommonURI: h.mat.pyFiles.common,
		Name:             "merge",
		WorkingPrefix:    path.Join(h.cfg.Compute.BucketPath, uuid.NewString()),
	})
}

// loadQuery renders the connector's load query for the given keys, with
// the staged key file the query's temporary view reads.
func (h *partitionHarness) loadQuery(ctx context.Context, mb partitionBinding, ids []string) (string, python.LoadBinding, error) {
	_, keys := columnsOf(mb)
	var values [][]any
	for _, id := range ids {
		values = append(values, []any{id})
	}
	var q strings.Builder
	if err := h.mat.templates.loadQuery.Execute(&q, templateInput{
		binding: binding{Idx: 0, Mapped: &mb},
		Bounds:  boundsOf(mb, ids),
	}); err != nil {
		return "", python.LoadBinding{}, err
	}
	file, err := h.stage(ctx, []string{"id"}, values)
	if err != nil {
		return "", python.LoadBinding{}, err
	}
	return q.String(), python.LoadBinding{Binding: 0, Keys: keys, Files: []string{file}}, nil
}

// runLoad runs a query through the load entry point and returns the
// lines of its output files, so it serves both a real load and an
// EXPLAIN of one.
func (h *partitionHarness) runLoad(ctx context.Context, query string, lb python.LoadBinding) ([]string, error) {
	outputPrefix := path.Join(h.cfg.Compute.BucketPath, uuid.NewString())
	if err := h.mat.compute.runJob(ctx, computeJob{
		Input: python.LoadInput{
			Query:          query,
			Bindings:       []python.LoadBinding{lb},
			OutputLocation: "s3://" + path.Join(h.cfg.Compute.Bucket, outputPrefix),
		},
		EntryPointURI:    h.mat.pyFiles.load,
		PyFilesCommonURI: h.mat.pyFiles.common,
		Name:             "load",
		WorkingPrefix:    outputPrefix,
	}); err != nil {
		return nil, err
	}

	var lines []string
	for obj := range h.mat.bucket.List(ctx, blob.Query{Prefix: outputPrefix}) {
		if !strings.HasPrefix(path.Base(obj.Key), "part-") {
			continue
		}
		r, err := h.mat.bucket.NewReader(ctx, obj.Key)
		if err != nil {
			return nil, err
		}
		gzr, err := gzip.NewReader(r)
		if err != nil {
			r.Close()
			return nil, err
		}
		scanner := bufio.NewScanner(gzr)
		scanner.Buffer(make([]byte, 0, 64*1024), math.MaxInt64)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			r.Close()
			return nil, err
		}
		if err := r.Close(); err != nil {
			return nil, err
		}
	}
	return lines, nil
}

// load runs the connector's load for the keys and returns n by id. It
// returns an error if the load returns any id more than once.
func (h *partitionHarness) load(ctx context.Context, mb partitionBinding, ids []string) (map[string]int64, error) {
	q, lb, err := h.loadQuery(ctx, mb, ids)
	if err != nil {
		return nil, err
	}
	lines, err := h.runLoad(ctx, q, lb)
	if err != nil {
		return nil, err
	}
	got := map[string]int64{}
	for _, line := range lines {
		idx, doc, found := strings.Cut(line, "\x00")
		if !found {
			return nil, fmt.Errorf("load output line without a binding separator: %q", line)
		} else if idx != "0" {
			return nil, fmt.Errorf("load output line for binding %s, want 0: %q", idx, line)
		}
		var parsed struct {
			ID string `json:"id"`
			N  int64  `json:"n"`
		}
		if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
			return nil, fmt.Errorf("%s: %w", doc, err)
		}
		if _, dup := got[parsed.ID]; dup {
			return nil, fmt.Errorf("load returned key %q twice", parsed.ID)
		}
		got[parsed.ID] = parsed.N
	}
	return got, nil
}
