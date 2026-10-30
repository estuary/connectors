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
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ensureDockerUp(t)

	for name, configPath := range localConfigTemplates {
		var name = strings.TrimSuffix(strings.TrimPrefix(path.Base(name), "config."), ".yaml")
		t.Run(name, func(t *testing.T) {
			var raw, err = boilerplate.ReadConfigFile(configPath)
			require.NoError(t, err)
			var cfg config
			require.NoError(t, json.Unmarshal(raw, &cfg))
			runPartitionedTable(t, cfg)
		})
	}
}

// partitionBuckets is small so that fifty keys populate every bucket, which
// keeps the runtime filter from being trivially satisfied.
const partitionBuckets = 8

func runPartitionedTable(t *testing.T, cfg config) {
	var ctx = context.Background()

	var flags = common.ResolveFlags(cfg.Advanced.FeatureFlags, featureFlagDefaults, common.CreatedAt{})
	var matIface, err = NewMaterializer(ctx, "acmeCo/tests/partitioned", cfg, flags)
	require.NoError(t, err)
	var mat = matIface.(*materialization)
	var spark, ok = mat.compute.(*sparkClient)
	require.True(t, ok, "this test drives the local spark daemon")

	// Setup stages the PySpark scripts and records their locations, which
	// runJob needs to pick its entry point.
	_, err = mat.Setup(ctx, nil)
	require.NoError(t, err)

	var suffix = fmt.Sprintf("_flow_test_%d", time.Now().Unix())

	var project = func(field, ptr string, types []string, isKey bool) realBoilerplate.MappedProjection[mapped] {
		var p = realBoilerplate.MapProjection(pf.Projection{
			Field:        field,
			Ptr:          ptr,
			IsPrimaryKey: isKey,
			Inference:    pf.Inference{Types: types, Exists: pf.Inference_MUST},
		}, fieldConfig{})
		var mt, _ = mat.MapType(p, fieldConfig{})
		return realBoilerplate.MappedProjection[mapped]{Projection: p, Mapped: mt}
	}
	var newBinding = func(table string) (realBoilerplate.MappedBinding[config, resource, mapped], []string) {
		var res = resource{Table: table}.WithDefaults(cfg)
		var path, _, err = res.Parameters()
		require.NoError(t, err)
		var doc = project("flow_document", "", []string{"object"}, false)
		return realBoilerplate.MappedBinding[config, resource, mapped]{
			MaterializationSpec_Binding: pf.MaterializationSpec_Binding{ResourcePath: path},
			Config:                      res,
			Keys:                        []realBoilerplate.MappedProjection[mapped]{project("id", "/id", []string{"string"}, true)},
			Values:                      []realBoilerplate.MappedProjection[mapped]{project("n", "/n", []string{"integer"}, false)},
			Document:                    &doc,
		}, path
	}
	var create = func(mb realBoilerplate.MappedBinding[config, resource, mapped], path []string) {
		var _, err = mat.CreateNamespace(ctx, path[0])
		require.NoError(t, err)
		_, apply, err := mat.CreateResource(ctx, mb)
		require.NoError(t, err)
		require.NoError(t, apply(ctx))
		t.Cleanup(func() {
			var _, del, err = mat.DeleteResource(context.Background(), path)
			require.NoError(t, err)
			_ = del(context.Background())
		})
	}
	var fqn = func(path []string) string {
		return fmt.Sprintf("`estuary`.%s.%s", quoteIdentifier(path[0]), quoteIdentifier(path[1]))
	}
	var sparkExec = func(query string) error {
		return spark.postDaemon(ctx, "exec", python.ExecInput{Query: query}, &daemonResult{})
	}
	var partitionSpec = func(path []string) string {
		var tbl, err = mat.catalog.GetTable(ctx, path[0], path[1])
		require.NoError(t, err)
		var spec = tbl.Metadata.PartitionSpec()
		return spec.String()
	}

	// The control table stays unpartitioned so the plans can be compared.
	var partitioned, partitionedPath = newBinding("partitioned" + suffix)
	var control, controlPath = newBinding("control" + suffix)
	create(partitioned, partitionedPath)
	create(control, controlPath)

	require.NoError(t, sparkExec(fmt.Sprintf("ALTER TABLE %s ADD PARTITION FIELD bucket(%d, `id`)", fqn(partitionedPath), partitionBuckets)))
	require.Contains(t, partitionSpec(partitionedPath), fmt.Sprintf("bucket[%d]", partitionBuckets), "the catalog must accept the spec change")

	// rows are the fifty documents merged into both tables, keyed k00..k49.
	type row struct {
		id  string
		n   int64
		doc string
	}
	var rows []row
	for i := range 50 {
		var id = fmt.Sprintf("k%02d", i)
		rows = append(rows, row{id: id, n: int64(i), doc: fmt.Sprintf(`{"id":%q,"n":%d}`, id, i)})
	}

	var stage = func(fields []string, values [][]any) string {
		var key = stagedFileClient{}.NewKey([]string{cfg.Compute.BucketPath, "partition-" + uuid.NewString(), "staged"})
		var w = stagedFileClient{}.NewWriter(mat.bucket.NewWriter(ctx, key), fields)
		for _, v := range values {
			require.NoError(t, w.Write(v))
		}
		require.NoError(t, w.Close())
		return mat.bucket.URI(key)
	}
	var columnsOf = func(mb realBoilerplate.MappedBinding[config, resource, mapped]) (cols, keys []python.NestedField) {
		for _, p := range mb.SelectedProjections() {
			var f = python.NestedField{Name: p.Field, Type: p.Mapped.type_.Type()}
			if p.IsPrimaryKey {
				keys = append(keys, f)
			}
			cols = append(cols, f)
		}
		return cols, keys
	}
	var boundsOf = func(mb realBoilerplate.MappedBinding[config, resource, mapped], ids []string) []mergeBound {
		var b = newMergeBoundsBuilder(mb.Keys)
		for _, id := range ids {
			b.nextKey([]any{id})
		}
		return b.build()
	}

	// merge runs the connector's merge query for the rows exactly as
	// Acknowledge would, through the merge entry point.
	var merge = func(mb realBoilerplate.MappedBinding[config, resource, mapped], rows []row) {
		var cols, _ = columnsOf(mb)
		var values [][]any
		var ids []string
		for _, r := range rows {
			values = append(values, []any{r.id, r.n, r.doc})
			ids = append(ids, r.id)
		}
		var q strings.Builder
		require.NoError(t, mat.templates.mergeQuery.Execute(&q, templateInput{
			binding: binding{Idx: 0, Mapped: &mb},
			Bounds:  boundsOf(mb, ids),
		}))
		require.NoError(t, mat.compute.runJob(ctx, computeJob{
			Input: python.MergeInput{Bindings: []python.MergeBinding{{
				Binding:          0,
				Query:            q.String(),
				Columns:          cols,
				Files:            []string{stage([]string{"id", "n", "flow_document"}, values)},
				IdempotencyToken: uuid.NewString(),
			}}},
			EntryPointURI:    mat.pyFiles.merge,
			PyFilesCommonURI: mat.pyFiles.common,
			Name:             "merge",
			WorkingPrefix:    path.Join(cfg.Compute.BucketPath, uuid.NewString()),
		}))
	}

	// loadQuery renders the connector's load query for the given keys, with
	// the staged key file the query's temporary view reads.
	var loadQuery = func(mb realBoilerplate.MappedBinding[config, resource, mapped], ids []string) (string, python.LoadBinding) {
		var _, keys = columnsOf(mb)
		var values [][]any
		for _, id := range ids {
			values = append(values, []any{id})
		}
		var q strings.Builder
		require.NoError(t, mat.templates.loadQuery.Execute(&q, templateInput{
			binding: binding{Idx: 0, Mapped: &mb},
			Bounds:  boundsOf(mb, ids),
		}))
		return q.String(), python.LoadBinding{Binding: 0, Keys: keys, Files: []string{stage([]string{"id"}, values)}}
	}

	// runLoad runs a query through the load entry point and returns the
	// lines of its output files, so it serves both a real load and an
	// EXPLAIN of one.
	var runLoad = func(query string, lb python.LoadBinding) []string {
		var outputPrefix = path.Join(cfg.Compute.BucketPath, uuid.NewString())
		require.NoError(t, mat.compute.runJob(ctx, computeJob{
			Input: python.LoadInput{
				Query:          query,
				Bindings:       []python.LoadBinding{lb},
				OutputLocation: "s3://" + path.Join(cfg.Compute.Bucket, outputPrefix),
			},
			EntryPointURI:    mat.pyFiles.load,
			PyFilesCommonURI: mat.pyFiles.common,
			Name:             "load",
			WorkingPrefix:    outputPrefix,
		}))

		var lines []string
		for obj := range mat.bucket.List(ctx, blob.Query{Prefix: outputPrefix}) {
			if !strings.HasPrefix(path.Base(obj.Key), "part-") {
				continue
			}
			var r, err = mat.bucket.NewReader(ctx, obj.Key)
			require.NoError(t, err)
			gzr, err := gzip.NewReader(r)
			require.NoError(t, err)
			var scanner = bufio.NewScanner(gzr)
			scanner.Buffer(make([]byte, 0, 64*1024), math.MaxInt64)
			for scanner.Scan() {
				lines = append(lines, scanner.Text())
			}
			require.NoError(t, scanner.Err())
			require.NoError(t, r.Close())
		}
		return lines
	}

	merge(partitioned, rows)
	merge(control, rows)

	// Every file the merge wrote belongs to the bucket spec, which is spec
	// 1 after the unpartitioned spec 0 the table was created with.
	var querier = mat.compute.(rowQuerier)
	_, files, err := querier.queryRows(ctx, fmt.Sprintf("SELECT spec_id, partition.id_bucket_%d FROM %s.files", partitionBuckets, fqn(partitionedPath)))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	var bucketsSeen = map[string]struct{}{}
	for _, f := range files {
		require.JSONEq(t, "1", string(f[0].(json.RawMessage)), "data file written under the unpartitioned spec")
		require.NotNil(t, f[1], "data file without a bucket partition value")
		bucketsSeen[string(f[1].(json.RawMessage))] = struct{}{}
	}
	require.Len(t, bucketsSeen, partitionBuckets, "fifty keys should populate every bucket")

	// A load of three keys returns exactly those documents.
	var wantIDs = []string{"k03", "k17", "k42"}
	var query, lb = loadQuery(partitioned, wantIDs)
	var gotIDs []string
	for _, line := range runLoad(query, lb) {
		var idx, doc, found = strings.Cut(line, "\x00")
		require.True(t, found, "load output line without a binding separator: %q", line)
		require.Equal(t, "0", idx)
		var parsed struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.Unmarshal([]byte(doc), &parsed), doc)
		gotIDs = append(gotIDs, parsed.ID)
	}
	require.ElementsMatch(t, wantIDs, gotIDs)

	// The same load query gets a dynamic partition pruning filter on the
	// partitioned table only. A filter Spark later decides not to plan
	// prints as dynamicpruningexpression(true), so the assertion requires
	// the IN form that carries the keys.
	var pruned = regexp.MustCompile(`dynamicpruningexpression\([^)]*\bIN\b`)
	var plan = strings.Join(runLoad("EXPLAIN "+query, lb), "\n")
	require.Regexp(t, pruned, plan, "no runtime filter on the partitioned table's load:\n%s", plan)
	for _, line := range strings.Split(plan, "\n") {
		if pruned.MatchString(line) {
			t.Logf("partitioned table scan: %s", strings.TrimSpace(line))
		}
	}

	var controlQuery, controlLB = loadQuery(control, wantIDs)
	var controlPlan = strings.Join(runLoad("EXPLAIN "+controlQuery, controlLB), "\n")
	require.NotContains(t, controlPlan, "dynamicpruningexpression(", "unexpected runtime filter on the unpartitioned table's load:\n%s", controlPlan)

	// A second merge updates every row, which is the copy-on-write rewrite
	// of bucketed files, and the table reads back with the new values.
	var updated []row
	for _, r := range rows {
		updated = append(updated, row{id: r.id, n: r.n + 100, doc: fmt.Sprintf(`{"id":%q,"n":%d}`, r.id, r.n+100)})
	}
	merge(partitioned, updated)
	_, got, err := querier.queryRows(ctx, fmt.Sprintf("SELECT `id`, `n` FROM %s ORDER BY `id`", fqn(partitionedPath)))
	require.NoError(t, err)
	require.Len(t, got, len(updated))
	for i, r := range got {
		require.JSONEq(t, fmt.Sprintf("%q", updated[i].id), string(r[0].(json.RawMessage)))
		require.JSONEq(t, fmt.Sprintf("%d", updated[i].n), string(r[1].(json.RawMessage)))
	}

	// Adding a column through the connector's apply path commits a new
	// schema, and the spec it did not ask for survives that commit.
	tbl, err := mat.catalog.GetTable(ctx, partitionedPath[0], partitionedPath[1])
	require.NoError(t, err)
	_, apply, err := mat.UpdateResource(ctx, partitionedPath, realBoilerplate.ExistingResource{Meta: tbl.Metadata}, realBoilerplate.BindingUpdate[config, resource, mapped]{
		Binding:        partitioned,
		NewProjections: []realBoilerplate.MappedProjection[mapped]{project("extra", "/extra", []string{"string"}, false)},
	})
	require.NoError(t, err)
	require.NoError(t, apply(ctx))
	require.Contains(t, partitionSpec(partitionedPath), fmt.Sprintf("bucket[%d]", partitionBuckets), "the spec must survive a schema update")
	t.Logf("partition spec after the schema update: %s", partitionSpec(partitionedPath))
}
