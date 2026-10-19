package connector

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"math/rand"
	"os/exec"
	"path"
	"regexp"
	"slices"
	"strconv"
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

// TestPartitionMigration migrates a table that already holds data under the
// unpartitioned spec the connector creates: ALTER TABLE ADD PARTITION FIELD,
// then rewrite_data_files, then the connector's load and merge. It reads the
// Spark driver log for Iceberg's own scan lines, since those are what a user
// can check on their own deployment.
func TestPartitionMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ensureDockerUp(t)

	containers := map[string]string{
		"rest-local":        "materialize-iceberg-spark-daemon-1",
		"rest-local-spark4": "materialize-iceberg-spark4-daemon-1",
	}
	for name, configPath := range localConfigTemplates {
		name := strings.TrimSuffix(strings.TrimPrefix(path.Base(name), "config."), ".yaml")
		t.Run(name, func(t *testing.T) {
			raw, err := boilerplate.ReadConfigFile(configPath)
			require.NoError(t, err)
			var cfg config
			require.NoError(t, json.Unmarshal(raw, &cfg))
			runPartitionMigration(t, cfg, containers[name])
		})
	}
}

const (
	migrateBuckets = 16
	migrateMerges  = 12
	migrateRows    = 200
)

func runPartitionMigration(t *testing.T, cfg config, container string) {
	ctx := context.Background()

	flags := common.ResolveFlags(cfg.Advanced.FeatureFlags, featureFlagDefaults, common.CreatedAt{})
	matIface, err := NewMaterializer(ctx, "acmeCo/tests/partition-migrate", cfg, flags)
	require.NoError(t, err)
	mat := matIface.(*materialization)
	spark, ok := mat.compute.(*sparkClient)
	require.True(t, ok, "this test drives the local spark daemon")
	_, err = mat.Setup(ctx, nil)
	require.NoError(t, err)

	project := func(field, ptr string, types []string, isKey bool) realBoilerplate.MappedProjection[mapped] {
		p := realBoilerplate.MapProjection(pf.Projection{
			Field:        field,
			Ptr:          ptr,
			IsPrimaryKey: isKey,
			Inference:    pf.Inference{Types: types, Exists: pf.Inference_MUST},
		}, fieldConfig{})
		mt, _ := mat.MapType(p, fieldConfig{})
		return realBoilerplate.MappedProjection[mapped]{Projection: p, Mapped: mt}
	}

	tableName := fmt.Sprintf("migrate_flow_test_%d", time.Now().Unix())
	res := resource{Table: tableName}.WithDefaults(cfg)
	tablePath, _, err := res.Parameters()
	require.NoError(t, err)
	doc := project("flow_document", "", []string{"object"}, false)
	mb := realBoilerplate.MappedBinding[config, resource, mapped]{
		MaterializationSpec_Binding: pf.MaterializationSpec_Binding{ResourcePath: tablePath},
		Config:                      res,
		Keys:                        []realBoilerplate.MappedProjection[mapped]{project("_id", "/_id", []string{"string"}, true)},
		Values:                      []realBoilerplate.MappedProjection[mapped]{project("n", "/n", []string{"integer"}, false)},
		Document:                    &doc,
	}

	_, err = mat.CreateNamespace(ctx, tablePath[0])
	require.NoError(t, err)
	_, apply, err := mat.CreateResource(ctx, mb)
	require.NoError(t, err)
	require.NoError(t, apply(ctx))
	t.Cleanup(func() {
		_, del, err := mat.DeleteResource(context.Background(), tablePath)
		require.NoError(t, err)
		_ = del(context.Background())
	})

	fqn := fmt.Sprintf("`estuary`.%s.%s", quoteIdentifier(tablePath[0]), quoteIdentifier(tablePath[1]))
	querier := mat.compute.(rowQuerier)
	sparkExec := func(query string) error {
		return spark.postDaemon(ctx, "exec", python.ExecInput{Query: query}, &daemonResult{})
	}
	scalar := func(query string) string {
		_, rows, err := querier.queryRows(ctx, query)
		require.NoError(t, err, query)
		require.Len(t, rows, 1, query)
		return strings.Trim(string(rows[0][0].(json.RawMessage)), `"`)
	}

	stage := func(fields []string, values [][]any) string {
		key := stagedFileClient{}.NewKey([]string{cfg.Compute.BucketPath, "migrate-" + uuid.NewString(), "staged"})
		w := stagedFileClient{}.NewWriter(mat.bucket.NewWriter(ctx, key), fields)
		for _, v := range values {
			require.NoError(t, w.Write(v))
		}
		require.NoError(t, w.Close())
		return mat.bucket.URI(key)
	}
	columns := func() (cols, keys []python.NestedField) {
		for _, p := range mb.SelectedProjections() {
			f := python.NestedField{Name: p.Field, Type: p.Mapped.type_.Type()}
			if p.IsPrimaryKey {
				keys = append(keys, f)
			}
			cols = append(cols, f)
		}
		return cols, keys
	}
	bounds := func(ids []string) []mergeBound {
		b := newMergeBoundsBuilder(mb.Keys)
		for _, id := range ids {
			b.nextKey([]any{id})
		}
		return b.build()
	}

	type row struct {
		id string
		n  int64
	}
	docOf := func(r row) string { return fmt.Sprintf(`{"_id":%q,"n":%d}`, r.id, r.n) }

	// The connector sends keys to merge and load in sorted order.
	merge := func(rows []row) {
		cols, _ := columns()
		var values [][]any
		var ids []string
		for _, r := range rows {
			values = append(values, []any{r.id, r.n, docOf(r)})
			ids = append(ids, r.id)
		}
		var q strings.Builder
		require.NoError(t, mat.templates.mergeQuery.Execute(&q, templateInput{binding: binding{Idx: 0, Mapped: &mb}, Bounds: bounds(ids)}))
		require.NoError(t, mat.compute.runJob(ctx, computeJob{
			Input: python.MergeInput{Bindings: []python.MergeBinding{{
				Binding: 0, Query: q.String(), Columns: cols,
				Files:            []string{stage([]string{"_id", "n", "flow_document"}, values)},
				IdempotencyToken: uuid.NewString(),
			}}},
			EntryPointURI: mat.pyFiles.merge, PyFilesCommonURI: mat.pyFiles.common,
			Name: "merge", WorkingPrefix: path.Join(cfg.Compute.BucketPath, uuid.NewString()),
		}))
	}

	load := func(ids []string) map[string]int64 {
		_, keys := columns()
		var values [][]any
		for _, id := range ids {
			values = append(values, []any{id})
		}
		var q strings.Builder
		require.NoError(t, mat.templates.loadQuery.Execute(&q, templateInput{binding: binding{Idx: 0, Mapped: &mb}, Bounds: bounds(ids)}))
		outputPrefix := path.Join(cfg.Compute.BucketPath, uuid.NewString())
		require.NoError(t, mat.compute.runJob(ctx, computeJob{
			Input: python.LoadInput{
				Query:          q.String(),
				Bindings:       []python.LoadBinding{{Binding: 0, Keys: keys, Files: []string{stage([]string{"_id"}, values)}}},
				OutputLocation: "s3://" + path.Join(cfg.Compute.Bucket, outputPrefix),
			},
			EntryPointURI: mat.pyFiles.load, PyFilesCommonURI: mat.pyFiles.common,
			Name: "load", WorkingPrefix: outputPrefix,
		}))
		got := map[string]int64{}
		for obj := range mat.bucket.List(ctx, blob.Query{Prefix: outputPrefix}) {
			if !strings.HasPrefix(path.Base(obj.Key), "part-") {
				continue
			}
			r, err := mat.bucket.NewReader(ctx, obj.Key)
			require.NoError(t, err)
			gzr, err := gzip.NewReader(r)
			require.NoError(t, err)
			sc := bufio.NewScanner(gzr)
			sc.Buffer(make([]byte, 0, 64*1024), math.MaxInt64)
			for sc.Scan() {
				_, d, found := strings.Cut(sc.Text(), "\x00")
				require.True(t, found)
				var parsed struct {
					ID string `json:"_id"`
					N  int64  `json:"n"`
				}
				require.NoError(t, json.Unmarshal([]byte(d), &parsed), d)
				got[parsed.ID] = parsed.N
			}
			require.NoError(t, sc.Err())
			require.NoError(t, r.Close())
		}
		return got
	}

	// driverLog returns the daemon's log lines since a time that mention this table.
	driverLog := func(since time.Time) []string {
		out, err := exec.Command("docker", "logs", "--since", since.UTC().Format(time.RFC3339Nano), container).CombinedOutput()
		require.NoError(t, err, string(out))
		var lines []string
		for _, l := range strings.Split(string(out), "\n") {
			if strings.Contains(l, tableName) && (strings.Contains(l, "matched runtime") || strings.Contains(l, "ScanReport")) {
				lines = append(lines, l)
			}
		}
		return lines
	}
	runtimeFilter := regexp.MustCompile(`(\d+) of (\d+) task\(s\) for table \S+ matched runtime filter`)
	resultDataFiles := regexp.MustCompile(`resultDataFiles=CounterResult\{unit=COUNT, value=(\d+)\}`)
	summarize := func(label string, lines []string) (matched, total int, scanFiles []int) {
		matched, total = -1, -1
		for _, l := range lines {
			if m := runtimeFilter.FindStringSubmatch(l); m != nil {
				matched, _ = strconv.Atoi(m[1])
				total, _ = strconv.Atoi(m[2])
			}
			if m := resultDataFiles.FindStringSubmatch(l); m != nil {
				n, _ := strconv.Atoi(m[1])
				scanFiles = append(scanFiles, n)
			}
		}
		t.Logf("%s: runtime filter matched %d of %d tasks, ScanReport resultDataFiles %v", label, matched, total, scanFiles)
		for _, l := range lines {
			if runtimeFilter.MatchString(l) {
				t.Logf("  %s", strings.TrimSpace(l))
			}
		}
		return matched, total, scanFiles
	}

	byID := func(a, b row) int { return strings.Compare(a.id, b.id) }

	// Random keys, so every file's min/max key range spans the whole key space
	// and min/max pruning alone can't skip any file.
	rng := rand.New(rand.NewSource(42))
	all := map[string]int64{}
	var ids []string
	for i := range migrateMerges {
		var batch []row
		for range migrateRows {
			id := fmt.Sprintf("%08x", rng.Uint32())
			if _, dup := all[id]; dup {
				continue
			}
			all[id] = int64(i)
			ids = append(ids, id)
			batch = append(batch, row{id: id, n: int64(i)})
		}
		slices.SortFunc(batch, byID)
		merge(batch)
	}
	filesBefore := scalar(fmt.Sprintf("SELECT count(*) FROM %s.files", fqn))
	t.Logf("unpartitioned table: %d rows, %s data files", len(all), filesBefore)

	pick := func(n int) []string {
		var out []string
		for _, i := range rng.Perm(len(ids))[:n] {
			out = append(out, ids[i])
		}
		slices.Sort(out)
		return out
	}

	// Baseline load on the unpartitioned table.
	want := pick(3)
	since := time.Now()
	got := load(want)
	require.Len(t, got, len(want))
	summarize("before, unpartitioned", driverLog(since))

	// Add the bucket partition.
	require.NoError(t, sparkExec(fmt.Sprintf("ALTER TABLE %s ADD PARTITION FIELD bucket(%d, `_id`)", fqn, migrateBuckets)))

	// A load between the ALTER and the rewrite: old files carry no bucket
	// value, so nothing should prune yet.
	since = time.Now()
	require.Len(t, load(want), len(want))
	summarize("after ALTER, before rewrite", driverLog(since))

	// Rewrite every file into the new spec. partial-progress commits each file
	// group as it finishes, so a failure on a large table keeps the work done.
	_, rewrite, err := querier.queryRows(ctx, fmt.Sprintf(
		"CALL estuary.system.rewrite_data_files(table => '%s.%s', strategy => 'sort', options => map('rewrite-all', 'true', 'partial-progress.enabled', 'true'))",
		tablePath[0], tablePath[1]))
	require.NoError(t, err)
	t.Logf("rewrite_data_files result: %v", rewrite)

	_, specs, err := querier.queryRows(ctx, fmt.Sprintf("SELECT spec_id, count(*) FROM %s.files GROUP BY spec_id", fqn))
	require.NoError(t, err)
	t.Logf("data files by spec_id after rewrite: %v", specs)
	require.Len(t, specs, 1, "files left under the old spec")
	require.JSONEq(t, "1", string(specs[0][0].(json.RawMessage)))
	t.Logf("partitioned table: %s data files", scalar(fmt.Sprintf("SELECT count(*) FROM %s.files", fqn)))
	require.Equal(t, strconv.Itoa(len(all)), scalar(fmt.Sprintf("SELECT count(*) FROM %s", fqn)), "row count changed in the rewrite")

	// The load after the rewrite prunes on the bucket.
	since = time.Now()
	got = load(want)
	for _, id := range want {
		require.Equal(t, all[id], got[id], id)
	}
	matched, total, _ := summarize("after rewrite, 3 keys", driverLog(since))
	require.Greater(t, total, 0, "no runtime filter line for the load")
	require.Less(t, matched, total, "the runtime filter dropped nothing")

	// A larger load touches more buckets.
	want = pick(40)
	since = time.Now()
	require.Len(t, load(want), len(want))
	summarize("after rewrite, 40 keys", driverLog(since))

	// The connector's merge still works on the rewritten table: update some
	// existing keys and insert new ones.
	var batch []row
	for _, id := range pick(50) {
		batch = append(batch, row{id: id, n: 1000})
		all[id] = 1000
	}
	for i := range 20 {
		// Not hex, so it can't collide with an existing key.
		id := fmt.Sprintf("new-%04d", i)
		batch = append(batch, row{id: id, n: 2000})
		all[id] = 2000
	}
	slices.SortFunc(batch, byID)
	since = time.Now()
	merge(batch)
	for _, l := range driverLog(since) {
		if strings.Contains(l, "matched runtime") {
			t.Logf("merge after rewrite: %s", strings.TrimSpace(l))
		}
	}
	require.Equal(t, strconv.Itoa(len(all)), scalar(fmt.Sprintf("SELECT count(*) FROM %s", fqn)))
	require.Equal(t, "70", scalar(fmt.Sprintf("SELECT count(*) FROM %s WHERE n >= 1000", fqn)))
	_, specs, err = querier.queryRows(ctx, fmt.Sprintf("SELECT spec_id, count(*) FROM %s.files GROUP BY spec_id", fqn))
	require.NoError(t, err)
	t.Logf("data files by spec_id after merge: %v", specs)
}
