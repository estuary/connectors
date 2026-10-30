package connector

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"path"
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

// TestMergeOnRead runs the connector's merge and load queries against a table
// whose write modes are merge-on-read, on the Spark 4 set of the compose stack.
// Merge-on-read needs Iceberg 1.10 or later, because earlier releases fail
// every merge into a merge-on-read table with identifier fields. Each table is
// created by the connector, so it carries the keys as identifier fields. The
// table is format v3 when its document column is a variant, and format v2
// otherwise.
//
// What it pins:
//
//   - merges that insert, update and delete succeed, and the ones that change
//     existing rows commit delete files without removing data files;
//   - a v3 table's delete files are deletion vectors, and each data file has
//     at most one;
//   - loads and a full scan return exactly the live documents after every
//     round, including updates of rows in a data file that already carries
//     deletes.
func TestMergeOnRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ensureDockerUp(t)

	var raw, err = boilerplate.ReadConfigFile(localConfigTemplates["testdata/config.rest-local-spark4.yaml"])
	require.NoError(t, err)
	var cfg config
	require.NoError(t, json.Unmarshal(raw, &cfg))
	require.Contains(t, strings.Split(cfg.Advanced.FeatureFlags, ","), "variant_columns")

	t.Run("v3", func(t *testing.T) { runMergeOnRead(t, cfg, 3) })

	var v2 = cfg
	v2.Advanced.FeatureFlags = strings.Replace(cfg.Advanced.FeatureFlags, "variant_columns", "no_variant_columns", 1)
	t.Run("v2", func(t *testing.T) { runMergeOnRead(t, v2, 2) })
}

func runMergeOnRead(t *testing.T, cfg config, wantFormatVersion int) {
	var h = newMORHarness(t, cfg)
	var mb, tablePath = h.createTable(fmt.Sprintf("mor_flow_test_%d", time.Now().UnixNano()))
	var table = morFQN(tablePath)

	h.exec(fmt.Sprintf("ALTER TABLE %s SET TBLPROPERTIES ('write.merge.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read')", table))

	var formatVersion = h.formatVersion(tablePath)
	require.Equal(t, wantFormatVersion, formatVersion)

	// want is the live table contents by id, kept in step with each merge.
	var want = map[string]int64{}
	var allIDs []string
	for i := range 60 {
		allIDs = append(allIDs, fmt.Sprintf("k%02d", i))
	}

	var check = func(round string) {
		require.Equal(t, want, h.load(mb, allIDs), "load after %s", round)

		var scanned = map[string]int64{}
		for _, r := range h.query(fmt.Sprintf("SELECT `id`, `n` FROM %s", table)) {
			var id string
			var n int64
			require.NoError(t, json.Unmarshal(r[0].(json.RawMessage), &id))
			require.NoError(t, json.Unmarshal(r[1].(json.RawMessage), &n))
			require.NotContains(t, scanned, id, "scan after %s returned a key twice", round)
			scanned[id] = n
		}
		require.Equal(t, want, scanned, "scan after %s", round)
	}

	// Round 1 inserts k00..k49 into the empty table.
	var round []morRow
	for i := range 50 {
		var id = fmt.Sprintf("k%02d", i)
		round = append(round, morRow{id: id, n: int64(i)})
		want[id] = int64(i)
	}
	h.merge(mb, round)
	check("inserts")
	var dataFilesBefore = h.liveDataFiles(tablePath)

	// Round 2 updates ten keys, deletes five, and inserts two new ones.
	round = nil
	for i := range 50 {
		var id = fmt.Sprintf("k%02d", i)
		switch {
		case i%5 == 0:
			round = append(round, morRow{id: id, n: int64(i) + 1000})
			want[id] = int64(i) + 1000
		case i%10 == 3:
			round = append(round, morRow{id: id, delete: true})
			delete(want, id)
		}
	}
	for _, id := range []string{"k50", "k51"} {
		round = append(round, morRow{id: id, n: 50})
		want[id] = 50
	}
	h.merge(mb, round)
	check("updates and deletes")
	h.requireMergeOnReadCommit(tablePath, dataFilesBefore, formatVersion)

	// Round 3 updates rows in both the first data file, which already
	// carries deletes from round 2, and the file round 2 wrote, and deletes
	// a key that round 2 inserted.
	dataFilesBefore = h.liveDataFiles(tablePath)
	round = nil
	for i := range 50 {
		if i%5 == 0 || i%5 == 1 {
			var id = fmt.Sprintf("k%02d", i)
			round = append(round, morRow{id: id, n: int64(i) + 2000})
			want[id] = int64(i) + 2000
		}
	}
	round = append(round, morRow{id: "k51", delete: true})
	delete(want, "k51")
	h.merge(mb, round)
	check("second updates")
	h.requireMergeOnReadCommit(tablePath, dataFilesBefore, formatVersion)

	for _, r := range h.query(fmt.Sprintf("SELECT content, file_format, record_count FROM %s.delete_files", table)) {
		t.Logf("live delete file: content=%s format=%s records=%s", r[0], r[1], r[2])
	}
}

type morBinding = realBoilerplate.MappedBinding[config, resource, mapped]

// morRow is a document {"id": id, "n": n}, or a deletion of id.
type morRow struct {
	id     string
	n      int64
	delete bool
}

// morHarness runs the connector's own merge and load queries against tables
// it creates through the connector, on the local Spark daemon.
type morHarness struct {
	t   *testing.T
	ctx context.Context
	cfg config
	mat *materialization
}

func newMORHarness(t *testing.T, cfg config) *morHarness {
	var ctx = context.Background()
	var flags = common.ResolveFlags(cfg.Advanced.FeatureFlags, featureFlagDefaults, common.CreatedAt{})
	var matIface, err = NewMaterializer(ctx, "acmeCo/tests/merge-on-read", cfg, flags)
	require.NoError(t, err)
	var mat = matIface.(*materialization)
	var _, ok = mat.compute.(*sparkClient)
	require.True(t, ok, "this test drives the local spark daemon")

	// Setup stages the PySpark scripts and records their locations, which
	// runJob needs to pick its entry point.
	_, err = mat.Setup(ctx, nil)
	require.NoError(t, err)

	return &morHarness{t: t, ctx: ctx, cfg: cfg, mat: mat}
}

func (h *morHarness) project(field, ptr string, types []string, isKey bool) realBoilerplate.MappedProjection[mapped] {
	var p = realBoilerplate.MapProjection(pf.Projection{
		Field:        field,
		Ptr:          ptr,
		IsPrimaryKey: isKey,
		Inference:    pf.Inference{Types: types, Exists: pf.Inference_MUST},
	}, fieldConfig{})
	var mt, _ = h.mat.MapType(p, fieldConfig{})
	return realBoilerplate.MappedProjection[mapped]{Projection: p, Mapped: mt}
}

// createTable creates a table keyed on a string id with an integer n, and
// drops it when the test ends.
func (h *morHarness) createTable(table string) (morBinding, []string) {
	var res = resource{Table: table}.WithDefaults(h.cfg)
	var path, _, err = res.Parameters()
	require.NoError(h.t, err)
	var doc = h.project("flow_document", "", []string{"object"}, false)
	var mb = morBinding{
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
		var _, del, err = h.mat.DeleteResource(context.Background(), path)
		require.NoError(h.t, err)
		_ = del(context.Background())
	})
	return mb, path
}

func morFQN(path []string) string {
	return fmt.Sprintf("`estuary`.%s.%s", quoteIdentifier(path[0]), quoteIdentifier(path[1]))
}

func (h *morHarness) exec(query string) {
	require.NoError(h.t, h.mat.compute.(*sparkClient).postDaemon(h.ctx, "exec", python.ExecInput{Query: query}, &daemonResult{}), query)
}

func (h *morHarness) query(query string) [][]any {
	var _, rows, err = h.mat.compute.(rowQuerier).queryRows(h.ctx, query)
	require.NoError(h.t, err, query)
	return rows
}

func (h *morHarness) stage(fields []string, values [][]any) string {
	var key = stagedFileClient{}.NewKey([]string{h.cfg.Compute.BucketPath, "mor-" + uuid.NewString(), "staged"})
	var w = stagedFileClient{}.NewWriter(h.mat.bucket.NewWriter(h.ctx, key), fields)
	for _, v := range values {
		require.NoError(h.t, w.Write(v))
	}
	require.NoError(h.t, w.Close())
	return h.mat.bucket.URI(key)
}

func morColumns(mb morBinding) (cols, keys []python.NestedField) {
	for _, p := range mb.SelectedProjections() {
		var f = python.NestedField{Name: p.Field, Type: p.Mapped.type_.Type()}
		if p.IsPrimaryKey {
			keys = append(keys, f)
		}
		cols = append(cols, f)
	}
	return cols, keys
}

func morBounds(mb morBinding, ids []string) []mergeBound {
	var b = newMergeBoundsBuilder(mb.Keys)
	for _, id := range ids {
		b.nextKey([]any{id})
	}
	return b.build()
}

// merge runs the connector's merge query exactly as Acknowledge would.
// Rows must be sorted by id, as the connector sends them.
func (h *morHarness) merge(mb morBinding, rows []morRow) {
	var cols, _ = morColumns(mb)
	var values [][]any
	var ids []string
	for _, r := range rows {
		if r.delete {
			values = append(values, []any{r.id, nil, `"delete"`})
		} else {
			values = append(values, []any{r.id, r.n, fmt.Sprintf(`{"id":%q,"n":%d}`, r.id, r.n)})
		}
		ids = append(ids, r.id)
	}
	var q strings.Builder
	require.NoError(h.t, h.mat.templates.mergeQuery.Execute(&q, templateInput{
		binding: binding{Idx: 0, Mapped: &mb},
		Bounds:  morBounds(mb, ids),
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
	}), q.String())
}

// load runs the connector's load query for the keys and returns n by id,
// failing the test if any id comes back twice.
func (h *morHarness) load(mb morBinding, ids []string) map[string]int64 {
	var _, keys = morColumns(mb)
	var values [][]any
	for _, id := range ids {
		values = append(values, []any{id})
	}
	var q strings.Builder
	require.NoError(h.t, h.mat.templates.loadQuery.Execute(&q, templateInput{
		binding: binding{Idx: 0, Mapped: &mb},
		Bounds:  morBounds(mb, ids),
	}))

	var outputPrefix = path.Join(h.cfg.Compute.BucketPath, uuid.NewString())
	require.NoError(h.t, h.mat.compute.runJob(h.ctx, computeJob{
		Input: python.LoadInput{
			Query:          q.String(),
			Bindings:       []python.LoadBinding{{Binding: 0, Keys: keys, Files: []string{h.stage([]string{"id"}, values)}}},
			OutputLocation: "s3://" + path.Join(h.cfg.Compute.Bucket, outputPrefix),
		},
		EntryPointURI:    h.mat.pyFiles.load,
		PyFilesCommonURI: h.mat.pyFiles.common,
		Name:             "load",
		WorkingPrefix:    outputPrefix,
	}), q.String())

	var got = map[string]int64{}
	for obj := range h.mat.bucket.List(h.ctx, blob.Query{Prefix: outputPrefix}) {
		if !strings.HasPrefix(path.Base(obj.Key), "part-") {
			continue
		}
		var r, err = h.mat.bucket.NewReader(h.ctx, obj.Key)
		require.NoError(h.t, err)
		gzr, err := gzip.NewReader(r)
		require.NoError(h.t, err)
		var scanner = bufio.NewScanner(gzr)
		scanner.Buffer(make([]byte, 0, 64*1024), math.MaxInt64)
		for scanner.Scan() {
			var idx, doc, found = strings.Cut(scanner.Text(), "\x00")
			require.True(h.t, found, "load output line without a binding separator: %q", scanner.Text())
			require.Equal(h.t, "0", idx)
			var parsed struct {
				ID string `json:"id"`
				N  int64  `json:"n"`
			}
			require.NoError(h.t, json.Unmarshal([]byte(doc), &parsed), doc)
			require.NotContains(h.t, got, parsed.ID, "load returned a key twice")
			got[parsed.ID] = parsed.N
		}
		require.NoError(h.t, scanner.Err())
		require.NoError(h.t, r.Close())
	}
	return got
}

func (h *morHarness) formatVersion(path []string) int {
	var tbl, err = h.mat.catalog.GetTable(h.ctx, path[0], path[1])
	require.NoError(h.t, err)
	return tbl.Metadata.Version()
}

// liveDataFiles returns the paths of the table's live data files.
func (h *morHarness) liveDataFiles(path []string) map[string]struct{} {
	var out = map[string]struct{}{}
	for _, r := range h.query(fmt.Sprintf("SELECT file_path FROM %s.data_files", morFQN(path))) {
		out[string(r[0].(json.RawMessage))] = struct{}{}
	}
	return out
}

// requireMergeOnReadCommit asserts that the latest commit added delete files
// and left every previously live data file in place, and that on a v3 table
// the live delete files are deletion vectors, at most one per data file.
func (h *morHarness) requireMergeOnReadCommit(path []string, dataFilesBefore map[string]struct{}, formatVersion int) {
	var table = morFQN(path)

	var rows = h.query(fmt.Sprintf("SELECT operation, summary FROM %s.snapshots ORDER BY committed_at DESC LIMIT 1", table))
	require.Len(h.t, rows, 1)
	var summary map[string]string
	require.NoError(h.t, json.Unmarshal(rows[0][1].(json.RawMessage), &summary))
	h.t.Logf("latest snapshot: operation=%s summary=%v", rows[0][0], summary)
	require.NotEmpty(h.t, summary["added-delete-files"], "the merge committed no delete files")
	require.Empty(h.t, summary["deleted-data-files"], "the merge removed data files, which is copy-on-write")

	var after = h.liveDataFiles(path)
	for f := range dataFilesBefore {
		require.Contains(h.t, after, f, "a data file live before the merge is gone")
	}

	require.NotEmpty(h.t, h.query(fmt.Sprintf("SELECT content FROM %s.delete_files", table)))
	if formatVersion < 3 {
		return
	}
	var referenced = map[string]struct{}{}
	for _, d := range h.query(fmt.Sprintf("SELECT content, file_format, referenced_data_file FROM %s.delete_files", table)) {
		require.JSONEq(h.t, "1", string(d[0].(json.RawMessage)), "delete file is not a position delete")
		require.JSONEq(h.t, `"PUFFIN"`, string(d[1].(json.RawMessage)), "v3 delete file is not a deletion vector")
		var ref = string(d[2].(json.RawMessage))
		require.NotContains(h.t, referenced, ref, "data file has more than one deletion vector")
		referenced[ref] = struct{}{}
	}
}
