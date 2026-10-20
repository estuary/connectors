package connector

import (
	"fmt"
	"math/rand"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/require"
)

// TestPartitionMigration migrates a table that already holds data under the
// unpartitioned spec the connector creates: ALTER TABLE ADD PARTITION FIELD,
// then rewrite_data_files, then the connector's load and merge. It reads the
// Spark driver log for Iceberg's own scan lines, since those are what a user
// can check on their own deployment.
func TestPartitionMigration(t *testing.T) {
	containers := map[string]string{
		"rest-local":        "materialize-iceberg-spark-daemon-1",
		"rest-local-spark4": "materialize-iceberg-spark4-daemon-1",
	}
	forEachLocalConfig(t, func(t *testing.T, name string, cfg config) {
		runPartitionMigration(t, cfg, containers[name])
	})
}

const migrateBuckets = 16

func runPartitionMigration(t *testing.T, cfg config, container string) {
	h := newPartitionHarness(t, cfg, "acmeCo/tests/partition-migrate")
	mb, tablePath := h.createTable(fmt.Sprintf("migrate_flow_test_%d", time.Now().Unix()))
	byID := func(a, b partitionRow) int { return strings.Compare(a.id, b.id) }
	count := func(where string) string {
		return strings.Trim(string(h.query(fmt.Sprintf("SELECT count(*) FROM %s %s", fqn(tablePath), where))[0][0].(json.RawMessage)), `"`)
	}
	specIDs := func() [][]any {
		return h.query(fmt.Sprintf("SELECT spec_id, count(*) FROM %s.files GROUP BY spec_id", fqn(tablePath)))
	}

	// Twelve merges of random keys, so every file's min/max key range spans
	// the whole key space and min/max pruning alone can't skip any file.
	rng := rand.New(rand.NewSource(42))
	all := map[string]int64{}
	var ids []string
	for i := range 12 {
		var batch []partitionRow
		for range 200 {
			id := fmt.Sprintf("%08x", rng.Uint32())
			if _, dup := all[id]; dup {
				continue
			}
			all[id] = int64(i)
			ids = append(ids, id)
			batch = append(batch, partitionRow{id: id, n: int64(i)})
		}
		slices.SortFunc(batch, byID)
		h.merge(mb, batch)
	}
	pick := func(n int) []string {
		var out []string
		for _, i := range rng.Perm(len(ids))[:n] {
			out = append(out, ids[i])
		}
		slices.Sort(out)
		return out
	}

	// Rewrite every file into the new spec. partial-progress commits each file
	// group as it finishes, so a failure on a large table keeps the work done.
	h.exec(fmt.Sprintf("ALTER TABLE %s ADD PARTITION FIELD bucket(%d, `id`)", fqn(tablePath), migrateBuckets))
	h.query(fmt.Sprintf(
		"CALL estuary.system.rewrite_data_files(table => '%s.%s', strategy => 'sort', options => map('rewrite-all', 'true', 'partial-progress.enabled', 'true'))",
		tablePath[0], tablePath[1]))
	specs := specIDs()
	require.Len(t, specs, 1, "files left under the old spec")
	require.JSONEq(t, "1", string(specs[0][0].(json.RawMessage)))
	require.Equal(t, strconv.Itoa(len(all)), count(""), "row count changed in the rewrite")

	// A load of three keys returns their documents, and Iceberg's runtime
	// filter drops the file tasks of the buckets that don't hold them.
	want := pick(3)
	since := time.Now()
	got := h.load(mb, want)
	require.Len(t, got, len(want))
	for _, id := range want {
		require.Equal(t, all[id], got[id], id)
	}
	out, err := exec.Command("docker", "logs", "--since", since.UTC().Format(time.RFC3339Nano), container).CombinedOutput()
	require.NoError(t, err, string(out))
	m := regexp.MustCompile(`(\d+) of (\d+) task\(s\) for table \S+\.` + tablePath[1] + ` matched runtime filter`).FindStringSubmatch(string(out))
	require.NotNil(t, m, "no runtime filter line for the load")
	matched, _ := strconv.Atoi(m[1])
	total, _ := strconv.Atoi(m[2])
	t.Logf("3-key load: runtime filter matched %d of %d tasks", matched, total)
	require.Less(t, matched, total, "the runtime filter dropped nothing")

	// The connector's merge still works on the rewritten table: update some
	// existing keys and insert new ones.
	var batch []partitionRow
	for _, id := range pick(50) {
		batch = append(batch, partitionRow{id: id, n: 1000})
		all[id] = 1000
	}
	for i := range 20 {
		// Not hex, so it can't collide with an existing key.
		id := fmt.Sprintf("new-%04d", i)
		batch = append(batch, partitionRow{id: id, n: 2000})
		all[id] = 2000
	}
	slices.SortFunc(batch, byID)
	h.merge(mb, batch)
	require.Equal(t, strconv.Itoa(len(all)), count(""))
	require.Equal(t, "70", count("WHERE n >= 1000"))
	require.Len(t, specIDs(), 1, "merge wrote files under the old spec")
}
