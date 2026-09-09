package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// testSidecarPython returns a python interpreter from a venv with the real
// sidecar package (and the snowpipe-streaming SDK) installed, building the
// venv on first use. The venv is shared across test runs; the sidecar package
// itself is installed editable so source changes are always picked up.
func testSidecarPython(t *testing.T) string {
	t.Helper()
	systemPython, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	var venv = filepath.Join(os.TempDir(), "snowpipe-sidecar-test-venv")
	var python = filepath.Join(venv, "bin", "python")
	if _, err := os.Stat(python); err != nil {
		t.Logf("building sidecar test venv at %s", venv)
		if out, err := exec.Command(systemPython, "-m", "venv", venv).CombinedOutput(); err != nil {
			t.Fatalf("creating venv: %v: %s", err, out)
		}
		if out, err := exec.Command(filepath.Join(venv, "bin", "pip"), "install", "--quiet", "-e", "./sidecar").CombinedOutput(); err != nil {
			os.RemoveAll(venv)
			t.Fatalf("installing sidecar package: %v: %s", err, out)
		}
	}
	return python
}

// testRunNonce distinguishes this run's channels from a previous run's.
// Snowflake's ingestion keeps serving the channels of a dropped table for some
// time, so a state key reused across runs can meet a committed offset token an
// earlier run left behind. Folded into every state key, the nonce rotates each
// run's channels the way a backfill would.
var testRunNonce = fmt.Sprintf("%d", time.Now().Unix())

// noncedStateKey scopes a state key to this run of the suite.
func noncedStateKey(stateKey string) string {
	return fmt.Sprintf("%s-%s", stateKey, testRunNonce)
}

// TestStreamV2Manager drives the direct-append write path against live
// Snowflake, which is the seam this design rests on: everything it guarantees
// is a statement about what Snowflake ends up holding.
func TestStreamV2Manager(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var ctx = context.Background()
	var cfg = mustGetCfg(t)
	t.Setenv("SNOWPIPE_SIDECAR_PYTHON", testSidecarPython(t))

	dsn, err := cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	// The columns writeRows stores, shared by every table this test appends to.
	const testTableColumns = "(KEY TEXT, INTCOL NUMBER, DOC VARIANT)"

	// The task every manager here stands for a shard of.
	const testMaterialization = "test/streamV2Materialization"

	var tableName = "STREAMV2_TEST"
	var cleanup = func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	}
	cleanup()
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s %s;", tableName, testTableColumns))
	require.NoError(t, err)
	defer cleanup()

	// Each subtest uses its own state key, and so its own channels, so that the
	// committed offset token one subtest leaves behind cannot reach another. The
	// run nonce isolates runs of the whole suite from each other the same way.
	var target = func(stateKey string) sql.Table {
		return sql.Table{
			TableShape: sql.TableShape{Binding: 0},
			Identifier: tableName,
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `INTCOL`}, {Identifier: `DOC`}},
			StateKey:   noncedStateKey(stateKey),
		}
	}

	var fullRange = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}

	// These subtests follow a single channel's counter, skip threshold, and token
	// against live Snowflake; the multi-channel routing and the split/join
	// inheritance have fake-sidecar suites of their own.
	singleChannelLayout(t)

	// priorOf keys checkpoint items by channel the way the connector's driver
	// checkpoint does, so that a shard reads back only the item it wrote.
	var priorOf = func(items ...*streamV2Item) map[string]*streamV2Item {
		var byChannel = make(map[string]*streamV2Item, len(items))
		for _, item := range items {
			byChannel[item.Channel] = item
		}
		return byChannel
	}

	var newManager = func(keyRange *pf.RangeSpec) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &cfg, testMaterialization, accountName, keyRange)
		m.listChannels = func(ctx context.Context, database, schema, table string) ([]string, error) {
			return streamV2ListChannels(ctx, db, testDialect, database, schema, table)
		}
		t.Cleanup(m.stop)
		return m
	}

	var countRowsIn = func(table string) int {
		var count int
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)).Scan(&count))
		return count
	}

	var countRows = func() int { return countRowsIn(tableName) }

	var truncate = func(t *testing.T) {
		_, err := db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s;", tableName))
		require.NoError(t, err)
	}

	// writeRows stores the documents of the half-open range [lo, hi), which is
	// what a replay of the same transaction produces again.
	var writeRows = func(m *streamV2Manager, lo, hi int) {
		for i := lo; i < hi; i++ {
			require.NoError(t, testWriteRow(ctx, m, 0, []any{
				fmt.Sprintf("key-%d", i),
				i,
				json.RawMessage(fmt.Sprintf(`{"nested":{"i":%d},"arr":[1,2]}`, i)),
			}))
		}
	}

	// The topology subtests below drive real multi-channel layouts, under the
	// single-channel pin the rest of the suite runs with.
	var channelsPerShard = func(t *testing.T, n int) {
		var restore = streamV2ChannelsPerShard
		t.Cleanup(func() { streamV2ChannelsPerShard = restore })
		streamV2ChannelsPerShard = n
	}

	// The midpoint split's two halves, as shard ranges and as key ranges.
	var lowHalf = &pf.RangeSpec{KeyEnd: math.MaxUint32 / 2, RClockEnd: math.MaxUint32}
	var highHalf = &pf.RangeSpec{KeyBegin: math.MaxUint32/2 + 1, KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}
	var lowShard = streamV2Range{keyEnd: math.MaxUint32 / 2}
	var highShard = streamV2Range{keyBegin: math.MaxUint32/2 + 1, keyEnd: math.MaxUint32}
	var fullShard = streamV2Range{keyEnd: math.MaxUint32}

	// keysHashingInto finds n keys whose packed-key hash the key range covers, by
	// scanning a deterministic candidate sequence: the same call always produces
	// the same keys in the same order, which is what lets a subtest replay them.
	var keysHashingInto = func(r streamV2Range, prefix string, n int) []string {
		var keys []string
		for i := 0; len(keys) < n; i++ {
			var k = fmt.Sprintf("%s-%d", prefix, i)
			if r.contains(packedKeyHashHH64(packKey(k))) {
				keys = append(keys, k)
			}
		}
		return keys
	}

	// interleave orders one key from each key range in turn, so every channel's
	// rows arrive interleaved the way a real transaction's would.
	var interleave = func(perRange [][]string) []string {
		var rows []string
		for i := 0; ; i++ {
			var appended = false
			for _, keys := range perRange {
				if i < len(keys) {
					rows = append(rows, keys[i])
					appended = true
				}
			}
			if !appended {
				return rows
			}
		}
	}

	// keysWithin filters rows to the ones a shard covering r is delivered,
	// preserving their order, which is what the runtime's replay routes to it.
	var keysWithin = func(keys []string, r streamV2Range) []string {
		var within []string
		for _, k := range keys {
			if r.contains(packedKeyHashHH64(packKey(k))) {
				within = append(within, k)
			}
		}
		return within
	}

	// storeKeys stores one document per key, in order.
	var storeKeys = func(t *testing.T, m *streamV2Manager, keys []string) {
		for i, k := range keys {
			require.NoError(t, testWriteRow(ctx, m, 0, []any{k, i, json.RawMessage(`{"k":1}`)}))
		}
	}

	// commitOutstanding waits until Snowflake commits every appended batch, the
	// way an interruption after the appends and before flush leaves a channel:
	// documents committed beyond what any checkpoint records.
	var commitOutstanding = func(t *testing.T, m *streamV2Manager) {
		for _, c := range m.bindings[0].channels {
			require.NoError(t, c.pipe.wait())
			if c.counter > c.committed {
				_, err := m.client.WaitCommit(ctx, c.name, c.offsetToken(c.counter))
				require.NoError(t, err)
			}
		}
	}

	// Carried from the join subtest into the split-back subtest, which
	// re-derives the very channel names the join's convergence dropped.
	var joinConverged map[string]*streamV2Item
	var joinRows int

	// rejectingTable creates a table Snowflake will reject a row of, and returns a
	// delta-updates target for it.
	//
	// A null against a NOT NULL column is the rejection it provokes: omitting a
	// column from the appended row object is how a nil converted value reaches
	// Snowflake as SQL NULL, and the connector marks a column NOT NULL only for a
	// field the collection schema requires, so the runtime does not in fact
	// deliver a nil for one. Nothing about the rejection is specific to nulls —
	// any rejected row takes the same path.
	var rejectingTable = func(t *testing.T, table, stateKey string) sql.Table {
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE TABLE %s (KEY TEXT, VAL VARIANT NOT NULL);", table))
		require.NoError(t, err)
		t.Cleanup(func() {
			db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", table))
		})

		return sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
			Identifier: table,
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}},
			StateKey:   noncedStateKey(stateKey),
		}
	}

	// smallBatches cuts batches every two documents, so that a handful of rows
	// exercises the boundaries a production batch needs ten thousand for.
	var smallBatches = func(t *testing.T) {
		var restore = streamV2BatchRows
		t.Cleanup(func() { streamV2BatchRows = restore })
		streamV2BatchRows = 2
	}

	t.Run("happy path", func(t *testing.T) {
		truncate(t)
		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, tableName, target("happy.v1"), nil)

		writeRows(m, 0, 100)
		entries, err := m.flush(ctx)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, int64(100), soleItem(t, entries, 0).Counter)
		require.Equal(t, 100, countRows())

		// VARIANT columns must round-trip as real JSON objects, not strings.
		var docType string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT TYPEOF(doc) FROM %s WHERE key = 'key-7';`, tableName)).Scan(&docType))
		require.Equal(t, "OBJECT", docType)

		// A transaction which stores nothing for the binding reports no item, so
		// the counter already in the checkpoint stands.
		entries, err = m.flush(ctx)
		require.NoError(t, err)
		require.Empty(t, entries)

		// The counter continues across transactions rather than restarting.
		writeRows(m, 100, 150)
		entries, err = m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(150), soleItem(t, entries, 0).Counter)
		require.Equal(t, 150, countRows())
	})

	t.Run("total buffered bytes are bounded", func(t *testing.T) {
		// A ceiling small enough to be crossed by nearly every document appends
		// buffers far short of a full batch. Batch boundaries are not part of the
		// contract, so the only thing that may change is the number of appends.
		truncate(t)
		var restore = streamV2MaxBufferedBytes
		t.Cleanup(func() { streamV2MaxBufferedBytes = restore })
		streamV2MaxBufferedBytes = 200

		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, tableName, target("bounded.v1"), nil)

		writeRows(m, 0, 50)
		entries, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(50), soleItem(t, entries, 0).Counter)
		require.Equal(t, 50, countRows())
	})

	t.Run("recovery skips documents Snowflake already committed", func(t *testing.T) {
		truncate(t)
		smallBatches(t)
		var tgt = target("recovery.v1")

		// An interrupted transaction: two batches reach Snowflake and commit,
		// then the sidecar dies before flush, so no checkpoint is ever written
		// for this channel.
		var m1 = newManager(fullRange)
		m1.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)

		writeRows(m1, 0, 5)
		var c1 = m1.bindings[0].channels[0]
		require.NoError(t, c1.pipe.wait())
		_, err = m1.client.WaitCommit(ctx, c1.name, c1.offsetToken(4))
		require.NoError(t, err)
		require.Equal(t, 4, countRows())
		m1.sup.kill()

		// The runtime replays the identical document sequence. Snowflake already
		// holds the first four, so only the fifth is appended.
		var m2 = newManager(fullRange)
		m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)

		writeRows(m2, 0, 5)
		var c2 = m2.bindings[0].channels[0]
		require.Equal(t, int64(4), c2.skip)
		entries, err := m2.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(5), soleItem(t, entries, 0).Counter)
		require.Equal(t, 5, countRows())

		// A second interruption, now with a checkpoint counter to reconcile the
		// committed token against.
		var checkpointed = soleItem(t, entries, 0)
		writeRows(m2, 5, 8)
		require.NoError(t, c2.pipe.wait())
		_, err = m2.client.WaitCommit(ctx, c2.name, c2.offsetToken(7))
		require.NoError(t, err)
		require.Equal(t, 7, countRows())
		m2.sup.kill()

		var m3 = newManager(fullRange)
		m3.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(checkpointed))

		writeRows(m3, 5, 8)
		require.Equal(t, int64(7), m3.bindings[0].channels[0].skip)
		entries, err = m3.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(8), soleItem(t, entries, 0).Counter)
		require.Equal(t, 8, countRows())
	})

	t.Run("a split hands each child the channels nested in its range", func(t *testing.T) {
		// A midpoint split lands on the boundary between the parent's second
		// and third channels, so each child inherits two whole channels with
		// their committed offset tokens, keeps routing to them, and converges
		// to four channels of its own at the first acknowledged boundary.
		truncate(t)
		channelsPerShard(t, 4)
		var tgt = target("split.v1")

		parentTargets, err := streamV2TargetLayout(0, math.MaxUint32)
		require.NoError(t, err)
		lowTargets, err := streamV2TargetLayout(0, math.MaxUint32/2)
		require.NoError(t, err)

		// Five keys per parent quarter, interleaved so every channel's rows
		// arrive in a deterministic order a replay could reproduce.
		var quarters = make([][]string, len(parentTargets))
		for i, r := range parentTargets {
			quarters[i] = keysHashingInto(r, "split", 5)
		}

		var parent = newManager(fullRange)
		parent.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		storeKeys(t, parent, interleave(quarters))
		parentEntries, err := parent.flush(ctx)
		require.NoError(t, err)
		require.Len(t, parentEntries[0], 4, "one item per channel of the target layout")
		for _, item := range parentEntries[0] {
			require.Equal(t, int64(5), item.Counter)
		}
		require.Equal(t, 20, countRows())
		parent.stop()

		// The low child. Its own targets are the low half's quarters, so the
		// two channels it inherits are not targets: its first flush writes
		// their advanced counters and declares the layout it converges to.
		var low = newManager(lowHalf)
		low.addBinding(cfg.Database, cfg.Schema, tableName, tgt, parentEntries[0])

		storeKeys(t, low, keysHashingInto(lowShard, "split-low-new", 6))
		require.Len(t, low.bindings[0].channels, 2, "the low child inherits the two nested channels")
		lowEntries, err := low.flush(ctx)
		require.NoError(t, err)
		require.Len(t, lowEntries[0], 6, "two inherited channels and four declared targets")
		var declaredCount int
		var advanced int64
		for _, item := range lowEntries[0] {
			require.NotNil(t, item)
			var r = streamV2Range{keyBegin: item.KeyBegin, keyEnd: item.KeyEnd}
			if slices.Contains(lowTargets, r) {
				require.Zero(t, item.Counter, "a declaration is durable before anything routes to its channel")
				declaredCount++
			} else {
				advanced += item.Counter
			}
		}
		require.Equal(t, 4, declaredCount)
		require.Equal(t, int64(16), advanced, "ten inherited documents and six new ones")
		require.Equal(t, 26, countRows())

		// The acknowledged boundary: the declaration is durable, so the child
		// converges — the inherited channels are dropped and the targets take over.
		var inherited, inheritedKeys []string
		for _, c := range low.bindings[0].channels {
			inherited = append(inherited, c.name)
			inheritedKeys = append(inheritedKeys, c.r.key())
		}
		require.NoError(t, low.acknowledged(ctx))
		require.Len(t, low.bindings[0].channels, 4)
		for i, c := range low.bindings[0].channels {
			require.Equal(t, lowTargets[i], c.r)
			require.Zero(t, c.counter)
		}

		storeKeys(t, low, keysHashingInto(lowShard, "split-low-post", 4))
		post, err := low.flush(ctx)
		require.NoError(t, err)
		for _, key := range inheritedKeys {
			del, ok := post[0][key]
			require.True(t, ok, "the drop of %s must ride the checkpoint", key)
			require.Nil(t, del)
		}
		require.Equal(t, 30, countRows())

		// The drop reached Snowflake: an inherited name reopens as a channel
		// which has committed nothing.
		status, err := low.client.OpenChannel(ctx, cfg.Database, cfg.Schema, tableName, inherited[0])
		require.NoError(t, err)
		require.Nil(t, status.CommittedToken)

		// The high child inherits the other two channels the same way.
		var high = newManager(highHalf)
		high.addBinding(cfg.Database, cfg.Schema, tableName, tgt, parentEntries[0])
		storeKeys(t, high, keysHashingInto(highShard, "split-high-new", 2))
		require.Len(t, high.bindings[0].channels, 2, "the high child inherits the other two")
		highEntries, err := high.flush(ctx)
		require.NoError(t, err)
		require.Len(t, highEntries[0], 6)
		require.Equal(t, 32, countRows())
	})

	t.Run("a split interrupted before the children commit", func(t *testing.T) {
		// The state #4983 wedged on: documents Snowflake committed that no
		// checkpoint accounts for, redistributed by a split. Each child now
		// inherits the whole channels those documents were appended under, so
		// each skips them by position and the replay duplicates nothing.
		truncate(t)
		channelsPerShard(t, 4)
		smallBatches(t)
		var tgt = target("splitcut.v1")

		parentTargets, err := streamV2TargetLayout(0, math.MaxUint32)
		require.NoError(t, err)
		var first = make([][]string, len(parentTargets))
		var second = make([][]string, len(parentTargets))
		for i, r := range parentTargets {
			var keys = keysHashingInto(r, "splitcut", 4)
			first[i], second[i] = keys[:2], keys[2:]
		}

		var parent = newManager(fullRange)
		parent.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		storeKeys(t, parent, interleave(first))
		parentEntries, err := parent.flush(ctx)
		require.NoError(t, err)

		// The interrupted transaction: two more documents per channel reach
		// Snowflake and commit, and no checkpoint ever records them.
		storeKeys(t, parent, interleave(second))
		commitOutstanding(t, parent)
		require.Equal(t, 16, countRows())
		parent.sup.kill()

		// Each child replays its half of the interrupted transaction. Every
		// inherited channel's token stands at four documents against a
		// checkpoint of two, and the token names the channel's own key range,
		// so the replay skips instead of re-appending.
		var replay = interleave(second)
		for _, child := range []struct {
			rng   *pf.RangeSpec
			cover streamV2Range
		}{{lowHalf, lowShard}, {highHalf, highShard}} {
			var m = newManager(child.rng)
			m.addBinding(cfg.Database, cfg.Schema, tableName, tgt, parentEntries[0])
			storeKeys(t, m, keysWithin(replay, child.cover))
			for _, c := range m.bindings[0].channels {
				require.Equal(t, int64(4), c.skip)
				require.Equal(t, c.skip, c.counter, "the replay must land exactly at the committed token")
			}
			entries, err := m.flush(ctx)
			require.NoError(t, err)
			var caughtUp, declared int
			for _, item := range entries[0] {
				switch item.Counter {
				case 4:
					caughtUp++
				case 0:
					declared++
				default:
					t.Fatalf("channel %s reports counter %d, want 4 (inherited) or 0 (declared)", item.Channel, item.Counter)
				}
			}
			require.Equal(t, 2, caughtUp)
			require.Equal(t, 4, declared)
			m.stop()
		}
		require.Equal(t, 16, countRows(), "the replays appended nothing Snowflake already held")
	})

	t.Run("joining two shards into one", func(t *testing.T) {
		// A join hands the parent all eight of its children's channels. One
		// child is left mid-transaction, so the parent both skips by an
		// inherited token and converges the mixed inheritance to its own four.
		truncate(t)
		channelsPerShard(t, 4)
		smallBatches(t)
		var tgt = target("join.v1")

		lowTargets, err := streamV2TargetLayout(0, math.MaxUint32/2)
		require.NoError(t, err)
		highTargets, err := streamV2TargetLayout(math.MaxUint32/2+1, math.MaxUint32)
		require.NoError(t, err)

		var prior = make(map[string]*streamV2Item)

		// The low child commits eight documents, then leaves two more
		// committed on its first channel with no checkpoint to account for
		// them, as an interruption does.
		var low = newManager(lowHalf)
		low.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		var lowKeys = make([][]string, len(lowTargets))
		for i, r := range lowTargets {
			lowKeys[i] = keysHashingInto(r, "join-low", 2)
		}
		storeKeys(t, low, interleave(lowKeys))
		lowEntries, err := low.flush(ctx)
		require.NoError(t, err)
		maps.Copy(prior, lowEntries[0])

		var extra = keysHashingInto(lowTargets[0], "join-extra", 2)
		storeKeys(t, low, extra)
		commitOutstanding(t, low)
		low.sup.kill()

		// The high child commits eight and stops cleanly.
		var high = newManager(highHalf)
		high.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		var highKeys = make([][]string, len(highTargets))
		for i, r := range highTargets {
			highKeys[i] = keysHashingInto(r, "join-high", 2)
		}
		storeKeys(t, high, interleave(highKeys))
		highEntries, err := high.flush(ctx)
		require.NoError(t, err)
		maps.Copy(prior, highEntries[0])
		high.stop()

		require.Equal(t, 18, countRows())

		// The joined shard inherits all eight channels and replays the low
		// child's interrupted documents into the channel whose token already
		// counts them.
		var parent = newManager(fullRange)
		parent.addBinding(cfg.Database, cfg.Schema, tableName, tgt, prior)
		storeKeys(t, parent, extra)
		require.Len(t, parent.bindings[0].channels, 8, "the parent inherits both children's channels")
		for _, c := range parent.bindings[0].channels {
			if c.r == lowTargets[0] {
				require.Equal(t, int64(4), c.skip)
			} else {
				require.Equal(t, int64(2), c.skip)
			}
			require.Equal(t, c.skip, c.counter)
		}

		storeKeys(t, parent, keysHashingInto(fullShard, "join-new", 4))
		parentEntries, err := parent.flush(ctx)
		require.NoError(t, err)
		require.Len(t, parentEntries[0], 12, "eight inherited channels and four declared quarters")
		require.Equal(t, 22, countRows())

		var inheritedKeys []string
		for _, c := range parent.bindings[0].channels {
			inheritedKeys = append(inheritedKeys, c.r.key())
		}
		require.NoError(t, parent.acknowledged(ctx))
		require.Len(t, parent.bindings[0].channels, 4)

		storeKeys(t, parent, keysHashingInto(fullShard, "join-post", 4))
		final, err := parent.flush(ctx)
		require.NoError(t, err)
		for _, key := range inheritedKeys {
			del, ok := final[0][key]
			require.True(t, ok, "the drop of %s must ride the checkpoint", key)
			require.Nil(t, del)
		}
		require.Equal(t, 26, countRows())
		parent.stop()

		joinConverged, joinRows = final[0], 26
	})

	t.Run("a joined task splits back onto the pivot the join removed", func(t *testing.T) {
		// Each child's targets are the very names the join's convergence
		// dropped. The switch rejects a declared channel which reports a
		// token, so its success is the proof the drop reached Snowflake and
		// the re-created boundary starts from fresh channels.
		if joinConverged == nil {
			t.Skip("the join subtest did not run to completion")
		}
		channelsPerShard(t, 4)
		var tgt = target("join.v1")

		var low = newManager(lowHalf)
		low.addBinding(cfg.Database, cfg.Schema, tableName, tgt, joinConverged)
		storeKeys(t, low, keysHashingInto(lowShard, "pivot-low", 2))
		require.Len(t, low.bindings[0].channels, 2, "the low child inherits two of the parent's quarters")
		lowEntries, err := low.flush(ctx)
		require.NoError(t, err)
		require.Len(t, lowEntries[0], 6, "two inherited channels and four declared targets")

		require.NoError(t, low.acknowledged(ctx))
		require.Len(t, low.bindings[0].channels, 4)

		storeKeys(t, low, keysHashingInto(lowShard, "pivot-post", 2))
		_, err = low.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, joinRows+4, countRows())

		var high = newManager(highHalf)
		high.addBinding(cfg.Database, cfg.Schema, tableName, tgt, joinConverged)
		storeKeys(t, high, keysHashingInto(highShard, "pivot-high", 2))
		require.Len(t, high.bindings[0].channels, 2, "the high child inherits the other two quarters")
		highEntries, err := high.flush(ctx)
		require.NoError(t, err)
		require.Len(t, highEntries[0], 6)
		require.Equal(t, joinRows+6, countRows())
	})

	t.Run("dropping a channel discards what Snowflake holds for it", func(t *testing.T) {
		// What a channel name outlives is the whole problem the channel drop exists for:
		// it is derived from (task, key-begin, state key), so a shard deleted today
		// and a shard given the same key-begin tomorrow derive the same name. This
		// is the experiment that says which SDK operation actually drops the
		// channel behind that name, since only Snowflake can answer it.
		truncate(t)
		var tgt = target("drop.v1")

		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)

		writeRows(m, 0, 3)
		var dropped = m.bindings[0].channels[0]
		var channel = dropped.name
		entries, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(3), soleItem(t, entries, 0).Counter)

		client, err := m.ensureStarted(ctx)
		require.NoError(t, err)
		var reopen = func() *channelStatusResult {
			status, err := client.OpenChannel(ctx, cfg.Database, cfg.Schema, tableName, channel)
			require.NoError(t, err)
			return status
		}

		t.Run("a close leaves the committed offset token behind, a drop does not", func(t *testing.T) {
			// Closing without dropping is the operation this connector used to
			// have, and the one the channel-drop design was provisionally built on.
			require.NoError(t, client.CloseChannel(ctx, channel, false))
			var afterClose = reopen()
			t.Logf("reopened %s after a close: committed token %v", channel, afterClose.committedToken())

			require.NoError(t, dropChannel(ctx, client, channel))
			var afterDrop = reopen()
			t.Logf("reopened %s after a drop: committed token %v", channel, afterDrop.committedToken())

			// A close only releases the local handle: Snowflake still holds the
			// channel, and hands its committed offset token to the next opener.
			require.NotNil(t, afterClose.CommittedToken)
			require.Equal(t, dropped.offsetToken(3), *afterClose.CommittedToken)

			// A drop is what takes it out of service, so the same name reopens as a channel
			// which has committed nothing.
			require.Nil(t, afterDrop.CommittedToken)

			// The drop is about the channel, not the rows it delivered: those
			// are committed to the table and stay there.
			require.Equal(t, 3, countRows())
		})

		t.Run("a later shard may reuse the dropped name", func(t *testing.T) {
			// A shard given the dropped channel's key-begin — a scale-down followed by a
			// scale-up — derives this same channel name, with no checkpoint item of
			// its own to reconcile against. Nothing of the dropped channel is
			// adopted as a skip threshold, so its documents are appended in full
			// rather than silently dropped as replays.
			var reused = newManager(fullRange)
			reused.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)

			writeRows(reused, 100, 104)
			require.Equal(t, channel, reused.bindings[0].channels[0].name)
			require.Zero(t, reused.bindings[0].channels[0].skip)
			entries, err := reused.flush(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(4), soleItem(t, entries, 0).Counter)
			require.Equal(t, 7, countRows())
		})

		t.Run("the row-error count goes with it", func(t *testing.T) {
			// The committed offset token is not the only thing a channel
			// accumulates: the row-error count which rejects a channel for good is
			// kept for its whole life too. Whether a drop takes that with it
			// is measured rather than assumed, because it is what the fake sidecar
			// and the sidecar's own tests model a drop as doing.
			var notNullTable = "STREAMV2_TEST_DROP_NOT_NULL"
			var rejecting = newManager(fullRange)
			rejecting.addBinding(cfg.Database, cfg.Schema, notNullTable,
				rejectingTable(t, notNullTable, "drop-notnull.v1"), nil)

			require.NoError(t, testWriteRow(ctx, rejecting, 0, []any{"dropped", nil}))
			var rejected = rejecting.bindings[0].channels[0].name
			_, err := rejecting.flush(ctx)
			require.ErrorContains(t, err, "rejected and discarded by Snowflake")

			rejectingClient, err := rejecting.ensureStarted(ctx)
			require.NoError(t, err)
			require.NoError(t, dropChannel(ctx, rejectingClient, rejected))

			afterRejection, err := rejectingClient.OpenChannel(ctx, cfg.Database, cfg.Schema, notNullTable, rejected)
			require.NoError(t, err)
			t.Logf("reopened %s after a drop: committed token %v, %d row(s) rejected",
				rejected, afterRejection.committedToken(), afterRejection.RowsErrorCount)
			require.Zero(t, afterRejection.RowsErrorCount)
			require.Nil(t, afterRejection.CommittedToken)
		})
	})

	t.Run("a backfill truncates its table", func(t *testing.T) {
		// A backfill rotates the binding's state key, which is baked into every
		// channel name (streamV2ChannelName) alongside the epoch, so a channel the
		// specification being replaced holds shares a name with nothing the
		// backfill's own shards ever derive: the table is the only thing they have
		// in common. The crux fact this rests on is empirical — a TRUNCATE, unlike a
		// DROP, does not touch a channel bound to the table — and is pinned here.
		var backfillTable = fmt.Sprintf("STREAMV2_TEST_BACKFILL_TRUNCATE_%d", time.Now().Unix())
		t.Cleanup(func() {
			db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", backfillTable))
		})
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s %s;", backfillTable, testTableColumns))
		require.NoError(t, err)

		var tgt = target("backfill-truncate.v1")
		tgt.Identifier = backfillTable
		tgt.Path = []string{cfg.Schema, backfillTable}
		tgt.DeltaUpdates = true

		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, backfillTable, tgt, nil)
		writeRows(m, 0, 3)
		var c = m.bindings[0].channels[0]
		entries, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(3), soleItem(t, entries, 0).Counter)
		require.Equal(t, 3, countRowsIn(backfillTable))

		// The backfill's effect on this table: a truncate, not a drop.
		_, err = db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s;", backfillTable))
		require.NoError(t, err)
		require.Zero(t, countRowsIn(backfillTable))

		// The channel survives it: its committed offset token stands, and it goes
		// on appending as though nothing happened to the table.
		status, err := m.client.ChannelStatus(ctx, c.name)
		require.NoError(t, err)
		require.Equal(t, c.offsetToken(3), status.committedToken())

		writeRows(m, 3, 5)
		entries, err = m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(5), soleItem(t, entries, 0).Counter)
		require.Equal(t, 2, countRowsIn(backfillTable))
	})

	t.Run("a second task on the table is rejected", func(t *testing.T) {
		// Because a backfill truncates, a second task streaming into the table would
		// wipe the first task's rows while the first task's channels and tokens stand.
		// The first task's channels on the pipe are what betray it to the second.
		var sharedTable = fmt.Sprintf("STREAMV2_TEST_TWO_TASKS_%d", time.Now().Unix())
		t.Cleanup(func() {
			db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", sharedTable))
		})
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s %s;", sharedTable, testTableColumns))
		require.NoError(t, err)

		var tgt = target("two-tasks.v1")
		tgt.Identifier = sharedTable
		tgt.Path = []string{cfg.Schema, sharedTable}
		tgt.DeltaUpdates = true

		var first = newManager(fullRange)
		first.addBinding(cfg.Database, cfg.Schema, sharedTable, tgt, nil)
		writeRows(first, 0, 3)
		_, err = first.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, countRowsIn(sharedTable))
		var firstChannel = first.bindings[0].channels[0].name

		var second = newStreamV2Manager(ctx, &cfg, testMaterialization+"-second", accountName, fullRange)
		second.listChannels = first.listChannels
		t.Cleanup(second.stop)
		second.addBinding(cfg.Database, cfg.Schema, sharedTable, tgt, nil)
		var rejection = testWriteRow(ctx, second, 0, []any{"k", 1, json.RawMessage(`{}`)})
		require.ErrorContains(t, rejection, sanitizeAndAppendHash(testMaterialization))
		require.ErrorContains(t, rejection, firstChannel)
		require.ErrorContains(t, rejection, "always_drop_tables_on_backfill")
		t.Logf("the second task was rejected with: %s", rejection)

		// The second task opened nothing: the first's channel is the only one on the
		// pipe, and the table holds only the first's rows.
		names, err := second.listChannels(ctx, cfg.Database, cfg.Schema, sharedTable)
		require.NoError(t, err)
		require.Equal(t, []string{firstChannel}, names)
		require.Equal(t, 3, countRowsIn(sharedTable))

		// The first task carries on unaffected.
		writeRows(first, 3, 5)
		_, err = first.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, 5, countRowsIn(sharedTable))
	})

	t.Run("rejections", func(t *testing.T) {
		truncate(t)
		smallBatches(t)
		var tgt = target("rejection.v1")

		// Commit a transaction, then leave a further batch committed in
		// Snowflake beyond it, as an interruption would.
		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		writeRows(m, 0, 2)
		entries, err := m.flush(ctx)
		require.NoError(t, err)
		var checkpointed = *soleItem(t, entries, 0)

		writeRows(m, 2, 4)
		var c = m.bindings[0].channels[0]
		require.NoError(t, c.pipe.wait())
		_, err = m.client.WaitCommit(ctx, c.name, c.offsetToken(4))
		require.NoError(t, err)
		m.stop()

		// Each rejection is reported before the document which provoked it can be
		// appended, and the row count is unchanged by the attempt.
		var rowsBefore = countRows()

		t.Run("committed token below the checkpoint counter", func(t *testing.T) {
			var ahead = checkpointed
			ahead.Counter = 100

			var m2 = newManager(fullRange)
			m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(&ahead))
			require.ErrorContains(t, testWriteRow(ctx, m2, 0, []any{"k", 1, json.RawMessage(`{}`)}), "has lost committed data")
			require.Equal(t, rowsBefore, countRows())
		})

		t.Run("a channel crossing this shard's boundary is rejected", func(t *testing.T) {
			// The shard now covers half the range while its checkpointed channel
			// covers the whole of it: a split that did not land on this binding's
			// channel boundaries. The rows outstanding on that channel belong to
			// both children, so neither may skip them.
			var m2 = newManager(&pf.RangeSpec{KeyEnd: math.MaxUint32 / 2, RClockEnd: math.MaxUint32})
			m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(&checkpointed))
			var rejection = testWriteRow(ctx, m2, 0, []any{"k", 1, json.RawMessage(`{}`)})
			require.ErrorContains(t, rejection, "crosses the boundary of this shard's range")
			require.Equal(t, rowsBefore, countRows())
			t.Logf("the replay was rejected with: %s", rejection)
		})
	})

	t.Run("a row Snowflake rejects fails the transaction", func(t *testing.T) {
		// A row Snowflake's ingestion rejects takes a silent path of its own:
		// the append succeeds, the commit wait succeeds because the committed
		// offset token advances over the row, and the row is simply not in the
		// table. Snowflake reports it only in the channel's row-error
		// statistics, so the commit wait is where the transaction has to learn
		// of it rather than acknowledging rows it did not deliver.
		var notNullTable = "STREAMV2_TEST_NOT_NULL"
		var notNullTarget = rejectingTable(t, notNullTable, "notnull.v1")

		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, notNullTable, notNullTarget, nil)

		require.NoError(t, testWriteRow(ctx, m, 0, []any{"kept", json.RawMessage(`{"a":1}`)}))
		require.NoError(t, testWriteRow(ctx, m, 0, []any{"dropped", nil}))
		var channel = m.bindings[0].channels[0].name
		entries, waitErr := m.flush(ctx)
		require.Error(t, waitErr)
		require.ErrorContains(t, waitErr, channel)
		require.Empty(t, entries)

		// The failure carries Snowflake's own account of the rejection, rather
		// than this connector's guess at what Snowflake objected to.
		status, err := m.client.ChannelStatus(ctx, channel)
		require.NoError(t, err)
		require.Equal(t, int64(1), status.RowsErrorCount)
		require.NotEmpty(t, status.LastErrorMessage)
		require.ErrorContains(t, waitErr, status.LastErrorMessage)
		t.Logf("commit wait failed with: %s", waitErr)

		// The rejected row really is absent, which is what the failure is about.
		var keys string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT LISTAGG(KEY, ',') FROM %s;", notNullTable)).Scan(&keys))
		require.Equal(t, "kept", keys)

		// The failure outlives the session that saw it. Snowflake's count is
		// cumulative for the life of the channel, and the transaction failed
		// before it could checkpoint, so a restart reconciles against a checkpoint
		// accounting for none of it: it is rejected as the channel opens rather than
		// replaying the transaction and acknowledging it with the row still
		// missing.
		m.sup.kill()
		var m2 = newManager(fullRange)
		m2.addBinding(cfg.Database, cfg.Schema, notNullTable, notNullTarget, nil)
		require.ErrorContains(t,
			testWriteRow(ctx, m2, 0, []any{"kept", json.RawMessage(`{"a":1}`)}),
			"rejected and discarded by Snowflake")

		// Backfilling the binding is the recovery the failure asks for: it
		// rotates the channel, whose row-error count starts at zero along with
		// its document counter.
		var backfilled = notNullTarget
		backfilled.StateKey = noncedStateKey("notnull.v2")

		var m3 = newManager(fullRange)
		m3.addBinding(cfg.Database, cfg.Schema, notNullTable, backfilled, nil)
		require.NoError(t, testWriteRow(ctx, m3, 0, []any{"backfilled", json.RawMessage(`{"a":2}`)}))
		_, err = m3.flush(ctx)
		require.NoError(t, err)

		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT LISTAGG(KEY, ',') WITHIN GROUP (ORDER BY KEY) FROM %s;", notNullTable)).Scan(&keys))
		require.Equal(t, "backfilled,kept", keys)
	})

	t.Run("unsupported table fails loud", func(t *testing.T) {
		// Streaming into a view is not possible. The SDK validates lazily, and
		// more lazily than the append: opening a channel against a view succeeds,
		// and so does appending to it — the rejection reaches the SDK's
		// background informer afterwards. So an incompatible table cannot be
		// detected before Store (there is no silent fall-through for one), and
		// what the connector sees is the commit wait which produces the
		// checkpoint failing promptly with a classified error rather than
		// hanging.
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW STREAMV2_TEST_VIEW AS SELECT * FROM %s;", tableName))
		require.NoError(t, err)
		defer db.ExecContext(ctx, "DROP VIEW IF EXISTS STREAMV2_TEST_VIEW;")

		var viewTarget = target("view.v1")
		viewTarget.Identifier = "STREAMV2_TEST_VIEW"

		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, "STREAMV2_TEST_VIEW", viewTarget, nil)

		require.NoError(t, testWriteRow(ctx, m, 0, []any{"k", 1, json.RawMessage(`{}`)}))

		var start = time.Now()
		_, err = m.flush(ctx)
		require.Error(t, err)
		var scErr *sidecarError
		require.ErrorAs(t, err, &scErr)
		t.Logf("view ingestion failed after %s: code %s message %s", time.Since(start), scErr.Code, scErr.Message)
	})
}

// TestStreamV2Datatypes sweeps every Snowflake column type this connector's
// dialect emits through the direct-append write path, one table and channel per
// type, and snapshots what Snowflake ends up holding.
//
// This coverage used to be a data-types binding of the integration-harness
// fixture. That fixture now proves only that the write path rejects a task which
// is not on the v2 runtime, which is all `flowctl preview` can drive, so the type
// sweep lives at the seam that actually appends: the manager against live
// Snowflake. The values of each type follow the classic path's sweep in
// TestStreamDatatypes, narrowed to the shapes this connector's converters emit,
// so that the two write paths can be compared column by column.
func TestStreamV2Datatypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var ctx = context.Background()
	var cfg = mustGetCfg(t)
	t.Setenv("SNOWPIPE_SIDECAR_PYTHON", testSidecarPython(t))

	dsn, err := cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	var templates = renderTemplates(testDialect)

	singleChannelLayout(t)

	// One manager, and so one sidecar, serves every type: each type is a binding
	// of its own, with a state key of its own and therefore a channel of its own.
	var m = newStreamV2Manager(ctx, &cfg, "test/streamV2Datatypes", accountName,
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	t.Cleanup(m.stop)

	// The three timestamp types take the same values: offsets Snowflake must
	// resolve rather than truncate, and both ends of the range it accepts.
	var timestamps = []any{
		"2022-08-17T04:32:19.987654321Z",
		"2022-08-17T04:32:19.987654321+02:00",
		"2022-08-17T04:32:19.987654321+03:30",
		"2022-08-17T04:32:19.987654321-07:00",
		"0000-01-01T00:00:00.000000000Z",
		"0000-01-01T00:00:00.000000000+23:59",
		"0000-01-01T00:00:00.000000000-23:59",
		"9999-12-31T23:59:59.999999999Z",
		"9999-12-31T23:59:59.999999999+23:59",
		"9999-12-31T23:59:59.999999999-23:59",
		nil,
	}

	for binding, tt := range []struct {
		ddl  string
		vals []any
	}{
		{
			ddl: "INTEGER",
			// Both ends of what NUMBER(38,0) holds, which a `format: integer`
			// string reaches as a big.Int, along with a magnitude only a float64
			// can carry.
			vals: []any{-1, 0, 1, 1234, math.MaxInt64, nil, math.MinInt64, float64(math.MaxUint64) * 10_000, minSnowflakeInteger.BigInt(), maxSnowflakeInteger.BigInt(), uint64(math.MaxUint64)},
		},
		{
			ddl: "TEXT",
			// A binary field materialized without the native BINARY column type
			// arrives as raw bytes and lands base64-encoded, as it does through a
			// staged file, which decodes only columns that are BINARY.
			vals: []any{"hello", "", nil, "1234", "true", "1234.1234", []byte("hello bytes")},
		},
		{
			ddl: "BINARY",
			// The column type native_binary_column_type produces, which is on by
			// default. A []byte is JSON-encoded as base64 on its way to the SDK,
			// so these prove Snowflake decodes that back into the column's bytes
			// — including bytes which are not valid UTF-8, and so could not have
			// survived as text.
			vals: []any{[]byte("hello bytes"), []byte{}, nil, []byte{0x00, 0xff, 0xfe}, []byte{0xde, 0xad, 0xbe, 0xef}},
		},
		{
			ddl: "VARIANT",
			// Every value of a VARIANT column passes through ToJsonBytes, so the
			// connector only ever appends pre-encoded JSON or nothing. An absent
			// or JSON-null field must land as SQL NULL, as it does through the
			// classic path, and not as a VARIANT holding null: the two encodings
			// answer `IS NULL` and `TYPEOF` differently, so a table written by
			// one path would not be interchangeable with one written by the other.
			vals: []any{
				json.RawMessage(`"hello"`),
				json.RawMessage(`""`),
				nil,
				json.RawMessage(`1234`),
				json.RawMessage(`true`),
				json.RawMessage(`1234.1234`),
				json.RawMessage(`[1,2,3]`),
				json.RawMessage(`{"some":"doc"}`),
				json.RawMessage(`{"nested":{"deep":["a",1,null,{"b":true},[]]},"unicode":"你好 🌟"}`),
			},
		},
		{
			ddl:  "BOOLEAN",
			vals: []any{true, false, nil},
		},
		{
			ddl: "FLOAT",
			// "NaN", "inf" and "-inf" are the sentinels this dialect substitutes
			// for the special values of a `format: number` string, which have no
			// JSON representation of their own.
			vals: []any{int64(1234), nil, 1234.0, 4321.1234, math.MaxFloat64, math.SmallestNonzeroFloat64, "NaN", "inf", "-inf"},
		},
		{
			ddl:  "DATE",
			vals: []any{"2022-03-21", nil, "0000-01-01", "9999-12-31"},
		},
		{
			ddl:  "TIMESTAMP_LTZ",
			vals: timestamps,
		},
		{
			ddl:  "TIMESTAMP_TZ",
			vals: timestamps,
		},
		{
			ddl:  "TIMESTAMP_NTZ",
			vals: timestamps,
		},
	} {
		t.Run(tt.ddl, func(t *testing.T) {
			var tbl = sql.Table{
				TableShape: sql.TableShape{Binding: binding, DeltaUpdates: true},
				Identifier: "STREAMV2_DATATYPES_" + tt.ddl,
				Keys:       []sql.Column{{Identifier: `KEY`, MappedType: sql.MappedType{DDL: "INTEGER"}}},
				Values:     []sql.Column{{Identifier: `VAL`, MappedType: sql.MappedType{DDL: tt.ddl}}},
				StateKey:   "datatypes." + tt.ddl,
			}

			// Dropping the table drops the channels opened against it, so each
			// run starts from a channel with no committed offset token and
			// nothing to skip.
			var cleanup = func() {
				db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tbl.Identifier))
			}
			cleanup()
			t.Cleanup(cleanup)

			var createQuery strings.Builder
			require.NoError(t, templates.createTargetTable.Execute(&createQuery, &tbl))
			_, err := db.ExecContext(ctx, createQuery.String())
			require.NoError(t, err)

			m.addBinding(cfg.Database, cfg.Schema, tbl.Identifier, tbl, nil)
			for i, val := range tt.vals {
				require.NoError(t, m.writeRow(ctx, binding, packKey(i), []any{i, val}))
			}

			entries, err := m.flush(ctx)
			require.NoError(t, err)
			require.Len(t, entries, 1, "only the binding stored to this transaction reports an item")
			require.Equal(t, int64(len(tt.vals)), soleItem(t, entries, binding).Counter)

			dump, err := sql.StdDumpTable(ctx, db, tbl)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, dump)
		})
	}
}

// TestStreamV2AdoptsATableWithNoStateKey covers a table that the streaming v2
// path did not create: one an older build of this connector created, or one a
// bdec build of the same binding created, carries no recorded state key in
// its comment.
//
// TestStreamV2ListChannels drives the channel listing against live Snowflake: a
// table that has never streamed reports no channels, and one that has reports the
// channel the write path opened on it, by the name the write path derived.
func TestStreamV2ListChannels(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var ctx = context.Background()
	var cfg = mustGetCfg(t)
	t.Setenv("SNOWPIPE_SIDECAR_PYTHON", testSidecarPython(t))
	singleChannelLayout(t)

	dsn, err := cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	// Upper case, which is what Snowflake stores for the unquoted identifier the
	// CREATE below uses, so the name passed to the listing is the name Snowflake
	// holds the table — and its default pipe — under.
	var tableName = fmt.Sprintf("STREAMV2_LIST_FLOW_TEST_%d", time.Now().Unix())
	var cleanup = func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	}
	cleanup()
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (KEY TEXT, VAL NUMBER);", tableName))
	require.NoError(t, err)
	// Registered before the manager's stop below, so the cleanup stack stops the
	// manager while the table it holds a channel on still exists.
	t.Cleanup(cleanup)

	// A table nothing has streamed into has no default pipe, so no channels.
	names, err := streamV2ListChannels(ctx, db, testDialect, cfg.Database, cfg.Schema, tableName)
	require.NoError(t, err)
	require.Empty(t, names)

	// Stream one row, which opens a channel on the table's default pipe.
	var m = newStreamV2Manager(ctx, &cfg, "test/streamV2List", accountName,
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	t.Cleanup(m.stop)
	m.addBinding(cfg.Database, cfg.Schema, tableName, sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: tableName,
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "list.v1",
	}, nil)
	require.NoError(t, testWriteRow(ctx, m, 0, []any{"k", 1}))
	var opened = m.bindings[0].channels[0].name
	_, err = m.flush(ctx)
	require.NoError(t, err)

	// The listing names that channel, by the name the write path derived for it.
	// The first stream is what creates the table's default pipe, so the listing
	// is polled to tolerate any delay in that pipe becoming visible. Eventually
	// runs its condition off the test goroutine, so the error is carried out and
	// checked here rather than failing inside.
	require.Eventually(t, func() bool {
		names, err = streamV2ListChannels(ctx, db, testDialect, cfg.Database, cfg.Schema, tableName)
		return err != nil || slices.Contains(names, opened)
	}, 3*time.Minute, 5*time.Second)
	require.NoError(t, err)
	require.Contains(t, names, opened)
}
