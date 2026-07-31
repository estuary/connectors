package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
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

	var tableName = "STREAMV2_TEST"
	var cleanup = func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	}
	cleanup()
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s %s;", tableName, testTableColumns))
	require.NoError(t, err)
	defer cleanup()

	// Each subtest uses its own state key, and so its own channel, so that the
	// committed offset token one subtest leaves behind cannot reach another.
	var target = func(stateKey string) sql.Table {
		return sql.Table{
			TableShape: sql.TableShape{Binding: 0},
			Identifier: tableName,
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `INTCOL`}, {Identifier: `DOC`}},
			StateKey:   stateKey,
		}
	}

	var fullRange = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}

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
		var m = newStreamV2Manager(ctx, &cfg, "test/streamV2Materialization", accountName, keyRange)
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
			require.NoError(t, m.writeRow(ctx, 0, []any{
				fmt.Sprintf("key-%d", i),
				i,
				json.RawMessage(fmt.Sprintf(`{"nested":{"i":%d},"arr":[1,2]}`, i)),
			}))
		}
	}

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
			StateKey:   stateKey,
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
		items, err := m.flush(ctx)
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.Equal(t, int64(100), items[0].Counter)
		require.Equal(t, 100, countRows())

		// VARIANT columns must round-trip as real JSON objects, not strings.
		var docType string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT TYPEOF(doc) FROM %s WHERE key = 'key-7';`, tableName)).Scan(&docType))
		require.Equal(t, "OBJECT", docType)

		// A transaction which stores nothing for the binding reports no item, so
		// the counter already in the checkpoint stands.
		items, err = m.flush(ctx)
		require.NoError(t, err)
		require.Empty(t, items)

		// The counter continues across transactions rather than restarting.
		writeRows(m, 100, 150)
		items, err = m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(150), items[0].Counter)
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
		items, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(50), items[0].Counter)
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
		var channel = m1.bindings[0].channel

		writeRows(m1, 0, 5)
		require.NoError(t, m1.bindings[0].pipe.wait())
		_, err = m1.client.WaitCommit(ctx, channel, "4")
		require.NoError(t, err)
		require.Equal(t, 4, countRows())
		m1.sup.kill()

		// The runtime replays the identical document sequence. Snowflake already
		// holds the first four, so only the fifth is appended.
		var m2 = newManager(fullRange)
		m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)

		writeRows(m2, 0, 5)
		require.Equal(t, int64(4), m2.bindings[0].skip)
		items, err := m2.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(5), items[0].Counter)
		require.Equal(t, 5, countRows())

		// A second interruption, now with a checkpoint counter to reconcile the
		// committed token against.
		var checkpointed = items[0]
		writeRows(m2, 5, 8)
		require.NoError(t, m2.bindings[0].pipe.wait())
		_, err = m2.client.WaitCommit(ctx, channel, "7")
		require.NoError(t, err)
		require.Equal(t, 7, countRows())
		m2.sup.kill()

		var m3 = newManager(fullRange)
		m3.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(checkpointed))

		writeRows(m3, 5, 8)
		require.Equal(t, int64(7), m3.bindings[0].skip)
		items, err = m3.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(8), items[0].Counter)
		require.Equal(t, 8, countRows())
	})

	t.Run("a key split gives the high child a channel of its own", func(t *testing.T) {
		// Both children of a key split inherit the parent's checkpoint, and Flow
		// gives the low child the parent's key-begin while the high child takes
		// the pivot (activate::map_shard_to_split). A channel is named for its
		// shard's key-begin, so the low child continues on the parent's channel
		// and its counter, while the high child derives a channel of its own.
		truncate(t)
		var tgt = target("split.v1")
		var pivot uint32 = math.MaxUint32/2 + 1

		var parent = newManager(fullRange)
		parent.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		var parentChannel = parent.bindings[0].channel

		writeRows(parent, 0, 5)
		items, err := parent.flush(ctx)
		require.NoError(t, err)
		var checkpointed = *items[0]
		parent.stop()

		// Each child stores documents of its own, standing in for the disjoint
		// key subsets a re-shuffled journal read would deliver to them.
		var low = newManager(&pf.RangeSpec{KeyEnd: pivot - 1, RClockEnd: math.MaxUint32})
		low.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(&checkpointed))
		require.Equal(t, parentChannel, low.bindings[0].channel)

		// The low child's range is narrower than the one recorded alongside the
		// counter it inherited, which is not by itself a problem: the guard
		// against a re-split shard's replay is consulted only when Snowflake is
		// ahead of that counter, and a split from a checkpointed parent leaves
		// the two level.
		writeRows(low, 5, 8)
		require.Equal(t, int64(5), low.bindings[0].skip)
		items, err = low.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(8), items[0].Counter)

		// The high child's key-begin is the pivot, so it opens a channel which has
		// committed nothing: there is no token to reconcile the inherited counter
		// against, and nothing to skip. Its documents are appended in full.
		var high = newManager(&pf.RangeSpec{KeyBegin: pivot, KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		high.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(&checkpointed))
		require.NotEqual(t, parentChannel, high.bindings[0].channel)

		writeRows(high, 100, 103)
		require.Zero(t, high.bindings[0].skip)

		items, err = high.flush(ctx)
		require.NoError(t, err)
		// The counter belongs to the channel rather than to the binding, so this
		// one starts from nothing: the counter the checkpoint carries is the low
		// child's to continue, and counting on from it here would leave this
		// channel's offset tokens claiming documents it never held.
		require.Equal(t, int64(3), items[0].Counter)
		// The item names the high child's own channel and range, not the parent's
		// it inherited.
		require.Equal(t, high.bindings[0].channel, items[0].Channel)
		require.Equal(t, pivot, items[0].KeyBegin)

		// Neither child re-appended what the parent had already stored, and
		// neither dropped a document of its own.
		require.Equal(t, 11, countRows())
	})

	t.Run("joining two shards into one", func(t *testing.T) {
		// Flow has no join command, but nothing in the runtime forbids one:
		// shard ranges need only tile the key space exactly, so widening a
		// surviving shard and deleting its sibling is a legal topology. The
		// survivor then covers keys whose documents the deleted shard had been
		// appending to a channel of its own, and whether it may carry on turns on
		// whether that shard left its channel settled.
		truncate(t)
		var tgt = target("join.v1")
		var pivot uint32 = math.MaxUint32/2 + 1
		var loRange = &pf.RangeSpec{KeyEnd: pivot - 1, RClockEnd: math.MaxUint32}

		var lo = newManager(loRange)
		lo.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		writeRows(lo, 0, 3)
		loItems, err := lo.flush(ctx)
		require.NoError(t, err)

		var hi = newManager(&pf.RangeSpec{KeyBegin: pivot, KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		hi.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		writeRows(hi, 100, 105)
		hiItems, err := hi.flush(ctx)
		require.NoError(t, err)
		require.NotEqual(t, loItems[0].Channel, hiItems[0].Channel)
		require.Equal(t, 8, countRows())
		lo.stop()
		hi.stop()

		// Steady state first: the two shards share one connector-state document,
		// so each reads the other's item back and must carry on regardless. The
		// sibling's key-begin is outside this shard's range, which is what says
		// the sibling is still running.
		var loAgain = newManager(loRange)
		loAgain.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(loItems[0], hiItems[0]))
		writeRows(loAgain, 3, 5)
		require.Equal(t, int64(3), loAgain.bindings[0].skip)
		items, err := loAgain.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(5), items[0].Counter)
		require.Equal(t, 10, countRows())
		loAgain.stop()

		// The join: the survivor keeps the low shard's key-begin, and so its
		// channel, but now covers the whole range — which puts the deleted
		// shard's key-begin inside it.
		var loItem = items[0]
		var joined = newManager(fullRange)
		joined.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(loItem, hiItems[0]))

		// The absorbed shard committed everything its checkpoint accounted for, so
		// the task's frontier accounts for those documents too and they are not
		// delivered here again. The join needs no backfill: the absorbed channel is
		// settled and retired, and this shard carries on appending to its own.
		writeRows(joined, 5, 8)
		require.Equal(t, []string{hiItems[0].Channel}, joined.bindings[0].retire)
		require.Equal(t, int64(5), joined.bindings[0].skip)

		items, err = joined.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(8), items[0].Counter)

		// The absorbed shard's rows are still in the table, and the joined shard
		// added its own without re-appending any of them.
		require.Equal(t, 13, countRows())

		// The retirement is reported as a deletion of the absorbed channel's
		// entry, which a merge patch applies while leaving this shard's own.
		var entries = joined.checkpointFor(0, items[0])
		require.Len(t, entries, 2)
		require.Equal(t, items[0], entries[items[0].Channel])
		require.Nil(t, entries[hiItems[0].Channel])
		joined.stop()

		// An absorbed shard which was interrupted is a different matter: it left
		// documents committed beyond what its checkpoint accounts for, so the
		// frontier will deliver them here, to a channel whose committed offset
		// token cannot tell them from new ones.
		var interrupted = newManager(&pf.RangeSpec{KeyBegin: pivot, KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		interrupted.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(hiItems[0]))
		writeRows(interrupted, 105, 108)
		_, err = interrupted.flush(ctx) // appended and committed, never checkpointed
		require.NoError(t, err)
		require.Equal(t, 16, countRows())
		interrupted.sup.kill()

		var rowsBefore = countRows()
		var joinedAgain = newManager(fullRange)
		joinedAgain.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(loItem, hiItems[0]))

		var refusal = joinedAgain.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)})
		require.ErrorContains(t, refusal, "will be replayed to this shard")
		require.ErrorContains(t, refusal, hiItems[0].Channel)
		require.Equal(t, rowsBefore, countRows())
		t.Logf("interrupted join refused with: %s", refusal)

		// Backfilling the binding is the recovery that refusal asks for: it
		// rotates the channel, and no item of the absorbed shard is carried into
		// the new state key.
		var afterBackfill = newManager(fullRange)
		afterBackfill.addBinding(cfg.Database, cfg.Schema, tableName, target("join.v2"), nil)
		require.NoError(t, afterBackfill.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)}))
		items, err = afterBackfill.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, rowsBefore+1, countRows())
	})

	t.Run("retiring a channel discards what Snowflake holds for it", func(t *testing.T) {
		// What a channel name outlives is the whole problem retirement exists for:
		// it is derived from (task, key-begin, state key), so a shard deleted today
		// and a shard given the same key-begin tomorrow derive the same name. This
		// is the experiment that says which SDK operation actually retires the
		// channel behind that name, since only Snowflake can answer it.
		truncate(t)
		var tgt = target("retire.v1")

		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		var channel = m.bindings[0].channel

		writeRows(m, 0, 3)
		items, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(3), items[0].Counter)

		client, err := m.ensureStarted(ctx)
		require.NoError(t, err)
		var reopen = func() *channelStatusResult {
			status, err := client.OpenChannel(ctx, cfg.Database, cfg.Schema, tableName, channel)
			require.NoError(t, err)
			return status
		}

		t.Run("a close leaves the committed offset token behind, a drop does not", func(t *testing.T) {
			// Closing without dropping is the operation this connector used to
			// have, and the one the retirement design was provisionally built on.
			require.NoError(t, client.CloseChannel(ctx, channel, false))
			var afterClose = reopen()
			t.Logf("reopened %s after a close: committed token %v", channel, afterClose.committedToken())

			require.NoError(t, retireChannel(ctx, client, channel))
			var afterDrop = reopen()
			t.Logf("reopened %s after a retirement: committed token %v", channel, afterDrop.committedToken())

			// A close only releases the local handle: Snowflake still holds the
			// channel, and hands its committed offset token to the next opener.
			require.NotNil(t, afterClose.CommittedToken)
			require.Equal(t, "3", *afterClose.CommittedToken)

			// A drop is what retires it, so the same name reopens as a channel
			// which has committed nothing.
			require.Nil(t, afterDrop.CommittedToken)

			// Retirement is about the channel, not the rows it delivered: those
			// are committed to the table and stay there.
			require.Equal(t, 3, countRows())
		})

		t.Run("a later shard may reuse the retired name", func(t *testing.T) {
			// A shard given the retired key-begin — a scale-down followed by a
			// scale-up — derives this same channel name, with no checkpoint item of
			// its own to reconcile against. Nothing of the retired channel is
			// adopted as a skip threshold, so its documents are appended in full
			// rather than silently dropped as replays.
			var reused = newManager(fullRange)
			reused.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
			require.Equal(t, channel, reused.bindings[0].channel)

			writeRows(reused, 100, 104)
			require.Zero(t, reused.bindings[0].skip)
			items, err := reused.flush(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(4), items[0].Counter)
			require.Equal(t, 7, countRows())
		})

		t.Run("the row-error count goes with it", func(t *testing.T) {
			// The committed offset token is not the only thing a channel
			// accumulates: the row-error count which refuses a channel for good is
			// kept for its whole life too. Whether a retirement takes that with it
			// is measured rather than assumed, because it is what the fake sidecar
			// and the sidecar's own tests model a drop as doing.
			var notNullTable = "STREAMV2_TEST_RETIRE_NOT_NULL"
			var rejecting = newManager(fullRange)
			rejecting.addBinding(cfg.Database, cfg.Schema, notNullTable,
				rejectingTable(t, notNullTable, "retire-notnull.v1"), nil)
			var rejected = rejecting.bindings[0].channel

			require.NoError(t, rejecting.writeRow(ctx, 0, []any{"dropped", nil}))
			_, err := rejecting.flush(ctx)
			require.ErrorContains(t, err, "rejected and discarded by Snowflake")

			rejectingClient, err := rejecting.ensureStarted(ctx)
			require.NoError(t, err)
			require.NoError(t, retireChannel(ctx, rejectingClient, rejected))

			afterRejection, err := rejectingClient.OpenChannel(ctx, cfg.Database, cfg.Schema, notNullTable, rejected)
			require.NoError(t, err)
			t.Logf("reopened %s after a retirement: committed token %v, %d row(s) rejected",
				rejected, afterRejection.committedToken(), afterRejection.RowsErrorCount)
			require.Zero(t, afterRejection.RowsErrorCount)
			require.Nil(t, afterRejection.CommittedToken)
		})
	})

	t.Run("a backfill the outgoing generation cannot race", func(t *testing.T) {
		// Bumping a binding's backfill re-materializes it into the same table,
		// and Apply does that while the outgoing generation's shards are still
		// storing: nothing stops them first. Their channels are bound to that
		// same table, so what Apply does to the table is what decides whether the
		// rows they append next end up in what the new generation materializes.
		// Only Snowflake can say which operation takes those channels with it, so
		// each is measured here in turn.
		var turnoverTable = "STREAMV2_TEST_TURNOVER"

		// recreateTable drops and re-creates the table, as Apply does for a
		// streaming v2 binding whose backfill counter has been bumped.
		var recreateTable = func(t *testing.T) {
			_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", turnoverTable))
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s %s;", turnoverTable, testTableColumns))
			require.NoError(t, err)
		}
		t.Cleanup(func() {
			db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", turnoverTable))
		})

		var turnoverTarget = func(stateKey string) sql.Table {
			var tgt = target(stateKey)
			tgt.Identifier = turnoverTable
			tgt.DeltaUpdates = true
			return tgt
		}

		// outgoingGeneration is a shard of the generation being replaced, holding
		// an open channel with three documents appended, committed, and accounted
		// for by its checkpoint.
		var outgoingGeneration = func(t *testing.T, stateKey string) (*streamV2Manager, streamV2Item) {
			recreateTable(t)

			var m = newManager(fullRange)
			m.addBinding(cfg.Database, cfg.Schema, turnoverTable, turnoverTarget(stateKey), nil)

			writeRows(m, 0, 3)
			items, err := m.flush(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(3), items[0].Counter)
			require.Equal(t, 3, countRowsIn(turnoverTable))
			return m, *items[0]
		}

		t.Run("a truncate leaves it appending", func(t *testing.T) {
			// This is the defect. The truncate empties the table for the new
			// generation while the outgoing generation's channel — untouched, and
			// still bound to that table — goes on appending into it. Those rows
			// land after the truncate, survive it, and are then re-materialized by
			// the new generation: a duplicate of each, for good, on a
			// delta-updates binding.
			var outgoing, _ = outgoingGeneration(t, "turnover-truncate.v1")

			_, err := db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s;", turnoverTable))
			require.NoError(t, err)
			require.Zero(t, countRowsIn(turnoverTable))

			writeRows(outgoing, 3, 5)
			items, err := outgoing.flush(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(5), items[0].Counter)
			require.Equal(t, 2, countRowsIn(turnoverTable))
			outgoing.stop()
		})

		t.Run("a drop and re-create does not", func(t *testing.T) {
			// Dropping the table takes every channel bound to it, named by the
			// checkpoint or not, so the outgoing generation's appends fail instead
			// of landing in the table the new generation is re-materializing.
			var outgoing, checkpointed = outgoingGeneration(t, "turnover-drop.v1")

			recreateTable(t)

			writeRows(outgoing, 3, 5)
			_, err := outgoing.flush(ctx)
			require.Error(t, err)
			require.Zero(t, countRowsIn(turnoverTable))
			t.Logf("the outgoing generation's append failed with: %s", err)
			outgoing.sup.kill()

			// Nor may it come back. A shard which has not yet been told to stop
			// restarts and reopens a channel Snowflake no longer holds: its
			// committed offset token is gone while the checkpoint still records
			// three documents appended to it. That is the same lost-state refusal
			// an absorbed channel gets from a join, and the only reading of it
			// which does not append this generation's documents into a table that
			// has been re-materialized under them.
			var restarted = newManager(fullRange)
			restarted.addBinding(cfg.Database, cfg.Schema, turnoverTable,
				turnoverTarget("turnover-drop.v1"), priorOf(&checkpointed))

			var refusal = restarted.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)})
			require.ErrorContains(t, refusal, "has lost committed data")
			require.ErrorContains(t, refusal, checkpointed.Channel)
			require.Zero(t, countRowsIn(turnoverTable))
			t.Logf("the outgoing generation's reopen was refused with: %s", refusal)
		})
	})

	t.Run("refusals", func(t *testing.T) {
		truncate(t)
		smallBatches(t)
		var tgt = target("refusal.v1")

		// Commit a transaction, then leave a further batch committed in
		// Snowflake beyond it, as an interruption would.
		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		writeRows(m, 0, 2)
		items, err := m.flush(ctx)
		require.NoError(t, err)
		var checkpointed = *items[0]

		writeRows(m, 2, 4)
		require.NoError(t, m.bindings[0].pipe.wait())
		_, err = m.client.WaitCommit(ctx, m.bindings[0].channel, "4")
		require.NoError(t, err)
		m.stop()

		// Each refusal is reported before the document which provoked it can be
		// appended, and the row count is unchanged by the attempt.
		var rowsBefore = countRows()

		t.Run("committed token below the checkpoint counter", func(t *testing.T) {
			var ahead = checkpointed
			ahead.Counter = 100

			var m2 = newManager(fullRange)
			m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(&ahead))
			require.ErrorContains(t, m2.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)}), "has lost committed data")
			require.Equal(t, rowsBefore, countRows())
		})

		t.Run("key range changed with uncommitted appends", func(t *testing.T) {
			// The shard kept its key-begin, and so its channel, but now covers
			// half the range: a replay would carry a different set of documents.
			var m2 = newManager(&pf.RangeSpec{KeyEnd: math.MaxUint32 / 2, RClockEnd: math.MaxUint32})
			m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(&checkpointed))
			require.ErrorContains(t, m2.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)}), "shard key range has changed")
			require.Equal(t, rowsBefore, countRows())
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
		var channel = m.bindings[0].channel

		require.NoError(t, m.writeRow(ctx, 0, []any{"kept", json.RawMessage(`{"a":1}`)}))
		require.NoError(t, m.writeRow(ctx, 0, []any{"dropped", nil}))
		items, waitErr := m.flush(ctx)
		require.Error(t, waitErr)
		require.ErrorContains(t, waitErr, channel)
		require.Empty(t, items)

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
		// accounting for none of it: it refuses as the channel opens rather than
		// replaying the transaction and acknowledging it with the row still
		// missing.
		m.sup.kill()
		var m2 = newManager(fullRange)
		m2.addBinding(cfg.Database, cfg.Schema, notNullTable, notNullTarget, nil)
		require.ErrorContains(t,
			m2.writeRow(ctx, 0, []any{"kept", json.RawMessage(`{"a":1}`)}),
			"rejected and discarded by Snowflake")

		// Backfilling the binding is the recovery the failure asks for: it
		// rotates the channel, whose row-error count starts at zero along with
		// its document counter.
		var backfilled = notNullTarget
		backfilled.StateKey = "notnull.v2"

		var m3 = newManager(fullRange)
		m3.addBinding(cfg.Database, cfg.Schema, notNullTable, backfilled, nil)
		require.NoError(t, m3.writeRow(ctx, 0, []any{"backfilled", json.RawMessage(`{"a":2}`)}))
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

		require.NoError(t, m.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)}))

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
// fixture. That fixture now proves only that the write path refuses a task which
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
				require.NoError(t, m.writeRow(ctx, binding, []any{i, val}))
			}

			items, err := m.flush(ctx)
			require.NoError(t, err)
			require.Len(t, items, 1, "only the binding stored to this transaction reports an item")
			require.Equal(t, int64(len(tt.vals)), items[binding].Counter)

			dump, err := sql.StdDumpTable(ctx, db, tbl)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, dump)
		})
	}
}
