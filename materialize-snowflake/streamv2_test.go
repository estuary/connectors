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
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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
		var m = newStreamV2Manager(ctx, &cfg, testMaterialization, accountName, keyRange)
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
		_, err = m1.client.WaitCommit(ctx, channel, m1.offsetToken(4))
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
		_, err = m2.client.WaitCommit(ctx, channel, m2.offsetToken(7))
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

	t.Run("a split interrupted before the low child commits", func(t *testing.T) {
		// A split under load. The low child inherits the parent's channel along with
		// the range recorded beside its counter, appends documents of its own, and is
		// interrupted before the transaction carrying them commits — Snowflake having
		// already committed what it was given, which during a backfill's long rounds
		// is the normal state to be interrupted in.
		//
		// Its committed offset token names the range which appended those documents,
		// and that range is this shard's own, so the replay is its own to skip.
		truncate(t)
		smallBatches(t)
		var tgt = target("splitload.v1")
		var pivot uint32 = math.MaxUint32/2 + 1
		var loRange = &pf.RangeSpec{KeyEnd: pivot - 1, RClockEnd: math.MaxUint32}

		var parent = newManager(fullRange)
		parent.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		writeRows(parent, 0, 4)
		items, err := parent.flush(ctx)
		require.NoError(t, err)
		var checkpointed = *items[0]
		parent.stop()

		var low = newManager(loRange)
		low.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(&checkpointed))
		require.Equal(t, checkpointed.Channel, low.bindings[0].channel)
		writeRows(low, 4, 7)
		_, err = low.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, 7, countRows())
		low.sup.kill()

		var replay = newManager(loRange)
		replay.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(&checkpointed))
		writeRows(replay, 4, 7)
		require.Equal(t, int64(7), replay.bindings[0].skip)

		items, err = replay.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(7), items[0].Counter)
		require.Equal(t, loRange.KeyEnd, items[0].KeyEnd)

		// And it appends its own documents from there.
		writeRows(replay, 7, 9)
		items, err = replay.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(9), items[0].Counter)

		// One row per document: the replay dropped none of its own and duplicated
		// none of what Snowflake already held.
		var distinct int
		require.NoError(t, db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(DISTINCT KEY) FROM %s;", tableName)).Scan(&distinct))
		require.Equal(t, 9, countRows())
		require.Equal(t, 9, distinct)
	})

	t.Run("a backfill the outgoing generation cannot race", func(t *testing.T) {
		// Bumping a binding's backfill re-materializes it into the same table,
		// and Apply does that while the outgoing generation's shards are still
		// storing: nothing stops them first. Their channels are bound to that
		// same table, so what Apply does to the table is what decides whether the
		// rows they append next end up in what the new generation materializes.
		// Only Snowflake can say which operation takes those channels with it, so
		// each is measured here in turn.
		// Each experiment takes a table of its own, named for this run. Snowflake's
		// ingestion goes on serving the table it knew under a name for some time
		// after that table is dropped, so a table re-created under a name another
		// experiment — or an earlier run — has streamed to cannot be appended to
		// promptly. Nothing in the connector waits for that: a shard which meets it
		// fails and is restarted, which is the very thing these experiments drive.
		var runNonce = fmt.Sprintf("%d", time.Now().Unix())
		// Named in upper case, which is what Snowflake stores for the unquoted
		// identifier the CREATE below uses, and so what a SHOW reports it as.
		var newTurnoverTable = func(t *testing.T, experiment string) string {
			var table = fmt.Sprintf("STREAMV2_TEST_TURNOVER_%s_%s", strings.ToUpper(experiment), runNonce)
			t.Cleanup(func() {
				db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", table))
			})
			return table
		}

		// recreateTable drops and re-creates the table, as Apply does for a
		// streaming v2 binding whose backfill counter has been bumped, recording the
		// generation it re-creates the table for the way client.CreateTable does. An
		// empty owner stands for a table created before this connector recorded
		// generations at all.
		var recreateTable = func(t *testing.T, turnoverTable, owner string) {
			_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", turnoverTable))
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s %s;", turnoverTable, testTableColumns))
			require.NoError(t, err)

			if owner == "" {
				return
			}
			var comment = streamV2GenerationComment(
				"Generated for materialization "+testMaterialization,
				streamV2Generation{materialization: testMaterialization, stateKey: owner})
			_, err = db.ExecContext(ctx, fmt.Sprintf("COMMENT ON TABLE %s IS %s;",
				turnoverTable, testDialect.Literal(comment)))
			require.NoError(t, err)
		}

		var turnoverTarget = func(turnoverTable, stateKey string) sql.Table {
			var tgt = target(stateKey)
			tgt.Identifier = turnoverTable
			tgt.Path = []string{cfg.Schema, turnoverTable}
			tgt.DeltaUpdates = true
			return tgt
		}

		// readComment is what the transactor hands the manager: the comment of the
		// binding's table as Snowflake reports it, read as each channel is opened.
		var readComment = func(ctx context.Context, database, schema, table string) (string, error) {
			return streamV2TableComment(ctx, db, testDialect, database, schema, table)
		}

		// outgoingGeneration is a shard of the generation being replaced, holding
		// an open channel with three documents appended, committed, and accounted
		// for by its checkpoint.
		var outgoingGeneration = func(t *testing.T, turnoverTable, stateKey string) (*streamV2Manager, streamV2Item) {
			recreateTable(t, turnoverTable, "")

			var m = newManager(fullRange)
			m.addBinding(cfg.Database, cfg.Schema, turnoverTable, turnoverTarget(turnoverTable, stateKey), nil)

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
			var turnoverTable = newTurnoverTable(t, "truncate")
			var outgoing, _ = outgoingGeneration(t, turnoverTable, "turnover-truncate.v1")

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
			var turnoverTable = newTurnoverTable(t, "drop")
			var outgoing, checkpointed = outgoingGeneration(t, turnoverTable, "turnover-drop.v1")

			// Re-created without recording a generation, which is what a table
			// created before this connector recorded them looks like: the committed
			// offset token is all this shard has to go on.
			recreateTable(t, turnoverTable, "")

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
				turnoverTarget(turnoverTable, "turnover-drop.v1"), priorOf(&checkpointed))

			// Reopening is what Snowflake's ingestion answers only once it has caught
			// up with the re-created table; until then it fails the open outright
			// rather than reporting the fresh channel. A shard meeting that restarts,
			// so the refusal is what it arrives at rather than what it sees first.
			var refusal error
			require.Eventually(t, func() bool {
				refusal = restarted.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)})
				require.Error(t, refusal)
				if strings.Contains(refusal.Error(), "has lost committed data") {
					return true
				}
				t.Logf("awaiting the reopen, which failed with: %s", refusal)
				return false
			}, 3*time.Minute, 5*time.Second)

			require.ErrorContains(t, refusal, checkpointed.Channel)
			require.Zero(t, countRowsIn(turnoverTable))
			t.Logf("the outgoing generation's reopen was refused with: %s", refusal)
		})

		t.Run("nor may a shard which committed nothing for the binding", func(t *testing.T) {
			// The refusal above rests on there being a checkpoint item to contradict.
			// A shard which has committed nothing for the binding has none, and a
			// channel Snowflake has never held is exactly what its first transaction
			// expects to open — so nothing about the channel tells it apart from a
			// shard of the generation the backfill published. The table is what tells
			// them apart: it records the generation it was created for.
			var turnoverTable = newTurnoverTable(t, "owned")
			recreateTable(t, turnoverTable, "turnover-owned.v1")

			// Steady state first: the table records this shard's own generation, so
			// it appends as usual.
			var owner = newManager(fullRange)
			owner.tableComment = readComment
			owner.addBinding(cfg.Database, cfg.Schema, turnoverTable, turnoverTarget(turnoverTable, "turnover-owned.v1"), nil)

			writeRows(owner, 0, 2)
			items, err := owner.flush(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(2), items[0].Counter)
			require.Equal(t, 2, countRowsIn(turnoverTable))
			owner.stop()

			// The turnover, with this shard having stored nothing for the binding of
			// its own. This session is built before the backfill lands, as a shard
			// which is still running through one is: what it may append is settled by
			// the table as its channel opens, not by the table as it started.
			var stale = newManager(fullRange)
			stale.tableComment = readComment
			stale.addBinding(cfg.Database, cfg.Schema, turnoverTable, turnoverTarget(turnoverTable, "turnover-owned.v1"), nil)

			recreateTable(t, turnoverTable, "turnover-owned.v2")

			var refusal = stale.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)})
			require.ErrorContains(t, refusal, "turnover-owned.v2")
			require.ErrorContains(t, refusal, "has been backfilled")
			require.Zero(t, countRowsIn(turnoverTable))
			t.Logf("the stale generation was refused with: %s", refusal)
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
		_, err = m.client.WaitCommit(ctx, m.bindings[0].channel, m.offsetToken(4))
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

		t.Run("uncommitted appends of another key range", func(t *testing.T) {
			// The shard kept its key-begin, and so its channel, but now covers
			// half the range — while the documents outstanding beyond the
			// checkpoint were appended over the whole of it, as the committed
			// offset token records. A replay here carries only part of them.
			var m2 = newManager(&pf.RangeSpec{KeyEnd: math.MaxUint32 / 2, RClockEnd: math.MaxUint32})
			m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, priorOf(&checkpointed))
			var refusal = m2.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)})
			require.ErrorContains(t, refusal, "appended by a shard covering [00000000, ffffffff]")
			require.Equal(t, rowsBefore, countRows())
			t.Logf("the replay was refused with: %s", refusal)
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

// TestStreamV2CreateTableRecordsGeneration drives the connector's own create path
// against live Snowflake and reads back what it left on the table.
//
// Apply is the only writer of the generation a streaming v2 table carries, and a
// generation naming the wrong state key would refuse every shard of the task
// rather than only the ones a backfill has replaced. So this covers the whole
// round trip — the table this connector creates, the comment Snowflake reports for
// it, and the generation read back out of it — rather than the rendering alone.
func TestStreamV2CreateTableRecordsGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var ctx = context.Background()
	var cfg = mustGetCfg(t)
	cfg.Advanced.FeatureFlags = flagSnowpipeStreamingV2

	const materialization = "test/streamV2CreateTable"
	const stateKey = "generation%2Frecorded.v2"
	var tableName = "STREAMV2_TEST_GENERATION"

	dsn, err := cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var cleanup = func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	}
	cleanup()
	t.Cleanup(cleanup)

	ep, err := newSnowflakeDriver().NewEndpoint(ctx, cfg, boilerplate.ParseFlags(cfg))
	require.NoError(t, err)
	client, err := newClient(ctx, materialization, ep)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	var tbl = sql.Table{
		TableShape: sql.TableShape{
			Path:         []string{cfg.Schema, tableName},
			Binding:      0,
			DeltaUpdates: true,
			Comment:      "Generated for materialization " + materialization + " of collection test/generation",
		},
		Identifier: tableName,
		Keys:       []sql.Column{{Identifier: `KEY`, MappedType: sql.MappedType{DDL: "TEXT"}}},
		Values:     []sql.Column{{Identifier: `VAL`, MappedType: sql.MappedType{DDL: "VARIANT"}}},
		StateKey:   stateKey,
	}

	var createQuery strings.Builder
	require.NoError(t, renderTemplates(ep.Dialect).createTargetTable.Execute(&createQuery, &tbl))
	require.NoError(t, client.CreateTable(ctx, sql.TableCreate{
		Table:          tbl,
		TableCreateSql: createQuery.String(),
		Resource:       tableConfig{Table: tableName, Schema: cfg.Schema, Delta: true},
	}))

	comment, err := streamV2TableComment(ctx, db, ep.Dialect, cfg.Database, cfg.Schema, tableName)
	require.NoError(t, err)
	t.Logf("created %s with comment: %s", tableName, comment)

	// The comment the table would have carried anyway is still there for whoever
	// reads the table, and the generation is recorded alongside it.
	require.Contains(t, comment, tbl.Comment)

	gen, ok := streamV2GenerationOf(comment)
	require.True(t, ok)
	require.Equal(t, streamV2Generation{materialization: materialization, stateKey: stateKey}, gen)

	// The generation this table was created for may append to it; the one a
	// backfill of the same binding replaced may not.
	require.NoError(t, streamV2GenerationConflict(comment, gen, tableName))
	require.ErrorContains(t,
		streamV2GenerationConflict(comment, streamV2Generation{materialization: materialization, stateKey: "generation%2Frecorded.v1"}, tableName),
		"has been backfilled")
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
