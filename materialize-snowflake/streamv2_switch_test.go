package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
)

// TestStreamV2CheckpointDoesNotSurviveAnotherWritePath establishes what moving a
// binding off the streaming v2 write path costs it, which is the whole reason
// the move is refused.
//
// Every other write path treats a state key's checkpoint item as pending work:
// Acknowledge applies it and then patches the key to null, which takes the whole
// item. The streaming v2 counter is not pending work but durable per-binding
// state, and it goes with it — while Snowflake keeps the channel and the
// documents it has committed, neither of which any checkpoint is left to
// account for.
func TestStreamV2CheckpointDoesNotSurviveAnotherWritePath(t *testing.T) {
	const stateKey, channel = "switch.v1", "task_00000000_switch_v1"

	var state, err = json.Marshal(checkpoint{stateKey: &checkpointItem{
		Table:    "TBL",
		StreamV2: map[string]*streamV2Item{channel: {Channel: channel, Counter: 3, KeyEnd: math.MaxUint32}},
	}})
	require.NoError(t, err)

	// A transaction on another write path records that path's pending work for
	// the state key. StreamV2 is omitted rather than nulled, so the reduce
	// leaves the counter standing: the item survives the switch itself.
	stored, err := json.Marshal(checkpoint{stateKey: &checkpointItem{
		Table:       "TBL",
		StreamBlobs: []*blobMetadata{{Path: "blob"}},
	}})
	require.NoError(t, err)

	state, err = jsonpatch.MergePatch(state, stored)
	require.NoError(t, err)
	require.Contains(t, string(state), channel)

	// Acknowledging that transaction is what takes it. The state key is drained,
	// and a drained key is patched to null, which removes the counter along with
	// the pending work it was recorded beside.
	cleared, err := json.Marshal(checkpoint{stateKey: nil})
	require.NoError(t, err)

	state, err = jsonpatch.MergePatch(state, cleared)
	require.NoError(t, err)

	var cp checkpoint
	require.NoError(t, json.Unmarshal(state, &cp))
	require.NotContains(t, cp, stateKey)
	require.NotContains(t, string(state), channel)
}

// TestStreamV2ReturningToTheWritePathSkipsNewDocuments is the consequence of
// that loss, and the outcome this connector must never reach: a binding which
// leaves the streaming v2 write path and later returns to it drops as many of
// the documents it is about to materialize as Snowflake's committed offset token
// counts.
//
// The channel name is derived from the binding's state key, so a return to the
// path derives the same name and finds the channel exactly as the departure left
// it. With no checkpoint item to contradict its token — and no item for any
// other channel of the task either, the state key having been cleared whole —
// the token reads as an interrupted first transaction of a fresh channel, which
// is a reading only a backfill can otherwise produce, and every document it
// counts is skipped.
func TestStreamV2ReturningToTheWritePathSkipsNewDocuments(t *testing.T) {
	var ctx = context.Background()

	// The fake sidecar's channel state outlives its process, so each manager
	// here stands for a separate session against one Snowflake.
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))

	var target = sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true, Path: []string{"DB", "SCH", "TBL"}},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "roundtrip.v1",
	}

	var newSession = func(t *testing.T, prior map[string]*streamV2Item) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/roundTrip", "acct",
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		m.argv = fakeSidecarArgv(t)
		t.Cleanup(m.stop)
		m.addBinding("DB", "SCH", "TBL", target, prior)
		return m
	}

	var before = newSession(t, nil)
	for i := range 3 {
		require.NoError(t, before.writeRow(ctx, 0, []any{"k", i}))
	}
	items, err := before.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), items[0].Counter)
	before.stop()

	// The task materializes through another write path for a while, which
	// clears this binding's checkpoint item, and is then moved back. Three
	// documents Snowflake never had are dropped.
	var after = newSession(t, nil)
	require.Equal(t, items[0].Channel, after.bindings[0].channel)
	require.NoError(t, after.writeRow(ctx, 0, []any{"k", "new"}))
	require.Equal(t, int64(3), after.bindings[0].skip)
}

// TestStreamV2SwitchOffTheWritePathIsRefused covers the switch itself, which is
// where it has to be caught: a feature-flag edit is reversible on its face,
// while what it does to this binding is not.
func TestStreamV2SwitchOffTheWritePathIsRefused(t *testing.T) {
	var ctx = context.Background()

	var target = func(stateKey string, delta bool) sql.Table {
		return sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: delta, Path: []string{"DB", "SCH", "TBL"}},
			Identifier: "TBL",
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}},
			StateKey:   stateKey,
		}
	}

	var newTransactor = func(t *testing.T, stateKey string, prior map[string]*streamV2Item) *transactor {
		var cfg = config{
			Database:    "DB",
			Schema:      "SCH",
			Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.JWT},
		}
		var d = &transactor{
			cfg:      cfg,
			ep:       &sql.Endpoint[config]{Dialect: snowflakeDialect("SCH", timestampTypeLTZ, nil)},
			_range:   &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
			version:  "v1",
			cp:       checkpoint{stateKey: &checkpointItem{StreamV2: prior}},
			streamV2: newStreamV2Manager(ctx, &cfg, "test/switch", "acct", &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}),
		}
		t.Cleanup(d.streamV2.stop)
		return d
	}

	// itemsFor names channels the way the driver checkpoint does, keyed so that
	// a shard reads back only the item it wrote.
	var itemsFor = func(items ...*streamV2Item) map[string]*streamV2Item {
		var byChannel = make(map[string]*streamV2Item, len(items))
		for _, item := range items {
			byChannel[item.Channel] = item
		}
		return byChannel
	}

	t.Run("a binding which has never streamed v2 is added as usual", func(t *testing.T) {
		var d = newTransactor(t, "fresh.v1", nil)
		require.NoError(t, d.addBinding(ctx, target("fresh.v1", false), false, false))
	})

	t.Run("turning the feature flag off is refused", func(t *testing.T) {
		const stateKey = "flag.v1"
		var d = newTransactor(t, stateKey, itemsFor(&streamV2Item{
			Channel: "task_00000000_flag_v1", Counter: 3, KeyEnd: math.MaxUint32,
		}))

		var err = d.addBinding(ctx, target(stateKey, true), false, false)
		require.ErrorContains(t, err, "snowpipe_streaming_v2")
		require.ErrorContains(t, err, "task_00000000_flag_v1")
		require.ErrorContains(t, err, "backfill")
	})

	t.Run("moving the binding to standard updates is refused", func(t *testing.T) {
		const stateKey = "delta.v1"
		var d = newTransactor(t, stateKey, itemsFor(&streamV2Item{
			Channel: "task_00000000_delta_v1", Counter: 3, KeyEnd: math.MaxUint32,
		}))

		require.Error(t, d.addBinding(ctx, target(stateKey, false), false, true))
	})

	t.Run("moving the binding to the snowpipe_streaming path is refused", func(t *testing.T) {
		const stateKey = "streaming.v1"
		var d = newTransactor(t, stateKey, itemsFor(&streamV2Item{
			Channel: "task_00000000_streaming_v1", Counter: 3, KeyEnd: math.MaxUint32,
		}))

		// The v1 streaming path would open a channel of its own against the same
		// table, so the refusal must come before the stream manager is reached —
		// this transactor has none.
		require.Error(t, d.addBinding(ctx, target(stateKey, true), true, false))
	})

	t.Run("channels the task has already retired do not hold the binding to the path", func(t *testing.T) {
		// A nil item is the deletion of a channel this task retired, which the
		// runtime has not yet reduced away. It records nothing.
		const stateKey = "retired.v1"
		var d = newTransactor(t, stateKey, map[string]*streamV2Item{"task_80000000_retired_v1": nil})

		require.NoError(t, d.addBinding(ctx, target(stateKey, false), false, false))
	})

	t.Run("staying on the write path is not refused", func(t *testing.T) {
		const stateKey = "stay.v1"
		var d = newTransactor(t, stateKey, itemsFor(&streamV2Item{
			Channel: "task_00000000_stay_v1", Counter: 3, KeyEnd: math.MaxUint32,
		}))

		require.NoError(t, d.addBinding(ctx, target(stateKey, true), false, true))
	})
}

// TestStreamV2SwitchIsRefusedAtPublication covers the same switch one step
// earlier, where the operator is still making it.
//
// Apply is the first RPC of a publication which carries both the specification
// being published and the connector state the task has accumulated, so it is the
// first point at which a binding's departure from this write path can be seen at
// all. Refusing here fails the publication rather than the task it would leave
// behind, which is the difference between an operator reading this message and
// an operator reading it from a task that has already stopped.
func TestStreamV2SwitchIsRefusedAtPublication(t *testing.T) {
	var ctx = context.Background()
	const stateKey, channel = "publish.v1", "task_00000000_publish_v1"

	var state, err = json.Marshal(checkpoint{stateKey: &checkpointItem{
		StreamV2: map[string]*streamV2Item{channel: {Channel: channel, Counter: 3, KeyEnd: math.MaxUint32}},
	}})
	require.NoError(t, err)

	var specOf = func(t *testing.T, featureFlags string, delta bool) *pf.MaterializationSpec {
		var spec = testStreamingSpec(t, featureFlags, true)
		spec.Bindings = []*pf.MaterializationSpec_Binding{{
			ResourcePath: []string{"mydb", "myschema", "TBL"},
			StateKey:     stateKey,
			DeltaUpdates: delta,
		}}
		return spec
	}

	t.Run("a publication which keeps the binding on the path is allowed", func(t *testing.T) {
		require.NoError(t, refuseAbandonedStreamV2Bindings(specOf(t, "snowpipe_streaming_v2", true), state))
	})

	t.Run("a publication which turns the feature flag off is refused", func(t *testing.T) {
		var err = refuseAbandonedStreamV2Bindings(specOf(t, "", true), state)
		require.ErrorContains(t, err, channel)
		require.ErrorContains(t, err, "backfill")
	})

	t.Run("a publication which moves the binding to standard updates is refused", func(t *testing.T) {
		require.Error(t, refuseAbandonedStreamV2Bindings(specOf(t, "snowpipe_streaming_v2", false), state))
	})

	t.Run("a binding the state records nothing for is allowed", func(t *testing.T) {
		var spec = specOf(t, "", true)
		spec.Bindings[0].StateKey = "other.v1"
		require.NoError(t, refuseAbandonedStreamV2Bindings(spec, state))
	})

	t.Run("a publication carrying no connector state is allowed", func(t *testing.T) {
		require.NoError(t, refuseAbandonedStreamV2Bindings(specOf(t, "", true), nil))
	})

	t.Run("a binding dropped from the specification is allowed", func(t *testing.T) {
		var spec = specOf(t, "", true)
		spec.Bindings = nil
		require.NoError(t, refuseAbandonedStreamV2Bindings(spec, state))
	})

	// The gate is what puts this ahead of the wrapped driver, so the refusal
	// reaches the operator without Snowflake being touched at all.
	t.Run("publishing is refused", func(t *testing.T) {
		_, err := NewGatedDriver().Apply(ctx, &pm.Request_Apply{
			Materialization: specOf(t, "", true),
			StateJson:       state,
		})
		require.ErrorContains(t, err, channel)
	})
}

// TestStreamV2WritePathSwitch drives both write paths against live Snowflake
// and one table, which is the only place the account they each keep of that
// table can be compared.
func TestStreamV2WritePathSwitch(t *testing.T) {
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

	const testMaterialization = "test/streamV2Switch"

	// The schema is shared with CI, so the table carries a name no other run can
	// derive. Both write paths store the same two columns into it.
	var tableName = fmt.Sprintf("STREAMV2_SWITCH_flow_test_%d", time.Now().Unix())
	var cleanup = func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	}
	cleanup()
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (KEY TEXT, INTCOL NUMBER);", tableName))
	require.NoError(t, err)
	defer cleanup()

	var target = func(stateKey string) sql.Table {
		return sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true, Path: []string{cfg.Database, cfg.Schema, tableName}},
			Identifier: tableName,
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `INTCOL`}},
			StateKey:   stateKey,
		}
	}

	var fullRange = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}

	var countRows = func() int {
		var count int
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", tableName)).Scan(&count))
		return count
	}

	var truncate = func(t *testing.T) {
		_, err := db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s;", tableName))
		require.NoError(t, err)
	}

	// storeV1 materializes the documents of [lo, hi) through the snowpipe
	// streaming path, as one transaction of it: rows are staged into blobs by
	// Store and registered by Acknowledge.
	var storeV1 = func(t *testing.T, token string, lo, hi int) {
		sm, err := newStreamManager(&cfg, testMaterialization, accountName, 0)
		require.NoError(t, err)
		require.NoError(t, sm.addBinding(ctx, cfg.Schema, tableName, target("v1.v1")))

		for i := lo; i < hi; i++ {
			require.NoError(t, sm.writeRow(ctx, 0, []any{fmt.Sprintf("key-%d", i), i}))
		}
		blobs, keys, err := sm.flush(token)
		require.NoError(t, err)
		require.NoError(t, sm.write(ctx, blobs[0], keys[0], false))
	}

	var newV2 = func(t *testing.T, tgt sql.Table, prior map[string]*streamV2Item) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &cfg, testMaterialization, accountName, fullRange)
		t.Cleanup(m.stop)
		m.addBinding(cfg.Database, cfg.Schema, tableName, tgt, prior)
		return m
	}

	var storeV2 = func(t *testing.T, m *streamV2Manager, lo, hi int) {
		for i := lo; i < hi; i++ {
			require.NoError(t, m.writeRow(ctx, 0, []any{fmt.Sprintf("key-%d", i), i}))
		}
	}

	t.Run("a binding moved onto this write path appends beside what filled the table", func(t *testing.T) {
		truncate(t)

		// Three documents through the snowpipe streaming path, which leaves no
		// state this path reads: its checkpoint item is drained by the
		// Acknowledge which registers its blobs.
		storeV1(t, "switch-onto-1", 0, 3)
		require.Equal(t, 3, countRows())

		// The move opens a channel this binding has never had, so the documents
		// which follow are appended in full rather than skipped.
		var m = newV2(t, target("onto.v1"), nil)
		storeV2(t, m, 3, 5)
		require.Zero(t, m.bindings[0].skip)

		items, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(2), items[0].Counter)
		require.Equal(t, 5, countRows())
	})

	t.Run("a binding moved off this write path leaves its channel behind", func(t *testing.T) {
		truncate(t)

		var tgt = target("off.v1")
		var m = newV2(t, tgt, nil)
		storeV2(t, m, 0, 3)
		items, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, countRows())
		m.stop()

		// Nothing retires a shard's own channel, so Snowflake goes on reporting
		// its committed offset token — to this binding's next session on this
		// path, whenever that is and whatever ran in between.
		var later = newV2(t, tgt, nil)
		client, err := later.ensureStarted(ctx)
		require.NoError(t, err)
		status, err := client.OpenChannel(ctx, cfg.Database, cfg.Schema, tableName, items[0].Channel)
		require.NoError(t, err)
		require.Equal(t, m.offsetToken(3), status.committedToken())
	})

	t.Run("a binding moved off this write path with rows in flight duplicates them", func(t *testing.T) {
		truncate(t)

		// Cutting a batch per document is what puts documents in flight without
		// a flush, which is the state an interruption has to find.
		var restore = streamV2BatchRows
		t.Cleanup(func() { streamV2BatchRows = restore })
		streamV2BatchRows = 1

		// An interrupted transaction: Snowflake commits three documents and the
		// session dies before any checkpoint records them.
		var tgt = target("inflight.v1")
		var m = newV2(t, tgt, nil)
		storeV2(t, m, 0, 3)
		require.NoError(t, m.bindings[0].pipe.wait())
		_, err := m.client.WaitCommit(ctx, m.bindings[0].channel, m.offsetToken(3))
		require.NoError(t, err)
		require.Equal(t, 3, countRows())
		m.sup.kill()

		// The runtime replays those same three documents. This path skips them,
		// because Snowflake's committed offset token says it holds them.
		var replay = newV2(t, tgt, nil)
		storeV2(t, replay, 0, 3)
		require.Equal(t, int64(3), replay.bindings[0].skip)
		_, err = replay.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, countRows())
		replay.stop()

		// Had the operator escaped the interruption by moving the binding off
		// this path instead, the same replay would have been materialized again:
		// no other path reads that token, and this one is delta-updates, so the
		// duplicates are permanent.
		truncate(t)
		storeV1(t, "switch-inflight-1", 0, 3)
		storeV1(t, "switch-inflight-2", 0, 3)
		require.Equal(t, 6, countRows())
	})
}

// TestStreamV2SwitchOntoTheWritePathWithPendingWorkIsRefused covers the other
// direction of a write path switch: a binding which moves onto streaming v2 while
// its checkpoint still holds work that another path staged and did not finish.
//
// Acknowledge drains such an item through the manager of the path which staged it.
// Only Snowpipe Streaming blobs need this binding registered with that manager: a
// staged-file query runs on the shared connection, and pipe files go through the
// shared pipe client, so both drain on a streaming v2 session as they stand. Blobs
// need the bdec manager to hold a channel for the table, which the binding arranges
// for the drain even though its rows now go to streaming v2. Where that cannot be
// arranged, the binding is refused rather than left to fail every transaction.
func TestStreamV2SwitchOntoTheWritePathWithPendingWorkIsRefused(t *testing.T) {
	var ctx = context.Background()

	var target = sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true, Path: []string{"DB", "SCH", "TBL"}},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "sk.v1",
	}

	var newTransactor = func(t *testing.T, item *checkpointItem) *transactor {
		var cfg = config{
			Database:    "DB",
			Schema:      "SCH",
			Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.JWT},
		}
		var rng = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}
		var d = &transactor{
			cfg:      cfg,
			ep:       &sql.Endpoint[config]{Dialect: snowflakeDialect("SCH", timestampTypeLTZ, nil)},
			_range:   rng,
			version:  "v1",
			cp:       checkpoint{target.StateKey: item},
			streamV2: newStreamV2Manager(ctx, &cfg, "test/onto", "acct", rng),
		}
		t.Cleanup(d.streamV2.stop)
		return d
	}

	// addBinding is called with the streaming v2 flag enabled throughout, which is
	// what routes the binding to the streaming v2 manager.
	var addBinding = func(d *transactor) error {
		return d.addBinding(ctx, target, true, true)
	}

	// The transactor of this test carries no bdec manager, which stands for every
	// reason the drain cannot be arranged: the manager is absent, or it cannot open
	// a channel on the table.
	t.Run("pending blobs with no way to drain them are refused", func(t *testing.T) {
		var d = newTransactor(t, &checkpointItem{
			Table:       "TBL",
			StreamBlobs: []*blobMetadata{{Path: "one.bdec"}, {Path: "two.bdec"}},
		})

		var err = addBinding(d)
		require.ErrorContains(t, err, "TBL")
		require.ErrorContains(t, err, "2")
		require.ErrorContains(t, err, "backfill")
	})

	// Neither of the next two needs anything of this binding to drain: the pipe
	// client and the database connection both belong to the transactor.
	t.Run("pending pipe files are added as usual", func(t *testing.T) {
		var d = newTransactor(t, &checkpointItem{
			Table:     "TBL",
			PipeName:  "PIPE",
			PipeFiles: []fileRecord{{Path: "one.json"}},
		})

		require.NoError(t, addBinding(d))
	})

	t.Run("a pending staged-file query is added as usual", func(t *testing.T) {
		var d = newTransactor(t, &checkpointItem{
			Table: "TBL",
			Query: "\nMERGE INTO TBL",
		})

		require.NoError(t, addBinding(d))
	})

	t.Run("a checkpoint holding only streaming v2 state is added as usual", func(t *testing.T) {
		var d = newTransactor(t, &checkpointItem{StreamV2: map[string]*streamV2Item{
			"task_00000000_sk_v1": {Channel: "task_00000000_sk_v1", Counter: 7, KeyEnd: math.MaxUint32},
		}})

		require.NoError(t, addBinding(d))
	})

	t.Run("an empty checkpoint item is added as usual", func(t *testing.T) {
		require.NoError(t, addBinding(newTransactor(t, &checkpointItem{})))
	})

	t.Run("no checkpoint item at all is added as usual", func(t *testing.T) {
		var d = newTransactor(t, nil)
		delete(d.cp, target.StateKey)
		require.NoError(t, addBinding(d))
	})
}
