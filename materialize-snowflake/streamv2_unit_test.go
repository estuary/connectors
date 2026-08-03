package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
)

// TestStreamV2CheckpointHoldsOneItemPerChannel pins the shape of the streaming
// v2 driver checkpoint against the only runtime this write path runs on. Every
// shard of a v2 task merge-patches its connector state into one task-global
// document, so two shards of the same binding write the same JSON path. The
// counter each records is durable state rather than pending work, so it needs a
// key of its own: keyed by state key alone it is overwritten by whichever
// sibling reduced last, and the survivor is then reconciled against a channel it
// does not describe.
func TestStreamV2CheckpointHoldsOneItemPerChannel(t *testing.T) {
	const stateKey = "shared.v1"
	const loChannel, hiChannel = "task_00000000_shared_v1", "task_80000000_shared_v1"
	var lo = fmt.Sprintf(`{"Channel":%q,"Counter":7,"KeyBegin":0,"KeyEnd":2147483647}`, loChannel)
	var hi = fmt.Sprintf(`{"Channel":%q,"Counter":3,"KeyBegin":2147483648,"KeyEnd":4294967295}`, hiChannel)

	// Under one key per state key the two shards collide: a merge patch replaces
	// the scalars of the object they share, so only the shard which reduced last
	// is left. This is the loss the per-channel key exists to prevent.
	collided, err := jsonpatch.MergePatch(
		fmt.Appendf(nil, `{%q:{"StreamV2":%s}}`, stateKey, lo),
		fmt.Appendf(nil, `{%q:{"StreamV2":%s}}`, stateKey, hi),
	)
	require.NoError(t, err)
	require.NotContains(t, string(collided), loChannel)

	// Under one key per channel, each shard patches a key of its own and both
	// counters survive the reduce.
	reduced, err := jsonpatch.MergePatch(
		fmt.Appendf(nil, `{%q:{"StreamV2":{%q:%s}}}`, stateKey, loChannel, lo),
		fmt.Appendf(nil, `{%q:{"StreamV2":{%q:%s}}}`, stateKey, hiChannel, hi),
	)
	require.NoError(t, err)

	var cp checkpoint
	require.NoError(t, json.Unmarshal(reduced, &cp))
	require.Len(t, cp[stateKey].StreamV2, 2)
}

// TestStreamV2CommitPrecedesCheckpoint pins where the commit is awaited. The
// driver checkpoint records a counter as appended, and Snowflake's committed
// offset token is what the next Open reconciles that counter against. The
// runtime's own checkpoint is durable before Acknowledge runs, so a commit
// awaited there cannot fail the transaction whose counter it belongs to: the
// task would resume from a counter Snowflake never committed, which Open can
// only refuse, and no replay can re-append rows the runtime considers
// delivered. So the wait belongs to the call which produces the counter.
func TestStreamV2CommitPrecedesCheckpoint(t *testing.T) {
	var ctx = context.Background()

	// Appends succeed and the committed token never advances, as a channel whose
	// rows Snowflake accepted but never committed would report.
	t.Setenv("FAKE_SIDECAR_MODE", "commit_never_lands")

	var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/commitFirst", "acct",
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	m.argv = fakeSidecarArgv(t)
	t.Cleanup(m.stop)

	m.addBinding("DB", "SCH", "TBL", sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "committed.v1",
	}, nil)

	require.NoError(t, m.writeRow(ctx, 0, []any{"k", "v"}))

	items, err := m.flush(ctx)
	require.ErrorContains(t, err, "not committed")
	require.Empty(t, items)
}

// TestStreamV2RejectedRows covers, without credentials, what the live test
// covers against Snowflake: a rejected row fails the transaction that produced
// it, and goes on refusing the channel it was appended to.
func TestStreamV2RejectedRows(t *testing.T) {
	var ctx = context.Background()

	var newManager = func(t *testing.T, prior *streamV2Item) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/rejectedRows", "acct",
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		m.argv = fakeSidecarArgv(t)
		t.Cleanup(m.stop)

		// The fake sidecar rejects a row which carries the REJECT column, as
		// Snowflake rejects one it cannot store: the append and the commit both
		// succeed, and only the channel's row-error count moves.
		var priorByChannel map[string]*streamV2Item
		if prior != nil {
			priorByChannel = map[string]*streamV2Item{m.channelFor("rejected.v1"): prior}
		}

		m.addBinding("DB", "SCH", "TBL", sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
			Identifier: "TBL",
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}, {Identifier: `REJECT`}},
			StateKey:   "rejected.v1",
		}, priorByChannel)
		return m
	}

	t.Run("a rejected row fails the commit", func(t *testing.T) {
		var m = newManager(t, nil)
		require.NoError(t, m.writeRow(ctx, 0, []any{"kept", "v", nil}))
		require.NoError(t, m.writeRow(ctx, 0, []any{"dropped", "v", true}))

		// The rejection is the transaction's own, so it must fail the transaction
		// rather than the one after it: the count is read as the commit is awaited,
		// which is before the counter reaches the checkpoint.
		items, err := m.flush(ctx)
		require.ErrorContains(t, err, "1 row(s) rejected")
		require.ErrorContains(t, err, m.bindings[0].channel)
		require.ErrorContains(t, err, "fake rejection")
		require.Empty(t, items)
	})

	t.Run("a rejection already on the channel is refused when it opens", func(t *testing.T) {
		// The transaction that provoked a rejection fails before acknowledging,
		// so a restart replays it. Refusing as the channel opens — before the
		// replay can append, and whether or not this session appends at all — is
		// what stops that replay from acknowledging rows Snowflake does not hold.
		t.Setenv("FAKE_SIDECAR_ROWS_ERROR_COUNT", "3")
		var m = newManager(t, &streamV2Item{Counter: 2})

		require.ErrorContains(t, m.writeRow(ctx, 0, []any{"kept", "v", nil}), "3 row(s) rejected")
	})
}

// TestStreamV2ChannelRetirement covers, without credentials, what the live test
// establishes against Snowflake: a channel name outlives the shard it was named
// for, and only retiring the channel stops the next shard to derive that name
// from adopting its committed offset token as a skip threshold.
func TestStreamV2ChannelRetirement(t *testing.T) {
	var ctx = context.Background()

	// The fake sidecar's channel state outlives its process, so each manager
	// below stands in for a separate session against one Snowflake.
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))

	var target = sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "retire.v1",
	}

	// Every manager here covers the whole key space, as the single shard of a
	// task does — which is the point: a name derived from a key-begin is
	// derived again, identically, by whichever shard next holds that key-begin.
	var newSession = func(t *testing.T) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/retirement", "acct",
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		m.argv = fakeSidecarArgv(t)
		t.Cleanup(m.stop)
		m.addBinding("DB", "SCH", "TBL", target, nil)
		return m
	}

	// A shard appends and commits three documents and is then deleted, leaving
	// the channel as its final transaction had it.
	var first = newSession(t)
	var channel = first.bindings[0].channel
	for i := range 3 {
		require.NoError(t, first.writeRow(ctx, 0, []any{"k", i}))
	}
	items, err := first.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), items[0].Counter)
	first.stop()

	// Un-retired, that channel wedges the next shard to derive its name: the
	// documents it is about to store are taken for a replay of an interrupted
	// transaction it never ran, and skipped.
	var reused = newSession(t)
	require.Equal(t, channel, reused.bindings[0].channel)
	require.NoError(t, reused.writeRow(ctx, 0, []any{"k", 0}))
	require.Equal(t, int64(3), reused.bindings[0].skip)
	reused.stop()

	// Retiring it is what makes the name reusable. The retiring session opens
	// the channel first, as a shard absorbing another's key range does.
	var retiring = newSession(t)
	client, err := retiring.ensureStarted(ctx)
	require.NoError(t, err)
	_, err = client.OpenChannel(ctx, "DB", "SCH", "TBL", channel)
	require.NoError(t, err)
	require.NoError(t, retireChannel(ctx, client, channel))

	// A retired channel is out of service: the handle it was retired through is
	// spent, so appending to it fails rather than quietly reviving it.
	require.ErrorContains(t, client.Append(ctx, channel, "1", "1", []byte("[]"), 0), "is not open")

	status, err := client.OpenChannel(ctx, "DB", "SCH", "TBL", channel)
	require.NoError(t, err)
	require.Nil(t, status.CommittedToken)
	retiring.stop()

	// So the name now opens as a channel which has committed nothing, and the
	// documents of the shard which reused it are appended in full.
	var after = newSession(t)
	require.Equal(t, channel, after.bindings[0].channel)
	require.NoError(t, after.writeRow(ctx, 0, []any{"k", 0}))
	require.Zero(t, after.bindings[0].skip)

	items, err = after.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), items[0].Counter)
}

// TestStreamV2JoinThenSplit covers scaling a task down and then back up, which is
// the most routine scaling action there is and the one which wedged this write
// path: a split re-creates exactly the pivot boundaries a join removed, so the
// shard which arrives derives the channel name of the shard the join absorbed.
// What that name may hold, and why, is retireAbsorbedChannels.
//
// The same sequence against live Snowflake is the corresponding subtest of
// TestStreamV2Manager. This one covers what no live test can drive cheaply: a
// join interrupted between its two halves, a split which outruns it, and a task
// wedged before any of this existed.
func TestStreamV2JoinThenSplit(t *testing.T) {
	var ctx = context.Background()

	// The fake sidecar's channel state outlives its process, so each session
	// below stands for a separate shard against one Snowflake. Sessions must not
	// overlap on it: each holds the whole of it and writes it back whole.
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))

	var pivot uint32 = math.MaxUint32/2 + 1
	var fullRange = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}
	var loRange = &pf.RangeSpec{KeyEnd: pivot - 1, RClockEnd: math.MaxUint32}
	var hiRange = &pf.RangeSpec{KeyBegin: pivot, KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}

	// Each subtest takes a state key, and so channel names, of its own: they share
	// one "Snowflake" but no channel.
	var newSession = func(t *testing.T, stateKey string, keyRange *pf.RangeSpec, prior map[string]*streamV2Item) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/rejoin", "acct", keyRange)
		m.argv = fakeSidecarArgv(t)
		t.Cleanup(m.stop)
		// The REJECT column is what the fake sidecar discards a row for, as
		// Snowflake discards one it cannot store; a row which carries no value for
		// it is stored as any other.
		m.addBinding("DB", "SCH", "TBL", sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
			Identifier: "TBL",
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}, {Identifier: `REJECT`}},
			StateKey:   stateKey,
		}, prior)
		return m
	}

	var priorOf = func(items ...*streamV2Item) map[string]*streamV2Item {
		var byChannel = make(map[string]*streamV2Item, len(items))
		for _, item := range items {
			byChannel[item.Channel] = item
		}
		return byChannel
	}

	var writeRows = func(t *testing.T, m *streamV2Manager, n int) {
		for i := range n {
			require.NoError(t, m.writeRow(ctx, 0, []any{"k", i, nil}))
		}
	}

	// splitTask brings up the two shards of a task split at the pivot, each having
	// appended and committed to a channel of its own, and reports what the
	// checkpoint holds for them.
	var splitTask = func(t *testing.T, stateKey string) (*streamV2Item, *streamV2Item) {
		var lo = newSession(t, stateKey, loRange, nil)
		writeRows(t, lo, 3)
		loItems, err := lo.flush(ctx)
		require.NoError(t, err)
		lo.stop()

		var hi = newSession(t, stateKey, hiRange, nil)
		writeRows(t, hi, 5)
		hiItems, err := hi.flush(ctx)
		require.NoError(t, err)
		require.NotEqual(t, loItems[0].Channel, hiItems[0].Channel)
		hi.stop()

		return loItems[0], hiItems[0]
	}

	t.Run("a joined shard's key range may be split back onto it", func(t *testing.T) {
		const stateKey = "rejoin.v1"
		var loItem, hiItem = splitTask(t, stateKey)

		// The scale-down: the survivor keeps the low shard's key-begin and channel,
		// retires the absorbed shard's channel, and reports its checkpoint item for
		// deletion.
		var joined = newSession(t, stateKey, fullRange, priorOf(loItem, hiItem))
		writeRows(t, joined, 2)
		require.Equal(t, []string{hiItem.Channel}, joined.bindings[0].retire)
		joinedItems, err := joined.flush(ctx)
		require.NoError(t, err)
		require.Nil(t, joined.checkpointFor(0, joinedItems[0])[hiItem.Channel])
		joined.stop()

		// The scale-up: the arriving shard is given the pivot as its key-begin, so
		// it derives the absorbed shard's channel name again, with no checkpoint
		// item of its own — the join deleted it. Nothing of the retired channel may
		// be adopted as a skip threshold, or this shard's documents are dropped as
		// replays of a transaction it never ran.
		var splitHi = newSession(t, stateKey, hiRange, priorOf(joinedItems[0]))
		require.Equal(t, hiItem.Channel, splitHi.bindings[0].channel)

		writeRows(t, splitHi, 4)
		require.Zero(t, splitHi.bindings[0].skip)

		items, err := splitHi.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(4), items[0].Counter)
		splitHi.stop()

		// And the cycle repeats: joining the re-created shard away retires its
		// channel again, and splitting back onto the same pivot is clean again.
		var rejoined = newSession(t, stateKey, fullRange, priorOf(joinedItems[0], items[0]))
		writeRows(t, rejoined, 2)
		require.Equal(t, []string{items[0].Channel}, rejoined.bindings[0].retire)
		rejoinedItems, err := rejoined.flush(ctx)
		require.NoError(t, err)
		rejoined.stop()

		var resplitHi = newSession(t, stateKey, hiRange, priorOf(rejoinedItems[0]))
		writeRows(t, resplitHi, 1)
		require.Zero(t, resplitHi.bindings[0].skip)
		items, err = resplitHi.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), items[0].Counter)
	})

	t.Run("a join interrupted after the retirement retires again", func(t *testing.T) {
		// The two halves of a retirement cannot be made durable together: the drop
		// reaches Snowflake as the channel is settled, while the deletion of its
		// checkpoint item only becomes durable with the transaction that carries it.
		// A shard which dies in between — the transaction failing for any reason at
		// all, a bounce, a rescheduling — restarts with the absorbed item still in
		// its checkpoint, and finds the channel that item describes holding nothing.
		//
		// That is the retirement's own footprint, so the join must repeat it rather
		// than read it as a channel which has lost committed data. Refusing here
		// would turn every interrupted join into the backfill this retirement exists
		// to avoid.
		const stateKey = "rejoin-restart.v1"
		var loItem, hiItem = splitTask(t, stateKey)

		var interrupted = newSession(t, stateKey, fullRange, priorOf(loItem, hiItem))
		writeRows(t, interrupted, 2)
		require.Equal(t, []string{hiItem.Channel}, interrupted.bindings[0].retire)
		interrupted.sup.kill() // before any checkpoint carries the deletion

		var restarted = newSession(t, stateKey, fullRange, priorOf(loItem, hiItem))
		writeRows(t, restarted, 2)
		require.Equal(t, []string{hiItem.Channel}, restarted.bindings[0].retire)

		items, err := restarted.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, loItem.Counter+2, items[0].Counter)
		require.Nil(t, restarted.checkpointFor(0, items[0])[hiItem.Channel])
	})

	t.Run("a split which outruns the retirement continues the channel", func(t *testing.T) {
		// A retirement happens as the survivor opens the binding's channel, which is
		// on its first document for that binding — so a binding the survivor is
		// delivered nothing for still carries the absorbed item, and its channel is
		// still the one Snowflake holds. A scale-up can therefore reach the key range
		// before anything retired it.
		//
		// That shard is safe for the same reason the wedge is not: the item which
		// describes the channel is still there, so its counter is carried forward and
		// the committed offset token accounts for exactly those documents. The channel
		// is continued rather than retired, and the retirement is simply abandoned —
		// this shard's own next item replaces the absorbed one.
		const stateKey = "rejoin-outrun.v1"
		var _, hiItem = splitTask(t, stateKey)

		var splitHi = newSession(t, stateKey, hiRange, priorOf(hiItem))
		require.Equal(t, hiItem.Channel, splitHi.bindings[0].channel)

		writeRows(t, splitHi, 2)
		require.Equal(t, hiItem.Counter, splitHi.bindings[0].skip)

		items, err := splitHi.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, hiItem.Counter+2, items[0].Counter)
		require.Equal(t, hiRange.KeyBegin, items[0].KeyBegin)
	})

	t.Run("a channel a scale-up resurrects is refused", func(t *testing.T) {
		// A retirement drops the absorbed channel in Snowflake, so the shard which
		// later re-derives that name finds it holding nothing. A scale-down which
		// only deleted the absorbed checkpoint item — which is what this connector
		// did before it retired anything, and what any failure of the drop leaves
		// behind — does not: the name goes on holding a committed offset token for
		// documents a shard which no longer exists appended.
		//
		// The arriving shard has no item of its own to contradict that token, and
		// adopting it as a skip threshold is the wedge itself. What makes the token
		// one the checkpoint cannot account for is that the checkpoint holds items
		// for this binding at all: the binding has been materializing under channels
		// of other names, so this name is not on its first transaction.
		const stateKey = "rejoin-resurrected.v1"
		var loItem, hiItem = splitTask(t, stateKey)

		// The scale-down as such a connector makes it: the survivor's own item
		// records the range it took over, in the very checkpoint which deletes the
		// absorbed item, while the absorbed channel lives on in Snowflake.
		var joinedItem = &streamV2Item{
			Channel: loItem.Channel, Counter: loItem.Counter, KeyBegin: 0, KeyEnd: math.MaxUint32,
		}

		var splitHi = newSession(t, stateKey, hiRange, priorOf(joinedItem))
		require.Equal(t, hiItem.Channel, splitHi.bindings[0].channel)

		var refusal = splitHi.writeRow(ctx, 0, []any{"k", 1, nil})
		require.ErrorContains(t, refusal, hiItem.Channel)
		require.ErrorContains(t, refusal, "cannot account for")
		require.ErrorContains(t, refusal, "Backfill this binding")
	})

	t.Run("a first transaction of the binding is continued rather than refused", func(t *testing.T) {
		// The refusal is for a token the checkpoint cannot account for, and a
		// checkpoint holding nothing for the binding accounts for a token in the
		// only way there is: the channel and the item start empty together, so a
		// token on an empty checkpoint is this channel's own first transaction,
		// interrupted between Snowflake's commit and the checkpoint recording it.
		// The documents it counts are the ones about to be replayed, and they are
		// skipped as any other replay is.
		//
		// This is also what a binding backfilled out of a refusal finds, since the
		// state key a backfill rotates is what the channel is named for.
		const stateKey = "rejoin-first.v1"

		var interrupted = newSession(t, stateKey, hiRange, nil)
		writeRows(t, interrupted, 5)
		_, err := interrupted.flush(ctx)
		require.NoError(t, err)
		interrupted.stop()

		var replay = newSession(t, stateKey, hiRange, nil)
		writeRows(t, replay, 5)
		require.Equal(t, int64(5), replay.bindings[0].skip)

		items, err := replay.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(5), items[0].Counter)
	})

	t.Run("a first transaction interrupted beside a sibling is refused too", func(t *testing.T) {
		// A shard whose own first transaction was interrupted is safe to continue —
		// but only this shard knows that, and the checkpoint it restarts from does
		// not: a sibling's item is there whether the token belongs to this shard's
		// interrupted attempt or to a shard a join retired, and nothing else
		// distinguishes them. The safe reading is the one which cannot drop rows,
		// so this shard is refused along with the resurrected one, and backfilling
		// the binding is what it costs.
		const stateKey = "rejoin-sibling.v1"

		var lo = newSession(t, stateKey, loRange, nil)
		writeRows(t, lo, 3)
		loItems, err := lo.flush(ctx)
		require.NoError(t, err)
		lo.stop()

		// The high shard commits to Snowflake and dies before the transaction
		// carrying its counter is durable, leaving the checkpoint with the low
		// shard's item alone.
		var hi = newSession(t, stateKey, hiRange, priorOf(loItems[0]))
		writeRows(t, hi, 5)
		_, err = hi.flush(ctx)
		require.NoError(t, err)
		hi.stop()

		var replay = newSession(t, stateKey, hiRange, priorOf(loItems[0]))
		require.ErrorContains(t, replay.writeRow(ctx, 0, []any{"k", 1, nil}), "cannot account for")
	})

	t.Run("a task already wedged recovers by joining the wedged shard away", func(t *testing.T) {
		// Retiring a channel only helps a join made from here on: a task wedged
		// before this connector retired anything is left with a channel Snowflake
		// still holds and no checkpoint item anywhere which names it, so no later
		// join can find it to retire. The recovery for those tasks has to work
		// without one, and this is it — join the refused shard away, and the survivor
		// carries on with its own channel.
		const stateKey = "rejoin-wedged.v1"
		var loItem, hiItem = splitTask(t, stateKey)

		// The state such a task is in: the absorbed shard's checkpoint item was
		// deleted by a join while the channel it described went on holding its
		// committed offset token, and a scale-up handed that channel to a new shard.
		var wedged = newSession(t, stateKey, hiRange, priorOf(loItem))
		require.Equal(t, hiItem.Channel, wedged.bindings[0].channel)
		require.ErrorContains(t, wedged.writeRow(ctx, 0, []any{"k", 1, nil}), "cannot account for")
		wedged.stop()

		// The recovery. No checkpoint item names the wedged shard's channel, so the
		// survivor has nothing to absorb and nothing of it reaches its own channel,
		// which it resumes from the counter it left off at.
		var survivor = newSession(t, stateKey, fullRange, priorOf(loItem))
		writeRows(t, survivor, 2)
		require.Empty(t, survivor.bindings[0].retire)
		require.Equal(t, loItem.Counter, survivor.bindings[0].skip)

		items, err := survivor.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, loItem.Counter+2, items[0].Counter)
	})

	t.Run("a replay which produces fewer documents than Snowflake holds is refused", func(t *testing.T) {
		// The skip threshold is positional, so it is only sound while the replayed
		// transaction produces the same documents the interrupted attempt did. A
		// replay which stops short leaves the skip covering documents Snowflake does
		// not hold, and the shard's own key range being unchanged means nothing else
		// catches it — this check is the one that does.
		const stateKey = "rejoin-short-replay.v1"

		var first = newSession(t, stateKey, fullRange, nil)
		writeRows(t, first, 5)
		items, err := first.flush(ctx)
		require.NoError(t, err)
		first.stop()

		// The interrupted attempt: Snowflake commits three more documents, and the
		// transaction which would have recorded them never checkpoints.
		var interrupted = newSession(t, stateKey, fullRange, priorOf(items[0]))
		writeRows(t, interrupted, 3)
		_, err = interrupted.flush(ctx)
		require.NoError(t, err)
		interrupted.stop()

		var replay = newSession(t, stateKey, fullRange, priorOf(items[0]))
		writeRows(t, replay, 1)
		require.Equal(t, int64(8), replay.bindings[0].skip)

		_, err = replay.flush(ctx)
		require.ErrorContains(t, err, "was not replayed identically")
	})

	t.Run("a rejection on an absorbed channel is refused ahead of the drop", func(t *testing.T) {
		// A retirement discards everything Snowflake holds for the channel,
		// including the cumulative row-error count which is the only place its
		// silent discarding of a rejected row is visible. That count never resets,
		// which is what refuses the binding for good, so retiring a channel which
		// carries one must not be how the refusal is cleared: the binding would
		// resume with rows missing from the destination and no record that they ever
		// went missing.
		const stateKey = "rejoin-rejected.v1"

		var lo = newSession(t, stateKey, loRange, nil)
		writeRows(t, lo, 3)
		loItems, err := lo.flush(ctx)
		require.NoError(t, err)
		lo.stop()

		// The absorbed shard appended two documents, one of which Snowflake
		// discarded. Its committed offset token advances over the discarded row all
		// the same, so the channel is settled at the counter it produced.
		var hi = newSession(t, stateKey, hiRange, nil)
		var hiChannel = hi.bindings[0].channel
		require.NoError(t, hi.writeRow(ctx, 0, []any{"kept", 1, nil}))
		require.NoError(t, hi.writeRow(ctx, 0, []any{"discarded", 2, true}))
		_, err = hi.flush(ctx)
		require.ErrorContains(t, err, "1 row(s) rejected")
		hi.stop()

		// That counter is put into the checkpoint by hand, because the transaction
		// which provoked the rejection could never have checkpointed it: the point
		// is to leave the rejection as the only thing the retirement can object to,
		// so that what refuses the join is unambiguous.
		var hiItem = &streamV2Item{Channel: hiChannel, Counter: 2, KeyBegin: pivot, KeyEnd: math.MaxUint32}

		var joined = newSession(t, stateKey, fullRange, priorOf(loItems[0], hiItem))
		var refusal = joined.writeRow(ctx, 0, []any{"k", 1, nil})
		require.ErrorContains(t, refusal, "1 row(s) rejected")
		require.ErrorContains(t, refusal, hiChannel)
		require.Empty(t, joined.bindings[0].retire)

		// And the channel is as it was: the refusal reached the caller before the
		// drop could take the count with it.
		client, err := joined.ensureStarted(ctx)
		require.NoError(t, err)
		status, err := client.OpenChannel(ctx, "DB", "SCH", "TBL", hiChannel)
		require.NoError(t, err)
		require.Equal(t, hi.offsetToken(2), *status.CommittedToken)
		require.Equal(t, int64(1), status.RowsErrorCount)
	})

	t.Run("a split interrupted before its first commit continues the channel", func(t *testing.T) {
		// A split under load leaves the low child's range narrower than the one
		// recorded alongside the counter it inherited, for as long as it takes that
		// child to commit a transaction of its own — which under a backfill's round
		// lengths is long enough to be interrupted in. The documents Snowflake holds
		// beyond the counter are then this shard's own, appended under its own
		// range, and skipping them is the ordinary replay this path is built for.
		//
		// What says they are its own is the committed offset token, which names the
		// range that appended them. The checkpoint item cannot say it: what it
		// records is the range of the last transaction which committed, and these
		// documents were appended after it.
		const stateKey = "rejoin-split-load.v1"

		var parent = newSession(t, stateKey, fullRange, nil)
		writeRows(t, parent, 5)
		parentItems, err := parent.flush(ctx)
		require.NoError(t, err)
		parent.stop()

		// The low child appends three documents and is interrupted before the
		// transaction carrying their counter is durable.
		var lo = newSession(t, stateKey, loRange, priorOf(parentItems[0]))
		require.Equal(t, parentItems[0].Channel, lo.bindings[0].channel)
		writeRows(t, lo, 3)
		_, err = lo.flush(ctx)
		require.NoError(t, err)
		lo.stop()

		var replay = newSession(t, stateKey, loRange, priorOf(parentItems[0]))
		writeRows(t, replay, 3)
		require.Equal(t, int64(8), replay.bindings[0].skip)

		items, err := replay.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(8), items[0].Counter)
		require.Equal(t, loRange.KeyEnd, items[0].KeyEnd)
	})

	t.Run("a tail the topology before the split appended is refused", func(t *testing.T) {
		// The same split, interrupted the other way round: the documents Snowflake
		// holds beyond the counter are the parent's, appended over the whole key
		// range, and this shard's replay carries only the part of them its own range
		// selects. Positional skipping would drop as many of its documents as the
		// parent's outstanding ones its sibling now covers.
		const stateKey = "rejoin-parent-tail.v1"

		var parent = newSession(t, stateKey, fullRange, nil)
		writeRows(t, parent, 5)
		items, err := parent.flush(ctx)
		require.NoError(t, err)

		// The parent goes on appending, and the transaction which would have
		// carried those documents' counter never commits.
		writeRows(t, parent, 3)
		_, err = parent.flush(ctx)
		require.NoError(t, err)
		parent.stop()

		var lo = newSession(t, stateKey, loRange, priorOf(items[0]))
		var refusal = lo.writeRow(ctx, 0, []any{"k", 1, nil})
		require.ErrorContains(t, refusal, "appended by a shard covering [00000000, ffffffff]")
		require.ErrorContains(t, refusal, "this shard covers [00000000, 7fffffff]")
		require.ErrorContains(t, refusal, "Backfill this binding")
	})

	t.Run("a join interrupted before its first commit continues the channel", func(t *testing.T) {
		// The scale-down has the same window the split does, in the other
		// direction: until the survivor commits a transaction of its own, the item
		// it restarts from records the range it covered before the join. The
		// documents outstanding beyond that counter are the ones it appended after
		// widening, and its replay carries them again in the same order.
		const stateKey = "rejoin-join-load.v1"

		var lo = newSession(t, stateKey, loRange, nil)
		writeRows(t, lo, 3)
		items, err := lo.flush(ctx)
		require.NoError(t, err)
		lo.stop()

		var joined = newSession(t, stateKey, fullRange, priorOf(items[0]))
		writeRows(t, joined, 2)
		_, err = joined.flush(ctx)
		require.NoError(t, err)
		joined.stop()

		var replay = newSession(t, stateKey, fullRange, priorOf(items[0]))
		writeRows(t, replay, 2)
		require.Equal(t, int64(5), replay.bindings[0].skip)

		items, err = replay.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(5), items[0].Counter)
	})

	t.Run("a tail a wider range appended is refused though the checkpoint agrees", func(t *testing.T) {
		// A join interrupted while the joined shard was appending, and then split
		// back onto the boundary it removed. The item this shard restarts from is one
		// it wrote itself, so the range it records is this shard's own — and the
		// documents outstanding in Snowflake are still the joined shard's, a superset
		// of what this shard replays. Only the token distinguishes this from an
		// interruption of its own, which is why it is the token that is consulted.
		const stateKey = "rejoin-wider-tail.v1"

		var lo = newSession(t, stateKey, loRange, nil)
		writeRows(t, lo, 3)
		items, err := lo.flush(ctx)
		require.NoError(t, err)
		lo.stop()

		var joined = newSession(t, stateKey, fullRange, priorOf(items[0]))
		writeRows(t, joined, 2)
		_, err = joined.flush(ctx)
		require.NoError(t, err)
		joined.stop()

		var resplit = newSession(t, stateKey, loRange, priorOf(items[0]))
		require.ErrorContains(t,
			resplit.writeRow(ctx, 0, []any{"k", 1, nil}),
			"appended by a shard covering [00000000, ffffffff]")
	})

	t.Run("a first transaction split before it committed is refused", func(t *testing.T) {
		// A checkpoint holding nothing for the binding is what makes a token read as
		// this channel's own interrupted first transaction. A split before that
		// transaction ever committed leaves exactly that state — with the attempt
		// belonging to a range which is not this shard's.
		const stateKey = "rejoin-first-split.v1"

		var parent = newSession(t, stateKey, fullRange, nil)
		writeRows(t, parent, 5)
		_, err := parent.flush(ctx)
		require.NoError(t, err)
		parent.stop()

		var lo = newSession(t, stateKey, loRange, nil)
		require.ErrorContains(t,
			lo.writeRow(ctx, 0, []any{"k", 1, nil}),
			"appended by a shard covering [00000000, ffffffff]")
	})
}

// TestStreamV2BackfillRecreatesTheTable pins how a backfill of a streaming v2
// binding must reach its table.
//
// Apply runs while the outgoing generation's shards are still storing, and their
// channels are bound to the very table the new generation is about to
// re-materialize. Truncating it leaves those channels valid and pointed at it, so
// the rows they append next land after the truncate, survive it, and are
// re-materialized a second time. Dropping the table is what takes the channels
// with it — every one of them, including those of a shard the checkpoint names no
// item for. The live experiment behind both halves of that is the backfill
// subtest of TestStreamV2Manager.
func TestStreamV2BackfillRecreatesTheTable(t *testing.T) {
	var streamingCfg = config{
		Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.JWT},
		Advanced:    advancedConfig{FeatureFlags: flagSnowpipeStreamingV2},
	}
	var noFlagCfg = config{
		Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.JWT},
	}
	var noKeyPairCfg = config{
		Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.UserPass},
		Advanced:    advancedConfig{FeatureFlags: flagSnowpipeStreamingV2},
	}
	var binding = func(deltaUpdates bool) *pf.MaterializationSpec_Binding {
		return &pf.MaterializationSpec_Binding{DeltaUpdates: deltaUpdates}
	}

	for _, tt := range []struct {
		name string
		// outgoing is the endpoint configuration of the generation this
		// publication replaces, and incoming the one it publishes. They differ
		// whenever a publication changes the write path in the same breath as it
		// bumps a backfill counter.
		outgoing *config
		// storedShape wraps the outgoing configuration the way the control plane
		// stores it — the connector image alongside it — which is how a request
		// carries the specification being replaced.
		storedShape bool
		incoming    config
		last, next  *pf.MaterializationSpec_Binding
		want        bool
		// unreadable carries a replaced specification whose configuration is
		// neither shape this connector understands.
		unreadable bool
	}{
		{
			name:     "a streaming v2 binding",
			outgoing: &streamingCfg,
			incoming: streamingCfg,
			last:     binding(true),
			next:     binding(true),
			want:     true,
		},
		{
			// The channels which must not outlive the turnover are the outgoing
			// generation's, so it is the write path that generation ran on which
			// decides this, not the one the new generation will run.
			name:     "a binding leaving the streaming v2 path",
			outgoing: &streamingCfg,
			incoming: streamingCfg,
			last:     binding(true),
			next:     binding(false),
			want:     true,
		},
		{
			// Nor does turning the write path off protect the turnover: the shards
			// this publication replaces are streaming while it is applied, whatever
			// the shards replacing them will do. The replaced configuration is read
			// in the shape the control plane stores it, which is how the request
			// carries it.
			name:        "a publication which also turns the write path off",
			outgoing:    &streamingCfg,
			storedShape: true,
			incoming:    noFlagCfg,
			last:        binding(true),
			next:        binding(true),
			want:        true,
		},
		{
			// Nor does taking the key pair away, which is the other way to leave
			// the write path.
			name:        "a publication which also changes the authentication type",
			outgoing:    &streamingCfg,
			storedShape: true,
			incoming:    noKeyPairCfg,
			last:        binding(true),
			next:        binding(true),
			want:        true,
		},
		{
			// A configuration in hand which streams settles the answer, so a
			// replaced configuration which cannot be read does not narrow it. This
			// is the ordinary case: the request carries the replaced specification
			// with its secrets still encrypted, and reading that as "no feature
			// flags" would empty a table the outgoing generation streams into.
			name:       "a replaced configuration which cannot be read",
			outgoing:   nil,
			incoming:   streamingCfg,
			last:       binding(true),
			next:       binding(true),
			want:       true,
			unreadable: true,
		},
		{
			// The outgoing generation wrote this table through staged files, which
			// hold no channel on it, so its backfill may empty it in place — even
			// though the generation replacing it will stream.
			name:     "a binding arriving on the streaming v2 path",
			outgoing: &streamingCfg,
			incoming: streamingCfg,
			last:     binding(false),
			next:     binding(true),
		},
		{
			// Standard updates are written through staged files, which a truncate
			// cannot race.
			name:     "a standard updates binding",
			outgoing: &streamingCfg,
			incoming: streamingCfg,
			last:     binding(false),
			next:     binding(false),
		},
		{
			name:     "the streaming v2 flag not set",
			outgoing: &noFlagCfg,
			incoming: noFlagCfg,
			last:     binding(true),
			next:     binding(true),
		},
		{
			// The write path needs a key pair to authenticate the sidecar with, so
			// a binding of a password-authenticated task never streams.
			name:     "a task which cannot stream at all",
			outgoing: &noKeyPairCfg,
			incoming: noKeyPairCfg,
			last:     binding(true),
			next:     binding(true),
		},
		{
			// A resource which exists while the last-applied specification is not
			// on hand says nothing about what has been writing to it, so it is
			// re-created rather than assumed safe to empty.
			name:     "no last applied specification",
			outgoing: nil,
			incoming: streamingCfg,
			last:     nil,
			next:     binding(false),
			want:     true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var req = new(pm.Request_Apply)
			if tt.unreadable {
				req.LastMaterialization = &pf.MaterializationSpec{
					ConfigJson: []byte(`{"image":"ghcr.io/estuary/materialize-snowflake:v1","config":"ENC[AES256_GCM,data:0Ld3]"}`),
				}
			} else if tt.outgoing != nil {
				configJson, err := json.Marshal(tt.outgoing)
				require.NoError(t, err)
				if tt.storedShape {
					configJson, err = json.Marshal(map[string]any{
						"image":  "ghcr.io/estuary/materialize-snowflake:v1",
						"config": json.RawMessage(configJson),
					})
					require.NoError(t, err)
				}
				req.LastMaterialization = &pf.MaterializationSpec{ConfigJson: configJson}
			}

			var c = &client{cfg: tt.incoming}
			got, err := c.MustRecreateResource(req, tt.last, tt.next)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestStreamV2GenerationComment covers the line a streaming v2 binding's table
// carries to name the generation it belongs to: it must survive the round trip
// through a comment which also carries prose meant for whoever reads the table.
func TestStreamV2GenerationComment(t *testing.T) {
	var gen = streamV2Generation{materialization: "acmeCo/materialize-snowflake", stateKey: "public%2Fwidgets.v3"}
	var comment = streamV2GenerationComment("Generated for materialization acmeCo/materialize-snowflake of collection acmeCo/widgets", gen)

	readBack, ok := streamV2GenerationOf(comment)
	require.True(t, ok)
	require.Equal(t, gen, readBack)

	for _, tt := range []struct {
		name    string
		comment string
	}{
		{name: "no comment at all"},
		{
			name:    "a comment this connector wrote before it recorded generations",
			comment: "Generated for materialization acmeCo/materialize-snowflake of collection acmeCo/widgets",
		},
		{
			// Whoever rewrites the comment of a table takes the record with it,
			// which reads as a table that never had one.
			name:    "a marker without a state key",
			comment: "flow_generation: acmeCo/materialize-snowflake",
		},
		{
			// A half-written record names no generation rather than naming one no
			// shard can match, which would turn every shard of the task away
			// instead of only those a backfill has replaced.
			name:    "a marker whose state key is empty",
			comment: "flow_generation: acmeCo/materialize-snowflake ",
		},
		{
			name:    "a marker whose task is empty",
			comment: "flow_generation:  widgets.v3",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := streamV2GenerationOf(tt.comment)
			require.False(t, ok)
		})
	}
}

// TestStreamV2CreateTableNeedsAStateKey pins what happens when the binding
// resolved for a streaming v2 table carries no state key.
//
// The generation a table records is only as good as the state key in it: one
// recording an empty state key matches no shard, so it would turn away the whole
// task rather than only the generations a backfill has replaced. Refusing to
// create the table at all is the only safe reading, and it has to happen before
// any statement runs — which is what lets this drive a client with no connection.
func TestStreamV2CreateTableNeedsAStateKey(t *testing.T) {
	var c = &client{cfg: config{
		Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.JWT},
		Advanced:    advancedConfig{FeatureFlags: flagSnowpipeStreamingV2},
	}}

	var tc = sql.TableCreate{
		Table: sql.Table{
			TableShape: sql.TableShape{DeltaUpdates: true},
			Identifier: "WIDGETS",
		},
		TableCreateSql: "CREATE TABLE WIDGETS (KEY TEXT);",
	}

	require.ErrorContains(t, c.CreateTable(context.Background(), tc), "carries no state key")
}

// TestStreamV2GenerationConflict covers which shards a table turns away.
//
// A backfill re-creates the table, which takes every channel bound to it, but the
// shards of the generation it replaces keep running and open channels against the
// table again — and a channel opened against a re-created table looks exactly like
// one opened against a table that was always empty. The generation the table
// records is what tells them apart.
func TestStreamV2GenerationConflict(t *testing.T) {
	const task = "acmeCo/materialize-snowflake"
	var mine = streamV2Generation{materialization: task, stateKey: "widgets.v3"}
	var commentOf = func(gen streamV2Generation) string {
		return streamV2GenerationComment("Generated for materialization "+gen.materialization, gen)
	}

	for _, tt := range []struct {
		name    string
		comment string
		wantErr bool
	}{
		{
			name:    "the table records this generation",
			comment: commentOf(mine),
		},
		{
			// The state key rotates with the backfill counter, so a table naming a
			// different one of this task's own is a table this task has already
			// been replaced on.
			name:    "the table records a later generation of this binding",
			comment: commentOf(streamV2Generation{materialization: task, stateKey: "widgets.v4"}),
			wantErr: true,
		},
		{
			// Two tasks materializing into one table are not generations of each
			// other, and whichever created the table last is the one it names.
			name:    "the table records another task",
			comment: commentOf(streamV2Generation{materialization: "acmeCo/other", stateKey: "widgets.v4"}),
		},
		{
			// A table created before this connector recorded generations, or by a
			// standard-updates generation of the binding, says nothing about which
			// generation may stream into it.
			name:    "the table records no generation",
			comment: "Generated for materialization " + task,
		},
		{
			name: "the table has no comment",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var err = streamV2GenerationConflict(tt.comment, mine, "WIDGETS")
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "WIDGETS")
			require.ErrorContains(t, err, "widgets.v3")
			require.ErrorContains(t, err, "widgets.v4")
		})
	}
}

func TestReconcileStreamV2Channel(t *testing.T) {
	var token = func(s string) *string { return &s }
	var prior = func(counter int64, keyBegin, keyEnd uint32) map[string]*streamV2Item {
		return map[string]*streamV2Item{"chan": {Channel: "chan", Counter: counter, KeyBegin: keyBegin, KeyEnd: keyEnd}}
	}

	const keyBegin, keyEnd uint32 = 0x10000000, 0x1fffffff

	for _, tt := range []struct {
		name      string
		committed *string
		prior     map[string]*streamV2Item
		wantSkip  int64
		wantErr   string
	}{
		{
			name:      "nothing committed and nothing checkpointed",
			committed: nil,
			prior:     nil,
		},
		{
			// The channel Snowflake held those documents on is gone, and with it
			// the token which says which of them it holds. A backfill of the
			// binding is what does that — it re-creates the table, taking every
			// channel bound to it — so this is what a shard of the outgoing
			// generation finds when it restarts into a turnover.
			name:      "nothing committed with a checkpointed counter is refused",
			committed: nil,
			prior:     prior(42, keyBegin, keyEnd),
			wantErr:   "has lost committed data",
		},
		{
			name:      "clean boundary",
			committed: token("42@10000000-1fffffff"),
			prior:     prior(42, keyBegin, keyEnd),
			wantSkip:  42,
		},
		{
			name:      "committed ahead of the checkpoint is skipped",
			committed: token("50@10000000-1fffffff"),
			prior:     prior(42, keyBegin, keyEnd),
			wantSkip:  50,
		},
		{
			// The split under load: the range recorded alongside the counter is the
			// one the last committing transaction ran under, and the documents
			// outstanding beyond it were appended after the split, by this shard.
			// The token says so, so they are skipped as any other replay's are.
			name:      "committed ahead by this shard's range after a recorded range change is skipped",
			committed: token("50@10000000-1fffffff"),
			prior:     prior(42, keyBegin, keyEnd+1),
			wantSkip:  50,
		},
		{
			name:      "committed ahead by another shard's range is refused",
			committed: token("50@10000000-2fffffff"),
			prior:     prior(42, keyBegin, keyEnd),
			wantErr:   "appended by a shard covering [10000000, 2fffffff]",
		},
		{
			// An interruption before this channel generation's first checkpoint:
			// there is no recorded range to contradict, and the documents
			// Snowflake holds are still the ones about to be replayed.
			name:      "committed ahead with nothing checkpointed is skipped",
			committed: token("50@10000000-1fffffff"),
			prior:     nil,
			wantSkip:  50,
		},
		{
			// Unless another range appended them, which is what a split of a
			// backfill's first transaction leaves: the checkpoint holds nothing for
			// the binding either way, so the token is the only account of who wrote
			// those documents.
			name:      "committed ahead of nothing checkpointed by another range is refused",
			committed: token("50@00000000-ffffffff"),
			prior:     nil,
			wantErr:   "appended by a shard covering [00000000, ffffffff]",
		},
		{
			// The binding has been materializing under other channels, so this one
			// is not on its first transaction and the documents Snowflake holds for
			// it are another shard's. Only a backfill leaves the checkpoint holding
			// nothing, and it rotates the channel name too.
			name:      "committed ahead with an item of another channel alone is refused",
			committed: token("50@10000000-1fffffff"),
			prior: map[string]*streamV2Item{
				"other": {Channel: "other", Counter: 7, KeyBegin: 0, KeyEnd: keyBegin - 1},
			},
			wantErr: "cannot account for",
		},
		{
			// The retirement of this very channel, marked in the checkpoint by a
			// deletion the runtime has not yet reduced away. That the name still
			// holds documents means the drop which retires it did not reach
			// Snowflake, so it is no more this shard's own than any other leftover.
			name:      "committed ahead against a pending retirement of this channel is refused",
			committed: token("50@10000000-1fffffff"),
			prior:     map[string]*streamV2Item{"chan": nil},
			wantErr:   "cannot account for",
		},
		{
			// The ordinary scale-up: the shard this one was split from records the
			// range it was carved out of, and the channel named for the new
			// key-begin holds nothing yet.
			name:      "nothing committed with the items of other channels is accepted",
			committed: nil,
			prior: map[string]*streamV2Item{
				"parent": {Channel: "parent", Counter: 7, KeyBegin: 0, KeyEnd: math.MaxUint32},
			},
		},
		{
			// An older build of this write path wrote a bare document count, which
			// accounts for no range at all. The range the checkpoint recorded is then
			// the only account there is of who appended the documents outstanding
			// beyond it, and it is read as it was before the token carried one.
			name:      "an older build's token committed ahead is skipped",
			committed: token("50"),
			prior:     prior(42, keyBegin, keyEnd),
			wantSkip:  50,
		},
		{
			name:      "an older build's token committed ahead after a key-begin change is refused",
			committed: token("50"),
			prior:     prior(42, keyBegin-1, keyEnd),
			wantErr:   "shard key range has changed",
		},
		{
			name:      "an older build's token committed ahead after a key-end change is refused",
			committed: token("50"),
			prior:     prior(42, keyBegin, keyEnd+1),
			wantErr:   "shard key range has changed",
		},
		{
			// The range is consulted only when Snowflake is ahead, so an
			// unrelated range change in steady state is not a problem: whichever
			// range appended what the channel holds, there is nothing left to skip.
			name:      "clean boundary after a key range change is accepted",
			committed: token("42@00000000-ffffffff"),
			prior:     prior(42, 0, math.MaxUint32),
			wantSkip:  42,
		},
		{
			// Every shard of the task keeps an item of its own in the checkpoint
			// they share. Only this channel's item bears on this channel's skip
			// threshold; the rest are settled by retireAbsorbedChannels.
			name:      "the items of other shards do not bear on this channel",
			committed: token("42@10000000-1fffffff"),
			prior: map[string]*streamV2Item{
				"chan":     {Channel: "chan", Counter: 42, KeyBegin: keyBegin, KeyEnd: keyEnd},
				"higher":   {Channel: "higher", Counter: 99, KeyBegin: keyEnd + 1, KeyEnd: math.MaxUint32},
				"absorbed": {Channel: "absorbed", Counter: 99, KeyBegin: keyBegin + 1, KeyEnd: keyEnd},
			},
			wantSkip: 42,
		},
		{
			name:      "committed behind the checkpoint is refused",
			committed: token("41@10000000-1fffffff"),
			prior:     prior(42, keyBegin, keyEnd),
			wantErr:   "has lost committed data",
		},
		{
			name:      "a token this connector could not have written is refused",
			committed: token("basetok0000000001:7"),
			prior:     prior(42, keyBegin, keyEnd),
			wantErr:   "which is not a document count",
		},
		{
			name:      "a token whose range this connector could not have written is refused",
			committed: token("50@10000000+1fffffff"),
			prior:     prior(42, keyBegin, keyEnd),
			wantErr:   "which is not a document count",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			skip, err := reconcileStreamV2Channel("chan", "WIDGETS", tt.committed, tt.prior, keyBegin, keyEnd)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantSkip, skip)
		})
	}
}

// TestAbsorbedChannels covers which of the checkpoint's channels a shard reads as
// belonging to a shard whose key range it has taken over. Shard ranges tile the
// key space exactly, so this is decided by the recorded key-begins alone.
func TestAbsorbedChannels(t *testing.T) {
	var item = func(channel string, keyBegin, keyEnd uint32) *streamV2Item {
		return &streamV2Item{Channel: channel, Counter: 10, KeyBegin: keyBegin, KeyEnd: keyEnd}
	}
	// A shard which has been widened over the upper half of the key space.
	const keyBegin, keyEnd uint32 = 0, 0xffffffff

	var channelsOf = func(items []*streamV2Item) []string {
		var names []string
		for _, item := range items {
			names = append(names, item.Channel)
		}
		return names
	}

	t.Run("a shard which has absorbed nothing", func(t *testing.T) {
		// The item of a shard covering the upper half is not absorbed by a shard
		// which covers only the lower half.
		var prior = map[string]*streamV2Item{
			"mine":   item("mine", 0, 0x7fffffff),
			"higher": item("higher", 0x80000000, 0xffffffff),
		}
		require.Empty(t, absorbedChannels("mine", prior, 0, 0x7fffffff))
	})

	t.Run("a shard's own channel is never absorbed", func(t *testing.T) {
		// The widened shard's own item still records the narrower range it had
		// when it was written, which is inside the range it now covers.
		var prior = map[string]*streamV2Item{"mine": item("mine", 0, 0x7fffffff)}
		require.Empty(t, absorbedChannels("mine", prior, keyBegin, keyEnd))
	})

	t.Run("the channels of absorbed shards, by range", func(t *testing.T) {
		var prior = map[string]*streamV2Item{
			"mine":     item("mine", 0, 0x3fffffff),
			"second":   item("second", 0x40000000, 0x7fffffff),
			"fourth":   item("fourth", 0xc0000000, 0xffffffff),
			"third":    item("third", 0x80000000, 0xbfffffff),
			"retired":  nil,
			"outsider": item("outsider", 0, 0xffffffff),
		}
		// A four-way split joined back into one: the three shards above this one
		// are absorbed, reported in the order of the ranges they covered. The item
		// recording a key-begin at or below this shard's own is not absorbed, and
		// neither is a channel already retired.
		require.Equal(t,
			[]string{"second", "third", "fourth"},
			channelsOf(absorbedChannels("mine", prior, keyBegin, keyEnd)))
	})
}

// TestAbsorbedChannelSettled covers whether an absorbed shard's channel holds
// exactly what the checkpoint accounts for, which is what decides whether a join
// can proceed or demands a backfill.
func TestAbsorbedChannelSettled(t *testing.T) {
	var token = func(s string) *string { return &s }
	var absorbed = &streamV2Item{Channel: "absorbed", Counter: 42, KeyBegin: 0x80000000, KeyEnd: 0xffffffff}

	for _, tt := range []struct {
		name      string
		committed *string
		wantErr   string
		// A channel this connector did not write is a misconfiguration rather
		// than lost state, and like the same case on a shard's own channel it
		// does not promise a backfill would help.
		noBackfillHint bool
	}{
		{
			// The absorbed shard committed everything it appended, so the task's
			// frontier accounts for those documents and they are not replayed here.
			name:      "settled at the checkpointed counter",
			committed: token("42@80000000-ffffffff"),
		},
		{
			// A bare document count is what an older build of this write path wrote.
			// Retirement turns on the count alone, so it is settled by the same
			// comparison.
			name:      "an older build's token settled at the checkpointed counter",
			committed: token("42"),
		},
		{
			name:      "interrupted beyond the checkpointed counter",
			committed: token("44@80000000-ffffffff"),
			wantErr:   "will be replayed to this shard",
		},
		{
			name:      "behind the checkpointed counter",
			committed: token("41@80000000-ffffffff"),
			wantErr:   "has lost committed data",
		},
		{
			// The drop is what discards the token, and it is reached only once the
			// channel was settled against this same item, so this is a join
			// interrupted before its checkpoint carried the deletion. Repeating the
			// retirement is the only reading which does not demand the backfill it
			// exists to avoid.
			name:      "nothing committed at all, as an already-retired channel reports",
			committed: nil,
		},
		{
			name:           "a token this connector could not have written",
			committed:      token("basetok0000000001:7"),
			wantErr:        "which is not a document count",
			noBackfillHint: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var err = absorbedChannelSettled(absorbed, tt.committed, 0, math.MaxUint32)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			require.ErrorContains(t, err, absorbed.Channel)
			if !tt.noBackfillHint {
				require.ErrorContains(t, err, "Backfill this binding")
			}
		})
	}
}
