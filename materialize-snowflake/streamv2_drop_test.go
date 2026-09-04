package connector

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/consumer/protocol"
)

// dropTestTarget is the table this suite's binding materializes into.
var dropTestTarget = sql.Table{
	TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true, Path: []string{"DB", "SCH", "TBL"}},
	Identifier: "TBL",
	Keys:       []sql.Column{{Identifier: `KEY`}},
	Values:     []sql.Column{{Identifier: `VAL`}},
	StateKey:   "drop.v1",
}

func newDropTestManager(t *testing.T, task string, rng *pf.RangeSpec) *streamV2Manager {
	t.Helper()
	var m = newStreamV2Manager(context.Background(),
		&config{Credentials: &snowflake_auth.CredentialConfig{}}, task, "acct", rng)
	m.argv = fakeSidecarArgv(t)
	t.Cleanup(m.stop)
	return m
}

// seedChannelToDrop writes a single channel three committed documents, then
// two more that reach Snowflake without a checkpoint recording them, leaving C
// (5, in the fake sidecar's "Snowflake") ahead of K (3, the checkpoint items).
// It pins the layout to one channel so tests can follow it by index.
func seedChannelToDrop(t *testing.T) (statePath string, items map[string]*streamV2Item) {
	t.Helper()
	singleChannelLayout(t)

	statePath = filepath.Join(t.TempDir(), "channels.json")
	t.Setenv("FAKE_SIDECAR_STATE", statePath)

	var restore = streamV2BatchRows
	t.Cleanup(func() { streamV2BatchRows = restore })
	streamV2BatchRows = 1

	var ctx = context.Background()
	var m = newDropTestManager(t, "test/drop", &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	m.addBinding("DB", "SCH", "TBL", dropTestTarget, nil)

	for i := range 3 {
		require.NoError(t, testWriteRow(ctx, m, 0, []any{fmt.Sprintf("k%d", i), i}))
	}
	entries, err := m.flush(ctx)
	require.NoError(t, err)
	items = mergeItems(entries)
	require.Len(t, items, 1)

	for i := 3; i < 5; i++ {
		require.NoError(t, testWriteRow(ctx, m, 0, []any{fmt.Sprintf("k%d", i), i}))
	}
	var c = m.bindings[0].channels[0]
	require.NoError(t, c.pipe.wait())
	_, err = m.client.WaitCommit(ctx, c.name, c.offsetToken(5))
	require.NoError(t, err)
	m.sup.kill()

	return statePath, items
}

func TestStreamV2ChannelDrop(t *testing.T) {
	var ctx = context.Background()
	var fullRange = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}

	t.Run("tombstones set Tombstone on every owned item and leave counters alone", func(t *testing.T) {
		var _, items = seedChannelToDrop(t)

		var r = newStreamV2ChannelDrop(newDropTestManager(t, "test/drop", fullRange))
		r.register(dropTestTarget.StateKey, "DB", "SCH", "TBL", items, fullRange)

		var marked = r.tombstones(dropTestTarget.StateKey)
		require.Len(t, marked, len(items))
		for key, orig := range items {
			require.True(t, marked[key].Tombstone)
			require.Equal(t, orig.Channel, marked[key].Channel)
			require.Equal(t, orig.Counter, marked[key].Counter)
			require.False(t, orig.Tombstone)
		}
	})

	t.Run("drop before fence is rejected", func(t *testing.T) {
		var _, items = seedChannelToDrop(t)

		var r = newStreamV2ChannelDrop(newDropTestManager(t, "test/drop", fullRange))
		r.register(dropTestTarget.StateKey, "DB", "SCH", "TBL", items, fullRange)

		_, err := r.drop(ctx, dropTestTarget.StateKey)
		require.Error(t, err)
	})

	t.Run("crash after the drop, before the clear", func(t *testing.T) {
		var statePath, items = seedChannelToDrop(t)
		var channel = items[soleKey(t, items)].Channel

		// Channel drop A: fence and drop. Its tombstones, captured before the drop, are
		// what a checkpoint durable ahead of a crash between the drop and the
		// clear would still hold.
		var mgrA = newDropTestManager(t, "test/drop", fullRange)
		var rA = newStreamV2ChannelDrop(mgrA)
		rA.register(dropTestTarget.StateKey, "DB", "SCH", "TBL", items, fullRange)
		var tombstonesA = rA.tombstones(dropTestTarget.StateKey)
		require.NoError(t, rA.fence(ctx))
		_, err := rA.drop(ctx, dropTestTarget.StateKey)
		require.NoError(t, err)
		require.NotContains(t, fakeCommittedTokens(t, statePath), channel)

		// Channel drop B, built from the checkpoint that outlived the drop: fence
		// reopens an empty channel, and the drop it repeats finds nothing new to
		// discard.
		var mgrB = newDropTestManager(t, "test/drop", fullRange)
		var rB = newStreamV2ChannelDrop(mgrB)
		rB.register(dropTestTarget.StateKey, "DB", "SCH", "TBL", tombstonesA, fullRange)
		require.NoError(t, rB.fence(ctx))
		_, err = rB.drop(ctx, dropTestTarget.StateKey)
		require.NoError(t, err)
		require.NotContains(t, fakeCommittedTokens(t, statePath), channel)

		// A v2 manager returning to the path with the outlived tombstones as its
		// prior: under the single-channel layout the tombstone's key range is the
		// target key range, so ensureOpened's tombstone pass leaves the live item
		// flush writes in its place, with no deletion of its own.
		var v2 = newDropTestManager(t, "test/drop", fullRange)
		v2.addBinding("DB", "SCH", "TBL", dropTestTarget, tombstonesA)
		for i := 5; i < 7; i++ {
			require.NoError(t, testWriteRow(ctx, v2, 0, []any{fmt.Sprintf("k%d", i), i}))
		}
		entries, err := v2.flush(ctx)
		require.NoError(t, err)
		var item = soleItem(t, entries, 0)
		require.Equal(t, int64(2), item.Counter)
		require.False(t, item.Tombstone)
		require.Empty(t, deletionsOf(entries))
		require.Zero(t, v2.bindings[0].channels[0].skip)
	})

	t.Run("a tombstone of another layout is deleted", func(t *testing.T) {
		singleChannelLayout(t)
		var statePath = filepath.Join(t.TempDir(), "channels.json")
		t.Setenv("FAKE_SIDECAR_STATE", statePath)

		// A tombstone whose key range is not the target this shard converges to —
		// built directly, standing for a layout that has since been superseded.
		const otherChannel = "task_00000000_drop_v1_other_layout"
		var tombstone = &streamV2Item{Channel: otherChannel, Counter: 3, KeyBegin: 0, KeyEnd: 0x7fffffff, Tombstone: true}
		var tombstones = map[string]*streamV2Item{streamV2Range{keyEnd: 0x7fffffff}.key(): tombstone}

		var v2 = newDropTestManager(t, "test/drop", fullRange)
		v2.addBinding("DB", "SCH", "TBL", dropTestTarget, tombstones)
		for i := 5; i < 7; i++ {
			require.NoError(t, testWriteRow(ctx, v2, 0, []any{fmt.Sprintf("k%d", i), i}))
		}
		entries, err := v2.flush(ctx)
		require.NoError(t, err)

		require.Contains(t, deletionsOf(entries), streamV2Range{keyEnd: 0x7fffffff}.key())
		var item = soleItem(t, entries, 0)
		require.Equal(t, int64(2), item.Counter)
		require.NotContains(t, fakeCommittedTokens(t, statePath), otherChannel)
	})

	t.Run("crash after the clear", func(t *testing.T) {
		var statePath, items = seedChannelToDrop(t)
		var channel = items[soleKey(t, items)].Channel

		var r = newStreamV2ChannelDrop(newDropTestManager(t, "test/drop", fullRange))
		r.register(dropTestTarget.StateKey, "DB", "SCH", "TBL", items, fullRange)
		require.NoError(t, r.fence(ctx))
		_, err := r.drop(ctx, dropTestTarget.StateKey)
		require.NoError(t, err)
		require.NotContains(t, fakeCommittedTokens(t, statePath), channel)

		// The clear landed: a return to v2 finds nothing at all for the binding.
		var v2 = newDropTestManager(t, "test/drop", fullRange)
		v2.addBinding("DB", "SCH", "TBL", dropTestTarget, nil)
		for i := 5; i < 7; i++ {
			require.NoError(t, testWriteRow(ctx, v2, 0, []any{fmt.Sprintf("k%d", i), i}))
		}
		entries, err := v2.flush(ctx)
		require.NoError(t, err)
		var item = soleItem(t, entries, 0)
		require.Equal(t, int64(2), item.Counter)
		require.Zero(t, v2.bindings[0].channels[0].skip)
	})

	t.Run("the v2 side drops a tombstone whose drop never happened", func(t *testing.T) {
		var statePath, items = seedChannelToDrop(t)
		var key = soleKey(t, items)
		var tombstone = *items[key]
		tombstone.Tombstone = true

		var v2 = newDropTestManager(t, "test/drop", fullRange)
		v2.addBinding("DB", "SCH", "TBL", dropTestTarget, map[string]*streamV2Item{key: &tombstone})
		require.NoError(t, testWriteRow(ctx, v2, 0, []any{"k", 0}))

		require.NotContains(t, fakeCommittedTokens(t, statePath), tombstone.Channel)
	})

	t.Run("the mark phase writes tombstones without blobs", func(t *testing.T) {
		var _, items = seedChannelToDrop(t)

		var v2 = newDropTestManager(t, "test/drop", fullRange)
		var r = newStreamV2ChannelDrop(v2)
		r.register(dropTestTarget.StateKey, "DB", "SCH", "TBL", items, fullRange)
		var expected = r.tombstones(dropTestTarget.StateKey)

		var d = &transactor{
			cp:                  make(checkpoint),
			bindings:            []*binding{{target: dropTestTarget, streaming: true}},
			snowpipeStreaming:   &streamManager{blobStats: map[int][]*blobStatsTracker{}},
			snowpipeStreamingV2: v2,
			channelDrop:         r,
		}

		_, err := d.buildDriverCheckpoint(ctx, &protocol.Checkpoint{})
		require.NoError(t, err)
		require.Nil(t, d.cp[dropTestTarget.StateKey].StreamBlobs)
		require.Equal(t, expected, d.cp[dropTestTarget.StateKey].StreamV2)
	})
}

// soleKey returns the one checkpoint key of a single-channel-layout items map.
func soleKey(t *testing.T, items map[string]*streamV2Item) string {
	t.Helper()
	require.Len(t, items, 1)
	for key := range items {
		return key
	}
	return ""
}
