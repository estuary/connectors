package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// These suites cover the property the subrange-channel design exists for: a
// channel's contents are a function of the data, so a shard topology change
// hands whole channels — committed offset tokens included — to the shards that
// inherit their subranges, and those shards skip by position and then converge
// back to their own target layout. Every scenario drives real managers against
// the fake sidecar, whose channel state outlives each manager through
// FAKE_SIDECAR_STATE, so successive managers stand for successive sessions —
// and successive topologies — against one "Snowflake".

// newTopologyManager builds a manager standing for one shard of a task. Its
// sessions share whatever FAKE_SIDECAR_STATE names.
func newTopologyManager(t *testing.T, task string, keyBegin, keyEnd uint32, prior map[string]*streamV2Item) *streamV2Manager {
	t.Helper()
	var m = newStreamV2Manager(context.Background(),
		&config{Credentials: &snowflake_auth.CredentialConfig{}}, task, "acct",
		&pf.RangeSpec{KeyBegin: keyBegin, KeyEnd: keyEnd, RClockEnd: math.MaxUint32})
	m.argv = fakeSidecarArgv(t)
	t.Cleanup(m.stop)

	m.addBinding("DB", "SCH", "TBL", sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "topology.v1",
	}, prior)
	return m
}

// keysHashingTo returns n distinct keys whose packed-key hash falls inside r.
// The tests pick their keys by the very hash the write path routes by, so each
// scenario knows — rather than hopes — which channel every row lands on.
func keysHashingTo(r streamV2Range, n int, salt string) []string {
	var keys []string
	for i := 0; len(keys) < n; i++ {
		var key = fmt.Sprintf("%s-%d", salt, i)
		if r.contains(packedKeyHashHH64(packKey(key))) {
			keys = append(keys, key)
		}
	}
	return keys
}

// storeKeys stores one row per key. The rows the runtime would deliver to a
// shard are exactly those whose hash its range covers, so callers filter with
// keysHashingTo per subrange rather than replaying rows a shard would not see.
func storeKeys(t *testing.T, m *streamV2Manager, keys []string) {
	t.Helper()
	for _, key := range keys {
		require.NoError(t, testWriteRow(context.Background(), m, 0, []any{key, "v"}))
	}
}

// mergeItems collects the non-nil items of a flush's entries for binding zero,
// keyed by subrange, as the durable checkpoint would carry them forward.
func mergeItems(entries map[int]map[string]*streamV2Item) map[string]*streamV2Item {
	var items = make(map[string]*streamV2Item)
	for key, item := range entries[0] {
		if item != nil {
			items[key] = item
		}
	}
	return items
}

// deletionsOf collects the item keys a flush's entries delete for binding zero.
func deletionsOf(entries map[int]map[string]*streamV2Item) []string {
	var deleted []string
	for key, item := range entries[0] {
		if item == nil {
			deleted = append(deleted, key)
		}
	}
	return deleted
}

// fakeCommittedTokens reads the committed offset tokens the fake sidecar's
// "Snowflake" holds, which is the ground truth every scenario ends on.
func fakeCommittedTokens(t *testing.T, path string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var state struct {
		Committed map[string]string `json:"committed"`
	}
	require.NoError(t, json.Unmarshal(raw, &state))
	return state.Committed
}

// rangeKeys maps a layout to the checkpoint item keys of its channels.
func rangeKeys(layout []streamV2Range) []string {
	var keys = make([]string, len(layout))
	for i, r := range layout {
		keys[i] = r.key()
	}
	return keys
}

// channelNames maps a layout to the channel names a binding derives for it.
func channelNames(task string, layout []streamV2Range) []string {
	var names = make([]string, len(layout))
	for i, r := range layout {
		names[i] = streamV2ChannelName(task, r, "topology.v1")
	}
	return names
}

// activeNames reports the names of a binding's active layout, in order.
func activeNames(m *streamV2Manager) []string {
	var names []string
	for _, c := range m.bindings[0].channels {
		names = append(names, c.name)
	}
	return names
}

// TestStreamV2SteadyStateRouting pins the steady state everything else builds
// on: rows route by key hash across the shard's four channels, the checkpoint
// holds one item per channel, and a later session recovers each channel at a
// clean boundary and continues its counter.
func TestStreamV2SteadyStateRouting(t *testing.T) {
	var ctx = context.Background()
	const task = "test/topologySteady"
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))

	quarters, err := streamV2TargetLayout(0, math.MaxUint32)
	require.NoError(t, err)

	var rows []string
	for _, q := range quarters {
		rows = append(rows, keysHashingTo(q, 3, "steady")...)
	}

	var first = newTopologyManager(t, task, 0, math.MaxUint32, nil)
	storeKeys(t, first, rows)

	entries, err := first.flush(ctx)
	require.NoError(t, err)
	var items = mergeItems(entries)
	require.Empty(t, deletionsOf(entries))
	require.Len(t, items, len(quarters))

	// Each channel counted exactly the rows whose hash its subrange covers, so
	// the item counters are the routing, read back.
	for i, q := range quarters {
		require.Contains(t, items, q.key())
		require.Equal(t, q.keyBegin, items[q.key()].KeyBegin)
		require.Equal(t, q.keyEnd, items[q.key()].KeyEnd)
		require.Equal(t, int64(3), items[q.key()].Counter)
		require.Equal(t, channelNames(task, quarters)[i], items[q.key()].Channel)
	}
	first.stop()

	// A later session opens every channel at a clean boundary — nothing to
	// skip, nothing refused — and continues the counters where they stood.
	var second = newTopologyManager(t, task, 0, math.MaxUint32, items)
	var more []string
	for _, q := range quarters {
		more = append(more, keysHashingTo(q, 1, "steady-more")...)
	}
	storeKeys(t, second, more)
	for _, c := range second.bindings[0].channels {
		require.Equal(t, int64(3), c.skip)
	}

	entries, err = second.flush(ctx)
	require.NoError(t, err)
	for _, item := range mergeItems(entries) {
		require.Equal(t, int64(4), item.Counter)
	}
}

// TestStreamV2SplitInheritsChannels is the scenario the design was drawn for: a
// shard is interrupted mid-transaction, its range is split at the midpoint, and
// each child inherits the two whole channels nested in its half — committed
// offset tokens and all, the interrupted transaction's unaccounted rows
// included. The replay skips by position per channel, the first flush declares
// the child's own layout, and the acknowledged cutover converges to it.
func TestStreamV2SplitInheritsChannels(t *testing.T) {
	var ctx = context.Background()
	const task = "test/topologySplit"
	var statePath = filepath.Join(t.TempDir(), "channels.json")
	t.Setenv("FAKE_SIDECAR_STATE", statePath)

	quarters, err := streamV2TargetLayout(0, math.MaxUint32)
	require.NoError(t, err)
	var parentNames = channelNames(task, quarters)

	var committed, interrupted []string
	for _, q := range quarters {
		committed = append(committed, keysHashingTo(q, 3, "split-a")...)
		interrupted = append(interrupted, keysHashingTo(q, 2, "split-b")...)
	}

	// The parent commits one transaction, is interrupted in the next — its
	// appends reached Snowflake, its checkpoint did not — and is then split.
	var parent = newTopologyManager(t, task, 0, math.MaxUint32, nil)
	storeKeys(t, parent, committed)
	entries, err := parent.flush(ctx)
	require.NoError(t, err)
	var prior = mergeItems(entries)

	storeKeys(t, parent, interrupted)
	_, err = parent.flush(ctx)
	require.NoError(t, err) // the crash lands after the appends: the entries are never checkpointed
	parent.stop()

	var children = []struct {
		name             string
		keyBegin, keyEnd uint32
		inherited        []string
		inheritedKeys    []string
		salt             string
	}{
		{name: "low child", keyBegin: 0, keyEnd: 0x7fffffff, inherited: parentNames[:2], inheritedKeys: rangeKeys(quarters)[:2], salt: "split-c-lo"},
		{name: "high child", keyBegin: 0x80000000, keyEnd: math.MaxUint32, inherited: parentNames[2:], inheritedKeys: rangeKeys(quarters)[2:], salt: "split-c-hi"},
	}
	for _, child := range children {
		t.Run(child.name, func(t *testing.T) {
			var shard = streamV2Range{keyBegin: child.keyBegin, keyEnd: child.keyEnd}
			targets, err := streamV2TargetLayout(child.keyBegin, child.keyEnd)
			require.NoError(t, err)

			// The runtime replays the interrupted transaction, routing to this
			// child exactly the documents whose hash its range covers.
			var replay []string
			for _, key := range interrupted {
				if shard.contains(packedKeyHashHH64(packKey(key))) {
					replay = append(replay, key)
				}
			}

			var m = newTopologyManager(t, task, child.keyBegin, child.keyEnd, prior)
			storeKeys(t, m, replay)

			// The child runs on the two inherited channels, each already
			// holding the interrupted appends the replay must not repeat.
			require.Equal(t, child.inherited, activeNames(m))
			for _, c := range m.bindings[0].channels {
				require.Equal(t, int64(5), c.skip)
				require.Equal(t, int64(5), c.counter)
			}

			// The first flush is the declaration: the inherited counters, plus
			// the child's own four channels at zero, written one durable
			// checkpoint ahead of any row routing to them.
			entries, err := m.flush(ctx)
			require.NoError(t, err)
			var declared = mergeItems(entries)
			require.Len(t, declared, 6)
			for _, key := range rangeKeys(targets) {
				require.Contains(t, declared, key)
				require.Zero(t, declared[key].Counter)
			}

			// The runtime made that checkpoint durable, so the cutover runs:
			// the inherited channels retire and the target layout takes over.
			require.NoError(t, m.acknowledged(ctx))
			require.Equal(t, channelNames(task, targets), activeNames(m))

			var fresh []string
			for _, r := range targets {
				fresh = append(fresh, keysHashingTo(r, 2, child.salt)...)
			}
			storeKeys(t, m, fresh)
			entries, err = m.flush(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, child.inheritedKeys, deletionsOf(entries))
			for _, item := range mergeItems(entries) {
				require.Equal(t, int64(2), item.Counter)
			}
			m.stop()

			// The ground truth: the inherited channels are gone from Snowflake,
			// and the child's own channels hold exactly the fresh rows.
			var tokens = fakeCommittedTokens(t, statePath)
			for _, name := range child.inherited {
				require.NotContains(t, tokens, name)
			}
			for i, name := range channelNames(task, targets) {
				require.Equal(t, streamV2Token(2, targets[i]), tokens[name])
			}
		})
	}
}

// TestStreamV2JoinInheritsChannels is the split's mirror: two shards are
// interrupted mid-transaction and joined, and the survivor inherits all eight
// channels, skips each by its own token, and converges to its four.
func TestStreamV2JoinInheritsChannels(t *testing.T) {
	var ctx = context.Background()
	const task = "test/topologyJoin"
	var statePath = filepath.Join(t.TempDir(), "channels.json")
	t.Setenv("FAKE_SIDECAR_STATE", statePath)

	var halves = []struct {
		keyBegin, keyEnd uint32
	}{
		{keyBegin: 0, keyEnd: 0x7fffffff},
		{keyBegin: 0x80000000, keyEnd: math.MaxUint32},
	}

	// Each child commits one transaction and is interrupted in the next, so
	// every one of the eight channels holds appends its checkpoint item does
	// not account for.
	var prior = make(map[string]*streamV2Item)
	var eighths []streamV2Range
	var interrupted []string
	for _, half := range halves {
		targets, err := streamV2TargetLayout(half.keyBegin, half.keyEnd)
		require.NoError(t, err)
		eighths = append(eighths, targets...)

		var committed, replay []string
		for _, r := range targets {
			committed = append(committed, keysHashingTo(r, 2, "join-a")...)
			replay = append(replay, keysHashingTo(r, 1, "join-b")...)
		}

		var child = newTopologyManager(t, task, half.keyBegin, half.keyEnd, nil)
		storeKeys(t, child, committed)
		entries, err := child.flush(ctx)
		require.NoError(t, err)
		for channel, item := range mergeItems(entries) {
			prior[channel] = item
		}
		storeKeys(t, child, replay)
		_, err = child.flush(ctx)
		require.NoError(t, err) // interrupted: appended and committed, never checkpointed
		child.stop()

		interrupted = append(interrupted, replay...)
	}

	quarters, err := streamV2TargetLayout(0, math.MaxUint32)
	require.NoError(t, err)

	var joined = newTopologyManager(t, task, 0, math.MaxUint32, prior)
	storeKeys(t, joined, interrupted)

	require.Equal(t, channelNames(task, eighths), activeNames(joined))
	for _, c := range joined.bindings[0].channels {
		require.Equal(t, int64(3), c.skip)
		require.Equal(t, int64(3), c.counter)
	}

	entries, err := joined.flush(ctx)
	require.NoError(t, err)
	require.Len(t, mergeItems(entries), 12) // eight inherited counters, four declared at zero

	require.NoError(t, joined.acknowledged(ctx))
	require.Equal(t, channelNames(task, quarters), activeNames(joined))

	var fresh []string
	for _, r := range quarters {
		fresh = append(fresh, keysHashingTo(r, 2, "join-c")...)
	}
	storeKeys(t, joined, fresh)
	entries, err = joined.flush(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, rangeKeys(eighths), deletionsOf(entries))

	var tokens = fakeCommittedTokens(t, statePath)
	for _, name := range channelNames(task, eighths) {
		require.NotContains(t, tokens, name)
	}
	for i, name := range channelNames(task, quarters) {
		require.Equal(t, streamV2Token(2, quarters[i]), tokens[name])
	}
}

// TestStreamV2RebalanceCrashWindows walks one binding through every crash
// window a rebalance has, in the order a crashing task would meet them. The
// declaration is what makes each window recoverable: it is durable before
// anything else happens, so every later session can tell a converging layout
// from a lost one.
func TestStreamV2RebalanceCrashWindows(t *testing.T) {
	var ctx = context.Background()
	const task = "test/topologyWindows"
	var statePath = filepath.Join(t.TempDir(), "channels.json")
	t.Setenv("FAKE_SIDECAR_STATE", statePath)

	quarters, err := streamV2TargetLayout(0, math.MaxUint32)
	require.NoError(t, err)
	var parentNames = channelNames(task, quarters)

	const loBegin, loEnd = 0, 0x7fffffff
	targets, err := streamV2TargetLayout(loBegin, loEnd)
	require.NoError(t, err)
	var targetNames = channelNames(task, targets)

	// A parent commits, is interrupted, and is split — the state every window
	// below descends from.
	var committed, interrupted []string
	for _, q := range quarters {
		committed = append(committed, keysHashingTo(q, 3, "win-a")...)
		interrupted = append(interrupted, keysHashingTo(q, 2, "win-b")...)
	}
	var parent = newTopologyManager(t, task, 0, math.MaxUint32, nil)
	storeKeys(t, parent, committed)
	entries, err := parent.flush(ctx)
	require.NoError(t, err)
	var parentItems = mergeItems(entries)
	storeKeys(t, parent, interrupted)
	_, err = parent.flush(ctx)
	require.NoError(t, err)
	parent.stop()

	var loShard = streamV2Range{keyBegin: loBegin, keyEnd: loEnd}
	var replay []string
	for _, key := range interrupted {
		if loShard.contains(packedKeyHashHH64(packKey(key))) {
			replay = append(replay, key)
		}
	}

	// The child replays, declares, and dies before the cutover. Its flush
	// entries are the declaration checkpoint every later session recovers.
	var declaring = newTopologyManager(t, task, loBegin, loEnd, parentItems)
	storeKeys(t, declaring, replay)
	entries, err = declaring.flush(ctx)
	require.NoError(t, err)
	var declaration = mergeItems(entries)
	require.Len(t, declaration, 6)
	declaring.stop()

	// Window one: declaration durable, cutover never ran. The next session
	// finds the inherited channels quiescent beside a full declaration and
	// completes the cutover at open.
	var fresh []string
	for _, r := range targets {
		fresh = append(fresh, keysHashingTo(r, 2, "win-c")...)
	}

	var cutover = newTopologyManager(t, task, loBegin, loEnd, declaration)
	storeKeys(t, cutover, fresh)
	require.Equal(t, targetNames, activeNames(cutover))
	require.ElementsMatch(t, rangeKeys(quarters)[:2], cutover.bindings[0].retire)
	entries, err = cutover.flush(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, rangeKeys(quarters)[:2], deletionsOf(entries))
	// The crash lands here: the inherited channels are dropped and the target
	// channels hold this transaction's appends, but neither the deletions nor
	// the counters ever reach a durable checkpoint.
	cutover.stop()

	var tokens = fakeCommittedTokens(t, statePath)
	require.NotContains(t, tokens, parentNames[0])
	require.NotContains(t, tokens, parentNames[1])

	// Windows two and three together: the recovered checkpoint still carries
	// the declaration and the inherited items, the channels behind those items
	// are gone, and the target channels hold appends their counter-zero items
	// do not account for. The deletions are re-recorded, and the replay skips
	// per channel — through tokens standing on counter-zero items, which only
	// the declaration makes safe to honor.
	var resumed = newTopologyManager(t, task, loBegin, loEnd, declaration)
	storeKeys(t, resumed, fresh)
	require.Equal(t, targetNames, activeNames(resumed))
	require.ElementsMatch(t, rangeKeys(quarters)[:2], resumed.bindings[0].retire)
	for _, c := range resumed.bindings[0].channels {
		require.Equal(t, int64(2), c.skip)
		require.Equal(t, int64(2), c.counter)
	}
	entries, err = resumed.flush(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, rangeKeys(quarters)[:2], deletionsOf(entries))
	var converged = mergeItems(entries)
	require.Len(t, converged, 4)
	for _, item := range converged {
		require.Equal(t, int64(2), item.Counter)
	}
	resumed.stop()

	// The checkpoint finally lands, and the next session is plain steady
	// state: four channels, clean boundaries, nothing retiring.
	var steady = newTopologyManager(t, task, loBegin, loEnd, converged)
	storeKeys(t, steady, keysHashingTo(targets[0], 1, "win-d"))
	require.Equal(t, targetNames, activeNames(steady))
	require.Empty(t, steady.bindings[0].retire)
	for _, c := range steady.bindings[0].channels {
		require.Equal(t, int64(2), c.skip)
	}
}

// TestStreamV2SplitThenJoinBack scales a task up and then back down after both
// children converged. The join inherits eight settled channels, continues them
// at clean boundaries, and converges — the down-and-up cycle that used to
// require retiring channels by shard key-begin now falls out of subrange
// inheritance alone.
func TestStreamV2SplitThenJoinBack(t *testing.T) {
	var ctx = context.Background()
	const task = "test/topologyRejoin"
	var statePath = filepath.Join(t.TempDir(), "channels.json")
	t.Setenv("FAKE_SIDECAR_STATE", statePath)

	var prior = make(map[string]*streamV2Item)
	var eighths []streamV2Range
	for _, half := range []struct{ keyBegin, keyEnd uint32 }{
		{keyBegin: 0, keyEnd: 0x7fffffff},
		{keyBegin: 0x80000000, keyEnd: math.MaxUint32},
	} {
		targets, err := streamV2TargetLayout(half.keyBegin, half.keyEnd)
		require.NoError(t, err)
		eighths = append(eighths, targets...)

		var rows []string
		for _, r := range targets {
			rows = append(rows, keysHashingTo(r, 2, "rejoin-a")...)
		}
		var child = newTopologyManager(t, task, half.keyBegin, half.keyEnd, nil)
		storeKeys(t, child, rows)
		entries, err := child.flush(ctx)
		require.NoError(t, err)
		for channel, item := range mergeItems(entries) {
			prior[channel] = item
		}
		child.stop()
	}

	quarters, err := streamV2TargetLayout(0, math.MaxUint32)
	require.NoError(t, err)

	// The joined shard is not replaying anything: both children committed and
	// checkpointed. It simply continues eight settled channels until the
	// rebalance converges them.
	var joined = newTopologyManager(t, task, 0, math.MaxUint32, prior)
	var rows []string
	for _, r := range eighths {
		rows = append(rows, keysHashingTo(r, 1, "rejoin-b")...)
	}
	storeKeys(t, joined, rows)
	require.Equal(t, channelNames(task, eighths), activeNames(joined))
	for _, c := range joined.bindings[0].channels {
		require.Equal(t, int64(2), c.skip)
		require.Equal(t, int64(3), c.counter)
	}

	entries, err := joined.flush(ctx)
	require.NoError(t, err)
	require.Len(t, mergeItems(entries), 12)
	require.NoError(t, joined.acknowledged(ctx))
	require.Equal(t, channelNames(task, quarters), activeNames(joined))

	storeKeys(t, joined, keysHashingTo(quarters[0], 1, "rejoin-c"))
	entries, err = joined.flush(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, rangeKeys(eighths), deletionsOf(entries))
}

// TestStreamV2MisalignedSplitRefused pins the one topology change the channels
// cannot survive: a split cutting through a channel's subrange, which midpoint
// splits only produce by splitting again before the layout converged past the
// depth the new boundary cuts through. The rows that channel holds beyond its
// counter belong to both children, so the only safe answer is a refusal.
func TestStreamV2MisalignedSplitRefused(t *testing.T) {
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))
	const task = "test/topologyMisaligned"

	// A channel covering the parent's low half, presented to a shard covering
	// the low quarter: the shard's upper boundary lands mid-channel.
	var half = streamV2Range{keyBegin: 0, keyEnd: 0x7fffffff}
	var name = streamV2ChannelName(task, half, "topology.v1")
	var m = newTopologyManager(t, task, 0, 0x3fffffff, map[string]*streamV2Item{
		half.key(): {Channel: name, Counter: 5, KeyBegin: half.keyBegin, KeyEnd: half.keyEnd},
	})

	var err = testWriteRow(context.Background(), m, 0, []any{"any", "v"})
	require.ErrorContains(t, err, "crosses the boundary")
	require.ErrorContains(t, err, name)
}

// TestStreamV2LostChannelRefused pins the reading of a channel that reports no
// committed offset token while the checkpoint records documents appended to it,
// with no declaration to explain the loss as an interrupted cutover: the
// account of what Snowflake holds is gone, so the binding refuses.
func TestStreamV2LostChannelRefused(t *testing.T) {
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))
	const task = "test/topologyLost"

	quarters, err := streamV2TargetLayout(0, math.MaxUint32)
	require.NoError(t, err)
	var name = streamV2ChannelName(task, quarters[1], "topology.v1")

	var m = newTopologyManager(t, task, 0, math.MaxUint32, map[string]*streamV2Item{
		quarters[1].key(): {Channel: name, Counter: 5, KeyBegin: quarters[1].keyBegin, KeyEnd: quarters[1].keyEnd},
	})

	err = testWriteRow(context.Background(), m, 0, []any{"any", "v"})
	require.ErrorContains(t, err, "has committed nothing while this task's checkpoint records")
}

// TestStreamV2ForeignTokenRefused pins the unconditional subrange guard at the
// seam where it matters: a channel whose committed offset token names a range
// other than the channel's own subrange was written by something this write
// path cannot account for, and nothing it counts may be skipped.
func TestStreamV2ForeignTokenRefused(t *testing.T) {
	var statePath = filepath.Join(t.TempDir(), "channels.json")
	t.Setenv("FAKE_SIDECAR_STATE", statePath)
	const task = "test/topologyForeign"

	quarters, err := streamV2TargetLayout(0, math.MaxUint32)
	require.NoError(t, err)
	var name = streamV2ChannelName(task, quarters[0], "topology.v1")

	// Snowflake holds a token for this channel's name written under the whole
	// key range — the footprint of a channel scheme this write path never ran.
	var seeded = map[string]any{
		"committed": map[string]string{name: streamV2Token(5, streamV2Range{keyBegin: 0, keyEnd: math.MaxUint32})},
		"errors":    map[string]int{},
	}
	raw, err := json.Marshal(seeded)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(statePath, raw, 0o644))

	var m = newTopologyManager(t, task, 0, math.MaxUint32, map[string]*streamV2Item{
		quarters[0].key(): {Channel: name, Counter: 5, KeyBegin: quarters[0].keyBegin, KeyEnd: quarters[0].keyEnd},
	})

	err = testWriteRow(context.Background(), m, 0, []any{"any", "v"})
	require.ErrorContains(t, err, "was not written against this channel's subrange")
}

// TestStreamV2SplitInheritsAnEmptyChannel pins the state a skewed transaction
// leaves for a later split: a channel of the parent's layout that took no rows
// legitimately stands in the checkpoint at counter zero with no committed
// offset token. The child that inherits it must adopt it as a fresh channel of
// the inherited layout — not read it as an orphaned declaration to retire,
// which leaves the surviving layout unable to tile the child's range and
// wedges the binding over rows that never existed.
func TestStreamV2SplitInheritsAnEmptyChannel(t *testing.T) {
	var ctx = context.Background()
	const task = "test/topologyEmptyInherit"
	var statePath = filepath.Join(t.TempDir(), "channels.json")
	t.Setenv("FAKE_SIDECAR_STATE", statePath)

	quarters, err := streamV2TargetLayout(0, math.MaxUint32)
	require.NoError(t, err)
	var parentNames = channelNames(task, quarters)

	// Every row of the parent's one transaction misses the first quarter, so
	// its item is checkpointed at counter zero and Snowflake holds no token
	// for it.
	var skewed []string
	for _, q := range quarters[1:] {
		skewed = append(skewed, keysHashingTo(q, 2, "empty-inherit")...)
	}
	var parent = newTopologyManager(t, task, 0, math.MaxUint32, nil)
	storeKeys(t, parent, skewed)
	entries, err := parent.flush(ctx)
	require.NoError(t, err)
	var prior = mergeItems(entries)
	require.Zero(t, prior[quarters[0].key()].Counter)
	parent.stop()

	// The low child inherits the empty first quarter alongside the second,
	// which holds rows. Both continue: the empty one as a fresh channel.
	var child = newTopologyManager(t, task, 0, 0x7fffffff, prior)
	targets, err := streamV2TargetLayout(0, 0x7fffffff)
	require.NoError(t, err)

	var fresh = append(
		keysHashingTo(quarters[0], 2, "empty-inherit-lo"),
		keysHashingTo(quarters[1], 1, "empty-inherit-lo")...,
	)
	storeKeys(t, child, fresh)
	require.Equal(t, parentNames[:2], activeNames(child))

	// The child converges as any inheriting shard does, and the formerly
	// empty channel's rows survive to its target channels.
	entries, err = child.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), mergeItems(entries)[quarters[0].key()].Counter)
	require.NoError(t, child.acknowledged(ctx))
	require.Equal(t, channelNames(task, targets), activeNames(child))
	entries, err = child.flush(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, rangeKeys(quarters)[:2], deletionsOf(entries))
	child.stop()
}
