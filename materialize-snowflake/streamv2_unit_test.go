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
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
)

// TestStreamV2CheckpointHoldsOneItemPerChannel pins the shape of the streaming
// v2 driver checkpoint against the only runtime this write path runs on. Every
// shard of a v2 task merge-patches its connector state into one task-global
// document, so two shards of the same binding write the same JSON path. The
// item each records is durable state rather than pending work, so it needs a
// key of its own: keyed by state key alone it is overwritten by whichever
// sibling reduced last, and the survivor is then reconciled against a channel it
// does not describe.
func TestStreamV2CheckpointHoldsOneItemPerChannel(t *testing.T) {
	const stateKey = "shared.v1"
	const loChannel, hiChannel = "task_00000000_shared_v1", "task_80000000_shared_v1"
	const loKey, hiKey = "00000000-7fffffff", "80000000-ffffffff"
	var lo = fmt.Sprintf(`{"ChannelName":%q,"Routed":7}`, loChannel)
	var hi = fmt.Sprintf(`{"ChannelName":%q,"Routed":3}`, hiChannel)

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
	// items survive the reduce.
	reduced, err := jsonpatch.MergePatch(
		fmt.Appendf(nil, `{%q:{"StreamV2":{%q:%s}}}`, stateKey, loKey, lo),
		fmt.Appendf(nil, `{%q:{"StreamV2":{%q:%s}}}`, stateKey, hiKey, hi),
	)
	require.NoError(t, err)

	var cp checkpoint
	require.NoError(t, json.Unmarshal(reduced, &cp))
	require.Len(t, cp[stateKey].StreamV2, 2)
}

// TestStreamV2CommitPrecedesCheckpoint pins where the commit is awaited. The
// driver checkpoint records a routed offset, and Snowflake's committed offset
// token is what the next Open reconciles that offset against. The
// runtime's own checkpoint is durable before Acknowledge runs, so a commit
// awaited there cannot fail the transaction whose item it belongs to: the
// task would resume from an offset Snowflake never committed, which Open can
// only reject, and no replay can re-append rows the runtime considers
// delivered. So the wait belongs to the call which produces the item.
func TestStreamV2CommitPrecedesCheckpoint(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)

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

	require.NoError(t, testWriteRow(ctx, m, 0, []any{"k", "v"}))

	entries, err := m.flush(ctx)
	require.ErrorContains(t, err, "not committed")
	require.Empty(t, entries)
}

// TestStreamV2RejectedRows covers, without credentials, what the live test
// covers against Snowflake: a rejected row fails the transaction that produced
// it, and goes on rejecting the channel it was appended to.
func TestStreamV2RejectedRows(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)

	// The single channel of the binding under a one-channel layout, at the epoch a
	// fresh binding mints.
	var channel = streamV2FormatChannelName("test/rejectedRows", 0,
		streamV2Range{keyBegin: 0, keyEnd: math.MaxUint32}, "rejected.v1")

	var newManager = func(t *testing.T, prior *streamV2ChannelCheckpointItem) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/rejectedRows", "acct",
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		m.argv = fakeSidecarArgv(t)
		t.Cleanup(m.stop)

		// The fake sidecar rejects a row which carries the REJECT column, as
		// Snowflake rejects one it cannot store: the append and the commit both
		// succeed, and only the channel's row-error count moves.
		var priorItems streamV2Checkpoint
		if prior != nil {
			prior.ChannelName = channel
			priorItems = streamV2Checkpoint{fullKeyRange: prior}
		}

		m.addBinding("DB", "SCH", "TBL", sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
			Identifier: "TBL",
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}, {Identifier: `REJECT`}},
			StateKey:   "rejected.v1",
		}, priorItems)
		return m
	}

	t.Run("a rejected row fails the commit", func(t *testing.T) {
		var m = newManager(t, nil)
		require.NoError(t, testWriteRow(ctx, m, 0, []any{"kept", "v", nil}))
		require.NoError(t, testWriteRow(ctx, m, 0, []any{"dropped", "v", true}))

		// The rejection is the transaction's own, so it must fail the transaction
		// rather than the one after it: the count is read as the commit is awaited,
		// which is before the routed offset reaches the checkpoint.
		entries, err := m.flush(ctx)
		require.ErrorContains(t, err, "1 row(s) rejected")
		require.ErrorContains(t, err, channel)
		require.ErrorContains(t, err, "fake rejection")
		require.Empty(t, entries)
	})

	t.Run("a channel carrying a row rejection is rejected when it opens", func(t *testing.T) {
		// The transaction that provoked a rejection fails before acknowledging,
		// so a restart replays it. Rejecting as the channel opens — before the
		// replay can append, and whether or not this session appends at all — is
		// what stops that replay from acknowledging rows Snowflake does not hold.
		t.Setenv("FAKE_SIDECAR_ROWS_ERROR_COUNT", "3")
		var m = newManager(t, &streamV2ChannelCheckpointItem{Routed: 2})

		require.ErrorContains(t, testWriteRow(ctx, m, 0, []any{"kept", "v", nil}), "3 row(s) rejected")
	})
}

// TestStreamV2DropChannel covers, without credentials, what the live test
// establishes against Snowflake: a channel name outlives the shard it was named
// for, and only dropping the channel stops the next shard to derive that name
// from adopting its committed offset token as its own.
func TestStreamV2DropChannel(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)

	// The fake sidecar's channel state outlives its process, so each manager
	// below stands in for a separate session against one Snowflake.
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))

	var target = sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "drop.v1",
	}

	// Every manager here covers the whole key space, as the single shard of a
	// task does — which is the point: a name derived from a key-begin is
	// derived again, identically, by whichever shard next holds that key-begin.
	var newSession = func(t *testing.T) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/channel-drop", "acct",
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		m.argv = fakeSidecarArgv(t)
		t.Cleanup(m.stop)
		m.addBinding("DB", "SCH", "TBL", target, nil)
		return m
	}

	// A shard appends and commits three documents and is then deleted, leaving
	// the channel as its final transaction had it.
	var first = newSession(t)
	for i := range 3 {
		require.NoError(t, testWriteRow(ctx, first, 0, []any{"k", i}))
	}
	var channel = first.bindings[0].channels[0].channelName
	entries, err := first.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), soleCheckpointItem(t, entries, 0).Routed)
	first.stop()

	// Undropped, that channel wedges the next shard to derive its name: the
	// documents it is about to store are taken for a replay of an interrupted
	// transaction it never ran, and skipped.
	var reused = newSession(t)
	require.NoError(t, testWriteRow(ctx, reused, 0, []any{"k", 0}))
	require.Equal(t, channel, reused.bindings[0].channels[0].channelName)
	require.Equal(t, int64(3), reused.bindings[0].channels[0].progress.committed)
	reused.stop()

	// Dropping it is what makes the name reusable. The dropping session opens
	// the channel first, as a shard absorbing another's key range does.
	var dropping = newSession(t)
	client, err := dropping.ensureStarted(ctx)
	require.NoError(t, err)
	_, err = client.OpenChannel(ctx, "DB", "SCH", "TBL", channel)
	require.NoError(t, err)
	require.NoError(t, dropChannel(ctx, client, channel))

	// A dropped channel is out of service: the handle it was dropped through is
	// spent, so appending to it fails rather than quietly reviving it.
	require.ErrorContains(t, client.Append(ctx, channel, "1", "1", []byte("[]"), 0), "is not open")

	status, err := client.OpenChannel(ctx, "DB", "SCH", "TBL", channel)
	require.NoError(t, err)
	require.Nil(t, status.CommittedToken)
	dropping.stop()

	// So the name now opens as a channel which has committed nothing, and the
	// documents of the shard which reused it are appended in full.
	var after = newSession(t)
	require.NoError(t, testWriteRow(ctx, after, 0, []any{"k", 0}))
	require.Equal(t, channel, after.bindings[0].channels[0].channelName)
	require.Zero(t, after.bindings[0].channels[0].progress.committed)

	entries, err = after.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), soleCheckpointItem(t, entries, 0).Routed)
}

// TestStreamV2RejectsForeignTask covers, without credentials, the rejection of a
// table another task already streams into. A backfill truncates the table, so a
// second task on it would silently wipe the first's rows while the first's channels
// and tokens stand; the listing of the pipe's channels is what reveals the first task
// before the second opens anything.
func TestStreamV2RejectsForeignTask(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))

	var fullRange = streamV2Range{keyEnd: math.MaxUint32}
	var foreignChannel = streamV2FormatChannelName("other/task", 0, fullRange, "theirs.v1")
	var unattributable = "someone_elses_channel"

	var newSession = func(t *testing.T, task string) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, task, "acct",
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		m.argv = fakeSidecarArgv(t)
		m.listChannels = fakeListChannels
		t.Cleanup(m.stop)
		m.addBinding("DB", "SCH", "TBL", sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
			Identifier: "TBL",
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}},
			StateKey:   "mine.v1",
		}, nil)
		return m
	}

	// A channel of no v2 shape is another high-performance client's. The connector
	// cannot attribute it, so it is left alone and the table is accepted.
	require.NoError(t, os.WriteFile(os.Getenv("FAKE_SIDECAR_STATE"),
		fmt.Appendf(nil, `{"committed":{%q:"1"},"errors":{}}`, unattributable), 0o644))
	var alone = newSession(t, "test/task")
	require.NoError(t, testWriteRow(ctx, alone, 0, []any{"k", "v"}))
	_, err := alone.flush(ctx)
	require.NoError(t, err)
	alone.stop()

	// The other task's committed channel joins the pipe. The next session of this
	// task is rejected before it opens a channel, naming the other task and both
	// remedies, and appending nothing.
	require.NoError(t, os.WriteFile(os.Getenv("FAKE_SIDECAR_STATE"),
		fmt.Appendf(nil, `{"committed":{%q:"1",%q:"3"},"errors":{}}`, unattributable, foreignChannel), 0o644))
	var rejected = newSession(t, "test/task")
	err = testWriteRow(ctx, rejected, 0, []any{"k", "v"})
	require.ErrorContains(t, err, sanitizeAndAppendHash("other/task"))
	require.ErrorContains(t, err, foreignChannel)
	require.ErrorContains(t, err, "always_drop_tables_on_backfill")
	require.NotContains(t, err.Error(), unattributable)
	require.Empty(t, rejected.bindings[0].channels)

	names, err := fakeListChannels(ctx, "", "", "")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{unattributable, foreignChannel}, names)

	// The other task itself is not rejected by its own channel.
	var owner = newSession(t, "other/task")
	require.NoError(t, testWriteRow(ctx, owner, 0, []any{"k", "v"}))
}

// TestStreamV2SweepsBackfilledChannels covers, without credentials, the sweep of the
// channels a backfill leaves standing: a backfill truncates the table, so the channels
// this task derived under the state key it rotated away survive it. The shard whose
// range holds a stale channel's first key drops it, so a channel spanning the whole
// key space falls to the first shard alone, and a channel inside one shard's range
// falls to that shard.
func TestStreamV2SweepsBackfilledChannels(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))

	var full = streamV2Range{keyEnd: math.MaxUint32}
	var upper = streamV2Range{keyBegin: 0x80000000, keyEnd: math.MaxUint32}
	var lower = streamV2Range{keyEnd: 0x7fffffff}
	var staleFull = streamV2FormatChannelName("test/task", 0, full, "topology.v0")
	var staleUpper = streamV2FormatChannelName("test/task", 3, upper, "topology.v0")

	require.NoError(t, os.WriteFile(os.Getenv("FAKE_SIDECAR_STATE"),
		fmt.Appendf(nil, `{"committed":{%q:"5",%q:"7"},"errors":{}}`, staleFull, staleUpper), 0o644))

	// The upper shard of the backfill drops the stale channel inside its range, and
	// leaves the one whose first key lies in the lower shard's range.
	var hi = newTopologyManager(t, "test/task", upper.keyBegin, upper.keyEnd, nil)
	storeKeys(t, hi, keysHashingTo(upper, 1, "hi"))
	_, err := hi.flush(ctx)
	require.NoError(t, err)
	hi.stop()

	names, err := fakeListChannels(ctx, "", "", "")
	require.NoError(t, err)
	require.Contains(t, names, staleFull)
	require.NotContains(t, names, staleUpper)
	require.Len(t, names, 2)

	// The lower shard drops the channel spanning the key space.
	var lo = newTopologyManager(t, "test/task", lower.keyBegin, lower.keyEnd, nil)
	storeKeys(t, lo, keysHashingTo(lower, 1, "lo"))
	_, err = lo.flush(ctx)
	require.NoError(t, err)

	names, err = fakeListChannels(ctx, "", "", "")
	require.NoError(t, err)
	require.NotContains(t, names, staleFull)
	require.Len(t, names, 2)
}

func TestReconcileStreamV2Channel(t *testing.T) {
	var token = func(s string) *string { return &s }

	// The channel under reconciliation covers one quarter of the key space.
	var r = streamV2Range{keyBegin: 0x10000000, keyEnd: 0x1fffffff}
	var sv2ChannelCheckpointItem = func(counter int64) *streamV2ChannelCheckpointItem {
		return &streamV2ChannelCheckpointItem{ChannelName: "chan", Routed: counter}
	}

	for _, tt := range []struct {
		name                     string
		committed                *string
		sv2ChannelCheckpointItem *streamV2ChannelCheckpointItem
		priorItems               int
		wantCommitted            int64
		wantErr                  string
	}{
		{
			name:      "nothing committed and nothing checkpointed",
			committed: nil,
		},
		{
			// The channel Snowflake held those documents on is gone, and with it
			// the token which says which of them it holds. A backfill of the
			// binding is what does that — it re-creates the table, taking every
			// channel bound to it — so this is what a shard of the last
			// specification finds when it restarts into a backfill. The one drop this
			// connector makes itself is recognized before reconciliation, by the
			// declaration in the checkpoint, so it never reaches here.
			name:                     "nothing committed with a checkpointed routed offset is rejected",
			committed:                nil,
			sv2ChannelCheckpointItem: sv2ChannelCheckpointItem(42),
			priorItems:               1,
			wantErr:                  "reports no committed offset token while this task's checkpoint records",
		},
		{
			name:                     "clean boundary",
			committed:                token("42@10000000-1fffffff"),
			sv2ChannelCheckpointItem: sv2ChannelCheckpointItem(42),
			priorItems:               1,
			wantCommitted:            42,
		},
		{
			// An interrupted attempt of the transaction now replayed. The channel's
			// contents are a function of the data, so the replay routes the same
			// documents here and skips them by offset.
			name:                     "committed ahead of the checkpoint is skipped",
			committed:                token("50@10000000-1fffffff"),
			sv2ChannelCheckpointItem: sv2ChannelCheckpointItem(42),
			priorItems:               1,
			wantCommitted:            50,
		},
		{
			// An interruption before this channel's first checkpoint, with the
			// checkpoint holding nothing for the binding at all: the token is this
			// channel's own interrupted first transaction, and the documents it
			// counts are the ones about to be replayed.
			name:          "committed ahead with an empty checkpoint is skipped",
			committed:     token("50@10000000-1fffffff"),
			wantCommitted: 50,
		},
		{
			// The binding holds state, so its channels' items are maintained by
			// every flush and its target channels are declared before anything
			// routes to them. A token no item accounts for is something else
			// appending under this binding's names.
			name:       "committed ahead with no item beside a sibling's is rejected",
			committed:  token("50@10000000-1fffffff"),
			priorItems: 1,
			wantErr:    "does not account for",
		},
		{
			// The token's range must be the channel's own key range whatever the
			// offsets say: the key range is in the channel's name, so every token
			// this write path appends under it carries that key range. This guard is
			// unconditional where the old shard-range guard applied only beyond the
			// routed offset — a channel and its token can no longer drift apart by a
			// topology change, so a mismatch is always foreign.
			name:                     "a token for another key range is rejected at a clean offset",
			committed:                token("42@10000000-2fffffff"),
			sv2ChannelCheckpointItem: sv2ChannelCheckpointItem(42),
			priorItems:               1,
			wantErr:                  "was not written against this channel's key range",
		},
		{
			name:                     "a token for another key range is rejected ahead of the offset",
			committed:                token("50@00000000-ffffffff"),
			sv2ChannelCheckpointItem: sv2ChannelCheckpointItem(42),
			priorItems:               1,
			wantErr:                  "was not written against this channel's key range",
		},
		{
			name:      "a token for another key range with an empty checkpoint is rejected",
			committed: token("50@00000000-ffffffff"),
			wantErr:   "was not written against this channel's key range",
		},
		{
			name:                     "committed behind the checkpoint is rejected",
			committed:                token("41@10000000-1fffffff"),
			sv2ChannelCheckpointItem: sv2ChannelCheckpointItem(42),
			priorItems:               1,
			wantErr:                  "has lost committed data",
		},
		{
			name:                     "a token this connector could not have written is rejected",
			committed:                token("basetok0000000001:7"),
			sv2ChannelCheckpointItem: sv2ChannelCheckpointItem(42),
			priorItems:               1,
			wantErr:                  "which carries no offset",
		},
		{
			name:                     "a token whose range this connector could not have written is rejected",
			committed:                token("50@10000000+1fffffff"),
			sv2ChannelCheckpointItem: sv2ChannelCheckpointItem(42),
			priorItems:               1,
			wantErr:                  "which carries no offset",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			committed, err := reconcileStreamV2Channel("chan", "WIDGETS", tt.committed, tt.sv2ChannelCheckpointItem, r, tt.priorItems)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantCommitted, committed)
		})
	}
}
