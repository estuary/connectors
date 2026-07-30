package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
		m.addBinding("DB", "SCH", "TBL", sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
			Identifier: "TBL",
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}, {Identifier: `REJECT`}},
			StateKey:   "rejected.v1",
		}, prior)
		return m
	}

	t.Run("a rejected row fails the commit", func(t *testing.T) {
		var m = newManager(t, nil)
		require.NoError(t, m.writeRow(ctx, 0, []any{"kept", "v", nil}))
		require.NoError(t, m.writeRow(ctx, 0, []any{"dropped", "v", true}))

		items, err := m.flush(ctx)
		require.NoError(t, err)

		err = m.waitCommit(ctx, items[0])
		require.ErrorContains(t, err, "1 row(s) rejected")
		require.ErrorContains(t, err, m.bindings[0].channel)
		require.ErrorContains(t, err, "fake rejection")
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

func TestReconcileStreamV2Channel(t *testing.T) {
	var token = func(s string) *string { return &s }
	var prior = func(counter int64, keyBegin, keyEnd uint32) *streamV2Item {
		return &streamV2Item{Channel: "chan", Counter: counter, KeyBegin: keyBegin, KeyEnd: keyEnd}
	}

	const keyBegin, keyEnd uint32 = 0x10000000, 0x1fffffff

	for _, tt := range []struct {
		name      string
		committed *string
		prior     *streamV2Item
		wantSkip  int64
		wantErr   string
	}{
		{
			name:      "nothing committed and nothing checkpointed",
			committed: nil,
			prior:     nil,
		},
		{
			name:      "nothing committed with a checkpointed counter",
			committed: nil,
			prior:     prior(42, keyBegin, keyEnd),
		},
		{
			name:      "clean boundary",
			committed: token("42"),
			prior:     prior(42, keyBegin, keyEnd),
			wantSkip:  42,
		},
		{
			name:      "committed ahead of the checkpoint is skipped",
			committed: token("50"),
			prior:     prior(42, keyBegin, keyEnd),
			wantSkip:  50,
		},
		{
			// An interruption before this channel generation's first checkpoint:
			// there is no recorded range to contradict, and the documents
			// Snowflake holds are still the ones about to be replayed.
			name:      "committed ahead with nothing checkpointed is skipped",
			committed: token("50"),
			prior:     nil,
			wantSkip:  50,
		},
		{
			name:      "committed ahead after a key-begin change is refused",
			committed: token("50"),
			prior:     prior(42, keyBegin-1, keyEnd),
			wantErr:   "shard key range has changed",
		},
		{
			name:      "committed ahead after a key-end change is refused",
			committed: token("50"),
			prior:     prior(42, keyBegin, keyEnd+1),
			wantErr:   "shard key range has changed",
		},
		{
			// The guard is consulted only when Snowflake is ahead, so an
			// unrelated range change in steady state is not a problem.
			name:      "clean boundary after a key range change is accepted",
			committed: token("42"),
			prior:     prior(42, 0, math.MaxUint32),
			wantSkip:  42,
		},
		{
			name:      "committed behind the checkpoint is refused",
			committed: token("41"),
			prior:     prior(42, keyBegin, keyEnd),
			wantErr:   "has lost committed data",
		},
		{
			name:      "a token this connector could not have written is refused",
			committed: token("basetok0000000001:7"),
			prior:     prior(42, keyBegin, keyEnd),
			wantErr:   "which is not a document count",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			skip, err := reconcileStreamV2Channel("chan", tt.committed, tt.prior, keyBegin, keyEnd)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantSkip, skip)
		})
	}
}
