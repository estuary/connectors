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
