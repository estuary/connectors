package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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
// it, and goes on refusing the channel it was appended to.
func TestStreamV2RejectedRows(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)

	// The single channel of the binding under a one-channel layout.
	var channel = streamV2ChannelName("test/rejectedRows",
		streamV2Range{keyBegin: 0, keyEnd: math.MaxUint32}, "rejected.v1")

	var newManager = func(t *testing.T, prior *streamV2Item) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/rejectedRows", "acct",
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		m.argv = fakeSidecarArgv(t)
		t.Cleanup(m.stop)

		// The fake sidecar rejects a row which carries the REJECT column, as
		// Snowflake rejects one it cannot store: the append and the commit both
		// succeed, and only the channel's row-error count moves.
		var priorItems map[string]*streamV2Item
		if prior != nil {
			prior.Channel, prior.KeyEnd = channel, math.MaxUint32
			priorItems = map[string]*streamV2Item{"00000000-ffffffff": prior}
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
		// which is before the counter reaches the checkpoint.
		entries, err := m.flush(ctx)
		require.ErrorContains(t, err, "1 row(s) rejected")
		require.ErrorContains(t, err, channel)
		require.ErrorContains(t, err, "fake rejection")
		require.Empty(t, entries)
	})

	t.Run("a rejection already on the channel is refused when it opens", func(t *testing.T) {
		// The transaction that provoked a rejection fails before acknowledging,
		// so a restart replays it. Refusing as the channel opens — before the
		// replay can append, and whether or not this session appends at all — is
		// what stops that replay from acknowledging rows Snowflake does not hold.
		t.Setenv("FAKE_SIDECAR_ROWS_ERROR_COUNT", "3")
		var m = newManager(t, &streamV2Item{Counter: 2})

		require.ErrorContains(t, testWriteRow(ctx, m, 0, []any{"kept", "v", nil}), "3 row(s) rejected")
	})
}

// TestStreamV2ChannelRetirement covers, without credentials, what the live test
// establishes against Snowflake: a channel name outlives the shard it was named
// for, and only retiring the channel stops the next shard to derive that name
// from adopting its committed offset token as a skip threshold.
func TestStreamV2ChannelRetirement(t *testing.T) {
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
	for i := range 3 {
		require.NoError(t, testWriteRow(ctx, first, 0, []any{"k", i}))
	}
	var channel = first.bindings[0].channels[0].name
	entries, err := first.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), soleItem(t, entries, 0).Counter)
	first.stop()

	// Un-retired, that channel wedges the next shard to derive its name: the
	// documents it is about to store are taken for a replay of an interrupted
	// transaction it never ran, and skipped.
	var reused = newSession(t)
	require.NoError(t, testWriteRow(ctx, reused, 0, []any{"k", 0}))
	require.Equal(t, channel, reused.bindings[0].channels[0].name)
	require.Equal(t, int64(3), reused.bindings[0].channels[0].skip)
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
	require.NoError(t, testWriteRow(ctx, after, 0, []any{"k", 0}))
	require.Equal(t, channel, after.bindings[0].channels[0].name)
	require.Zero(t, after.bindings[0].channels[0].skip)

	entries, err = after.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), soleItem(t, entries, 0).Counter)
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

			var c = &client{cfg: tt.incoming, streamingV2Enabled: boilerplate.ParseFlags(tt.incoming)[flagSnowpipeStreamingV2]}
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
	var comment = streamV2EmbedGenerationInTableComment("Generated for materialization acmeCo/materialize-snowflake of collection acmeCo/widgets", gen)

	readBack, ok := streamV2GenerationFromTableComment(comment)
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
			_, ok := streamV2GenerationFromTableComment(tt.comment)
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
	var c = &client{
		cfg:                config{Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.JWT}},
		streamingV2Enabled: true,
	}

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
		return streamV2EmbedGenerationInTableComment("Generated for materialization "+gen.materialization, gen)
	}

	for _, tt := range []struct {
		name    string
		comment string
		wantErr bool
		// wantContains names what the error must say. It is empty for the conflict
		// between generations of one binding, which every such error reports the
		// same way.
		wantContains []string
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
			// other, and a backfill of either drops the table and every channel
			// appending to it. Neither task can account for what the other holds.
			name:         "the table records another task",
			comment:      commentOf(streamV2Generation{materialization: "acmeCo/other", stateKey: "widgets.v4"}),
			wantErr:      true,
			wantContains: []string{"WIDGETS", "acmeCo/other", task, "renamed"},
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
			var err = streamV2CheckGenerationConflict(tt.comment, mine, "WIDGETS")
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			if len(tt.wantContains) > 0 {
				for _, want := range tt.wantContains {
					require.ErrorContains(t, err, want)
				}
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

	// The channel under reconciliation covers one quarter of the key space.
	var r = streamV2Range{keyBegin: 0x10000000, keyEnd: 0x1fffffff}
	var item = func(counter int64) *streamV2Item {
		return &streamV2Item{Channel: "chan", Counter: counter, KeyBegin: r.keyBegin, KeyEnd: r.keyEnd}
	}

	for _, tt := range []struct {
		name       string
		committed  *string
		item       *streamV2Item
		priorItems int
		wantSkip   int64
		wantErr    string
	}{
		{
			name:      "nothing committed and nothing checkpointed",
			committed: nil,
		},
		{
			// The channel Snowflake held those documents on is gone, and with it
			// the token which says which of them it holds. A backfill of the
			// binding is what does that — it re-creates the table, taking every
			// channel bound to it — so this is what a shard of the outgoing
			// generation finds when it restarts into a turnover. The one drop this
			// connector makes itself is recognized before reconciliation, by the
			// declaration in the checkpoint, so it never reaches here.
			name:       "nothing committed with a checkpointed counter is refused",
			committed:  nil,
			item:       item(42),
			priorItems: 1,
			wantErr:    "has committed nothing while this task's checkpoint records",
		},
		{
			name:       "clean boundary",
			committed:  token("42@10000000-1fffffff"),
			item:       item(42),
			priorItems: 1,
			wantSkip:   42,
		},
		{
			// An interrupted attempt of the transaction now replayed. The channel's
			// contents are a function of the data, so the replay routes the same
			// documents here and skips them by position.
			name:       "committed ahead of the checkpoint is skipped",
			committed:  token("50@10000000-1fffffff"),
			item:       item(42),
			priorItems: 1,
			wantSkip:   50,
		},
		{
			// An interruption before this channel's first checkpoint, with the
			// checkpoint holding nothing for the binding at all: the token is this
			// channel's own interrupted first transaction, and the documents it
			// counts are the ones about to be replayed.
			name:      "committed ahead with an empty checkpoint is skipped",
			committed: token("50@10000000-1fffffff"),
			wantSkip:  50,
		},
		{
			// The binding holds state, so its channels' items are maintained by
			// every flush and its target channels are declared before anything
			// routes to them. A token no item accounts for is something else
			// appending under this binding's names.
			name:       "committed ahead with no item beside a sibling's is refused",
			committed:  token("50@10000000-1fffffff"),
			priorItems: 1,
			wantErr:    "cannot account for",
		},
		{
			// The token's range must be the channel's own subrange whatever the
			// counters say: the subrange is in the channel's name, so every token
			// this write path appends under it carries that subrange. This guard is
			// unconditional where the old shard-range guard applied only beyond the
			// counter — a channel and its token can no longer drift apart by a
			// topology change, so a mismatch is always foreign.
			name:       "a token for another subrange is refused at a clean count",
			committed:  token("42@10000000-2fffffff"),
			item:       item(42),
			priorItems: 1,
			wantErr:    "was not written against this channel's subrange",
		},
		{
			name:       "a token for another subrange is refused ahead of the count",
			committed:  token("50@00000000-ffffffff"),
			item:       item(42),
			priorItems: 1,
			wantErr:    "was not written against this channel's subrange",
		},
		{
			name:      "a token for another subrange with an empty checkpoint is refused",
			committed: token("50@00000000-ffffffff"),
			wantErr:   "was not written against this channel's subrange",
		},
		{
			name:       "committed behind the checkpoint is refused",
			committed:  token("41@10000000-1fffffff"),
			item:       item(42),
			priorItems: 1,
			wantErr:    "has lost committed data",
		},
		{
			name:       "a token this connector could not have written is refused",
			committed:  token("basetok0000000001:7"),
			item:       item(42),
			priorItems: 1,
			wantErr:    "which is not a document count",
		},
		{
			name:       "a token whose range this connector could not have written is refused",
			committed:  token("50@10000000+1fffffff"),
			item:       item(42),
			priorItems: 1,
			wantErr:    "which is not a document count",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			skip, err := reconcileStreamV2Channel("chan", "WIDGETS", tt.committed, tt.item, r, tt.priorItems)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantSkip, skip)
		})
	}
}
