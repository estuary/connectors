package connector

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/rsa"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	"resty.dev/v3"
)

// TestStreamV2CheckpointDoesNotSurviveAnotherWritePath establishes what moving a
// binding off the streaming v2 write path costs it, which is the whole reason
// the move sweeps the binding's channels.
//
// Every other write path treats a state key's checkpoint item as pending work:
// Acknowledge applies it and then patches the key to null, which takes the whole
// item. The streaming v2 item is not pending work but durable per-binding
// state, and it goes with it — while Snowflake keeps the channel and the
// documents it has committed, neither of which any checkpoint is left to
// account for.
func TestStreamV2CheckpointDoesNotSurviveAnotherWritePath(t *testing.T) {
	const stateKey, channel = "switch.v1", "task_00000000_switch_v1"

	var state, err = json.Marshal(checkpoint{stateKey: &checkpointItem{
		Table:    "TBL",
		StreamV2: streamV2Checkpoint{fullKeyRange: {ChannelName: channel, Routed: 3}},
	}})
	require.NoError(t, err)

	// A transaction on another write path records that path's pending work for
	// the state key. StreamV2 is omitted rather than nulled, so the reduce
	// leaves the routed offset standing: the item survives the switch itself.
	stored, err := json.Marshal(checkpoint{stateKey: &checkpointItem{
		Table:       "TBL",
		StreamBlobs: []*blobMetadata{{Path: "blob"}},
	}})
	require.NoError(t, err)

	state, err = jsonpatch.MergePatch(state, stored)
	require.NoError(t, err)
	require.Contains(t, string(state), channel)

	// Acknowledging that transaction is what takes it. The state key is drained,
	// and a drained key is patched to null, which removes the routed offset along with
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

	var newSession = func(t *testing.T, prior streamV2Checkpoint) *streamV2Manager {
		var m = newFakeStreamV2Manager(t, ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/roundTrip",
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		m.addBinding("DB", "SCH", "TBL", target, prior)
		return m
	}

	singleChannelLayout(t)

	var before = newSession(t, nil)
	for i := range 3 {
		require.NoError(t, testWriteRow(ctx, before, 0, []any{"k", i}))
	}
	entries, err := before.flush(ctx)
	require.NoError(t, err)
	var channel = before.bindings[0].activeChannels[0].channelName
	require.Equal(t, int64(3), entries[0][fullKeyRange].Routed)
	before.stop()

	// The task materializes through another write path for a while, which
	// clears this binding's checkpoint item, and is then moved back. Three
	// documents Snowflake never had are dropped.
	var after = newSession(t, nil)
	require.NoError(t, testWriteRow(ctx, after, 0, []any{"k", "new"}))
	require.Equal(t, channel, after.bindings[0].activeChannels[0].channelName)
	require.Equal(t, int64(3), after.bindings[0].activeChannels[0].progress.committed)
}

// openChannelServer answers every "POST /channels/open" the way status tells
// it to, and every other route with a bare success — enough for the stream
// manager's addBinding to reach a verdict.
func openChannelServer(t *testing.T, status int) *streamManager {
	t.Helper()
	var mux = http.NewServeMux()
	mux.HandleFunc("POST /v1/streaming/channels/open", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"message":"Success","status_code":%d,"table_columns":[]}`, status)
	})
	var ts = httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	pkey, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	var role = "TEST_ROLE"

	return &streamManager{
		c: &streamClient{
			r:        resty.New().SetBaseURL(ts.URL + "/v1/streaming").SetDisableWarn(true),
			key:      pkey,
			user:     "TEST_USER",
			database: "TEST_DB",
			account:  "TEST_ACCOUNT",
			role:     &role,
		},
		tableStreams: map[int]*tableStream{},
		channelName:  "x",
		lastBinding:  -1,
		blobStats:    map[int][]*blobStatsTracker{},
		counter:      -1,
	}
}

// TestStreamV2LeavingTheWritePathSweepsItsChannels covers the switch itself, which
// is where the channels have to go: a binding that leaves this write path with its
// channels standing is the binding the two tests above describe.
func TestStreamV2LeavingTheWritePathSweepsItsChannels(t *testing.T) {
	var ctx = context.Background()
	const task = "test/switch"
	singleChannelLayout(t)

	var target = func(stateKey string, delta bool) sql.Table {
		return sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: delta, Path: []string{"DB", "SCH", "TBL"}},
			Identifier: "TBL",
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}},
			StateKey:   stateKey,
		}
	}

	// seed runs one streaming v2 session of a binding through the fake sidecar,
	// whose state file then holds the committed channel the way Snowflake would,
	// and returns the checkpoint that session flushed.
	var seed = func(t *testing.T, stateKey string) (streamV2Checkpoint, string) {
		t.Helper()
		var m = newFakeStreamV2Manager(t, ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, task,
			&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
		m.addBinding("DB", "SCH", "TBL", target(stateKey, true), nil)
		for i := range 3 {
			require.NoError(t, testWriteRow(ctx, m, 0, []any{"k", i}))
		}
		entries, err := m.flush(ctx)
		require.NoError(t, err)
		var channel = m.bindings[0].activeChannels[0].channelName
		m.stop()
		require.Contains(t, fakeCommittedTokens(t, os.Getenv("FAKE_SIDECAR_STATE")), channel)
		return entries[0], channel
	}

	var newTransactor = func(t *testing.T, stateKey string, prior streamV2Checkpoint) *transactor {
		var cfg = config{
			Database:    "DB",
			Schema:      "SCH",
			Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.JWT},
		}
		var d = &transactor{
			cfg:                 cfg,
			ep:                  &sql.Endpoint[config]{Dialect: snowflakeDialect("SCH", timestampTypeLTZ, nil)},
			_range:              &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
			version:             "v1",
			cp:                  checkpoint{stateKey: &checkpointItem{StreamV2: prior}},
			snowpipeStreamingV2: newFakeStreamV2Manager(t, ctx, &cfg, task, &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}),
		}
		return d
	}

	var lastBinding = func(d *transactor) *binding {
		return d.bindings[len(d.bindings)-1]
	}

	t.Run("a binding which has never streamed v2 is added as usual", func(t *testing.T) {
		t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))
		var d = newTransactor(t, "fresh.v1", nil)
		require.NoError(t, d.addBinding(ctx, target("fresh.v1", false), *d.cp["fresh.v1"]))
	})

	t.Run("staying on the write path keeps the channel", func(t *testing.T) {
		t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))
		const stateKey = "stay.v1"
		var prior, channel = seed(t, stateKey)
		var d = newTransactor(t, stateKey, prior)
		d.cfg.Advanced.FeatureFlags = "snowpipe_streaming_v2"

		require.NoError(t, d.addBinding(ctx, target(stateKey, true), *d.cp[stateKey]))
		require.True(t, lastBinding(d).streamingV2)
		require.Contains(t, fakeCommittedTokens(t, os.Getenv("FAKE_SIDECAR_STATE")), channel)
	})

	t.Run("turning the feature flag off downgrades to snowpipe_streaming and sweeps the channel", func(t *testing.T) {
		t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))
		const stateKey = "flag.v1"
		var prior, channel = seed(t, stateKey)
		var d = newTransactor(t, stateKey, prior)
		// snowpipe_streaming is enabled by default, so dropping the v2 flag alone
		// is the downgrade.
		d.cfg.Advanced.FeatureFlags = ""
		d.snowpipeStreaming = openChannelServer(t, 0)

		require.NoError(t, d.addBinding(ctx, target(stateKey, true), *d.cp[stateKey]))
		require.True(t, lastBinding(d).streaming)
		require.NotContains(t, fakeCommittedTokens(t, os.Getenv("FAKE_SIDECAR_STATE")), channel)
	})

	t.Run("a table the snowpipe_streaming path cannot open falls back to staged files after the sweep", func(t *testing.T) {
		t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))
		const stateKey = "fallback.v1"
		var prior, channel = seed(t, stateKey)
		var d = newTransactor(t, stateKey, prior)
		d.cfg.Advanced.FeatureFlags = "snowpipe_streaming"
		d.snowpipeStreaming = openChannelServer(t, 6)

		require.NoError(t, d.addBinding(ctx, target(stateKey, true), *d.cp[stateKey]))
		require.NotContains(t, fakeCommittedTokens(t, os.Getenv("FAKE_SIDECAR_STATE")), channel)
	})

	t.Run("moving the binding to standard updates sweeps the channel", func(t *testing.T) {
		t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))
		const stateKey = "delta.v1"
		var prior, channel = seed(t, stateKey)
		var d = newTransactor(t, stateKey, prior)
		d.cfg.Advanced.FeatureFlags = "snowpipe_streaming_v2"

		require.NoError(t, d.addBinding(ctx, target(stateKey, false), *d.cp[stateKey]))
		require.False(t, lastBinding(d).streaming)
		require.False(t, lastBinding(d).streamingV2)
		require.NotContains(t, fakeCommittedTokens(t, os.Getenv("FAKE_SIDECAR_STATE")), channel)
	})

	t.Run("a binding which leaves and returns starts its channel afresh", func(t *testing.T) {
		t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))
		const stateKey = "roundtrip.v1"
		var prior, channel = seed(t, stateKey)

		var away = newTransactor(t, stateKey, prior)
		away.cfg.Advanced.FeatureFlags = "snowpipe_streaming_v2"
		require.NoError(t, away.addBinding(ctx, target(stateKey, false), *away.cp[stateKey]))

		// The return derives the same channel name and finds nothing under it, so
		// the documents it is about to materialize are not skipped: compare
		// TestStreamV2ReturningToTheWritePathSkipsNewDocuments.
		var back = newTransactor(t, stateKey, nil)
		back.cfg.Advanced.FeatureFlags = "snowpipe_streaming_v2"
		require.NoError(t, back.addBinding(ctx, target(stateKey, true), *back.cp[stateKey]))
		require.NoError(t, testWriteRow(ctx, back.snowpipeStreamingV2, 0, []any{"k", "new"}))
		var c = back.snowpipeStreamingV2.bindings[0].activeChannels[0]
		require.Equal(t, channel, c.channelName)
		require.Zero(t, c.progress.committed)
	})

	t.Run("channels the task has already dropped need no sweep", func(t *testing.T) {
		t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))
		// A nil item is the deletion of a channel this task dropped, which the
		// runtime has not yet reduced away. It records nothing.
		const stateKey = "dropped.v1"
		var d = newTransactor(t, stateKey, streamV2Checkpoint{streamV2Range{keyBegin: 0x80000000, keyEnd: math.MaxUint32}: nil})

		require.NoError(t, d.addBinding(ctx, target(stateKey, false), *d.cp[stateKey]))
	})
}

// TestStreamV2WritePathSwitch drives both write paths against live Snowflake
// and one table, which is the only place the account they each keep of that
// table can be compared.
func TestStreamV2WritePathSwitch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	singleChannelLayout(t)

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

	// The snowpipe_streaming manager of the same task, which the streaming v2
	// managers below drop that path's channel through.
	sm, err := newStreamManager(&cfg, testMaterialization, accountName, 0)
	require.NoError(t, err)

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

	var newV2 = func(t *testing.T, tgt sql.Table, prior streamV2Checkpoint) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &cfg, db, testDialect, sm, testMaterialization, accountName, fullRange)
		t.Cleanup(m.stop)
		m.addBinding(cfg.Database, cfg.Schema, tableName, tgt, prior)
		return m
	}

	var storeV2 = func(t *testing.T, m *streamV2Manager, lo, hi int) {
		for i := lo; i < hi; i++ {
			require.NoError(t, testWriteRow(ctx, m, 0, []any{fmt.Sprintf("key-%d", i), i}))
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
		var c = m.bindings[0].activeChannels[0]
		require.Zero(t, c.progress.committed)

		entries, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(2), entries[0][c.keyRange].Routed)
		require.Equal(t, 5, countRows())
	})

	t.Run("a binding moved onto this write path drops the snowpipe_streaming channel", func(t *testing.T) {
		truncate(t)
		storeV1(t, "switch-drop-1", 0, 3)
		require.Equal(t, 3, countRows())

		// The snowpipe_streaming path holds one deterministic channel per shard,
		// with the token of everything it committed. Dropping it leaves Snowflake
		// with nothing under that name, so a later open starts from no token.
		sm, err := newStreamManager(&cfg, testMaterialization, accountName, 0)
		require.NoError(t, err)
		require.NoError(t, sm.dropChannel(ctx, cfg.Schema, tableName, sm.channelName))

		// Dropping what is already gone is not an error.
		require.NoError(t, sm.dropChannel(ctx, cfg.Schema, tableName, sm.channelName))

		reopened, err := sm.c.openChannel(ctx, cfg.Schema, tableName, sm.channelName)
		require.NoError(t, err)
		require.Nil(t, reopened.OffsetToken)
		require.Equal(t, 3, countRows())
	})

	t.Run("a binding moved off this write path drops its channel", func(t *testing.T) {
		truncate(t)

		var tgt = target("off.v1")
		var m = newV2(t, tgt, nil)
		storeV2(t, m, 0, 3)
		var c = m.bindings[0].activeChannels[0]
		entries, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, countRows())
		m.stop()

		// The manager alone drops nothing of its own, so Snowflake goes on
		// reporting the committed offset token to whoever opens the channel next.
		var later = newV2(t, tgt, nil)
		client, err := later.ensureStarted(ctx)
		require.NoError(t, err)
		status, err := client.OpenChannel(ctx, cfg.Database, cfg.Schema, tableName, c.channelName)
		require.NoError(t, err)
		require.Equal(t, c.offsetToken(3), status.committedToken())
		later.stop()

		// The transactor is what moves a binding off the path, and it sweeps the
		// channel as it does, so a return to the path starts from no token.
		// The transactor locates the table from the resource path, which names the
		// schema and table only.
		var off = tgt
		off.DeltaUpdates = false
		off.Path = []string{cfg.Schema, tableName}
		var d = &transactor{
			cfg:                 cfg,
			ep:                  &sql.Endpoint[config]{Dialect: testDialect},
			_range:              fullRange,
			version:             "v1",
			cp:                  checkpoint{tgt.StateKey: &checkpointItem{StreamV2: entries[0]}},
			snowpipeStreamingV2: newStreamV2Manager(ctx, &cfg, db, testDialect, sm, testMaterialization, accountName, fullRange),
		}
		t.Cleanup(d.snowpipeStreamingV2.stop)
		require.NoError(t, d.addBinding(ctx, off, *d.cp[tgt.StateKey]))

		var back = newV2(t, tgt, nil)
		client, err = back.ensureStarted(ctx)
		require.NoError(t, err)
		status, err = client.OpenChannel(ctx, cfg.Database, cfg.Schema, tableName, c.channelName)
		require.NoError(t, err)
		require.Nil(t, status.committedToken())
	})

	t.Run("a binding moved off this write path with rows pending duplicates them", func(t *testing.T) {
		truncate(t)

		// Cutting a batch per document is what leaves documents pending without
		// a flush, which is the state an interruption has to find.
		var restore = streamV2BatchRows
		t.Cleanup(func() { streamV2BatchRows = restore })
		streamV2BatchRows = 1

		// An interrupted transaction: Snowflake commits three documents and the
		// session dies before any checkpoint records them.
		var tgt = target("pending.v1")
		var m = newV2(t, tgt, nil)
		storeV2(t, m, 0, 3)
		var c = m.bindings[0].activeChannels[0]
		require.NoError(t, c.wait())
		_, err := m.client.WaitCommit(ctx, c.channelName, c.offsetToken(3))
		require.NoError(t, err)
		require.Equal(t, 3, countRows())
		m.sup.kill()

		// The runtime replays those same three documents. This path skips them,
		// because Snowflake's committed offset token says it holds them.
		var replay = newV2(t, tgt, nil)
		storeV2(t, replay, 0, 3)
		require.Equal(t, int64(3), replay.bindings[0].activeChannels[0].progress.committed)
		_, err = replay.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, countRows())
		replay.stop()

		// Had the operator escaped the interruption by moving the binding off
		// this path instead, the same replay would have been materialized again:
		// no other path reads that token, and this one is delta-updates, so the
		// duplicates are permanent.
		truncate(t)
		storeV1(t, "switch-pending-1", 0, 3)
		storeV1(t, "switch-pending-2", 0, 3)
		require.Equal(t, 6, countRows())
	})
}

// TestStreamV2SwitchOntoTheWritePathWithPendingWorkIsRejected covers the other
// direction of a write path switch: a binding which moves onto streaming v2 while
// its checkpoint still holds work that another path staged and did not finish.
//
// Acknowledge drains such an item through the manager of the path which staged it.
// Only Snowpipe Streaming blobs need this binding registered with that manager: a
// staged-file query runs on the shared connection, and pipe files go through the
// shared pipe client, so both drain on a streaming v2 session as they stand. Blobs
// need the bdec manager to hold a channel for the table, which the binding arranges
// for the drain even though its rows now go to streaming v2. Where that cannot be
// arranged, the binding is rejected rather than left to fail every transaction.
func TestStreamV2SwitchOntoTheWritePathWithPendingWorkIsRejected(t *testing.T) {
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
			Advanced:    advancedConfig{FeatureFlags: "snowpipe_streaming_v2"},
		}
		var rng = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}
		var d = &transactor{
			cfg:                 cfg,
			ep:                  &sql.Endpoint[config]{Dialect: snowflakeDialect("SCH", timestampTypeLTZ, nil)},
			_range:              rng,
			version:             "v1",
			cp:                  checkpoint{target.StateKey: item},
			snowpipeStreamingV2: newFakeStreamV2Manager(t, ctx, &cfg, "test/onto", rng),
		}
		return d
	}

	// The configuration enables the streaming v2 flag throughout, which is what
	// routes the binding to the streaming v2 manager.
	var addBinding = func(d *transactor) error {
		return d.addBinding(ctx, target, *cmp.Or(d.cp[target.StateKey], &checkpointItem{}))
	}

	t.Run("pending blobs whose channel cannot be opened are rejected", func(t *testing.T) {
		var d = newTransactor(t, &checkpointItem{
			Table:       "TBL",
			StreamBlobs: []*blobMetadata{{Path: "one.bdec"}, {Path: "two.bdec"}},
		})
		d.snowpipeStreaming = openChannelServer(t, 6)

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
		var d = newTransactor(t, &checkpointItem{StreamV2: streamV2Checkpoint{
			fullKeyRange: {ChannelName: "task_00000000_sk_v1", Routed: 7},
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

// TestStreamV2SweepDropsTheStreamingChannel covers the sweep of the snowpipe_streaming
// channel on a table whose binding moved to streaming v2: it stands while that path
// holds it open, and is dropped once it does not.
func TestStreamV2SweepDropsTheStreamingChannel(t *testing.T) {
	var ctx = context.Background()
	t.Setenv("FAKE_SIDECAR_STATE", filepath.Join(t.TempDir(), "channels.json"))

	// The lower shard of two. Snowflake lists the names upper-cased. Only the first
	// channel is this shard's to drop.
	sm, drops := fakeStreamManager(t, "test/onto")
	var m = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, fakeSnowflakeDB(t), testDialect, sm, "test/onto", "acct",
		&pf.RangeSpec{KeyEnd: 0x7fffffff, RClockEnd: math.MaxUint32})
	var listed = strings.ToUpper(sm.channelName)
	require.NoError(t, os.WriteFile(os.Getenv("FAKE_SIDECAR_STATE"), fmt.Appendf(nil, `{"committed":{%q:"1",%q:"1",%q:"1"},"errors":{}}`,
		listed, strings.ToUpper(newChannelName("test/onto", 0x80000000)), strings.ToUpper(newChannelName("other/task", 0))), 0o644))

	// Held open, to register blobs through: it stands.
	sm.tableStreams[0] = &tableStream{channel: &channel{ChannelName: sm.channelName}}
	require.NoError(t, m.sweep(ctx, "DB", "SCH", "TBL", "sk.v1", nil))
	require.Empty(t, drops())

	// Let go of: it is dropped, and nothing else is.
	delete(sm.tableStreams, 0)
	require.NoError(t, m.sweep(ctx, "DB", "SCH", "TBL", "sk.v1", nil))
	require.Equal(t, []string{listed}, drops())
}
