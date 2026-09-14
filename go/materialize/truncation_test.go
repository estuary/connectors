package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestBackfillSignalsLoggedAndFlushedStillSent(t *testing.T) {
	var fixedTime = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	ts, err := types.TimestampProto(fixedTime)
	require.NoError(t, err)

	var txns = []txn{{
		flush: &pm.Request_Flush{
			BackfillBegins:    []*pm.Request_Flush_BackfillBegin{{Binding: 0, Timestamp: ts}},
			BackfillCompletes: []*pm.Request_Flush_BackfillComplete{{Binding: 0, Timestamp: ts}},
		},
	}}

	var tr = &scriptedTransactor{bindings: twoBindings}
	var open = openRequest(twoBindings, nil)
	var stream = &scriptedStream{requests: scriptRequests(txns)}
	var wantStateKey = open.Materialization.Bindings[0].StateKey

	var hook = logtest.NewGlobal()
	defer hook.Reset()

	var done = make(chan error, 1)
	go func() { done <- RunTransactions(context.Background(), tr, stream, open, log.InfoLevel) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("RunTransactions did not return")
	}

	// Flushed is sent between the recovery Acknowledged and the round's own
	// StartedCommit/Acknowledged, so the loop must tolerate both signals
	// without stalling the transaction.
	require.Len(t, stream.responses, 5)
	require.NotNil(t, stream.responses[0].Opened)
	require.NotNil(t, stream.responses[1].Acknowledged)
	require.NotNil(t, stream.responses[2].Flushed)
	require.NotNil(t, stream.responses[3].StartedCommit)
	require.NotNil(t, stream.responses[4].Acknowledged)

	var begins, completes int
	for _, e := range hook.AllEntries() {
		switch e.Message {
		case "backfill begins":
			begins++
			require.Equal(t, log.InfoLevel, e.Level)
			require.EqualValues(t, 0, e.Data["binding"])
			require.Equal(t, wantStateKey, e.Data["stateKey"])
			require.Equal(t, fixedTime, e.Data["timestamp"])
		case "backfill completes":
			completes++
			require.Equal(t, log.InfoLevel, e.Level)
			require.EqualValues(t, 0, e.Data["binding"])
			require.Equal(t, wantStateKey, e.Data["stateKey"])
			require.Equal(t, fixedTime, e.Data["timestamp"])
		}
	}
	require.Equal(t, 1, begins)
	require.Equal(t, 1, completes)
}

var oneBinding = []testBinding{{path: []string{"schema", "alpha"}}}

// runTruncationScenario drives RunTransactions over a scripted stream wired
// so the transactor's Truncate calls record how many responses had already
// been sent, and returns the stream for inspecting sent responses.
func runTruncationScenario(t *testing.T, tr *scriptedTransactor, open *pm.Request_Open, txns []txn) *scriptedStream {
	t.Helper()
	var stream = &scriptedStream{requests: scriptRequests(txns)}
	tr.stream = stream

	var done = make(chan error, 1)
	go func() { done <- RunTransactions(context.Background(), tr, stream, open, log.InfoLevel) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("RunTransactions did not return")
	}
	return stream
}

// decodeTruncations extracts the "__truncations" subtree from a
// ConnectorState update. A JSON null entry decodes to a nil pointer, so a
// clearing patch is distinguishable from an absent one.
func decodeTruncations(t *testing.T, state *pf.ConnectorState) (map[string]map[string]*string, bool) {
	t.Helper()
	require.NotNil(t, state)
	var top map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(state.UpdatedJson, &top))
	raw, ok := top["__truncations"]
	require.True(t, ok, "state update missing __truncations")
	var sub map[string]map[string]*string
	require.NoError(t, json.Unmarshal(raw, &sub))
	return sub, state.MergePatch
}

func strPtr(s string) *string { return &s }

func completeFlush(t *testing.T, binding int, at time.Time) *pm.Request_Flush {
	t.Helper()
	ts, err := types.TimestampProto(at)
	require.NoError(t, err)
	return &pm.Request_Flush{BackfillCompletes: []*pm.Request_Flush_BackfillComplete{{Binding: uint32(binding), Timestamp: ts}}}
}

func TestTruncateSingleShard(t *testing.T) {
	var fixedT = time.Date(2026, 9, 14, 21, 0, 0, 123456789, time.UTC)
	var wantT = fixedT.Format(time.RFC3339Nano)

	t.Run("truncates once, after StartedCommit and before Acknowledged", func(t *testing.T) {
		var open = openRequest(oneBinding, nil)
		var stateKey = open.Materialization.Bindings[0].StateKey
		var txns = []txn{{flush: completeFlush(t, 0, fixedT)}, {}, {}}

		var tr = &scriptedTransactor{bindings: oneBinding}
		var stream = runTruncationScenario(t, tr, open, txns)

		require.Len(t, tr.truncates, 1)
		var call = tr.truncates[0]
		require.Equal(t, 0, call.binding)
		require.True(t, call.before.Equal(fixedT))
		require.Equal(t, fixedT.UnixNano(), call.before.UnixNano())
		require.Equal(t, 4, call.responses, "must run after StartedCommit-0 (idx3) and before Acknowledged-0 (idx4)")

		require.Len(t, stream.responses, 11)
		require.NotNil(t, stream.responses[3].StartedCommit, "response 3 is StartedCommit-0")
		sub, merge := decodeTruncations(t, stream.responses[3].StartedCommit.State)
		require.True(t, merge)
		require.Equal(t, map[string]map[string]*string{
			"00000000-ffffffff": {stateKey: strPtr(wantT)},
		}, sub)

		require.NotNil(t, stream.responses[4].Acknowledged, "response 4 is Acknowledged-0")
		sub, merge = decodeTruncations(t, stream.responses[4].Acknowledged.State)
		require.True(t, merge)
		require.Equal(t, map[string]map[string]*string{
			"00000000-ffffffff": {stateKey: nil},
		}, sub)
	})

	t.Run("no complete scripted, no truncate", func(t *testing.T) {
		var open = openRequest(oneBinding, nil)
		var txns = []txn{{}, {}, {}}

		var tr = &scriptedTransactor{bindings: oneBinding}
		runTruncationScenario(t, tr, open, txns)

		require.Empty(t, tr.truncates)
	})
}

func TestTruncateWaitsForPeers(t *testing.T) {
	var fixedT = time.Date(2026, 9, 14, 21, 0, 0, 123456789, time.UTC)
	var wantT = fixedT.Format(time.RFC3339Nano)

	t.Run("primary waits then truncates once peers agree", func(t *testing.T) {
		var rng = &pf.RangeSpec{KeyBegin: 0, KeyEnd: 0x7fffffff, RClockEnd: math.MaxUint32}
		var open = openRequest(oneBinding, rng)
		var stateKey = open.Materialization.Bindings[0].StateKey
		var peerPatch = json.RawMessage(fmt.Sprintf(`[{"__truncations":{"80000000-ffffffff":{%q:%q}}}]`, stateKey, wantT))
		var txns = []txn{
			{flush: completeFlush(t, 0, fixedT)},
			{},
			{},
			{ack: &pm.Request_Acknowledge{StatePatchesJson: peerPatch}},
		}

		var tr = &scriptedTransactor{bindings: oneBinding}
		var stream = runTruncationScenario(t, tr, open, txns)

		require.Len(t, stream.responses, 14)
		require.Len(t, tr.truncates, 1)
		var call = tr.truncates[0]
		require.Equal(t, 0, call.binding)
		require.True(t, call.before.Equal(fixedT))
		require.Equal(t, 10, call.responses, "must run only after transaction 3's Acknowledge (idx10) was read")
	})

	t.Run("peer disagrees on T, never truncates", func(t *testing.T) {
		var rng = &pf.RangeSpec{KeyBegin: 0, KeyEnd: 0x7fffffff, RClockEnd: math.MaxUint32}
		var open = openRequest(oneBinding, rng)
		var stateKey = open.Materialization.Bindings[0].StateKey
		var otherT = fixedT.Add(time.Second).Format(time.RFC3339Nano)
		var peerPatch = json.RawMessage(fmt.Sprintf(`[{"__truncations":{"80000000-ffffffff":{%q:%q}}}]`, stateKey, otherT))
		var txns = []txn{
			{flush: completeFlush(t, 0, fixedT)},
			{},
			{},
			{ack: &pm.Request_Acknowledge{StatePatchesJson: peerPatch}},
		}

		var tr = &scriptedTransactor{bindings: oneBinding}
		runTruncationScenario(t, tr, open, txns)

		require.Empty(t, tr.truncates)
	})

	t.Run("non-zero shard never truncates but still records and emits", func(t *testing.T) {
		var rng = &pf.RangeSpec{KeyBegin: 0x80000000, KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}
		var open = openRequest(oneBinding, rng)
		var stateKey = open.Materialization.Bindings[0].StateKey
		var peerPatch = json.RawMessage(fmt.Sprintf(`[{"__truncations":{"00000000-7fffffff":{%q:%q}}}]`, stateKey, wantT))
		var txns = []txn{
			{flush: completeFlush(t, 0, fixedT)},
			{},
			{},
			{ack: &pm.Request_Acknowledge{StatePatchesJson: peerPatch}},
		}

		var tr = &scriptedTransactor{bindings: oneBinding}
		var stream = runTruncationScenario(t, tr, open, txns)

		require.Empty(t, tr.truncates)

		require.NotNil(t, stream.responses[3].StartedCommit, "response 3 is StartedCommit-0")
		sub, merge := decodeTruncations(t, stream.responses[3].StartedCommit.State)
		require.True(t, merge)
		require.Equal(t, map[string]map[string]*string{
			"80000000-ffffffff": {stateKey: strPtr(wantT)},
		}, sub)
	})
}

func TestTruncateRecoversPendingRecord(t *testing.T) {
	var fixedT = time.Date(2026, 9, 14, 21, 0, 0, 123456789, time.UTC)
	var wantT = fixedT.Format(time.RFC3339Nano)

	t.Run("full agreement recovers and truncates before the recovery Acknowledged", func(t *testing.T) {
		var open = openRequest(oneBinding, nil)
		var stateKey = open.Materialization.Bindings[0].StateKey
		open.StateJson = json.RawMessage(fmt.Sprintf(
			`{"__truncations":{"00000000-7fffffff":{%q:%q},"80000000-ffffffff":{%q:%q}}}`,
			stateKey, wantT, stateKey, wantT))

		var tr = &scriptedTransactor{bindings: oneBinding}
		var stream = runTruncationScenario(t, tr, open, nil)

		require.Len(t, tr.truncates, 1)
		var call = tr.truncates[0]
		require.Equal(t, 0, call.binding)
		require.True(t, call.before.Equal(fixedT))
		require.Equal(t, 1, call.responses, "must run before the recovery Acknowledged (idx1), after only Opened (idx0)")

		require.Len(t, stream.responses, 2)
		require.NotNil(t, stream.responses[1].Acknowledged)
		sub, merge := decodeTruncations(t, stream.responses[1].Acknowledged.State)
		require.True(t, merge)
		require.Equal(t, map[string]map[string]*string{
			"00000000-7fffffff": {stateKey: nil},
			"80000000-ffffffff": {stateKey: nil},
		}, sub)
	})

	t.Run("only one of two ranges recorded, waits", func(t *testing.T) {
		var open = openRequest(oneBinding, nil)
		var stateKey = open.Materialization.Bindings[0].StateKey
		open.StateJson = json.RawMessage(fmt.Sprintf(`{"__truncations":{"00000000-7fffffff":{%q:%q}}}`, stateKey, wantT))

		var tr = &scriptedTransactor{bindings: oneBinding}
		runTruncationScenario(t, tr, open, nil)

		require.Empty(t, tr.truncates)
	})
}

// firstRoundLine returns the health line covering round 0 alone.
func firstRoundLine(t *testing.T, lines []healthLine) healthLine {
	t.Helper()
	for _, l := range lines {
		if l.FirstRound == 0 && l.LastRound == 0 {
			return l
		}
	}
	t.Fatal("no health line covers round 0 alone")
	return healthLine{}
}

func TestTruncateHealthBucket(t *testing.T) {
	var fixedT = time.Date(2026, 9, 14, 21, 0, 0, 123456789, time.UTC)

	t.Run("truncation is counted on expected and actual sides", func(t *testing.T) {
		var tr = &scriptedTransactor{bindings: oneBinding, truncateReturns: 42, report: exactReport(1, 0, 0)}
		var txns = []txn{
			{flush: completeFlush(t, 0, fixedT), stores: []pm.Request{storeReq(0, "a", false, false)}},
			{}, {},
		}
		var lines = runHealthScenario(t, tr, openRequest(oneBinding, nil), txns, nil, nil)

		var l = firstRoundLine(t, lines)
		require.Equal(t, "ok", l.Verdict)
		require.Equal(t, int64(1), l.Expected.Truncated)
		require.Equal(t, int64(42), l.Actual.Truncated)
	})

	t.Run("no truncation reports zero on both sides", func(t *testing.T) {
		var tr = &scriptedTransactor{bindings: oneBinding, report: exactReport(1, 0, 0)}
		var txns = []txn{
			{stores: []pm.Request{storeReq(0, "a", false, false)}},
			{}, {},
		}
		var lines = runHealthScenario(t, tr, openRequest(oneBinding, nil), txns, nil, nil)

		var l = firstRoundLine(t, lines)
		require.Equal(t, "ok", l.Verdict)
		require.Equal(t, int64(0), l.Expected.Truncated)
		require.Equal(t, int64(0), l.Actual.Truncated)
	})
}

func TestTruncateSkipped(t *testing.T) {
	var resourcePath = []string{"schema", "alpha"}
	var before = time.Date(2026, 9, 14, 21, 0, 0, 0, time.UTC)
	var reason = "no usable flow_published_at column"

	var hook = logtest.NewGlobal()
	defer hook.Reset()

	TruncateSkipped(resourcePath, before, reason)

	var entries = hook.AllEntries()
	require.Len(t, entries, 2)

	var warn = entries[0]
	require.Equal(t, log.WarnLevel, warn.Level)
	require.Equal(t, "backfill truncation skipped", warn.Message)
	require.Equal(t, "schema.alpha", warn.Data["resourcePath"])
	require.Equal(t, before, warn.Data["before"])
	require.Equal(t, reason, warn.Data["reason"])

	var status = entries[1]
	require.Equal(t, log.InfoLevel, status.Level)
	require.Equal(t, "connectorStatus", status.Data["eventType"])
	require.Equal(t,
		"Backfill truncation skipped for schema.alpha: no usable flow_published_at column. "+
			"Rows published before 2026-09-14T21:00:00Z were not deleted.",
		status.Message)
}
