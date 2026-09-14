package materialize

import (
	"context"
	"testing"
	"time"

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
