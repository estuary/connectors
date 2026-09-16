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

func TestFlushBackfillSignalsAreLogged(t *testing.T) {
	var boundary = time.Date(2026, 9, 14, 12, 30, 45, 123456789, time.UTC)
	var ts, err = types.TimestampProto(boundary)
	require.NoError(t, err)

	var hook = logtest.NewGlobal()
	defer hook.Reset()

	var tr = &scriptedTransactor{bindings: twoBindings}
	var txns = []txn{
		{flush: &pm.Request_Flush{
			BackfillBegins: []*pm.Request_Flush_BackfillBegin{{Binding: 1, Timestamp: ts}},
		}},
		{stores: []pm.Request{storeReq(1, "a", false, false)}},
		{flush: &pm.Request_Flush{
			BackfillCompletes: []*pm.Request_Flush_BackfillComplete{{Binding: 1, Timestamp: ts}},
		}},
	}
	var stream = &scriptedStream{requests: scriptRequests(txns)}
	require.NoError(t, RunTransactions(context.Background(), tr, stream, openRequest(twoBindings, nil), log.InfoLevel))

	var flushed int
	for _, r := range stream.responses {
		if r.Flushed != nil {
			flushed++
		}
	}
	require.Equal(t, len(txns), flushed, "every Flush is answered with Flushed")

	var begins, completes []*log.Entry
	for _, e := range hook.AllEntries() {
		switch e.Message {
		case "backfill begins":
			begins = append(begins, e)
		case "backfill completes":
			completes = append(completes, e)
		}
	}
	require.Len(t, begins, 1)
	require.Len(t, completes, 1)
	for _, e := range []*log.Entry{begins[0], completes[0]} {
		require.Equal(t, log.InfoLevel, e.Level)
		require.Equal(t, []string{"schema", "beta"}, e.Data["resourcePath"])
		require.Equal(t, boundary, e.Data["boundary"])
	}
}
