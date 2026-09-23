package materialize

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestStateKeyFilter(t *testing.T) {
	// nil processes everything, including keys that can't be enumerated from
	// the active bindings.
	all := StateKeyFilter(nil)
	require.True(t, all("a_table.v1"))
	require.True(t, all("removed_table.v1"))

	// A non-nil list processes exactly those keys; an empty list processes
	// nothing.
	some := StateKeyFilter([]string{"a_table.v1"})
	require.True(t, some("a_table.v1"))
	require.False(t, some("b_table.v1"))
	require.False(t, StateKeyFilter([]string{})("a_table.v1"))
}

func TestSplitStatePatches(t *testing.T) {
	// Fixtures mirror the wire format produced by the runtime's patch
	// encoder: a JSON array whose elements are each followed by a tab.
	for _, tt := range []struct {
		name    string
		payload string
		want    []json.RawMessage
		wantErr bool
	}{
		{name: "empty payload", payload: "", want: nil},
		{name: "empty array", payload: "[]", want: nil},
		{
			name:    "single patch",
			payload: "[{\"a\":1}\t]",
			want:    []json.RawMessage{json.RawMessage(`{"a":1}`)},
		},
		{
			name:    "reset followed by document",
			payload: "[null\t,{\"a\":1}\t]",
			want:    []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`{"a":1}`)},
		},
		{
			name:    "multiple shard patches",
			payload: "[{\"00000000-7fffffff\":{}}\t,{\"80000000-ffffffff\":{}}\t]",
			want: []json.RawMessage{
				json.RawMessage(`{"00000000-7fffffff":{}}`),
				json.RawMessage(`{"80000000-ffffffff":{}}`),
			},
		},
		{name: "malformed", payload: "[{\"a\":1}", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SplitStatePatches(json.RawMessage(tt.payload))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.want))
			for i := range tt.want {
				require.JSONEq(t, string(tt.want[i]), string(got[i]))
			}
		})
	}
}

// flushRecorder records the arguments of each Flush call.
type flushRecorder struct {
	scriptedTransactor
	err       error
	patches   [][]json.RawMessage
	begins    []map[int]time.Time
	completes []map[int]time.Time
}

func (t *flushRecorder) NewTransactor(_ context.Context, _ pm.Request_Open, be *BindingEvents) (Transactor, *pm.Response_Opened, *MaterializeOptions, error) {
	t.be = be
	return t, &pm.Response_Opened{}, nil, nil
}

func (t *flushRecorder) Flush(_ context.Context, statePatches []json.RawMessage, begins, completes map[int]time.Time) error {
	t.patches = append(t.patches, statePatches)
	t.begins = append(t.begins, begins)
	t.completes = append(t.completes, completes)
	return t.err
}

func flushRequests(flush *pm.Request_Flush) []pm.Request {
	return []pm.Request{
		{Acknowledge: &pm.Request_Acknowledge{}},
		{Flush: flush},
		{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}},
		{Acknowledge: &pm.Request_Acknowledge{}},
	}
}

func runFlush(t *testing.T, tr *flushRecorder, flush *pm.Request_Flush) (*scriptedStream, error) {
	t.Helper()
	var stream = &scriptedStream{requests: flushRequests(flush)}
	var done = make(chan error, 1)
	go func() {
		done <- RunTransactions(context.Background(), tr, stream, openRequest(twoBindings, nil), log.InfoLevel)
	}()
	select {
	case err := <-done:
		return stream, err
	case <-time.After(10 * time.Second):
		t.Fatal("RunTransactions did not return")
		return nil, nil
	}
}

func TestFlushSignals(t *testing.T) {
	var begin = time.Date(2026, 9, 23, 12, 0, 0, 123456789, time.UTC)
	var complete = time.Date(2026, 9, 22, 8, 30, 0, 987654321, time.UTC)
	var beginProto, _ = types.TimestampProto(begin)
	var completeProto, _ = types.TimestampProto(complete)

	t.Run("signals and patches reach the transactor", func(t *testing.T) {
		var hook = logtest.NewGlobal()
		defer hook.Reset()

		var tr = &flushRecorder{scriptedTransactor: scriptedTransactor{bindings: twoBindings}}
		var stream, err = runFlush(t, tr, &pm.Request_Flush{
			StatePatchesJson:  json.RawMessage("[{\"a\":1}\t,{\"b\":2}\t]"),
			BackfillBegins:    []*pm.Request_Flush_BackfillBegin{{Binding: 1, Timestamp: beginProto}},
			BackfillCompletes: []*pm.Request_Flush_BackfillComplete{{Binding: 0, Timestamp: completeProto}},
		})
		require.NoError(t, err)

		require.Len(t, tr.patches, 1)
		require.Len(t, tr.patches[0], 2)
		require.JSONEq(t, `{"a":1}`, string(tr.patches[0][0]))
		require.JSONEq(t, `{"b":2}`, string(tr.patches[0][1]))
		require.Equal(t, map[int]time.Time{1: begin}, tr.begins[0])
		require.Equal(t, map[int]time.Time{0: complete}, tr.completes[0])

		var flushed int
		for _, r := range stream.responses {
			if r.Flushed != nil {
				flushed++
			}
		}
		require.Equal(t, 1, flushed)

		var logged = make(map[string]log.Fields)
		for _, e := range hook.AllEntries() {
			if e.Message == "backfill began" || e.Message == "backfill completed" {
				require.Equal(t, log.InfoLevel, e.Level)
				logged[e.Message] = e.Data
			}
		}
		require.Equal(t, log.Fields{"binding": uint32(1), "boundary": begin}, logged["backfill began"])
		require.Equal(t, log.Fields{"binding": uint32(0), "boundary": complete}, logged["backfill completed"])
	})

	t.Run("no signals", func(t *testing.T) {
		var tr = &flushRecorder{scriptedTransactor: scriptedTransactor{bindings: twoBindings}}
		var _, err = runFlush(t, tr, &pm.Request_Flush{})
		require.NoError(t, err)
		require.Len(t, tr.patches, 1)
		require.Empty(t, tr.patches[0])
		require.Empty(t, tr.begins[0])
		require.Empty(t, tr.completes[0])
	})

	t.Run("missing timestamp fails", func(t *testing.T) {
		var tr = &flushRecorder{scriptedTransactor: scriptedTransactor{bindings: twoBindings}}
		var _, err = runFlush(t, tr, &pm.Request_Flush{
			BackfillCompletes: []*pm.Request_Flush_BackfillComplete{{Binding: 0}},
		})
		require.ErrorContains(t, err, "invalid timestamp in Flush.BackfillCompletes")
		require.Empty(t, tr.patches)
	})

	t.Run("transactor error is wrapped", func(t *testing.T) {
		var boom = errors.New("boom")
		var tr = &flushRecorder{scriptedTransactor: scriptedTransactor{bindings: twoBindings}, err: boom}
		var stream, err = runFlush(t, tr, &pm.Request_Flush{})
		require.ErrorIs(t, err, boom)
		require.ErrorContains(t, err, "transactor.Flush: boom")
		for _, r := range stream.responses {
			require.Nil(t, r.Flushed)
		}
	})
}
