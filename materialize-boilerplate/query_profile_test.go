package boilerplate

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type fakeHistory struct {
	queries  map[string]log.Fields
	err      error
	requests [][]string
}

func (h *fakeHistory) Fetch(_ context.Context, ids []string) (map[string]log.Fields, error) {
	h.requests = append(h.requests, ids)
	if h.err != nil {
		return nil, h.err
	}
	var out = make(map[string]log.Fields)
	for _, id := range ids {
		if q, ok := h.queries[id]; ok {
			out[id] = q
		}
	}
	return out, nil
}

type fakeBounds map[string]any

func (b fakeBounds) ProfileFields() map[string]any { return b }

type queryIDKey struct{}

// fakeReportIDs reports the ID in the context's queryIDKey value once the query
// finishes.
func fakeReportIDs(ctx context.Context, setID func(string)) (context.Context, func()) {
	return ctx, func() {
		if id, ok := ctx.Value(queryIDKey{}).(string); ok {
			setID(id)
		}
	}
}

// testProfiler is a profiler without its background loop, collecting what it
// logs.
func testProfiler(history QueryHistory, minDuration time.Duration) (*QueryProfiler, *[]log.Fields) {
	var emitted []log.Fields
	return &QueryProfiler{
		history:       history,
		reportIDs:     fakeReportIDs,
		resourcePaths: [][]string{{"a", "b"}, {"c", "d"}},
		minDuration:   minDuration,
		queue:         make(chan *profiledQuery, queryProfileQueueSize),
		emit:          func(fields log.Fields) { emitted = append(emitted, fields) },
	}, &emitted
}

func withID(id string) context.Context {
	return context.WithValue(context.Background(), queryIDKey{}, id)
}

func TestQueryTrace(t *testing.T) {
	t.Run("queues a slow query with its binding's profile", func(t *testing.T) {
		var p, _ = testProfiler(nil, 0)
		var profile = p.NewProfile()
		profile.AddKeys(1, 2)
		profile.AddKeys(1, 1)
		profile.SetBounds(1, fakeBounds{"id": nil})
		profile.Set(1, "version", 7)
		profile.AddKeys(0, 5)

		var _, trace = profile.Trace(withID("q1"), 1, "load")
		trace.Executed()
		trace.AddDocument(10)
		trace.AddDocument(5)
		profile.Set(1, "matchedFiles", 4)
		trace.Finish(errors.New("boom"))

		var q = <-p.queue
		require.Equal(t, "q1", q.id)
		require.Contains(t, q.fields, "took")
		require.Contains(t, q.fields, "executeTook")
		delete(q.fields, "took")
		delete(q.fields, "executeTook")
		require.Equal(t, log.Fields{
			"query": "load", "queryId": "q1", "failed": true,
			"resourcePath": []string{"c", "d"}, "keys": 3, "bounds": map[string]any{"id": nil},
			"version": 7, "matchedFiles": 4, "docs": 2, "docBytes": 15,
		}, q.fields)
	})

	t.Run("logs only the resource path of a query without a profile", func(t *testing.T) {
		var p, _ = testProfiler(nil, 0)
		var _, trace = p.Trace(withID("q1"), 0, "delete")
		trace.Finish(nil)

		var q = <-p.queue
		delete(q.fields, "took")
		require.Equal(t, log.Fields{"query": "delete", "queryId": "q1", "resourcePath": []string{"a", "b"}}, q.fields)
	})

	t.Run("skips quick queries", func(t *testing.T) {
		var p, _ = testProfiler(nil, time.Hour)
		var _, trace = p.Trace(withID("q1"), 0, "merge")
		require.False(t, trace.Slow())
		trace.Finish(nil)
		require.Empty(t, p.queue)
	})

	t.Run("queues a query without an id, with the fields set on it", func(t *testing.T) {
		var p, _ = testProfiler(nil, 0)
		var _, trace = p.Trace(context.Background(), 0, "merge")
		require.True(t, trace.Slow())
		trace.Set("slotMs", 12)
		trace.Finish(nil)

		var q = <-p.queue
		require.Empty(t, q.id)
		delete(q.fields, "took")
		require.Equal(t, log.Fields{"query": "merge", "resourcePath": []string{"a", "b"}, "slotMs": 12}, q.fields)
	})

	t.Run("logs the bindings of a query over all of them, most keys first", func(t *testing.T) {
		var p, _ = testProfiler(nil, 0)
		var profile = p.NewProfile()
		profile.AddKeys(0, 1)
		profile.AddKeys(1, 4)
		profile.SetBounds(1, fakeBounds{"id": nil})

		var _, trace = profile.TraceAll(withID("q1"), "load")
		trace.Finish(nil)

		var q = <-p.queue
		delete(q.fields, "took")
		require.Equal(t, log.Fields{"query": "load", "queryId": "q1", "bindings": []log.Fields{
			{"resourcePath": []string{"c", "d"}, "keys": 4, "bounds": map[string]any{"id": nil}},
			{"resourcePath": []string{"a", "b"}, "keys": 1},
		}}, q.fields)
	})

	t.Run("leaves out the bindings of a query over too many", func(t *testing.T) {
		var p, _ = testProfiler(nil, 0)
		var profile = p.NewProfile()
		for binding := range queryProfileMaxBindings + 2 {
			profile.AddKeys(binding, binding+1)
		}

		var _, trace = profile.TraceAll(withID("q1"), "load")
		trace.Finish(nil)

		var q = <-p.queue
		require.Len(t, q.fields["bindings"], queryProfileMaxBindings)
		require.Equal(t, 2, q.fields["bindingsOmitted"])
		require.Equal(t, queryProfileMaxBindings+1, q.tables[0].binding)
	})

	t.Run("drops queries when the queue is full", func(t *testing.T) {
		var p, _ = testProfiler(nil, 0)
		p.queue = make(chan *profiledQuery, 1)
		for range 2 {
			var _, trace = p.Trace(withID("q1"), 0, "merge")
			trace.Finish(nil)
		}
		require.Len(t, p.queue, 1)
	})
}

func TestQueryProfilerFetch(t *testing.T) {
	var now = time.Now()
	var awaiting = func(finished time.Time, ids ...string) []*profiledQuery {
		var out []*profiledQuery
		for _, id := range ids {
			out = append(out, &profiledQuery{id: id, fields: log.Fields{"queryId": id}, finished: finished})
		}
		return out
	}
	var ids = func(qs []*profiledQuery) []string {
		var out []string
		for _, q := range qs {
			out = append(out, q.id)
		}
		return out
	}

	t.Run("logs final history, waits for the rest", func(t *testing.T) {
		var history = &fakeHistory{queries: map[string]log.Fields{
			"done": {"totalMs": int64(9000)},
		}}
		var p, emitted = testProfiler(history, 0)

		var rest = p.fetch(context.Background(), awaiting(now, "done", "missing"), now)
		require.Equal(t, []string{"missing"}, ids(rest))
		require.Equal(t, []log.Fields{{"queryId": "done", "totalMs": int64(9000)}}, *emitted)
	})

	t.Run("logs queries without history after the max wait", func(t *testing.T) {
		var p, emitted = testProfiler(&fakeHistory{}, 0)
		var rest = p.fetch(context.Background(), append(awaiting(now.Add(-QueryProfileMaxWait), "old"), awaiting(now, "new")...), now)
		require.Equal(t, []string{"new"}, ids(rest))
		require.Equal(t, []log.Fields{{"queryId": "old"}}, *emitted)
	})

	t.Run("requests history in batches", func(t *testing.T) {
		var history = &fakeHistory{}
		var p, _ = testProfiler(history, 0)
		var many []*profiledQuery
		for i := range 2*queryProfileBatchSize + 1 {
			many = append(many, awaiting(now, fmt.Sprintf("q%d", i))...)
		}
		p.fetch(context.Background(), many, now)
		require.Len(t, history.requests, 3)
		require.Len(t, history.requests[0], queryProfileBatchSize)
		require.Len(t, history.requests[2], 1)
	})

	t.Run("doesn't request history when nothing awaits it", func(t *testing.T) {
		var history = &fakeHistory{}
		var p, _ = testProfiler(history, 0)
		require.Empty(t, p.fetch(context.Background(), nil, now))
		require.Empty(t, history.requests)
	})

	t.Run("keeps waiting on a failed request", func(t *testing.T) {
		var p, emitted = testProfiler(&fakeHistory{err: errors.New("unavailable")}, 0)
		require.Len(t, p.fetch(context.Background(), awaiting(now, "q1"), now), 1)
		require.True(t, p.warned)
		require.Empty(t, *emitted)
	})

	t.Run("logs without history once it's not permitted", func(t *testing.T) {
		var history = &fakeHistory{err: fmt.Errorf("listing: %w", ErrQueryHistoryDenied)}
		var p, emitted = testProfiler(history, 0)
		require.Empty(t, p.fetch(context.Background(), awaiting(now, "q1"), now))
		require.Nil(t, p.history)
		require.Equal(t, []log.Fields{{"queryId": "q1"}}, *emitted)
	})

	t.Run("logs a query without an id right away", func(t *testing.T) {
		var history = &fakeHistory{}
		var p, emitted = testProfiler(history, 0)
		var queries = append(awaiting(now, "q1"), &profiledQuery{fields: log.Fields{"query": "load"}, finished: now})
		require.Equal(t, []string{"q1"}, ids(p.fetch(context.Background(), queries, now)))
		require.Equal(t, [][]string{{"q1"}}, history.requests)
		require.Equal(t, []log.Fields{{"query": "load"}}, *emitted)
	})
}

func TestStatementKind(t *testing.T) {
	require.Equal(t, "merge", StatementKind("\n\tMERGE INTO t AS l USING ..."))
	require.Equal(t, "copy", StatementKind("COPY INTO t FROM ..."))
	require.Equal(t, "", StatementKind("  "))
}

func TestQueryProfilerTableContext(t *testing.T) {
	var now = time.Now()
	var slow = func(binding int) []*profiledQuery {
		var fields = log.Fields{"query": "merge"}
		return []*profiledQuery{{id: "q", fields: fields, tables: []profiledTable{{binding: binding, fields: fields}}, finished: now}}
	}
	var withContext = func(fields log.Fields, err error) (*QueryProfiler, *[]log.Fields, *[]int) {
		var p, emitted = testProfiler(nil, 0)
		var calls []int
		p.tableContext = func(_ context.Context, binding int) (log.Fields, error) {
			calls = append(calls, binding)
			return fields, err
		}
		p.contextRead = make([]bool, len(p.resourcePaths))
		return p, emitted, &calls
	}

	t.Run("adds a table's context to its first profile only", func(t *testing.T) {
		var p, emitted, calls = withContext(log.Fields{"table": map[string]any{"numFiles": 3}}, nil)
		p.fetch(context.Background(), slow(1), now)
		p.fetch(context.Background(), slow(1), now)
		p.fetch(context.Background(), slow(0), now)
		require.Equal(t, []int{1, 0}, *calls)
		require.Equal(t, []log.Fields{
			{"query": "merge", "table": map[string]any{"numFiles": 3}},
			{"query": "merge"},
			{"query": "merge", "table": map[string]any{"numFiles": 3}},
		}, *emitted)
	})

	t.Run("adds what was read despite an error, and doesn't retry", func(t *testing.T) {
		var p, emitted, calls = withContext(log.Fields{"warehouse": "x"}, errors.New("history denied"))
		p.fetch(context.Background(), slow(0), now)
		p.fetch(context.Background(), slow(0), now)
		require.Equal(t, []int{0}, *calls)
		require.Equal(t, []log.Fields{{"query": "merge", "warehouse": "x"}, {"query": "merge"}}, *emitted)
	})

	t.Run("skips an unknown binding and a profiler without it", func(t *testing.T) {
		var p, emitted, calls = withContext(log.Fields{"table": "x"}, nil)
		p.fetch(context.Background(), slow(-1), now)
		p.fetch(context.Background(), slow(2), now)
		require.Empty(t, *calls)

		var plain, plainEmitted = testProfiler(nil, 0)
		plain.fetch(context.Background(), slow(0), now)
		require.Equal(t, []log.Fields{{"query": "merge"}}, *plainEmitted)
		require.Len(t, *emitted, 2)
	})

	t.Run("adds each table's context to a query over several bindings", func(t *testing.T) {
		var p, emitted, calls = withContext(log.Fields{"table": "x"}, nil)
		var first, second = log.Fields{"keys": 1}, log.Fields{"keys": 2}
		var q = &profiledQuery{
			fields:   log.Fields{"bindings": []log.Fields{first, second}},
			tables:   []profiledTable{{binding: 1, fields: first}, {binding: 0, fields: second}},
			finished: now,
		}
		p.fetch(context.Background(), []*profiledQuery{q}, now)
		require.Equal(t, []int{1, 0}, *calls)
		require.Equal(t, []log.Fields{{"bindings": []log.Fields{
			{"keys": 1, "table": "x"}, {"keys": 2, "table": "x"},
		}}}, *emitted)
	})
}

func TestQueryProfilerStop(t *testing.T) {
	var p = NewQueryProfiler(nil, time.Hour, nil, nil, nil)
	p.Stop()
}
