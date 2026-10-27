package connector

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	dbsql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/databricks/databricks-sql-go/driverctx"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type fakeHistory struct {
	queries  map[string]dbsql.QueryInfo
	err      error
	requests [][]string
}

func (h *fakeHistory) List(_ context.Context, req dbsql.ListQueryHistoryRequest) (*dbsql.ListQueriesResponse, error) {
	h.requests = append(h.requests, req.FilterBy.StatementIds)
	if h.err != nil {
		return nil, h.err
	}
	var resp = &dbsql.ListQueriesResponse{}
	for _, id := range req.FilterBy.StatementIds {
		if q, ok := h.queries[id]; ok {
			resp.Res = append(resp.Res, q)
		}
	}
	return resp, nil
}

// testProfiler is a profiler without its background loop, collecting what it
// logs.
func testProfiler(history queryHistory, minDuration time.Duration) (*queryProfiler, *[]log.Fields) {
	var emitted []log.Fields
	return &queryProfiler{
		history:     history,
		minDuration: minDuration,
		queue:       make(chan *profiledQuery, profileQueueSize),
		emit:        func(fields log.Fields) { emitted = append(emitted, fields) },
	}, &emitted
}

func TestQueryTrace(t *testing.T) {
	var fields = log.Fields{"resourcePath": []string{"a", "b"}, "query": "merge"}

	t.Run("nil profiler traces nothing", func(t *testing.T) {
		var p *queryProfiler
		var ctx = context.Background()
		var traced, trace = p.trace(ctx, fields)
		require.Equal(t, ctx, traced)
		trace.finish(nil, nil)
		p.stop()
	})

	t.Run("queues a slow query with its id and fields", func(t *testing.T) {
		var p, _ = testProfiler(nil, 0)
		var ctx, trace = p.trace(context.Background(), fields)
		driverctx.NewContextWithQueryId(ctx, "q1")
		trace.finish(errors.New("boom"), log.Fields{"docs": 3})

		var q = <-p.queue
		require.Equal(t, "q1", q.id)
		require.Equal(t, "q1", q.fields["queryId"])
		require.Equal(t, "merge", q.fields["query"])
		require.Equal(t, 3, q.fields["docs"])
		require.Equal(t, true, q.fields["failed"])
		require.Contains(t, q.fields, "took")
	})

	t.Run("skips quick queries and queries without an id", func(t *testing.T) {
		var p, _ = testProfiler(nil, time.Hour)
		var ctx, trace = p.trace(context.Background(), fields)
		driverctx.NewContextWithQueryId(ctx, "q1")
		trace.finish(nil, nil)

		p.minDuration = 0
		_, trace = p.trace(context.Background(), fields)
		trace.finish(nil, nil)

		require.Empty(t, p.queue)
	})

	t.Run("drops queries when the queue is full", func(t *testing.T) {
		var p, _ = testProfiler(nil, 0)
		p.queue = make(chan *profiledQuery, 1)
		for range 2 {
			var ctx, trace = p.trace(context.Background(), fields)
			driverctx.NewContextWithQueryId(ctx, "q1")
			trace.finish(nil, nil)
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

	t.Run("logs complete history, waits for the rest", func(t *testing.T) {
		var history = &fakeHistory{queries: map[string]dbsql.QueryInfo{
			"done": {QueryId: "done", Status: dbsql.QueryStatusFinished, IsFinal: true, Metrics: &dbsql.QueryMetrics{
				TotalTimeMs: 9000, ExecutionTimeMs: 7000, ReadFilesCount: 2, PrunedFilesCount: 158,
				ProvisioningQueueStartTimestamp: 1000, OverloadingQueueStartTimestamp: 1500, QueryCompilationStartTimestamp: 4000,
			}},
			"filling": {QueryId: "filling", Status: dbsql.QueryStatusFinished, Metrics: &dbsql.QueryMetrics{TotalTimeMs: 9000}},
			"running": {QueryId: "running", Status: dbsql.QueryStatusRunning, IsFinal: true, Metrics: &dbsql.QueryMetrics{}},
		}}
		var p, emitted = testProfiler(history, 0)

		var rest = p.fetch(context.Background(), awaiting(now, "done", "filling", "running", "missing"), now)
		require.Equal(t, []string{"filling", "running", "missing"}, ids(rest))
		require.Len(t, *emitted, 1)
		require.Equal(t, log.Fields{
			"queryId": "done", "status": dbsql.QueryStatusFinished,
			"totalMs": int64(9000), "compilationMs": int64(0), "executionMs": int64(7000), "resultFetchMs": int64(0),
			"taskTotalMs": int64(0), "photonTotalMs": int64(0),
			"readFiles": int64(2), "prunedFiles": int64(158), "readFilesBytes": int64(0), "prunedBytes": int64(0),
			"readBytes": int64(0), "readRemoteBytes": int64(0), "readCacheBytes": int64(0),
			"rowsRead": int64(0), "rowsProduced": int64(0),
			"spillToDiskBytes": int64(0), "networkSentBytes": int64(0), "writeRemoteBytes": int64(0),
			"queuedMs": int64(3000),
		}, (*emitted)[0])
	})

	t.Run("logs queries without history after the max wait", func(t *testing.T) {
		var p, emitted = testProfiler(&fakeHistory{}, 0)
		var rest = p.fetch(context.Background(), append(awaiting(now.Add(-profileMaxWait), "old"), awaiting(now, "new")...), now)
		require.Equal(t, []string{"new"}, ids(rest))
		require.Equal(t, []log.Fields{{"queryId": "old"}}, *emitted)
	})

	t.Run("requests history in batches", func(t *testing.T) {
		var history = &fakeHistory{}
		var p, _ = testProfiler(history, 0)
		var many []*profiledQuery
		for i := range 2*profileBatchSize + 1 {
			many = append(many, awaiting(now, fmt.Sprintf("q%d", i))...)
		}
		p.fetch(context.Background(), many, now)
		require.Len(t, history.requests, 3)
		require.Len(t, history.requests[0], profileBatchSize)
		require.Len(t, history.requests[2], 1)
	})

	t.Run("keeps waiting on a failed request", func(t *testing.T) {
		var p, emitted = testProfiler(&fakeHistory{err: errors.New("unavailable")}, 0)
		require.Len(t, p.fetch(context.Background(), awaiting(now, "q1"), now), 1)
		require.True(t, p.warned)
		require.Empty(t, *emitted)
	})

	t.Run("logs without history once it's not permitted", func(t *testing.T) {
		var history = &fakeHistory{err: fmt.Errorf("listing: %w", apierr.ErrPermissionDenied)}
		var p, emitted = testProfiler(history, 0)
		require.Empty(t, p.fetch(context.Background(), awaiting(now, "q1"), now))
		require.Nil(t, p.history)
		require.Equal(t, []log.Fields{{"queryId": "q1"}}, *emitted)
	})
}

func TestStatementKind(t *testing.T) {
	require.Equal(t, "merge", statementKind("\n\tMERGE INTO t AS l USING ..."))
	require.Equal(t, "copy", statementKind("COPY INTO t FROM ..."))
	require.Equal(t, "", statementKind("  "))
}
