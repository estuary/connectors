package connector

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	dbsql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/databricks/databricks-sql-go/driverctx"
	log "github.com/sirupsen/logrus"
)

const (
	// profileMinDuration is how long a query must run to be profiled.
	profileMinDuration = 5 * time.Second
	// profileQueueSize bounds the queries awaiting their history. A query
	// finishing while it's full isn't profiled.
	profileQueueSize = 256
	// profileInterval is how often the history of awaiting queries is fetched.
	profileInterval = 30 * time.Second
	// profileBatchSize is the most queries one history request asks for.
	profileBatchSize = 50
	// profileMaxWait is how long a query waits for its history to be complete
	// before it's logged without it.
	profileMaxWait = 10 * time.Minute
)

type queryHistory interface {
	List(ctx context.Context, request dbsql.ListQueryHistoryRequest) (*dbsql.ListQueriesResponse, error)
}

// queryProfiler logs slow queries with the metrics the warehouse records of
// them in its query history: time spent queued, compiling, executing and
// returning results, and the files and bytes read and pruned. History is
// fetched in the background, so profiling never holds up a query.
type queryProfiler struct {
	history     queryHistory
	minDuration time.Duration
	queue       chan *profiledQuery
	emit        func(log.Fields)
	warned      bool
	cancel      context.CancelFunc
	done        chan struct{}
}

// profiledQuery is a finished query awaiting its history.
type profiledQuery struct {
	id       string
	fields   log.Fields
	finished time.Time
}

func newQueryProfiler(history queryHistory) *queryProfiler {
	var ctx, cancel = context.WithCancel(context.Background())
	var p = &queryProfiler{
		history:     history,
		minDuration: profileMinDuration,
		queue:       make(chan *profiledQuery, profileQueueSize),
		emit:        func(fields log.Fields) { log.WithFields(fields).Info("query profile") },
		cancel:      cancel,
		done:        make(chan struct{}),
	}
	go p.run(ctx)
	return p
}

// stop ends the profiler, dropping the queries still awaiting their history.
func (p *queryProfiler) stop() {
	if p != nil {
		p.cancel()
		<-p.done
	}
}

// queryTrace measures one query run.
type queryTrace struct {
	p       *queryProfiler
	fields  log.Fields
	started time.Time
	id      atomic.Pointer[string]
}

// trace starts measuring a query, which must be run with the returned context
// for the driver to report its ID. A nil profiler traces nothing.
func (p *queryProfiler) trace(ctx context.Context, fields log.Fields) (context.Context, *queryTrace) {
	var t = &queryTrace{p: p, fields: fields, started: time.Now()}
	if p == nil {
		return ctx, t
	}
	return driverctx.NewContextWithQueryIdCallback(ctx, func(id string) { t.id.Store(&id) }), t
}

// finish queues the query to be logged with its history if it ran long
// enough, adding fields to those it was traced with.
func (t *queryTrace) finish(err error, fields log.Fields) {
	var took = time.Since(t.started)
	var id = t.id.Load()
	if t.p == nil || id == nil || took < t.p.minDuration {
		return
	}

	var q = &profiledQuery{id: *id, fields: log.Fields{"queryId": *id, "took": took.String()}, finished: time.Now()}
	maps.Copy(q.fields, t.fields)
	maps.Copy(q.fields, fields)
	if err != nil {
		q.fields["failed"] = true
	}

	select {
	case t.p.queue <- q:
	default:
	}
}

func (p *queryProfiler) run(ctx context.Context) {
	defer close(p.done)
	var ticker = time.NewTicker(profileInterval)
	defer ticker.Stop()

	var awaiting []*profiledQuery
	for {
		select {
		case <-ctx.Done():
			return
		case q := <-p.queue:
			awaiting = append(awaiting, q)
		case <-ticker.C:
			awaiting = p.fetch(ctx, awaiting, time.Now())
		}
	}
}

// fetch logs each awaiting query whose history is complete, or which has
// waited too long for it, and returns the others.
func (p *queryProfiler) fetch(ctx context.Context, awaiting []*profiledQuery, now time.Time) []*profiledQuery {
	var history = make(map[string]*dbsql.QueryInfo)
	for batch := range slices.Chunk(awaiting, profileBatchSize) {
		if p.history == nil {
			break
		}
		var ids = make([]string, len(batch))
		for i, q := range batch {
			ids[i] = q.id
		}
		resp, err := p.history.List(ctx, dbsql.ListQueryHistoryRequest{
			FilterBy:       &dbsql.QueryFilter{StatementIds: ids},
			IncludeMetrics: true,
			MaxResults:     len(ids),
		})
		if errors.Is(err, apierr.ErrPermissionDenied) {
			log.WithError(err).Warn("not permitted to read query history; query profiles won't include it")
			p.history = nil
		} else if err != nil {
			var entry = log.WithError(err)
			if !p.warned {
				entry.Warn("fetching query history for query profiles failed")
				p.warned = true
			} else {
				entry.Debug("fetching query history for query profiles failed")
			}
		} else {
			for i := range resp.Res {
				history[resp.Res[i].QueryId] = &resp.Res[i]
			}
		}
	}

	var rest = awaiting[:0]
	for _, q := range awaiting {
		if info := history[q.id]; info != nil && historyComplete(info) {
			p.emit(withHistory(q.fields, info))
		} else if p.history == nil || now.Sub(q.finished) >= profileMaxWait {
			p.emit(q.fields)
		} else {
			rest = append(rest, q)
		}
	}
	return rest
}

// historyComplete reports whether a query's history has its final metrics,
// which fill in a while after the query finishes.
func historyComplete(info *dbsql.QueryInfo) bool {
	switch info.Status {
	case dbsql.QueryStatusFinished, dbsql.QueryStatusFailed, dbsql.QueryStatusCanceled:
		return info.IsFinal && info.Metrics != nil
	}
	return false
}

func withHistory(fields log.Fields, info *dbsql.QueryInfo) log.Fields {
	var mt = info.Metrics
	var out = maps.Clone(fields)
	maps.Copy(out, log.Fields{
		"status":           info.Status,
		"totalMs":          mt.TotalTimeMs,
		"compilationMs":    mt.CompilationTimeMs,
		"executionMs":      mt.ExecutionTimeMs,
		"resultFetchMs":    mt.ResultFetchTimeMs,
		"taskTotalMs":      mt.TaskTotalTimeMs,
		"photonTotalMs":    mt.PhotonTotalTimeMs,
		"readFiles":        mt.ReadFilesCount,
		"prunedFiles":      mt.PrunedFilesCount,
		"readFilesBytes":   mt.ReadFilesBytes,
		"prunedBytes":      mt.PrunedBytes,
		"readBytes":        mt.ReadBytes,
		"readRemoteBytes":  mt.ReadRemoteBytes,
		"readCacheBytes":   mt.ReadCacheBytes,
		"rowsRead":         mt.RowsReadCount,
		"rowsProduced":     mt.RowsProducedCount,
		"spillToDiskBytes": mt.SpillToDiskBytes,
		"networkSentBytes": mt.NetworkSentBytes,
		"writeRemoteBytes": mt.WriteRemoteBytes,
	})
	if queued := queuedMs(mt); queued > 0 {
		out["queuedMs"] = queued
	}
	return out
}

// queuedMs is how long a query waited for the warehouse to provision or to
// have capacity for it, in milliseconds, or zero if it didn't wait.
func queuedMs(mt *dbsql.QueryMetrics) int64 {
	var start int64
	for _, ts := range []int64{mt.ProvisioningQueueStartTimestamp, mt.OverloadingQueueStartTimestamp} {
		if ts > 0 && (start == 0 || ts < start) {
			start = ts
		}
	}
	if start == 0 || mt.QueryCompilationStartTimestamp <= start {
		return 0
	}
	return mt.QueryCompilationStartTimestamp - start
}

// statementKind names a commit query by its leading keyword, such as "merge"
// or "copy".
func statementKind(query string) string {
	if fields := strings.Fields(query); len(fields) > 0 {
		return strings.ToLower(fields[0])
	}
	return ""
}
