package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	dbsql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/databricks/databricks-sql-go/driverctx"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	log "github.com/sirupsen/logrus"
)

// profileInterval is how often the history of awaiting queries is fetched.
const profileInterval = 30 * time.Second

type queryHistoryAPI interface {
	List(ctx context.Context, request dbsql.ListQueryHistoryRequest) (*dbsql.ListQueriesResponse, error)
}

// queryHistory reads the warehouse's query history: time spent queued,
// compiling, executing and returning results, and the files and bytes read
// and pruned.
type queryHistory struct {
	api queryHistoryAPI
}

func newQueryProfiler(api queryHistoryAPI, bindings []*binding) *boilerplate.QueryProfiler {
	var paths = make([][]string, len(bindings))
	for i, b := range bindings {
		paths[i] = b.target.Path
	}
	return boilerplate.NewQueryProfiler(&queryHistory{api: api}, profileInterval, reportQueryID, paths)
}

// reportQueryID has the driver report the ID of a query run with the returned
// context.
func reportQueryID(ctx context.Context, setID func(string)) (context.Context, func()) {
	return driverctx.NewContextWithQueryIdCallback(ctx, setID), nil
}

func (h *queryHistory) Fetch(ctx context.Context, ids []string) (map[string]log.Fields, error) {
	resp, err := h.api.List(ctx, dbsql.ListQueryHistoryRequest{
		FilterBy:       &dbsql.QueryFilter{StatementIds: ids},
		IncludeMetrics: true,
		MaxResults:     len(ids),
	})
	if errors.Is(err, apierr.ErrPermissionDenied) {
		return nil, fmt.Errorf("%w: %w", boilerplate.ErrQueryHistoryDenied, err)
	} else if err != nil {
		return nil, err
	}

	var out = make(map[string]log.Fields)
	for i := range resp.Res {
		if info := &resp.Res[i]; historyComplete(info) {
			out[info.QueryId] = historyFields(info)
		}
	}
	return out, nil
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

func historyFields(info *dbsql.QueryInfo) log.Fields {
	var mt = info.Metrics
	var out = log.Fields{
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
	}
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
