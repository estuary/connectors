package connector

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/databricks/databricks-sdk-go/apierr"
	dbsql "github.com/databricks/databricks-sdk-go/service/sql"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type fakeHistoryAPI struct {
	queries map[string]dbsql.QueryInfo
	err     error
}

func (h *fakeHistoryAPI) List(_ context.Context, req dbsql.ListQueryHistoryRequest) (*dbsql.ListQueriesResponse, error) {
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

func TestQueryHistoryFetch(t *testing.T) {
	t.Run("returns complete history only", func(t *testing.T) {
		var h = &queryHistory{api: &fakeHistoryAPI{queries: map[string]dbsql.QueryInfo{
			"done": {QueryId: "done", Status: dbsql.QueryStatusFinished, IsFinal: true, Metrics: &dbsql.QueryMetrics{
				TotalTimeMs: 9000, ExecutionTimeMs: 7000, ReadFilesCount: 2, PrunedFilesCount: 158,
				ProvisioningQueueStartTimestamp: 1000, OverloadingQueueStartTimestamp: 1500, QueryCompilationStartTimestamp: 4000,
			}},
			"filling": {QueryId: "filling", Status: dbsql.QueryStatusFinished, Metrics: &dbsql.QueryMetrics{TotalTimeMs: 9000}},
			"running": {QueryId: "running", Status: dbsql.QueryStatusRunning, IsFinal: true, Metrics: &dbsql.QueryMetrics{}},
		}}}

		history, err := h.Fetch(context.Background(), []string{"done", "filling", "running", "missing"})
		require.NoError(t, err)
		require.Equal(t, map[string]log.Fields{"done": {
			"status":  dbsql.QueryStatusFinished,
			"totalMs": int64(9000), "compilationMs": int64(0), "executionMs": int64(7000), "resultFetchMs": int64(0),
			"taskTotalMs": int64(0), "photonTotalMs": int64(0),
			"readFiles": int64(2), "prunedFiles": int64(158), "readFilesBytes": int64(0), "prunedBytes": int64(0),
			"readBytes": int64(0), "readRemoteBytes": int64(0), "readCacheBytes": int64(0),
			"rowsRead": int64(0), "rowsProduced": int64(0),
			"spillToDiskBytes": int64(0), "networkSentBytes": int64(0), "writeRemoteBytes": int64(0),
			"queuedMs": int64(3000),
		}}, history)
	})

	t.Run("reports a denied request", func(t *testing.T) {
		var h = &queryHistory{api: &fakeHistoryAPI{err: fmt.Errorf("listing: %w", apierr.ErrPermissionDenied)}}
		var _, err = h.Fetch(context.Background(), []string{"q1"})
		require.ErrorIs(t, err, boilerplate.ErrQueryHistoryDenied)
	})

	t.Run("passes other errors through", func(t *testing.T) {
		var unavailable = errors.New("unavailable")
		var h = &queryHistory{api: &fakeHistoryAPI{err: unavailable}}
		var _, err = h.Fetch(context.Background(), []string{"q1"})
		require.ErrorIs(t, err, unavailable)
		require.NotErrorIs(t, err, boilerplate.ErrQueryHistoryDenied)
	})
}
