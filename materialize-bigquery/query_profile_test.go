package connector

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestJobFields(t *testing.T) {
	var created = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("a merge that waited for slots", func(t *testing.T) {
		require.Equal(t, log.Fields{
			"totalMs": int64(9000), "executionMs": int64(7000), "queuedMs": int64(2000),
			"slotMs": int64(40000), "bytesProcessed": int64(1 << 30), "bytesBilled": int64(1 << 31),
			"reservation": "proj:US.res", "edition": bigquery.ReservationEditionEnterprise,
			"statementType": "MERGE", "rowsInserted": int64(3), "rowsUpdated": int64(2), "rowsDeleted": int64(1),
			"stages": 3, "rowsRead": int64(110), "shuffleBytes": int64(60), "shuffleSpilledBytes": int64(5),
			"stageWaitMaxMs": int64(1500), "stageReadMaxMs": int64(300), "stageComputeMaxMs": int64(4000), "stageWriteMaxMs": int64(20),
			"insights": []string{"slotContention", "partitionSkew"},
		}, jobFields(&bigquery.JobStatistics{
			CreationTime:        created,
			StartTime:           created.Add(2 * time.Second),
			EndTime:             created.Add(9 * time.Second),
			TotalBytesProcessed: 1 << 30,
			TotalSlotDuration:   40 * time.Second,
			ReservationID:       "proj:US.res",
			Edition:             bigquery.ReservationEditionEnterprise,
			Details: &bigquery.QueryStatistics{
				StatementType:    "MERGE",
				TotalBytesBilled: 1 << 31,
				DMLStats:         &bigquery.DMLStatistics{InsertedRowCount: 3, UpdatedRowCount: 2, DeletedRowCount: 1},
				QueryPlan: []*bigquery.ExplainQueryStage{
					{RecordsRead: 100, ShuffleOutputBytes: 40, WaitMax: 1500 * time.Millisecond, ReadMax: 300 * time.Millisecond},
					{RecordsRead: 10, ShuffleOutputBytes: 20, ShuffleOutputBytesSpilled: 5, ComputeMax: 4 * time.Second},
					{RecordsRead: 50, InputStages: []int64{0, 1}, WriteMax: 20 * time.Millisecond},
				},
				PerformanceInsights: &bigquery.PerformanceInsights{StagePerformanceStandaloneInsights: []*bigquery.StagePerformanceStandaloneInsight{
					{SlotContention: true},
					{SlotContention: true, PartitionSkew: &bigquery.PartitionSkew{}},
				}},
			},
		}))
	})

	t.Run("a job with no query statistics", func(t *testing.T) {
		require.Equal(t, log.Fields{
			"totalMs": int64(1000), "executionMs": int64(1000), "slotMs": int64(0), "bytesProcessed": int64(0),
		}, jobFields(&bigquery.JobStatistics{CreationTime: created, StartTime: created, EndTime: created.Add(time.Second)}))
	})
}

func TestTableFields(t *testing.T) {
	var oldest = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	require.Equal(t, log.Fields{
		"numRows": uint64(10), "numBytes": int64(2048),
		"timePartitioning":       log.Fields{"type": bigquery.DayPartitioningType, "field": "ts", "expiration": "24h0m0s"},
		"rangePartitioning":      log.Fields{"field": "id", "start": int64(0), "end": int64(100), "interval": int64(10)},
		"requirePartitionFilter": true,
		"clustering":             []string{"id"},
		"streamingBuffer":        log.Fields{"estimatedRows": uint64(4), "estimatedBytes": uint64(512), "oldestEntry": "2026-01-02T03:04:05Z"},
	}, tableFields(&bigquery.TableMetadata{
		NumRows:                10,
		NumBytes:               2048,
		TimePartitioning:       &bigquery.TimePartitioning{Type: bigquery.DayPartitioningType, Field: "ts", Expiration: 24 * time.Hour},
		RangePartitioning:      &bigquery.RangePartitioning{Field: "id", Range: &bigquery.RangePartitioningRange{End: 100, Interval: 10}},
		RequirePartitionFilter: true,
		Clustering:             &bigquery.Clustering{Fields: []string{"id"}},
		StreamingBuffer:        &bigquery.StreamingBuffer{EstimatedRows: 4, EstimatedBytes: 512, OldestEntryTime: oldest},
	}))

	require.Equal(t, log.Fields{"numRows": uint64(0), "numBytes": int64(0)}, tableFields(&bigquery.TableMetadata{}))
}
