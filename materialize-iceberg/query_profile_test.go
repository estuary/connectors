package connector

import (
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	emrTypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-iceberg/python"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestMergeBoundsProfileFields(t *testing.T) {
	var field = func(name string) boilerplate.MappedProjection[mapped] {
		return boilerplate.MappedProjection[mapped]{Projection: boilerplate.Projection{Projection: pf.Projection{Field: name}}}
	}
	require.Equal(t, map[string]any{
		"ts":   map[string]any{"lower": "'2024-01-01T00:00:00.000000Z'", "upper": "'2024-01-01T06:00:00.000000Z'", "span": "6h0m0s"},
		"id":   map[string]any{"lower": "'a'", "upper": "'c'"},
		"flag": nil,
	}, mergeBounds{
		{MappedProjection: field("ts"), LiteralLower: "'2024-01-01T00:00:00.000000Z'", LiteralUpper: "'2024-01-01T06:00:00.000000Z'"},
		{MappedProjection: field("id"), LiteralLower: "'a'", LiteralUpper: "'c'"},
		{MappedProjection: field("flag")},
	}.ProfileFields())
}

func TestMergeProfiles(t *testing.T) {
	var merge = func(idx int, rows int64) mergedBinding {
		return mergedBinding{idx: idx, pendingMerge: &pendingMerge{
			MergeBinding: python.MergeBinding{Binding: idx, Query: "MERGE"},
			Rows:         rows,
		}}
	}

	t.Run("logs what the job measured of a binding", func(t *testing.T) {
		var truncating = merge(1, 0)
		truncating.Query, truncating.TruncateQuery = "", "DELETE"
		var stats = map[int]python.BindingStats{
			0: {Binding: 0, MergeMs: 9000, Retries: 1, Commit: map[string]string{"operation": "overwrite", "added-data-files": "2"}},
			1: {Binding: 1, TruncateMs: 400},
		}
		require.Equal(t, log.Fields{
			"rows": int64(7), "mergeMs": int64(9000), "retries": 1,
			"commit": log.Fields{"operation": "overwrite", "added-data-files": int64(2)},
		}, mergeFields(merge(0, 7), stats))
		require.Equal(t, log.Fields{"truncateMs": int64(400)}, mergeFields(truncating, stats))
		require.Equal(t, log.Fields{"rows": int64(7)}, mergeFields(merge(0, 7), nil))
	})

	t.Run("keeps the slowest bindings of a job over many", func(t *testing.T) {
		var merged []mergedBinding
		var stats = make(map[int]python.BindingStats)
		for idx := range profileMaxBindings + 2 {
			merged = append(merged, merge(idx, 1))
			stats[idx] = python.BindingStats{Binding: idx, MergeMs: int64(idx)}
		}
		stats[0] = python.BindingStats{Binding: 0, TruncateMs: 1000}

		kept, omitted := slowestMerges(merged, stats)
		require.Equal(t, 2, omitted)
		require.Len(t, kept, profileMaxBindings)
		require.Equal(t, 0, kept[0].idx)
		require.Equal(t, profileMaxBindings+1, kept[1].idx)

		// A job that failed measured nothing, so every binding is logged.
		kept, omitted = slowestMerges(merged, nil)
		require.Len(t, kept, len(merged))
		require.Zero(t, omitted)
	})
}

func TestSummaryFields(t *testing.T) {
	require.Equal(t, log.Fields{
		"operation": "overwrite", "added-data-files": int64(3), "deleted-data-files": int64(2),
		"removed-files-size": int64(900), "changed-partition-count": int64(1), "total-records": int64(50),
	}, summaryFields(map[string]string{
		"operation": "overwrite", "added-data-files": "3", "deleted-data-files": "2",
		"removed-files-size": "900", "changed-partition-count": "1", "total-records": "50",
		"spark.app.id": "app-1", "engine-version": "3.5.3", "app-id": "app-1",
	}))
}

func TestTableFields(t *testing.T) {
	md, err := table.ParseMetadataString(`{
		"format-version": 2,
		"table-uuid": "cefe08fd-c720-4f53-8a07-e142d48a4d6d",
		"location": "s3://warehouse/ns/tbl",
		"last-sequence-number": 2,
		"last-updated-ms": 1783445069235,
		"last-column-id": 3,
		"current-schema-id": 0,
		"schemas": [{"type": "struct", "schema-id": 0, "identifier-field-ids": [1], "fields": [
			{"id": 1, "name": "id", "required": true, "type": "long"},
			{"id": 2, "name": "ts", "required": false, "type": "timestamptz"},
			{"id": 3, "name": "val", "required": false, "type": "string"}
		]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": [
			{"source-id": 2, "field-id": 1000, "name": "ts_day", "transform": "day"},
			{"source-id": 1, "field-id": 1001, "name": "id_bucket", "transform": "bucket[16]"}
		]}],
		"last-partition-id": 1001,
		"default-sort-order-id": 1,
		"sort-orders": [{"order-id": 1, "fields": [
			{"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
			{"source-id": 2, "transform": "hour", "direction": "desc", "null-order": "nulls-last"}
		]}],
		"properties": {"write.merge.mode": "merge-on-read", "commit.retry.num-retries": "10", "owner": "someone"},
		"current-snapshot-id": 2,
		"refs": {"main": {"snapshot-id": 2, "type": "branch"}},
		"snapshots": [
			{"sequence-number": 1, "snapshot-id": 1, "timestamp-ms": 1783445060000, "manifest-list": "s3://warehouse/ns/tbl/m1.avro",
				"summary": {"operation": "append", "total-records": "1"}},
			{"sequence-number": 2, "snapshot-id": 2, "parent-snapshot-id": 1, "timestamp-ms": 1783445069235, "manifest-list": "s3://warehouse/ns/tbl/m2.avro",
				"summary": {"operation": "overwrite", "total-records": "9", "total-data-files": "4", "total-delete-files": "2", "engine-name": "spark"}}
		]
	}`)
	require.NoError(t, err)

	require.Equal(t, log.Fields{
		"formatVersion": 2,
		"snapshots":     2,
		"partitionSpec": []string{"day(ts)", "bucket[16](id)"},
		"sortOrder":     []string{"id asc", "hour(ts) desc"},
		"properties":    map[string]string{"write.merge.mode": "merge-on-read", "commit.retry.num-retries": "10"},
		"snapshotAt":    "2026-07-07T17:24:29Z",
		"snapshot": log.Fields{
			"operation": "overwrite", "total-records": int64(9), "total-data-files": int64(4), "total-delete-files": int64(2),
		},
	}, tableFields(md))
}

func TestJobRunFields(t *testing.T) {
	var created = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	require.Equal(t, log.Fields{
		"status": emrTypes.JobRunStateSuccess, "releaseLabel": "emr-7.7.0",
		"pendingMs": int64(40000), "executionMs": int64(90000), "queuedMs": int64(1500), "attempt": int32(2),
	}, jobRunFields(&emrTypes.JobRun{
		State:                      emrTypes.JobRunStateSuccess,
		ReleaseLabel:               aws.String("emr-7.7.0"),
		CreatedAt:                  aws.Time(created),
		StartedAt:                  aws.Time(created.Add(40 * time.Second)),
		EndedAt:                    aws.Time(created.Add(130 * time.Second)),
		QueuedDurationMilliseconds: aws.Int64(1500),
		Attempt:                    aws.Int32(2),
	}))

	require.Equal(t, log.Fields{"status": emrTypes.JobRunStateFailed}, jobRunFields(&emrTypes.JobRun{State: emrTypes.JobRunStateFailed}))
}

func TestUtilizationFields(t *testing.T) {
	var total = &emrTypes.TotalResourceUtilization{VCPUHour: aws.Float64(0.5), MemoryGBHour: aws.Float64(2), StorageGBHour: aws.Float64(4)}
	require.Equal(t, log.Fields{
		"vCPUHours": 0.5, "memoryGBHours": 2.0, "storageGBHours": 4.0,
		"billedVCPUHours": 0.6, "billedMemoryGBHours": 2.5, "billedStorageGBHours": 0.0,
	}, utilizationFields(&emrTypes.JobRun{
		TotalResourceUtilization:  total,
		BilledResourceUtilization: &emrTypes.ResourceUtilization{VCPUHour: aws.Float64(0.6), MemoryGBHour: aws.Float64(2.5)},
	}))

	// A job run that just ended hasn't reported its resources yet.
	require.Nil(t, utilizationFields(&emrTypes.JobRun{}))
	require.Nil(t, utilizationFields(&emrTypes.JobRun{TotalResourceUtilization: total}))
}

func TestApplicationFields(t *testing.T) {
	require.Equal(t, log.Fields{
		"releaseLabel": "emr-7.7.0", "architecture": emrTypes.ArchitectureArm64,
		"maximumCapacity":     log.Fields{"cpu": "64 vCPU", "memory": "256 GB"},
		"initialCapacity":     log.Fields{"driver": log.Fields{"workers": int64(1), "cpu": "4 vCPU", "memory": "16 GB"}},
		"autoStopIdleMinutes": int32(15),
		"maxConcurrentRuns":   int32(4),
	}, applicationFields(&emrTypes.Application{
		ReleaseLabel:    aws.String("emr-7.7.0"),
		Architecture:    emrTypes.ArchitectureArm64,
		MaximumCapacity: &emrTypes.MaximumAllowedResources{Cpu: aws.String("64 vCPU"), Memory: aws.String("256 GB")},
		InitialCapacity: map[string]emrTypes.InitialCapacityConfig{"DRIVER": {
			WorkerCount:         aws.Int64(1),
			WorkerConfiguration: &emrTypes.WorkerResourceConfig{Cpu: aws.String("4 vCPU"), Memory: aws.String("16 GB")},
		}},
		AutoStopConfiguration:  &emrTypes.AutoStopConfig{Enabled: aws.Bool(true), IdleTimeoutMinutes: aws.Int32(15)},
		SchedulerConfiguration: &emrTypes.SchedulerConfiguration{MaxConcurrentRuns: aws.Int32(4)},
	}))

	require.Equal(t, log.Fields{"releaseLabel": "", "architecture": emrTypes.Architecture("")}, applicationFields(&emrTypes.Application{}))
}
