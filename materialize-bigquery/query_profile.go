package connector

import (
	"context"
	"slices"
	"time"

	"cloud.google.com/go/bigquery"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	log "github.com/sirupsen/logrus"
)

// profileInterval is how often the profiles of finished queries are logged.
// A job's statistics arrive with it, so there's no history to wait for.
const profileInterval = 5 * time.Second

func newQueryProfiler(c *client, bindings []*binding) *boilerplate.QueryProfiler {
	var paths = make([][]string, len(bindings))
	var tables = make([]*bigquery.Table, len(bindings))
	for i, b := range bindings {
		paths[i] = b.target.Path
		tables[i] = c.bigqueryClient.DatasetInProject(c.cfg.ProjectID, b.target.InfoLocation.TableSchema).Table(b.target.InfoLocation.TableName)
	}
	return boilerplate.NewQueryProfiler(nil, profileInterval, nil, paths, func(ctx context.Context, binding int) (log.Fields, error) {
		md, err := tables[binding].Metadata(ctx, bigquery.WithMetadataView(bigquery.FullMetadataView))
		if err != nil {
			return nil, err
		}
		return log.Fields{"table": tableFields(md)}, nil
	})
}

// traceJob adds a finished job's ID and statistics to its query's profile.
func traceJob(trace *boilerplate.QueryTrace, id string, status *bigquery.JobStatus) {
	if id != "" {
		trace.SetID(id)
	}
	if status == nil || status.Statistics == nil {
		return
	}
	for k, v := range jobFields(status.Statistics) {
		trace.Set(k, v)
	}
}

// jobFields are a job's statistics: the time it spent queued and executing,
// the slots and bytes it took, and what its query plan says of its stages.
func jobFields(stats *bigquery.JobStatistics) log.Fields {
	var out = log.Fields{
		"totalMs":        stats.EndTime.Sub(stats.CreationTime).Milliseconds(),
		"executionMs":    stats.EndTime.Sub(stats.StartTime).Milliseconds(),
		"slotMs":         stats.TotalSlotDuration.Milliseconds(),
		"bytesProcessed": stats.TotalBytesProcessed,
	}
	if queued := stats.StartTime.Sub(stats.CreationTime).Milliseconds(); queued > 0 {
		out["queuedMs"] = queued
	}
	if stats.ReservationID != "" {
		out["reservation"] = stats.ReservationID
	}
	if stats.Edition != "" {
		out["edition"] = stats.Edition
	}

	qs, ok := stats.Details.(*bigquery.QueryStatistics)
	if !ok {
		return out
	}
	out["statementType"] = qs.StatementType
	out["bytesBilled"] = qs.TotalBytesBilled
	if qs.CacheHit {
		out["cacheHit"] = true
	}
	if dml := qs.DMLStats; dml != nil {
		out["rowsInserted"] = dml.InsertedRowCount
		out["rowsUpdated"] = dml.UpdatedRowCount
		out["rowsDeleted"] = dml.DeletedRowCount
	}

	if len(qs.QueryPlan) > 0 {
		var rowsRead, shuffleBytes, shuffleSpilledBytes int64
		var waitMax, readMax, computeMax, writeMax time.Duration
		for _, stage := range qs.QueryPlan {
			// Only a stage with no input stage reads its records from tables.
			if len(stage.InputStages) == 0 {
				rowsRead += stage.RecordsRead
			}
			shuffleBytes += stage.ShuffleOutputBytes
			shuffleSpilledBytes += stage.ShuffleOutputBytesSpilled
			waitMax = max(waitMax, stage.WaitMax)
			readMax = max(readMax, stage.ReadMax)
			computeMax = max(computeMax, stage.ComputeMax)
			writeMax = max(writeMax, stage.WriteMax)
		}
		out["stages"] = len(qs.QueryPlan)
		out["rowsRead"] = rowsRead
		out["shuffleBytes"] = shuffleBytes
		out["shuffleSpilledBytes"] = shuffleSpilledBytes
		// The longest any stage's slowest worker spent waiting for a slot,
		// reading its input, computing, and writing its output.
		out["stageWaitMaxMs"] = waitMax.Milliseconds()
		out["stageReadMaxMs"] = readMax.Milliseconds()
		out["stageComputeMaxMs"] = computeMax.Milliseconds()
		out["stageWriteMaxMs"] = writeMax.Milliseconds()
	}

	if insights := qs.PerformanceInsights; insights != nil {
		var found []string
		var add = func(insight string) {
			if !slices.Contains(found, insight) {
				found = append(found, insight)
			}
		}
		for _, stage := range insights.StagePerformanceStandaloneInsights {
			if stage.SlotContention {
				add("slotContention")
			}
			if stage.InsufficientShuffleQuota {
				add("insufficientShuffleQuota")
			}
			if stage.PartitionSkew != nil {
				add("partitionSkew")
			}
			if len(stage.HighCardinalityJoins) > 0 {
				add("highCardinalityJoin")
			}
		}
		if len(found) > 0 {
			out["insights"] = found
		}
	}
	return out
}

// tableFields are a table's size, partitioning and clustering, and the rows
// in its streaming buffer, which queries can't prune.
func tableFields(md *bigquery.TableMetadata) log.Fields {
	var out = log.Fields{
		"numRows":  md.NumRows,
		"numBytes": md.NumBytes,
	}
	if p := md.TimePartitioning; p != nil {
		var partitioning = log.Fields{"type": p.Type}
		if p.Field != "" {
			partitioning["field"] = p.Field
		}
		if p.Expiration > 0 {
			partitioning["expiration"] = p.Expiration.String()
		}
		out["timePartitioning"] = partitioning
	}
	if p := md.RangePartitioning; p != nil {
		var partitioning = log.Fields{"field": p.Field}
		if r := p.Range; r != nil {
			partitioning["start"], partitioning["end"], partitioning["interval"] = r.Start, r.End, r.Interval
		}
		out["rangePartitioning"] = partitioning
	}
	if md.RequirePartitionFilter {
		out["requirePartitionFilter"] = true
	}
	if c := md.Clustering; c != nil {
		out["clustering"] = c.Fields
	}
	if s := md.StreamingBuffer; s != nil {
		out["streamingBuffer"] = log.Fields{
			"estimatedRows":  s.EstimatedRows,
			"estimatedBytes": s.EstimatedBytes,
			"oldestEntry":    s.OldestEntryTime.UTC().Format(time.RFC3339),
		}
	}
	return out
}
