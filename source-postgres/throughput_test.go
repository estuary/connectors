package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

func createBenchmarkTable(ctx context.Context, t testing.TB, tb *testBackend) (string, string) {
	t.Helper()
	var uniqueID = uniqueTableID(t)
	var tableName = tb.CreateTable(ctx, t, uniqueID, `(
        id BIGINT PRIMARY KEY,
        small_int SMALLINT,
        normal_int INTEGER,
        big_int BIGINT,
        float_val FLOAT8,
        decimal_val DECIMAL(10,2),
        text_val TEXT,
        varchar_val VARCHAR(100),
        bool_val BOOLEAN,
        date_val DATE,
        timestamp_val TIMESTAMP WITH TIME ZONE,
        json_val JSONB,
        uuid_val UUID,
        array_val INTEGER[]
    )`)
	return tableName, uniqueID
}

func insertBenchmarkRows(ctx context.Context, t testing.TB, tb *testBackend, tableName string, minID, maxID int) {
	t.Helper()
	log.WithFields(log.Fields{
		"table": tableName,
		"min":   minID,
		"max":   maxID,
		"count": maxID - minID,
	}).Info("inserting benchmark rows")

	// Insert rows in batches so we get periodic transaction commits
	const batchSize = 5000
	for i := minID; i < maxID; i += batchSize {
		var j = i + batchSize
		if j > maxID {
			j = maxID
		}
		tb.Query(ctx, t, fmt.Sprintf(`
			INSERT INTO %s (
				id, small_int, normal_int, big_int, float_val, decimal_val,
				text_val, varchar_val, bool_val, date_val,
				timestamp_val, json_val, uuid_val, array_val
			)
			SELECT
				id,
				(id %% 32767)::smallint,
				(id %% 2147483647)::integer,
				(id %% 1000) * 1000000::bigint,
				sqrt(id::float8),
				(id %% 1000 * 3.14)::decimal(10,2),
				'text-' || id::text,
				'varchar-' || md5(id::text),
				id %% 2 = 0,
				'2024-01-01'::date + ((id %% 365) || ' days')::interval,
				'2024-01-01 00:00:00 UTC'::timestamptz + ((id %% 8760) || ' hours')::interval,
				json_build_object(
					'key1', id %% 1000000,
					'key2', 'value-' || id::text,
					'key3', ARRAY[id %% 100, id %% 100 + 1, id %% 100 + 2]
				),
				gen_random_uuid(),
				ARRAY[id %% 1000, (id %% 1000) * 2, (id %% 1000) * 3]
			FROM generate_series(%d, %d) AS id
		`, tableName, i, j-1))
	}
}

type benchmarkByteCountValidator struct {
	TotalCheckpoints    int
	TotalSourcedSchemas int
	TotalDocuments      int
	TotalBytes          int
}

func (v *benchmarkByteCountValidator) Checkpoint(data json.RawMessage) {
	v.TotalCheckpoints++
}

func (v *benchmarkByteCountValidator) SourcedSchema(collection string, schema json.RawMessage) {
	v.TotalSourcedSchemas++
}

func (v *benchmarkByteCountValidator) Output(collection string, data json.RawMessage) {
	// log.WithField("data", string(data)).WithField("len", len(data)).Debug("output document")
	v.TotalDocuments++
	v.TotalBytes += len(data)
}

func (v *benchmarkByteCountValidator) Summarize(w io.Writer) error {
	fmt.Fprintf(w, "Total Checkpoints: %d\n", v.TotalCheckpoints)
	fmt.Fprintf(w, "Total Documents: %d\n", v.TotalDocuments)
	fmt.Fprintf(w, "Total Bytes: %d\n", v.TotalBytes)
	return nil
}

func (v *benchmarkByteCountValidator) Reset() {
	v.TotalCheckpoints = 0
	v.TotalDocuments = 0
	v.TotalBytes = 0
}

func benchmarkBackfillN(b *testing.B, rowCount int) {
	var ctx, tb = context.Background(), postgresTestBackend(b)
	var tableName, uniqueID = createBenchmarkTable(ctx, b, tb)
	insertBenchmarkRows(ctx, b, tb, tableName, 0, rowCount)

	// Set up the capture. The backfill chunk size is set to the production default of
	// 50k rows, and the validator is set to a special benchmark one which discards the
	// output data but counts how many documents and bytes we get.
	var cs = tb.CaptureSpec(ctx, b, regexp.MustCompile(uniqueID))
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 50000
	cs.Sanitizers = nil // Don't waste time sanitizing the data, we're not validating against a snapshot
	var validator = &benchmarkByteCountValidator{}
	cs.Validator = validator
	sqlcapture.TestShutdownAfterCaughtUp = true
	b.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Reset timer after setup
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Run the capture
		cs.Capture(ctx, b, nil)

		// Reset state between iterations
		cs.Checkpoint = nil
	}

	// Report rows/sec and MBps instead of ns/op
	log.WithFields(log.Fields{
		"rows":    validator.TotalDocuments,
		"bytes":   validator.TotalBytes,
		"seconds": b.Elapsed().Seconds(),
	}).Info("backfill benchmark complete")
	b.ReportMetric(0, "ns/op")
	b.ReportMetric(b.Elapsed().Seconds(), "seconds")
	b.ReportMetric(float64(validator.TotalDocuments)/float64(b.Elapsed().Seconds()), "rows/sec")
	b.ReportMetric(float64(validator.TotalBytes)/float64(1000000*b.Elapsed().Seconds()), "MBps")
}

func BenchmarkBackfill10k(b *testing.B)  { benchmarkBackfillN(b, 10000) }
func BenchmarkBackfill100k(b *testing.B) { benchmarkBackfillN(b, 100000) }
func BenchmarkBackfill1M(b *testing.B)   { benchmarkBackfillN(b, 1000000) }
func BenchmarkBackfill10M(b *testing.B)  { benchmarkBackfillN(b, 10000000) }
func BenchmarkBackfill100M(b *testing.B) { benchmarkBackfillN(b, 100000000) }

func benchmarkReplicationN(b *testing.B, rowCount int) {
	var ctx, tb = context.Background(), postgresTestBackend(b)
	var tableName, uniqueID = createBenchmarkTable(ctx, b, tb)

	// Set up the capture. The backfill chunk size is set to the production default of
	// 50k rows, and the validator is set to a special benchmark one which discards the
	// output data but counts how many documents and bytes we get.
	var cs = tb.CaptureSpec(ctx, b, regexp.MustCompile(uniqueID))
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 50000
	cs.Sanitizers = nil // Don't waste time sanitizing the data, we're not validating against a snapshot
	var validator = &benchmarkByteCountValidator{}
	cs.Validator = validator
	sqlcapture.TestShutdownAfterCaughtUp = true
	b.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Run the capture to get the initial backfill out of the way.
	cs.Capture(ctx, b, nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Insert another batch of rows, excluding insert time from the benchmark timer
		b.StopTimer()
		var minID = validator.TotalDocuments
		insertBenchmarkRows(ctx, b, tb, tableName, minID, minID+rowCount)
		b.StartTimer()

		// Run the capture, which should consume all the new rows via replication.
		// Unlike the backfill benchmark we don't need to clear the checkpoint after
		// each run because we actually want it to keep going and only consume new
		// replication events from the current iteration.
		cs.Capture(ctx, b, nil)
	}

	// Report rows/sec and MBps instead of ns/op
	log.WithFields(log.Fields{
		"rows":    validator.TotalDocuments,
		"bytes":   validator.TotalBytes,
		"seconds": b.Elapsed().Seconds(),
	}).Info("backfill benchmark complete")
	b.ReportMetric(0, "ns/op")
	b.ReportMetric(b.Elapsed().Seconds(), "seconds")
	b.ReportMetric(float64(validator.TotalDocuments)/float64(b.Elapsed().Seconds()), "rows/sec")
	b.ReportMetric(float64(validator.TotalBytes)/float64(1000000*b.Elapsed().Seconds()), "MBps")
}

func BenchmarkReplication10k(b *testing.B)  { benchmarkReplicationN(b, 10000) }
func BenchmarkReplication100k(b *testing.B) { benchmarkReplicationN(b, 100000) }
func BenchmarkReplication1M(b *testing.B)   { benchmarkReplicationN(b, 1000000) }
func BenchmarkReplication10M(b *testing.B)  { benchmarkReplicationN(b, 10000000) }
func BenchmarkReplication100M(b *testing.B) { benchmarkReplicationN(b, 100000000) }
