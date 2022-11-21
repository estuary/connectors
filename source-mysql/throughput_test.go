package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// The benchmarks 'BenchmarkBackfill' and 'BenchmarkReplication' initialize
// several tables with a bunch of synthetic data and benchmark how long it
// takes to capture the data (via Backfill and Replication, respectively).
//
// In both cases the tests will run one capture, spreading the data across
// 10 tables, with the benchmark 'N' parameter controlling the number of
// rows in each table. To be precise, the number of rows is set to N*100,
// so each 'op' in the 'ns/op' measurement reflects the marginal cost of
// 1,000 row-captures.
//
// These benchmarks have a rather high setup cost, so the default `benchtime`
// target of one second isn't nearly enough. Aim for at least 30 seconds to
// get useful numbers (and 300 seconds for perfect accuracy):
//
//     $ LOG_LEVEL=warn go test -run=None -bench=. -benchtime=30s ./source-mysql/
//     $ LOG_LEVEL=warn go test -run=None -bench=. -benchtime=30s -memprofile memprofile.out -cpuprofile profile.out ./source-mysql/
//

func BenchmarkBackfill(b *testing.B) { benchmarkBackfills(b, 1, 10, b.N*100) }

func BenchmarkReplication(b *testing.B) { benchmarkReplication(b, 1, 10, b.N*100) }

func benchmarkBackfills(b *testing.B, iterations, numTables, rowsPerTable int) {
	b.StopTimer()
	b.ResetTimer()

	// Set up multiple tables full of data
	log.WithFields(log.Fields{
		"rows":         rowsPerTable * numTables,
		"rowsPerTable": rowsPerTable,
		"tables":       numTables,
	}).Info("initializing tables")

	var tb, ctx = TestBackend, context.Background()
	var tables []string
	for i := 0; i < numTables; i++ {
		var table = tb.CreateTable(ctx, b, fmt.Sprintf("table%d", i), "(id INTEGER PRIMARY KEY, uid TEXT, name TEXT, status INTEGER, modified DATE, foo_id INTEGER, padding TEXT)")
		tables = append(tables, table)
		populateTable(ctx, b, tb, table, rowsPerTable)
	}

	log.WithField("iterations", iterations).Info("running backfill benchmark")
	for i := 0; i < iterations; i++ {
		var cs = tb.CaptureSpec(b, tables...)
		var validator = &benchmarkCaptureValidator{}
		cs.Validator = validator
		b.StartTimer()
		tests.RunCapture(ctx, b, cs)
		b.StopTimer()
		if len(cs.Errors) > 0 {
			b.Fatalf("capture failed with error: %v", cs.Errors[0])
		}
		var expectedRecords = numTables * rowsPerTable
		if validator.Total != expectedRecords {
			b.Fatalf("incorrect document count: got %d, expected %d", validator.Total, expectedRecords)
		}
	}
}

func benchmarkReplication(b *testing.B, iterations, numTables, rowsPerTable int) {
	b.StopTimer()
	b.ResetTimer()

	var tb, ctx = TestBackend, context.Background()
	var tables []string
	for i := 0; i < numTables; i++ {
		var table = tb.CreateTable(ctx, b, fmt.Sprintf("table%d", i), "(id INTEGER PRIMARY KEY, uid TEXT, name TEXT, status INTEGER, modified DATE, foo_id INTEGER, padding TEXT)")
		tables = append(tables, table)
	}

	var cs = tb.CaptureSpec(b, tables...)
	tests.RunCapture(ctx, b, cs)
	if len(cs.Errors) > 0 {
		b.Fatalf("capture failed with error: %v", cs.Errors[0])
	}
	var initialState = cs.Checkpoint

	for _, table := range tables {
		var table = table
		populateTable(ctx, b, tb, table, rowsPerTable)
	}

	for i := 0; i < iterations; i++ {
		var validator = &benchmarkCaptureValidator{}
		cs.Validator = validator
		cs.Checkpoint = initialState
		b.StartTimer()
		tests.RunCapture(ctx, b, cs)
		b.StopTimer()
		if len(cs.Errors) > 0 {
			b.Fatalf("capture failed with error: %v", cs.Errors[0])
		}
		var expectedRecords = numTables * rowsPerTable
		if validator.Total != expectedRecords {
			b.Fatalf("incorrect document count: got %d, expected %d", validator.Total, expectedRecords)
		}
	}
}

func populateTable(ctx context.Context, t testing.TB, tb tests.TestBackend, table string, numRows int) error {
	t.Helper()

	const chunkSize = 8192

	var columnNames = []string{"id", "uid", "name", "status", "modified", "foo_id", "padding"}
	var buffer [][]interface{}
	for i := 0; i < numRows; i++ {
		var date = time.Unix(683640000+rand.Int63n(974764800), 0).Format("2006-01-02")
		var padding = strings.Repeat("PADDING.", rand.Intn(33))
		buffer = append(buffer, []interface{}{
			// Total size: 192 +/- 132 bytes per row
			i,                           // (4) Serial Integer Primary Key
			uuid.New().String(),         // (36) Random UUID as a string
			fmt.Sprintf("Thing #%d", i), // (8-16) Human readable name
			100 + rand.Intn(400),        // (4) Integer status code
			date,                        // (4) Random YYYY-MM-DD date within the past 30 years
			rand.Int31(),                // (4) Random integer ID
			padding,                     // (0-256) Variable amount of padding
		})
		if len(buffer) >= chunkSize {
			bulkLoadData(ctx, t, table, columnNames, buffer)
			buffer = nil
		}
	}
	if len(buffer) > 0 {
		bulkLoadData(ctx, t, table, columnNames, buffer)
	}
	return nil
}

func bulkLoadData(ctx context.Context, t testing.TB, table string, columnNames []string, rows [][]interface{}) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	if err := TestBackend.conn.Begin(); err != nil {
		t.Fatalf("error beginning transaction: %v", err)
	}

	log.WithFields(log.Fields{"table": table, "count": len(rows)}).Trace("inserting bulk data")
	var argc = len(rows[0])
	var placeholder = argsTuple(argc)
	var placeholders []string
	var values []interface{}
	for _, row := range rows {
		if len(row) != argc {
			t.Fatalf("incorrect number of values in row %q (expected %d)", row, len(rows[0]))
		}
		placeholders = append(placeholders, placeholder)
		values = append(values, row...)
	}

	var query = fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(placeholders, ","))
	TestBackend.Query(ctx, t, query, values...)
	log.WithFields(log.Fields{"table": table, "count": len(rows)}).Trace("inserted bulk data")

	if err := TestBackend.conn.Commit(); err != nil {
		t.Fatalf("error committing transaction: %v", err)
	}
}

type benchmarkCaptureValidator struct {
	Total int
}

func (v *benchmarkCaptureValidator) Output(collection string, data json.RawMessage) {
	v.Total++
}

func (v *benchmarkCaptureValidator) Summarize(w io.Writer) error {
	var _, err = fmt.Fprintf(w, "Total Documents Captured: %d", v.Total)
	return err
}

func (v *benchmarkCaptureValidator) Reset() {
	v.Total = 0
}
