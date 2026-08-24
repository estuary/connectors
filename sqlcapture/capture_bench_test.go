package sqlcapture

// These benchmarks measure the in-memory work of the capture loop as a
// function of binding count. The loop does this work once per backfill chunk
// or once per rediscovery, so per-binding costs multiply on large captures.
//
// Run with: go test ./sqlcapture/ -run='^$' -bench=.

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
)

// benchmarkBindingCounts are the capture sizes at which each benchmark runs.
var benchmarkBindingCounts = []int{100, 1_000, 10_000, 50_000, 100_000}

// quietLogging discards log output for the duration of a benchmark. The log
// level does not change, so each log call keeps its usual formatting cost.
func quietLogging(b *testing.B) {
	var prevOut = log.StandardLogger().Out
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(prevOut) })
}

// newBenchmarkCapture makes a Capture with the given number of bindings, all
// in the given table state, plus a matching discovery map. The mock database
// serves zero-row chunks which never complete, and row counts with the same
// cost as the warm-cache path of the postgres implementation. The replication
// stream is a default mockReplicationStream, and the rediscovery and periodic
// check tasks are scheduled far in the future so no iteration triggers them.
func newBenchmarkCapture(b *testing.B, numBindings int, mode string) *Capture {
	b.Helper()

	var bindings = make(map[StreamID]*Binding, numBindings)
	var streams = make(map[boilerplate.StateKey]*TableState, numBindings)
	var discovery = make(map[StreamID]*DiscoveryInfo, numBindings)
	var rowCounts = make(map[StreamID]int, numBindings)
	for i := range numBindings {
		var table = fmt.Sprintf("table_%06d", i)
		var streamID = JoinStreamID("public", table)
		var stateKey = boilerplate.StateKey("public%2F" + table)
		bindings[streamID] = &Binding{
			Index:         uint32(i),
			StreamID:      streamID,
			StateKey:      stateKey,
			Resource:      Resource{Namespace: "public", Stream: table},
			CollectionKey: []string{"/id"},
		}
		var state = &TableState{Mode: mode, KeyColumns: []string{"id"}}
		switch mode {
		case TableStatePreciseBackfill, TableStateUnfilteredBackfill, TableStateKeylessBackfill:
			state.Scanned = []byte{0x01}
		}
		streams[stateKey] = state
		discovery[streamID] = &DiscoveryInfo{
			Name:       table,
			Schema:     "public",
			BaseTable:  true,
			PrimaryKey: []string{"id"},
		}
		rowCounts[streamID] = 1_000_000
	}

	var db = &mockDatabase{
		FallbackKey: []string{"/_meta/source/fallback"},
		ScanTableChunkFunc: func(ctx context.Context, info *DiscoveryInfo, state *TableState, backfillFilter string, callback func(event ChangeEvent) error) (bool, []byte, error) {
			return false, state.Scanned, nil
		},
		EstimatedRowCountsFunc: func(ctx context.Context, tables []TableID) (map[TableID]int, error) {
			var result = make(map[TableID]int)
			for _, table := range tables {
				result[table] = rowCounts[JoinStreamID(table.Schema, table.Table)]
			}
			return result, nil
		},
	}

	var capture = &Capture{
		Name:     "benchmark/capture",
		Bindings: bindings,
		State:    &PersistentState{Streams: streams},
		Output:   newMockPullOutput(),
		Database: db,
	}
	capture.dirtyStreams = make(map[boilerplate.StateKey]struct{})
	for stateKey, state := range streams {
		capture.updateBackfillSet(stateKey, state.Mode)
	}
	capture.replStream = &mockReplicationStream{}
	capture.lastDiscovery = discovery
	capture.rediscoverAfter = time.Now().Add(time.Hour)
	capture.periodicChecksAfter = time.Now().Add(time.Hour)
	return capture
}

// BenchmarkMainLoopBackfill measures one backfill iteration of the main loop
// with a mock database and a discarded output stream. All real work is free,
// so this measures pure framework overhead.
func BenchmarkMainLoopBackfill(b *testing.B) {
	quietLogging(b)
	var ctx = context.Background()
	for _, numBindings := range benchmarkBindingCounts {
		b.Run(fmt.Sprintf("bindings=%d", numBindings), func(b *testing.B) {
			var c = newBenchmarkCapture(b, numBindings, TableStatePreciseBackfill)
			c.replStream = &mockReplicationStream{
				StreamToFenceFunc: func(ctx context.Context, fenceAfter time.Duration, callback func(event DatabaseEvent) error) error {
					return callback(&mockCommitEvent{Cursor: json.RawMessage(`"0/1"`)})
				},
			}
			b.ReportAllocs()
			for b.Loop() {
				if done, err := c.mainLoopIteration(ctx); err != nil {
					b.Fatal(err)
				} else if done {
					b.Fatal("unexpected shutdown")
				}
				// In real operation acknowledgements trim the pending cursor list.
				c.pending.cursors = c.pending.cursors[:0]
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "chunks/s")
		})
	}
}

// BenchmarkMainLoopStreaming measures one streaming cycle of the main loop.
// The mock delivers prebuilt change events in transactions, with a checkpoint
// after each. Replication decode, document serialization, and output I/O cost
// nothing so this measures framework overhead at different workload shapes.

// A cycle also does fixed bookkeeping which scales with binding count but not
// with event count, but in production a cycle runs for StreamingFenceInterval
// so that should amortize to nothing significant.
func BenchmarkMainLoopStreaming(b *testing.B) {
	quietLogging(b)
	var ctx = context.Background()
	var workloads = []struct {
		commits int // Transactions per streaming cycle.
		changes int // Change events per transaction.
	}{
		{1_000, 1},   // Many single-row transactions.
		{100, 100},   // Mid-size transactions.
		{10, 10_000}, // A few bulk transactions.
	}
	for _, workload := range workloads {
		for _, numBindings := range benchmarkBindingCounts {
			b.Run(fmt.Sprintf("commits=%d/changes=%d/bindings=%d", workload.commits, workload.changes, numBindings), func(b *testing.B) {
				var c = newBenchmarkCapture(b, numBindings, TableStateActive)

				// Spread change events across all bindings so lookups touch distinct keys.
				var streamIDs = make([]StreamID, 0, len(c.Bindings))
				for streamID := range c.Bindings {
					streamIDs = append(streamIDs, streamID)
				}
				var batch []DatabaseEvent
				for commit := range workload.commits {
					for change := range workload.changes {
						batch = append(batch, &mockChangeEvent{
							ID:     streamIDs[(commit*workload.changes+change)%len(streamIDs)],
							RowKey: []byte{0x02},
							Doc:    json.RawMessage(`{"id":12345,"name":"some benchmark row value","updated_at":"2026-08-24T00:00:00Z"}`),
						})
					}
					batch = append(batch, &mockCommitEvent{Cursor: json.RawMessage(`"0/1"`)})
				}

				c.replStream = &mockReplicationStream{
					StreamToFenceFunc: func(ctx context.Context, fenceAfter time.Duration, callback func(event DatabaseEvent) error) error {
						for _, event := range batch {
							if err := callback(event); err != nil {
								return err
							}
						}
						return nil
					},
				}
				b.ReportAllocs()
				for b.Loop() {
					if done, err := c.mainLoopIteration(ctx); err != nil {
						b.Fatal(err)
					} else if done {
						b.Fatal("unexpected shutdown")
					}
					c.pending.cursors = c.pending.cursors[:0]
				}
				b.ReportMetric(float64(b.N)*float64(workload.commits*workload.changes)/b.Elapsed().Seconds(), "events/s")
			})
		}
	}
}

// BenchmarkBackfillStreams measures backfill stream selection plus a zero-row
// chunk scan. This is the per-chunk cost without row processing or streaming.
func BenchmarkBackfillStreams(b *testing.B) {
	quietLogging(b)
	var ctx = context.Background()
	for _, numBindings := range benchmarkBindingCounts {
		b.Run(fmt.Sprintf("bindings=%d", numBindings), func(b *testing.B) {
			var c = newBenchmarkCapture(b, numBindings, TableStatePreciseBackfill)
			b.ReportAllocs()
			for b.Loop() {
				if err := c.backfillStreams(ctx); err != nil {
					b.Fatal(err)
				}
				// In real operation emitState empties the dirty set after each chunk.
				clear(c.dirtyStreams)
			}
		})
	}
}

// BenchmarkStatusUpdate measures the backfill progress computation, which the
// main loop periodically during backfills. Since it involves calculating stats
// across all ongoing backfills it can be a bit expensive.
func BenchmarkStatusUpdate(b *testing.B) {
	quietLogging(b)
	for _, numBindings := range benchmarkBindingCounts {
		b.Run(fmt.Sprintf("bindings=%d", numBindings), func(b *testing.B) {
			var c = newBenchmarkCapture(b, numBindings, TableStatePreciseBackfill)
			b.ReportAllocs()
			for b.Loop() {
				c.statusUpdate("Backfilling Tables")
			}
		})
	}
}

// BenchmarkActivatePendingStreams measures the activation pass which runs
// after each rediscovery. With no pending or missing streams, only the
// binding state scans remain.
func BenchmarkActivatePendingStreams(b *testing.B) {
	quietLogging(b)
	var ctx = context.Background()
	var scenarios = []struct {
		name string
		mode string
	}{
		{"backfilling", TableStatePreciseBackfill},
		{"active", TableStateActive},
	}
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			for _, numBindings := range benchmarkBindingCounts {
				b.Run(fmt.Sprintf("bindings=%d", numBindings), func(b *testing.B) {
					var c = newBenchmarkCapture(b, numBindings, scenario.mode)
					b.ReportAllocs()
					for b.Loop() {
						if err := c.activatePendingStreams(ctx); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

// BenchmarkBindingTableIDs measures the sorted table ID list built for each rediscovery.
func BenchmarkBindingTableIDs(b *testing.B) {
	quietLogging(b)
	for _, numBindings := range benchmarkBindingCounts {
		b.Run(fmt.Sprintf("bindings=%d", numBindings), func(b *testing.B) {
			var c = newBenchmarkCapture(b, numBindings, TableStateActive)
			b.ReportAllocs()
			for b.Loop() {
				c.bindingTableIDs()
			}
		})
	}
}
