package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// toSpannerValue converts a Go value to a Spanner-compatible value.
// This handles type conversions needed for Spanner's type system.
// columnType is the DDL type (e.g., "TIMESTAMP", "DATE", "INT64", "JSON")
func toSpannerValue(val interface{}, columnName string, columnType string) (interface{}, error) {
	if val == nil {
		// Return appropriate null type based on column type
		switch {
		case strings.Contains(columnType, "INT64"):
			return spanner.NullInt64{Valid: false}, nil
		case strings.Contains(columnType, "FLOAT64"):
			return spanner.NullFloat64{Valid: false}, nil
		case strings.Contains(columnType, "BOOL"):
			return spanner.NullBool{Valid: false}, nil
		case strings.Contains(columnType, "TIMESTAMP"):
			return spanner.NullTime{Valid: false}, nil
		case strings.Contains(columnType, "DATE"):
			return spanner.NullDate{Valid: false}, nil
		case strings.Contains(columnType, "JSON"):
			return spanner.NullJSON{Valid: false}, nil
		case strings.Contains(columnType, "BYTES"):
			return []byte(nil), nil
		default:
			// STRING or unknown types
			return spanner.NullString{Valid: false}, nil
		}
	}

	switch v := val.(type) {
	case json.RawMessage:
		return spanner.NullJSON{Value: v, Valid: len(v) > 0}, nil
	case []byte:
		return v, nil
	case string:
		// Check column type to determine how to handle strings
		if strings.Contains(columnType, "TIMESTAMP") {
			// Parse as timestamp - try multiple formats
			formats := []string{
				time.RFC3339Nano,
				time.RFC3339,
				"2006-01-02T15:04:05.999999999Z07:00", // Extended precision
				"2006-01-02T15:04:05Z07:00",
			}

			for _, format := range formats {
				if t, err := time.Parse(format, v); err == nil {
					return t, nil
				}
			}

			// Log the actual value and column for debugging
			return nil, fmt.Errorf("cannot parse string %q as TIMESTAMP for column %s (tried %d formats)", v, columnName, len(formats))
		} else if strings.Contains(columnType, "DATE") {
			// Parse as civil.Date for DATE columns
			if d, err := civil.ParseDate(v); err == nil {
				return d, nil
			}
			return nil, fmt.Errorf("cannot parse string %q as DATE for column %s", v, columnName)
		}
		// For other types, return string as-is
		return v, nil
	case time.Time:
		// time.Time is directly supported by Spanner for TIMESTAMP columns
		return v, nil
	case int:
		// Spanner FLOAT64 columns require float64 values - convert integers
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		return int64(v), nil
	case int8:
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		return int64(v), nil
	case int16:
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		return int64(v), nil
	case int32:
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		return int64(v), nil
	case int64:
		// Spanner FLOAT64 columns require float64 values - convert integers
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		return v, nil
	case uint:
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		return int64(v), nil
	case uint8:
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		return int64(v), nil
	case uint16:
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		return int64(v), nil
	case uint32:
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		return int64(v), nil
	case uint64:
		// Spanner FLOAT64 columns require float64 values - convert integers
		if strings.Contains(columnType, "FLOAT64") {
			return float64(v), nil
		}
		// For NUMERIC columns, use big.Rat to handle the full uint64 range
		if strings.Contains(columnType, "NUMERIC") {
			return big.NewRat(0, 1).SetUint64(v), nil
		}
		// Spanner's INT64 type only supports -2^63 to 2^63-1
		if v > 9223372036854775807 { // math.MaxInt64
			return nil, fmt.Errorf("uint64 value %d exceeds Spanner INT64 maximum (9223372036854775807) for column %s - consider using NUMERIC or STRING column type",
				v, columnName)
		}
		return int64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case bool:
		return v, nil
	case *big.Int:
		return v.String(), nil
	default:
		// For unknown types, pass through as-is
		return v, nil
	}
}

// calculateValueSize calculates the approximate byte size of a value.
// This is used to track mutation batch sizes more accurately than the fixed estimate.
func calculateValueSize(val interface{}) int {
	if val == nil {
		return 1 // Just the validity flag
	}

	switch v := val.(type) {
	case int64:
		return 8
	case float64:
		return 8
	case bool:
		return 1
	case string:
		return len(v)
	case []byte:
		return len(v)
	case time.Time:
		return 24 // Time struct has 3 int64 fields
	case civil.Date:
		return 12 // Year(4) + Month(1) + Day(1) + padding
	case spanner.NullInt64:
		if !v.Valid {
			return 1
		}
		return 9 // 8 for int64 + 1 for Valid flag
	case spanner.NullFloat64:
		if !v.Valid {
			return 1
		}
		return 9 // 8 for float64 + 1 for Valid flag
	case spanner.NullBool:
		if !v.Valid {
			return 1
		}
		return 2 // 1 for bool + 1 for Valid flag
	case spanner.NullString:
		if !v.Valid {
			return 1
		}
		return len(v.StringVal) + 1
	case spanner.NullTime:
		if !v.Valid {
			return 1
		}
		return 25 // 24 for Time + 1 for Valid flag
	case spanner.NullDate:
		if !v.Valid {
			return 1
		}
		return 13 // 12 for Date + 1 for Valid flag
	case spanner.NullJSON:
		if !v.Valid {
			return 1
		}
		// JSON is stored as []byte, string, or json.RawMessage
		switch jsonVal := v.Value.(type) {
		case []byte:
			return len(jsonVal) + 1
		case string:
			return len(jsonVal) + 1
		case json.RawMessage:
			return len(jsonVal) + 1
		default:
			// Fallback: estimate for complex JSON
			return 100
		}
	case json.RawMessage:
		return len(v)
	default:
		// For unknown types, use a conservative estimate
		return 100
	}
}

// calculateMutationByteSize calculates the approximate byte size of a mutation
// based on the values that will be used to create it. Only counts data values,
// not metadata like table names or column names.
func calculateMutationByteSize(values []interface{}) int {
	size := 0
	for _, val := range values {
		size += calculateValueSize(val)
	}
	return size
}

// mutationBatch accumulates mutations for efficient batching
type mutationBatch struct {
	mutations    []*spanner.Mutation
	mutationCount int // Spanner mutation count (columns * operations)
	byteSize     int // Approximate size in bytes
}

const (
	// Maximum mutations per commit (Spanner limit is 80,000)
	// Note: Each column counts as one mutation, e.g. an insert of 5 columns = 5 mutations
	// We use 75,000 to provide a safety margin since we check after adding mutations
	maxMutationsPerBatch = 75000

	// Maximum byte size per batch (1-5 MB is recommended by Spanner)
	maxBytesPerBatch = 4 * 1024 * 1024 // 4 MB
)

// addMutation adds a mutation to the batch
// columnCount is the number of columns affected (each column counts as one mutation in Spanner)
// actualByteSize is the calculated byte size of the mutation values
func (mb *mutationBatch) addMutation(m *spanner.Mutation, columnCount int, actualByteSize int) {
	mb.mutations = append(mb.mutations, m)
	mb.mutationCount += columnCount
	mb.byteSize += actualByteSize
}

// shouldFlush returns true if the batch should be flushed
func (mb *mutationBatch) shouldFlush() bool {
	return mb.mutationCount >= maxMutationsPerBatch || mb.byteSize >= maxBytesPerBatch
}

// reset clears the batch
func (mb *mutationBatch) reset() {
	mb.mutations = make([]*spanner.Mutation, 0, 1000) // Preallocate reasonable size
	mb.mutationCount = 0
	mb.byteSize = 0
}

// partitionedBatches manages multiple mutation batches, one per partition.
// This enables parallel flushing of mutations based on key hash distribution.
type partitionedBatches struct {
	batches       []*mutationBatch
	numPartitions int
}

// newPartitionedBatches creates a new partitionedBatches with the specified number of partitions
func newPartitionedBatches(numPartitions int) *partitionedBatches {
	batches := make([]*mutationBatch, numPartitions)
	for i := 0; i < numPartitions; i++ {
		batches[i] = &mutationBatch{
			mutations: make([]*spanner.Mutation, 0, 1000),
		}
	}
	return &partitionedBatches{
		batches:       batches,
		numPartitions: numPartitions,
	}
}

// getPartitionIndex calculates which partition a hash should be routed to.
// Uses modulo to distribute hashes evenly across partitions.
// Negative and positive hashes map to different partitions to preserve distribution properties.
func (pb *partitionedBatches) getPartitionIndex(hash int64) int {
	idx := hash % int64(pb.numPartitions)
	if idx < 0 {
		idx += int64(pb.numPartitions)
	}
	return int(idx)
}

// addMutation adds a mutation to the specified partition's batch
func (pb *partitionedBatches) addMutation(partitionIdx int, m *spanner.Mutation, columnCount int, actualByteSize int) error {
	if partitionIdx < 0 || partitionIdx >= pb.numPartitions {
		return fmt.Errorf("partition index out of bounds: %d (numPartitions: %d)", partitionIdx, pb.numPartitions)
	}
	pb.batches[partitionIdx].addMutation(m, columnCount, actualByteSize)
	return nil
}

// getReadyPartitions returns the indices of partitions that are ready to flush
func (pb *partitionedBatches) getReadyPartitions() []int {
	var ready []int
	for i, batch := range pb.batches {
		if batch.shouldFlush() {
			ready = append(ready, i)
		}
	}
	return ready
}

// getBatch returns the mutation batch for a specific partition
func (pb *partitionedBatches) getBatch(partitionIdx int) (*mutationBatch, error) {
	if partitionIdx < 0 || partitionIdx >= pb.numPartitions {
		return nil, fmt.Errorf("partition index out of bounds: %d (numPartitions: %d)", partitionIdx, pb.numPartitions)
	}
	return pb.batches[partitionIdx], nil
}

// getNonEmptyPartitions returns the indices of all partitions that have mutations
func (pb *partitionedBatches) getNonEmptyPartitions() []int {
	var nonEmpty []int
	for i, batch := range pb.batches {
		if len(batch.mutations) > 0 {
			nonEmpty = append(nonEmpty, i)
		}
	}
	return nonEmpty
}

// reset clears the batch for a specific partition
func (pb *partitionedBatches) reset(partitionIdx int) error {
	if partitionIdx < 0 || partitionIdx >= pb.numPartitions {
		return fmt.Errorf("partition index out of bounds: %d (numPartitions: %d)", partitionIdx, pb.numPartitions)
	}
	pb.batches[partitionIdx].reset()
	return nil
}

// asyncBatchFlusher manages asynchronous parallel flushing of partitioned mutation batches.
// It provides unified flush logic used by both Load and Store operations.
type asyncBatchFlusher struct {
	partitionedBatch *partitionedBatches
	flushGroup       *errgroup.Group
	batchCounter     int
	counterMutex     sync.Mutex
	t                *transactor
	ctx              context.Context
	operation        string // "load" or "store" for logging
	durationMutex    sync.Mutex
	totalDuration    time.Duration
}

// newAsyncBatchFlusher creates a new asyncBatchFlusher for managing parallel mutation flushes
func newAsyncBatchFlusher(ctx context.Context, t *transactor, operation string) *asyncBatchFlusher {
	flushGroup, groupCtx := errgroup.WithContext(ctx)
	flushGroup.SetLimit(t.numPartitions)

	return &asyncBatchFlusher{
		partitionedBatch: newPartitionedBatches(t.numPartitions),
		flushGroup:       flushGroup,
		batchCounter:     0,
		t:                t,
		ctx:              groupCtx,
		operation:        operation,
	}
}

// flushSingleBatch flushes a single batch of mutations to Spanner
func (f *asyncBatchFlusher) flushSingleBatch(mutations []*spanner.Mutation, mutationCount int, byteSize int, batchNum int) (time.Duration, error) {
	if len(mutations) == 0 {
		return 0, nil
	}

	_, batchDuration, err := f.t.timedSpannerApply(f.ctx, mutations, fmt.Sprintf("%s-batch-%d", f.operation, batchNum))
	if err != nil {
		return 0, fmt.Errorf("applying %s batch %d (%d operations, %d mutations): %w", f.operation, batchNum, len(mutations), mutationCount, err)
	}

	log.WithFields(log.Fields{
		"operation":     f.operation,
		"operations":    len(mutations),
		"mutationCount": mutationCount,
		"bytes":         byteSize,
		"batch":         batchNum,
		"duration":      batchDuration.Seconds(),
	}).Info(fmt.Sprintf("%s: applied mutation batch", f.operation))

	return batchDuration, nil
}

// flushPartitionsAsync launches async flushes for the specified partition indices
func (f *asyncBatchFlusher) flushPartitionsAsync(partitionIndices []int) error {
	if len(partitionIndices) == 0 {
		return nil
	}

	for _, partIdx := range partitionIndices {
		batch, err := f.partitionedBatch.getBatch(partIdx)
		if err != nil {
			return fmt.Errorf("getting batch for partition %d: %w", partIdx, err)
		}

		if len(batch.mutations) == 0 {
			continue
		}

		// Make a copy of the mutations for this batch
		mutations := make([]*spanner.Mutation, len(batch.mutations))
		copy(mutations, batch.mutations)
		mutationCount := batch.mutationCount
		byteSize := batch.byteSize

		// Reset the partition batch before launching goroutine
		if err := f.partitionedBatch.reset(partIdx); err != nil {
			return fmt.Errorf("resetting batch for partition %d: %w", partIdx, err)
		}

		// Launch async flush via the shared errgroup
		f.flushGroup.Go(func() error {
			f.counterMutex.Lock()
			f.batchCounter++
			batchNum := f.batchCounter
			f.counterMutex.Unlock()

			duration, err := f.flushSingleBatch(mutations, mutationCount, byteSize, batchNum)
			if err != nil {
				return err
			}

			f.durationMutex.Lock()
			f.totalDuration += duration
			f.durationMutex.Unlock()

			return nil
		})
	}
	return nil
}

// wait waits for all pending flush operations to complete
func (f *asyncBatchFlusher) wait() error {
	return f.flushGroup.Wait()
}

// getTotalDuration returns the cumulative duration of all flush operations
func (f *asyncBatchFlusher) getTotalDuration() time.Duration {
	f.durationMutex.Lock()
	defer f.durationMutex.Unlock()
	return f.totalDuration
}

// getBatchCount returns the total number of batches flushed
func (f *asyncBatchFlusher) getBatchCount() int {
	f.counterMutex.Lock()
	defer f.counterMutex.Unlock()
	return f.batchCounter
}
