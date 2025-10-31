package main

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/spanner"
	sql "github.com/estuary/connectors/materialize-sql"
)

// buildInsertOrUpdateMutation creates a Spanner InsertOrUpdate mutation from a binding and values.
// InsertOrUpdate will insert a new row if the primary key doesn't exist, or update if it does.
func buildInsertOrUpdateMutation(
	binding *binding,
	values []interface{},
) (*spanner.Mutation, error) {
	if len(values) != len(binding.columns) {
		return nil, fmt.Errorf("value count mismatch: expected %d, got %d", len(binding.columns), len(values))
	}

	// Convert values to Spanner-compatible types
	spannerValues := make([]interface{}, len(values))
	for i, val := range values {
		spannerVal, err := toSpannerValue(val, binding.columns[i])
		if err != nil {
			return nil, fmt.Errorf("converting value for column %s: %w", binding.columns[i], err)
		}
		spannerValues[i] = spannerVal
	}

	mutation := spanner.InsertOrUpdate(binding.target.Identifier, binding.columns, spannerValues)
	return mutation, nil
}

// buildDeleteMutation creates a Spanner Delete mutation from a binding and key values.
// TODO: Implement deletions using one of:
//   - Option 1: spanner.Delete() mutation (simple, same batch)
//   - Option 2: DML DELETE statement (for Partitioned DML support)
//   - Option 3: Soft delete with flow_deleted column
func buildDeleteMutation(
	binding *binding,
	keyValues []interface{},
) (*spanner.Mutation, error) {
	// Convert key values to Spanner key
	spannerKeyValues := make([]interface{}, len(keyValues))
	for i, val := range keyValues {
		// Keys are at the beginning of the columns list
		if i >= len(binding.columns) {
			return nil, fmt.Errorf("key index out of bounds: %d >= %d", i, len(binding.columns))
		}
		spannerVal, err := toSpannerValue(val, binding.columns[i])
		if err != nil {
			return nil, fmt.Errorf("converting key value for column %s: %w", binding.columns[i], err)
		}
		spannerKeyValues[i] = spannerVal
	}

	key := spanner.Key(spannerKeyValues)
	mutation := spanner.Delete(binding.target.Identifier, spanner.KeySetFromKeys(key))
	return mutation, nil
}

// toSpannerValue converts a Go value to a Spanner-compatible value.
// This handles type conversions needed for Spanner's type system.
func toSpannerValue(val interface{}, columnName string) (interface{}, error) {
	if val == nil {
		return spanner.NullString{Valid: false}, nil
	}

	switch v := val.(type) {
	case json.RawMessage:
		// JSON data - Spanner can store as JSON or STRING
		return string(v), nil
	case []byte:
		// Binary data - store as BYTES
		return v, nil
	case string:
		return v, nil
	case int, int8, int16, int32, int64:
		// All integers map to INT64
		return toInt64(v), nil
	case uint, uint8, uint16, uint32, uint64:
		// Unsigned integers need careful conversion
		return toInt64(v), nil
	case float32, float64:
		// All floats map to FLOAT64
		return toFloat64(v), nil
	case bool:
		return v, nil
	default:
		// For unknown types, convert to string
		return fmt.Sprintf("%v", v), nil
	}
}

// toInt64 converts various integer types to int64
func toInt64(val interface{}) int64 {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	default:
		return 0
	}
}

// toFloat64 converts various float types to float64
func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return 0
	}
}

// binding represents a materialized binding with its target table and columns
type binding struct {
	target  sql.Table
	columns []string
}

// storeOperation represents a single store operation (insert, update, or delete)
// Note: This type is currently unused but kept for future enhancements
type storeOperation struct {
	binding   int
	values    []interface{}
	keyValues []interface{}
}

// mutationBatch accumulates mutations for efficient batching
type mutationBatch struct {
	mutations []*spanner.Mutation
	byteSize  int // Approximate size in bytes
}

const (
	// Maximum mutations per commit (Spanner limit is 80,000, but we stay well below)
	// With indexes, the effective limit is lower (e.g., 1 index = 40,000 rows)
	maxMutationsPerBatch = 10000

	// Maximum byte size per batch (1-5 MB is recommended)
	maxBytesPerBatch = 4 * 1024 * 1024 // 4 MB

	// Estimated average bytes per mutation (conservative estimate)
	estimatedBytesPerMutation = 400
)

// addMutation adds a mutation to the batch
func (mb *mutationBatch) addMutation(m *spanner.Mutation) {
	mb.mutations = append(mb.mutations, m)
	mb.byteSize += estimatedBytesPerMutation
}

// shouldFlush returns true if the batch should be flushed
func (mb *mutationBatch) shouldFlush() bool {
	return len(mb.mutations) >= maxMutationsPerBatch || mb.byteSize >= maxBytesPerBatch
}

// reset clears the batch
func (mb *mutationBatch) reset() {
	mb.mutations = make([]*spanner.Mutation, 0, maxMutationsPerBatch)
	mb.byteSize = 0
}
