package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"cloud.google.com/go/civil"
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
		// Note: This function is not currently used. Column type is unknown here.
		spannerVal, err := toSpannerValue(val, binding.columns[i], "")
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
		// Note: This function is not currently used. Column type is unknown here.
		spannerVal, err := toSpannerValue(val, binding.columns[i], "")
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
// columnType is the DDL type (e.g., "TIMESTAMP", "DATE", "INT64", "JSON")
func toSpannerValue(val interface{}, columnName string, columnType string) (interface{}, error) {
	if val == nil {
		return spanner.NullString{Valid: false}, nil
	}

	switch v := val.(type) {
	case json.RawMessage:
		// JSON data - Spanner expects spanner.NullJSON for JSON columns
		return spanner.NullJSON{Value: v, Valid: len(v) > 0}, nil
	case []byte:
		// Check if this might be JSON data (common for arrays/objects)
		// If it starts with [ or {, treat it as JSON
		if len(v) > 0 && (v[0] == '[' || v[0] == '{' || v[0] == '"') {
			return spanner.NullJSON{Value: v, Valid: true}, nil
		}
		// Otherwise, binary data - store as BYTES
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
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		// uint64 needs careful handling to avoid overflow
		if v > 9223372036854775807 { // math.MaxInt64
			return fmt.Sprintf("%d", v), nil // Store as string if too large
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
