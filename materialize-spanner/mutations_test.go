package main

import (
	"math"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToSpannerValue_NullTypes(t *testing.T) {
	tests := []struct {
		columnType string
		expected   interface{}
	}{
		{"INT64", spanner.NullInt64{Valid: false}},
		{"INT64 NOT NULL", spanner.NullInt64{Valid: false}}, // Should still return NullInt64
		{"FLOAT64", spanner.NullFloat64{Valid: false}},
		{"BOOL", spanner.NullBool{Valid: false}},
		{"TIMESTAMP", spanner.NullTime{Valid: false}},
		{"DATE", spanner.NullDate{Valid: false}},
		{"JSON", spanner.NullJSON{Valid: false}},
		{"STRING", spanner.NullString{Valid: false}},
		{"STRING(MAX)", spanner.NullString{Valid: false}},
		{"BYTES", []byte(nil)},
		{"BYTES(1024)", []byte(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.columnType, func(t *testing.T) {
			result, err := toSpannerValue(nil, "test_col", tt.columnType)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, "Null value should return correct type for "+tt.columnType)
		})
	}
}

func TestToSpannerValue_UnknownTypeDefaultsToNullString(t *testing.T) {
	result, err := toSpannerValue(nil, "test_col", "UNKNOWN_TYPE")
	require.NoError(t, err)
	assert.Equal(t, spanner.NullString{Valid: false}, result)
}

func TestToSpannerValue_Uint64Overflow(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		error bool
	}{
		{"max int64", uint64(math.MaxInt64), false},
		{"max int64 + 1", uint64(math.MaxInt64) + 1, true},
		{"max uint64", uint64(math.MaxUint64), true},
		{"small value", uint64(100), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toSpannerValue(tt.value, "test_col", "INT64")
			if tt.error {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "exceeds Spanner INT64 maximum")
				assert.Contains(t, err.Error(), "test_col")
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, int64(tt.value), result)
			}
		})
	}
}

func TestPartitionedBatches_AddMutation_BoundsChecking(t *testing.T) {
	pb := newPartitionedBatches(5)
	mutation := spanner.Insert("test_table", []string{"col1"}, []interface{}{123})

	tests := []struct {
		name          string
		partitionIdx  int
		expectError   bool
		errorContains string
	}{
		{"negative index", -1, true, "partition index out of bounds"},
		{"zero index (valid)", 0, false, ""},
		{"middle index (valid)", 2, false, ""},
		{"max valid index", 4, false, ""},
		{"beyond max index", 5, true, "partition index out of bounds"},
		{"far beyond max", 100, true, "partition index out of bounds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pb.addMutation(tt.partitionIdx, mutation, 1)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPartitionedBatches_GetBatch_BoundsChecking(t *testing.T) {
	pb := newPartitionedBatches(5)

	tests := []struct {
		name          string
		partitionIdx  int
		expectError   bool
		errorContains string
	}{
		{"negative index", -1, true, "partition index out of bounds"},
		{"zero index (valid)", 0, false, ""},
		{"middle index (valid)", 2, false, ""},
		{"max valid index", 4, false, ""},
		{"beyond max index", 5, true, "partition index out of bounds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := pb.getBatch(tt.partitionIdx)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
				assert.Nil(t, batch)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, batch)
			}
		})
	}
}

func TestPartitionedBatches_Reset_BoundsChecking(t *testing.T) {
	pb := newPartitionedBatches(5)
	mutation := spanner.Insert("test_table", []string{"col1"}, []interface{}{123})

	// Add some mutations first
	require.NoError(t, pb.addMutation(0, mutation, 1))
	require.NoError(t, pb.addMutation(2, mutation, 1))

	tests := []struct {
		name          string
		partitionIdx  int
		expectError   bool
		errorContains string
	}{
		{"negative index", -1, true, "partition index out of bounds"},
		{"zero index (valid)", 0, false, ""},
		{"middle index (valid)", 2, false, ""},
		{"max valid index", 4, false, ""},
		{"beyond max index", 5, true, "partition index out of bounds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pb.reset(tt.partitionIdx)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPartitionedBatches_GetPartitionIndex(t *testing.T) {
	pb := newPartitionedBatches(10)

	tests := []struct {
		name     string
		hash     int64
		expected int
	}{
		{"positive hash", 12345, 5}, // 12345 % 10 = 5
		{"negative hash", -12345, 5}, // abs(-12345) % 10 = 5
		{"zero hash", 0, 0},
		{"exactly divisible", 100, 0}, // 100 % 10 = 0
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pb.getPartitionIndex(tt.hash)
			assert.Equal(t, tt.expected, result)
			// Verify result is always in valid range
			assert.GreaterOrEqual(t, result, 0)
			assert.Less(t, result, pb.numPartitions)
		})
	}
}
