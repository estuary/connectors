package main

import (
	"encoding/json"
	"math"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
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

func TestToSpannerValue_Uint64ToNumeric(t *testing.T) {
	// Test that uint64 values beyond INT64 range can be stored in NUMERIC columns
	tests := []struct {
		name     string
		value    uint64
		expected *big.Rat
	}{
		{"small value", uint64(100), big.NewRat(100, 1)},
		{"max int64", uint64(math.MaxInt64), big.NewRat(math.MaxInt64, 1)},
		{"max int64 + 1", uint64(math.MaxInt64) + 1, big.NewRat(0, 1).SetUint64(uint64(math.MaxInt64) + 1)},
		{"max uint64", uint64(math.MaxUint64), big.NewRat(0, 1).SetUint64(math.MaxUint64)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toSpannerValue(tt.value, "test_col", "NUMERIC")
			require.NoError(t, err)
			rat, ok := result.(*big.Rat)
			require.True(t, ok, "expected *big.Rat, got %T", result)
			assert.Equal(t, 0, tt.expected.Cmp(rat), "expected %s, got %s", tt.expected.String(), rat.String())
		})
	}
}

func TestToSpannerValue_IntegerToFloat64(t *testing.T) {
	// Test that integer values are properly converted to float64 for FLOAT64 columns.
	// This is important because Spanner requires float64 values for FLOAT64 columns,
	// and integer values in JSON may arrive as int64 when they have no decimal point.
	tests := []struct {
		name       string
		value      interface{}
		columnType string
		expected   float64
	}{
		{"int64 to FLOAT64", int64(220), "FLOAT64", 220.0},
		{"int64 to FLOAT64 NOT NULL", int64(220), "FLOAT64 NOT NULL", 220.0},
		{"int to FLOAT64", int(100), "FLOAT64", 100.0},
		{"int32 to FLOAT64", int32(500), "FLOAT64", 500.0},
		{"int8 to FLOAT64", int8(42), "FLOAT64", 42.0},
		{"int16 to FLOAT64", int16(1000), "FLOAT64", 1000.0},
		{"uint64 to FLOAT64", uint64(300), "FLOAT64", 300.0},
		{"uint to FLOAT64", uint(200), "FLOAT64", 200.0},
		{"uint32 to FLOAT64", uint32(400), "FLOAT64", 400.0},
		{"uint8 to FLOAT64", uint8(128), "FLOAT64", 128.0},
		{"uint16 to FLOAT64", uint16(2000), "FLOAT64", 2000.0},
		{"negative int64 to FLOAT64", int64(-500), "FLOAT64", -500.0},
		{"large int64 to FLOAT64", int64(9007199254740992), "FLOAT64", 9007199254740992.0}, // 2^53
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toSpannerValue(tt.value, "score", tt.columnType)
			require.NoError(t, err)
			f64, ok := result.(float64)
			require.True(t, ok, "expected float64, got %T", result)
			assert.Equal(t, tt.expected, f64)
		})
	}
}

func TestToSpannerValue_IntegerStaysInt64ForNonFloat(t *testing.T) {
	// Test that integer values remain int64 for INT64 columns
	tests := []struct {
		name       string
		value      interface{}
		columnType string
		expected   int64
	}{
		{"int64 to INT64", int64(220), "INT64", 220},
		{"int64 to INT64 NOT NULL", int64(220), "INT64 NOT NULL", 220},
		{"int to INT64", int(100), "INT64", 100},
		{"int32 to INT64", int32(500), "INT64", 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toSpannerValue(tt.value, "count", tt.columnType)
			require.NoError(t, err)
			i64, ok := result.(int64)
			require.True(t, ok, "expected int64, got %T", result)
			assert.Equal(t, tt.expected, i64)
		})
	}
}

func TestPartitionedBatches_AddMutation_BoundsChecking(t *testing.T) {
	pb := newPartitionedBatches(5)
	values := []interface{}{int64(123)}
	mutation := spanner.Insert("test_table", []string{"col1"}, values)
	mutationSize := calculateMutationByteSize(values)

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
			err := pb.addMutation(tt.partitionIdx, mutation, 1, mutationSize)
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
	values := []interface{}{int64(123)}
	mutation := spanner.Insert("test_table", []string{"col1"}, values)
	mutationSize := calculateMutationByteSize(values)

	// Add some mutations first
	require.NoError(t, pb.addMutation(0, mutation, 1, mutationSize))
	require.NoError(t, pb.addMutation(2, mutation, 1, mutationSize))

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

func TestCalculateValueSize(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected int
	}{
		// Nil values
		{"nil", nil, 1},

		// Primitives
		{"int64", int64(123), 8},
		{"float64", float64(123.456), 8},
		{"bool", true, 1},

		// Variable-length types
		{"empty string", "", 0},
		{"short string", "hello", 5},
		{"long string", "hello world with more text", 26},
		{"empty bytes", []byte{}, 0},
		{"bytes", []byte{1, 2, 3, 4, 5}, 5},

		// Time types
		{"time.Time", time.Now(), 24},
		{"civil.Date", civil.Date{Year: 2024, Month: 1, Day: 15}, 12},

		// Spanner Null types - invalid
		{"NullInt64 invalid", spanner.NullInt64{Valid: false}, 1},
		{"NullFloat64 invalid", spanner.NullFloat64{Valid: false}, 1},
		{"NullBool invalid", spanner.NullBool{Valid: false}, 1},
		{"NullString invalid", spanner.NullString{Valid: false}, 1},
		{"NullTime invalid", spanner.NullTime{Valid: false}, 1},
		{"NullDate invalid", spanner.NullDate{Valid: false}, 1},
		{"NullJSON invalid", spanner.NullJSON{Valid: false}, 1},

		// Spanner Null types - valid
		{"NullInt64 valid", spanner.NullInt64{Valid: true, Int64: 123}, 9},
		{"NullFloat64 valid", spanner.NullFloat64{Valid: true, Float64: 123.456}, 9},
		{"NullBool valid", spanner.NullBool{Valid: true, Bool: true}, 2},
		{"NullString valid", spanner.NullString{Valid: true, StringVal: "test"}, 5},
		{"NullTime valid", spanner.NullTime{Valid: true, Time: time.Now()}, 25},
		{"NullDate valid", spanner.NullDate{Valid: true, Date: civil.Date{Year: 2024, Month: 1, Day: 15}}, 13},

		// JSON types
		{"json.RawMessage empty", json.RawMessage(""), 0},
		{"json.RawMessage small", json.RawMessage(`{"key":"value"}`), 15},
		{"json.RawMessage large", json.RawMessage(`{"name":"John","age":30,"city":"New York"}`), 42},
		{"NullJSON with bytes", spanner.NullJSON{Valid: true, Value: []byte(`{"test":123}`)}, 13},
		{"NullJSON with string", spanner.NullJSON{Valid: true, Value: "test"}, 5},
		{"NullJSON with RawMessage", spanner.NullJSON{Valid: true, Value: json.RawMessage(`{"a":1}`)}, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateValueSize(tt.value)
			assert.Equal(t, tt.expected, result, "Size mismatch for %s", tt.name)
		})
	}
}

func TestCalculateValueSize_UnknownType(t *testing.T) {
	// Test that unknown types fallback to conservative estimate
	type unknownType struct {
		field string
	}
	result := calculateValueSize(unknownType{field: "test"})
	assert.Equal(t, 100, result, "Unknown types should return 100 byte estimate")
}

func TestCalculateMutationByteSize(t *testing.T) {
	tests := []struct {
		name     string
		values   []interface{}
		expected int
	}{
		{
			name:     "empty values",
			values:   []interface{}{},
			expected: 0,
		},
		{
			name:     "single int64",
			values:   []interface{}{int64(123)},
			expected: 8,
		},
		{
			name:     "multiple primitives",
			values:   []interface{}{int64(123), float64(456.789), true},
			expected: 8 + 8 + 1,
		},
		{
			name:     "with strings",
			values:   []interface{}{int64(1), "hello", "world"},
			expected: 8 + 5 + 5,
		},
		{
			name:     "with nil values",
			values:   []interface{}{int64(1), nil, "test"},
			expected: 8 + 1 + 4,
		},
		{
			name:     "with JSON",
			values:   []interface{}{int64(1), json.RawMessage(`{"key":"value"}`), "test"},
			expected: 8 + 15 + 4,
		},
		{
			name: "complex mutation",
			values: []interface{}{
				int64(123),                    // 8
				"user@example.com",            // 16
				true,                          // 1
				json.RawMessage(`{"age":30}`), // 10
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), // 24
			},
			expected: 8 + 16 + 1 + 10 + 24,
		},
		{
			name: "with Spanner null types",
			values: []interface{}{
				spanner.NullInt64{Valid: true, Int64: 100},     // 9
				spanner.NullString{Valid: true, StringVal: "x"}, // 2
				spanner.NullBool{Valid: false},                 // 1
			},
			expected: 9 + 2 + 1,
		},
		{
			name: "large document",
			values: []interface{}{
				int64(1),
				"key",
				json.RawMessage(`{"name":"John Doe","email":"john@example.com","address":{"street":"123 Main St","city":"New York","zip":"10001"},"tags":["user","active","premium"]}`),
			},
			expected: 8 + 3 + 148,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateMutationByteSize(tt.values)
			assert.Equal(t, tt.expected, result, "Size mismatch for %s", tt.name)
		})
	}
}

func TestCalculateMutationByteSize_AccuracyVsEstimate(t *testing.T) {
	// This test demonstrates that the actual calculation is more accurate than
	// the old fixed estimate of 100 bytes per column

	tests := []struct {
		name              string
		values            []interface{}
		oldEstimate       int // numColumns * 100
		actualSize        int
		accuracyImproved bool
	}{
		{
			name:              "small values - old estimate too high",
			values:            []interface{}{int64(1), int64(2), int64(3)},
			oldEstimate:       3 * 100, // 300
			actualSize:        3 * 8,   // 24
			accuracyImproved: true,
		},
		{
			name:              "mixed small and medium - old estimate still too high",
			values:            []interface{}{int64(1), "test", true, float64(1.5)},
			oldEstimate:       4 * 100,    // 400
			actualSize:        8 + 4 + 1 + 8, // 21
			accuracyImproved: true,
		},
		{
			name: "large JSON - old estimate too low",
			values: []interface{}{
				int64(1),
				json.RawMessage(`{"very":"large","json":"document","with":"many","fields":"and","nested":{"objects":"here","array":[1,2,3,4,5]}}`),
			},
			oldEstimate:       2 * 100, // 200
			actualSize:        8 + 111, // 119
			accuracyImproved: false, // Old estimate was actually closer in this case
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateMutationByteSize(tt.values)
			assert.Equal(t, tt.actualSize, result)

			// The new calculation is different from the old estimate
			assert.NotEqual(t, tt.oldEstimate, result)

			t.Logf("Old estimate: %d bytes, Actual: %d bytes, Difference: %d bytes",
				tt.oldEstimate, result, tt.oldEstimate-result)
		})
	}
}
