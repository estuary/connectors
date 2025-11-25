package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeKeyHash_Float64(t *testing.T) {
	tests := []struct {
		name   string
		values []interface{}
	}{
		{"positive float", []interface{}{3.14}},
		{"negative float", []interface{}{-2.5}},
		{"zero", []interface{}{0.0}},
		{"multiple floats", []interface{}{1.1, 2.2, 3.3}},
		{"NaN", []interface{}{math.NaN()}},
		{"positive infinity", []interface{}{math.Inf(1)}},
		{"negative infinity", []interface{}{math.Inf(-1)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1, err := computeKeyHash(tt.values)
			require.NoError(t, err)

			hash2, err := computeKeyHash(tt.values)
			require.NoError(t, err)

			// Verify deterministic hashing
			assert.Equal(t, hash1, hash2, "Hash should be deterministic")

			// Verify hash is non-zero (except for special cases)
			if tt.name != "zero" {
				assert.NotEqual(t, int64(0), hash1, "Hash should be non-zero")
			}
		})
	}
}

func TestComputeKeyHash_DifferentFloatsProduceDifferentHashes(t *testing.T) {
	// Test that different float values produce different hashes
	hash1, err := computeKeyHash([]interface{}{3.14})
	require.NoError(t, err)

	hash2, err := computeKeyHash([]interface{}{3.15})
	require.NoError(t, err)

	hash3, err := computeKeyHash([]interface{}{3.1})
	require.NoError(t, err)

	// All hashes should be different
	assert.NotEqual(t, hash1, hash2, "3.14 and 3.15 should have different hashes")
	assert.NotEqual(t, hash1, hash3, "3.14 and 3.1 should have different hashes")
	assert.NotEqual(t, hash2, hash3, "3.15 and 3.1 should have different hashes")
}

func TestComputeKeyHash_JsonMarshalError(t *testing.T) {
	// Channels cannot be marshaled to JSON
	_, err := computeKeyHash([]interface{}{make(chan int)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "marshaling key value")
	assert.Contains(t, err.Error(), "chan int")
}

func TestComputeKeyHash_VariousTypes(t *testing.T) {
	tests := []struct {
		name   string
		values []interface{}
	}{
		{"int64", []interface{}{int64(123)}},
		{"string", []interface{}{"test"}},
		{"bytes", []interface{}{[]byte("test")}},
		{"bool true", []interface{}{true}},
		{"bool false", []interface{}{false}},
		{"nil", []interface{}{nil}},
		{"mixed types", []interface{}{int64(1), "test", 3.14, true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1, err := computeKeyHash(tt.values)
			require.NoError(t, err)

			hash2, err := computeKeyHash(tt.values)
			require.NoError(t, err)

			// Verify deterministic hashing
			assert.Equal(t, hash1, hash2, "Hash should be deterministic for "+tt.name)
		})
	}
}

func TestComputeKeyHash_OrderMatters(t *testing.T) {
	// Test that order of values affects hash
	hash1, err := computeKeyHash([]interface{}{int64(1), int64(2), int64(3)})
	require.NoError(t, err)

	hash2, err := computeKeyHash([]interface{}{int64(3), int64(2), int64(1)})
	require.NoError(t, err)

	assert.NotEqual(t, hash1, hash2, "Different order should produce different hash")
}
