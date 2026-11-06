package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// rawValue marshals v into the *bson.RawValue shape that backfillState.LastCursorValue holds.
func rawValue(t *testing.T, v any) *bson.RawValue {
	t.Helper()
	typ, data, err := bson.MarshalValue(v)
	require.NoError(t, err)
	return &bson.RawValue{Type: typ, Value: data}
}

func TestBuildResumeFilter(t *testing.T) {
	const cursorField = "_id"

	t.Run("a new backfill scans everything", func(t *testing.T) {
		filter, err := buildResumeFilter(cursorField, nil)
		require.NoError(t, err)
		require.Equal(t, bson.D{}, filter)
	})

	// For a supported, non-final bracket, the filter is an $or of: the remainder of the resume
	// value's own bracket ($gt), followed by the entirety of every bracket that sorts after it
	// ($gte bracketMin), in canonical order.
	supported := []struct {
		name string
		v    any
	}{
		{"number", int64(42)},
		{"string", "abc"},
		{"objectId", primitive.NewObjectID()},
		{"date", primitive.DateTime(1700000000000)},
	}
	for _, tc := range supported {
		t.Run("crosses type brackets from "+tc.name, func(t *testing.T) {
			last := rawValue(t, tc.v)
			filter, err := buildResumeFilter(cursorField, last)
			require.NoError(t, err)
			require.Len(t, filter, 1)
			require.Equal(t, "$or", filter[0].Key)

			clauses, ok := filter[0].Value.(bson.A)
			require.True(t, ok, "$or value should be a bson.A")

			idx := bracketIndex(last.Type)
			require.GreaterOrEqual(t, idx, 0)
			later := bsonOrder[idx+1:]
			require.NotEmpty(t, later, "test inputs must not be in the final bracket")
			require.Len(t, clauses, 1+len(later))

			// First clause: remainder of the resume value's own bracket.
			require.Equal(t, bson.D{{Key: cursorField, Value: bson.D{{Key: "$gt", Value: tc.v}}}}, clauses[0])

			// Remaining clauses: each later bracket in order, lower-bounded by its min sentinel.
			for i, b := range later {
				require.Equal(t,
					bson.D{{Key: cursorField, Value: bson.D{{Key: "$gte", Value: b.min}}}},
					clauses[i+1],
					"clause for later bracket %d", i,
				)
			}
		})
	}

	t.Run("final bracket falls back to plain $gt", func(t *testing.T) {
		// Timestamp is the last bracket in bsonOrder, so there are no later brackets to union.
		last := rawValue(t, primitive.Timestamp{T: 123, I: 1})
		filter, err := buildResumeFilter(cursorField, last)
		require.NoError(t, err)
		require.Equal(t, bson.D{{Key: cursorField, Value: bson.D{{Key: "$gt", Value: primitive.Timestamp{T: 123, I: 1}}}}}, filter)
	})

	t.Run("unsupported cursor types are rejected", func(t *testing.T) {
		// These types cannot be range-scanned (a regex operand is rejected by comparison operators,
		// arrays are multikey, MinKey/MaxKey/JavaScript are internal or deprecated), so
		// buildResumeFilter must fail loudly rather than emit a filter the server would mishandle.
		for _, v := range []any{
			primitive.Regex{Pattern: "^x", Options: ""},
			primitive.MinKey{},
			primitive.MaxKey{},
			primitive.JavaScript("function () {}"),
			bson.A{1, 2, 3},
		} {
			last := rawValue(t, v)
			require.Equal(t, -1, bracketIndex(last.Type), "type %q should be unsupported", last.Type)
			_, err := buildResumeFilter(cursorField, last)
			require.Error(t, err)
		}
	})
}
