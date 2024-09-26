package main

import (
	"math"
	"testing"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestSanitizeDocument(t *testing.T) {
	var cases = map[string][]map[string]interface{}{
		"primitive": {
			map[string]interface{}{
				"x":           math.NaN(),
				"y":           "untouched",
				"z":           1,
				"inf":         math.Inf(1),
				"infnegative": math.Inf(-1),
				"highDate":    primitive.DateTime(maxTimeMilli + 1),
				"lowDate":     primitive.DateTime(minTimeMilli - 1),
			},
			map[string]interface{}{
				"x":           "NaN",
				"y":           "untouched",
				"z":           1,
				"inf":         "Infinity",
				"infnegative": "-Infinity",
				"highDate":    primitive.DateTime(maxTimeMilli),
				"lowDate":     primitive.DateTime(minTimeMilli),
			},
		},
		"nested": {
			map[string]interface{}{
				"obj": map[string]interface{}{
					"x":           math.NaN(),
					"y":           "untouched",
					"z":           1,
					"inf":         math.Inf(1),
					"infnegative": math.Inf(-1),
					"highDate":    primitive.DateTime(maxTimeMilli + 1),
					"lowDate":     primitive.DateTime(minTimeMilli - 1),
				},
			},
			map[string]interface{}{
				"obj": map[string]interface{}{
					"x":           "NaN",
					"y":           "untouched",
					"z":           1,
					"inf":         "Infinity",
					"infnegative": "-Infinity",
					"highDate":    primitive.DateTime(maxTimeMilli),
					"lowDate":     primitive.DateTime(minTimeMilli),
				},
			},
		},
		"array": {
			map[string]interface{}{
				"arr": []interface{}{math.NaN(), "untouched", 1, math.Inf(1), math.Inf(-1), primitive.DateTime(maxTimeMilli + 1), primitive.DateTime(minTimeMilli - 1)},
			},
			map[string]interface{}{
				"arr": []interface{}{"NaN", "untouched", 1, "Infinity", "-Infinity", primitive.DateTime(maxTimeMilli), primitive.DateTime(minTimeMilli)},
			},
		},
	}

	for name, cs := range cases {
		require.Equal(t, cs[1], sanitizeDocument(cs[0]), name)
	}
}

func TestUpdateResourceStates(t *testing.T) {
	prevState := captureState{
		Resources: map[boilerplate.StateKey]resourceState{
			"sk1": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("first")}}},
			"sk2": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("second")}}},
			"sk3": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("third")}}},
			"sk4": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("fourth")}}},
			"sk5": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("fifth")}}},
		},
		DatabaseResumeTokens: map[string]bson.Raw{
			"firstDb":  bson.Raw("firstDbToken"),
			"secondDb": bson.Raw("secondDbToken"),
		},
	}

	binding1 := bindingInfo{index: 0, stateKey: "sk1", resource: resource{Database: "firstDb", Collection: "firstDbCollection1"}}
	binding2 := bindingInfo{index: 1, stateKey: "sk2", resource: resource{Database: "firstDb", Collection: "firstDbCollection2"}}
	binding3 := bindingInfo{index: 2, stateKey: "sk3", resource: resource{Database: "secondDb", Collection: "secondDbCollection1"}}
	binding4 := bindingInfo{index: 3, stateKey: "sk4", resource: resource{Database: "firstDb", Collection: "firstDbBatchCollection", Mode: captureModeSnapshot}}
	binding5 := bindingInfo{index: 4, stateKey: "sk5", resource: resource{Database: "secondDb", Collection: "secondDbBatchCollection", Mode: captureModeIncremental}}

	t.Run("all bindings are included", func(t *testing.T) {
		bindings := []bindingInfo{binding1, binding2, binding3, binding4, binding5}
		got, err := updateResourceStates(prevState, bindings)
		require.NoError(t, err)
		require.Equal(t, prevState, got)
	})

	t.Run("first database change stream bindings are excluded", func(t *testing.T) {
		bindings := []bindingInfo{binding3, binding4, binding5}

		want := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				"sk3": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("third")}}},
				"sk4": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("fourth")}}},
				"sk5": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("fifth")}}},
			},
			DatabaseResumeTokens: map[string]bson.Raw{
				"secondDb": bson.Raw("secondDbToken"),
			},
		}

		got, err := updateResourceStates(prevState, bindings)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("excluded batch bindings also get reset", func(t *testing.T) {
		// It's not strictly necessary that batch bindings get reset if they are
		// excluded, but this is how other batch captures work, and it makes the
		// connector code more simple. We could reconsider this behavior in the
		// future if there is a need to.
		bindings := []bindingInfo{binding1, binding2, binding3}

		want := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				"sk1": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("first")}}},
				"sk2": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("second")}}},
				"sk3": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("third")}}},
			},
			DatabaseResumeTokens: map[string]bson.Raw{
				"firstDb":  bson.Raw("firstDbToken"),
				"secondDb": bson.Raw("secondDbToken"),
			},
		}

		got, err := updateResourceStates(prevState, bindings)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("reset one database via state key updates", func(t *testing.T) {
		bindings := []bindingInfo{binding1, binding2, binding3, binding4, binding5}
		bindings[0].stateKey = "sk1.v1"
		bindings[1].stateKey = "sk2.v1"

		want := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				"sk3": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("third")}}},
				"sk4": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("fourth")}}},
				"sk5": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("fifth")}}},
			},
			DatabaseResumeTokens: map[string]bson.Raw{
				"secondDb": bson.Raw("secondDbToken"),
			},
		}

		got, err := updateResourceStates(prevState, bindings)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("reset all databases via state key updates", func(t *testing.T) {
		bindings := []bindingInfo{binding1, binding2, binding3, binding4, binding5}
		bindings[0].stateKey = "sk1.v1"
		bindings[1].stateKey = "sk2.v1"
		bindings[2].stateKey = "sk3.v1"

		want := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				"sk4": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("fourth")}}},
				"sk5": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("fifth")}}},
			},
			DatabaseResumeTokens: map[string]bson.Raw{},
		}

		got, err := updateResourceStates(prevState, bindings)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}
