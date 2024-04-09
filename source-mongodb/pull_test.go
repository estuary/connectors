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
			"sk1": {Backfill: backfillState{Done: true, LastId: bson.RawValue{Value: []byte("first")}}},
			"sk2": {Backfill: backfillState{Done: true, LastId: bson.RawValue{Value: []byte("second")}}},
			"sk3": {Backfill: backfillState{Done: true, LastId: bson.RawValue{Value: []byte("third")}}},
		},
		DatabaseResumeTokens: map[string]bson.Raw{
			"firstDb":  bson.Raw("firstDbToken"),
			"secondDb": bson.Raw("secondDbToken"),
		},
	}

	binding1 := bindingInfo{
		resource: resource{
			Database:   "firstDb",
			Collection: "firstDbCollection1",
		},
		index:    0,
		stateKey: "sk1",
	}

	binding2 := bindingInfo{
		resource: resource{
			Database:   "firstDb",
			Collection: "firstDbCollection2",
		},
		index:    1,
		stateKey: "sk2",
	}

	binding3 := bindingInfo{
		resource: resource{
			Database:   "secondDb",
			Collection: "secondDbCollection1",
		},
		index:    2,
		stateKey: "sk3",
	}

	t.Run("all bindings are included", func(t *testing.T) {
		bindings := []bindingInfo{binding1, binding2, binding3}
		got, err := updateResourceStates(prevState, bindings)
		require.NoError(t, err)
		require.Equal(t, prevState, got)
	})

	t.Run("reset one database via state key updates", func(t *testing.T) {
		bindings := []bindingInfo{binding1, binding2, binding3}
		bindings[0].stateKey = "sk1.v1"
		bindings[1].stateKey = "sk2.v1"

		want := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				"sk3": {Backfill: backfillState{Done: true, LastId: bson.RawValue{Value: []byte("third")}}},
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
		bindings := []bindingInfo{binding1, binding2, binding3}
		bindings[0].stateKey = "sk1.v1"
		bindings[1].stateKey = "sk2.v1"
		bindings[2].stateKey = "sk3.v1"

		want := captureState{
			Resources:            map[boilerplate.StateKey]resourceState{},
			DatabaseResumeTokens: map[string]bson.Raw{},
		}

		got, err := updateResourceStates(prevState, bindings)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}
