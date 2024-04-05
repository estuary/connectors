package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
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
