package main

import (
	"math"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestCheckBackfillState(t *testing.T) {
	// Starting fresh, with an empty oplog
	var state = &captureState{}
	var oldestOplogEntry time.Time

	var updated, restartReason = checkBackfillState(state, oldestOplogEntry)
	require.True(t, updated)
	require.Equal(t, "", restartReason)
	require.True(t, *state.OplogEmptyBeforeBackfill)

	// Starting fresh with a non-empty oplog
	state = &captureState{}
	oldestOplogEntry = time.Unix(5000, 0).UTC()
	updated, restartReason = checkBackfillState(state, oldestOplogEntry)
	require.True(t, updated)
	require.Equal(t, "", restartReason)
	require.False(t, *state.OplogEmptyBeforeBackfill)

	// Resuming an in progress backfill, which started after the oldest oplog
	// event and had previously observed the non-empty oplog
	state.Resources = map[boilerplate.StateKey]resourceState{
		"testdb.testcollection": {
			Backfill: backfillState{
				Done:      false,
				StartedAt: time.Unix(6000, 0),
			},
		},
	}
	updated, restartReason = checkBackfillState(state, oldestOplogEntry)
	require.False(t, updated)
	require.Equal(t, "", restartReason)
	require.False(t, *state.OplogEmptyBeforeBackfill)

	// Resuming an in progress backfill, which started before the oldest oplog event
	oldestOplogEntry = time.Unix(8000, 0).UTC()
	updated, restartReason = checkBackfillState(state, oldestOplogEntry)
	require.False(t, updated)
	require.Equal(t, "the oldest event in the mongodb oplog (1970-01-01 02:13:20 +0000 UTC) is more recent than the starting time of the backfill", restartReason)

	// Resuming an in progress backfill, which had previously observed a non-empty oplog
	// and now observes an empty oplog
	oldestOplogEntry = time.Time{}
	updated, restartReason = checkBackfillState(state, oldestOplogEntry)
	require.False(t, updated)
	require.Equal(t, "previously non-empty change stream is now empty", restartReason)

	// Resuming an in progress backfill, which had previously observed an empty oplog
	// and now again observes it being empty
	var superHighTechTrue = true
	state.OplogEmptyBeforeBackfill = &superHighTechTrue
	updated, restartReason = checkBackfillState(state, oldestOplogEntry)
	require.False(t, updated)
	require.Equal(t, "", restartReason)
	require.True(t, *state.OplogEmptyBeforeBackfill)
}

func TestSanitizeDocument(t *testing.T) {
	var cases = map[string][]map[string]interface{}{
		"primitive": []map[string]interface{}{
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
		"nested": []map[string]interface{}{
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
		"array": []map[string]interface{}{
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
