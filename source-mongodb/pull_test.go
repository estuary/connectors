package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	state.Resources = map[string]resourceState{
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
