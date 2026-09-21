package main

import (
	"testing"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestSkipConfiguredBackfills(t *testing.T) {
	// sk1 has never been backfilled, sk2 is partway through, and sk3 is finished.
	initialState := func() captureState {
		return captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				"sk2": {Backfill: backfillState{Done: makePtr(false), LastCursorValue: &bson.RawValue{Value: []byte("second")}, BackfilledDocs: 100}},
				"sk3": {Backfill: backfillState{Done: makePtr(true), LastCursorValue: &bson.RawValue{Value: []byte("third")}, BackfilledDocs: 300}},
			},
			DatabaseResumeTokens: map[string]bson.Raw{"db": bson.Raw("dbToken")},
		}
	}

	changeStreamBindings := []bindingInfo{
		{index: 0, stateKey: "sk1", resource: resource{Database: "db", Collection: "one"}},
		{index: 1, stateKey: "sk2", resource: resource{Database: "db", Collection: "two"}},
		{index: 2, stateKey: "sk3", resource: resource{Database: "db", Collection: "three"}},
	}

	t.Run("unset leaves state untouched", func(t *testing.T) {
		state := initialState()
		skipConfiguredBackfills(&state, changeStreamBindings, &config{})
		require.Equal(t, initialState(), state)
	})

	t.Run("a never-started backfill is marked done", func(t *testing.T) {
		state := initialState()
		cfg := &config{Advanced: advancedConfig{SkipBackfills: "db:one"}}
		skipConfiguredBackfills(&state, changeStreamBindings, cfg)

		want := initialState()
		want.Resources["sk1"] = resourceState{Backfill: backfillState{Done: makePtr(true)}}
		require.Equal(t, want, state)
	})

	t.Run("an in-progress backfill is terminated and its resume position discarded", func(t *testing.T) {
		state := initialState()
		cfg := &config{Advanced: advancedConfig{SkipBackfills: "db:two"}}
		skipConfiguredBackfills(&state, changeStreamBindings, cfg)

		want := initialState()
		// BackfilledDocs is retained as a record of what was already emitted.
		want.Resources["sk2"] = resourceState{Backfill: backfillState{Done: makePtr(true), BackfilledDocs: 100}}
		require.Equal(t, want, state)
	})

	t.Run("a completed backfill keeps its resume position", func(t *testing.T) {
		state := initialState()
		cfg := &config{Advanced: advancedConfig{SkipBackfills: "db:three"}}
		skipConfiguredBackfills(&state, changeStreamBindings, cfg)
		require.Equal(t, initialState(), state)
	})

	t.Run("the wildcard skips every binding", func(t *testing.T) {
		state := initialState()
		cfg := &config{Advanced: advancedConfig{SkipBackfills: skipAllBackfills}}
		skipConfiguredBackfills(&state, changeStreamBindings, cfg)

		want := initialState()
		want.Resources["sk1"] = resourceState{Backfill: backfillState{Done: makePtr(true)}}
		want.Resources["sk2"] = resourceState{Backfill: backfillState{Done: makePtr(true), BackfilledDocs: 100}}
		require.Equal(t, want, state)
		require.True(t, state.isChangeStreamBackfillComplete(changeStreamBindings))
	})

	t.Run("batch bindings are never skipped", func(t *testing.T) {
		state := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				"sk4": {Backfill: backfillState{Done: makePtr(false), LastCursorValue: &bson.RawValue{Value: []byte("fourth")}}},
			},
		}
		cfg := &config{Advanced: advancedConfig{SkipBackfills: skipAllBackfills}}
		skipConfiguredBackfills(&state, nil, cfg)

		require.Equal(t, captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				"sk4": {Backfill: backfillState{Done: makePtr(false), LastCursorValue: &bson.RawValue{Value: []byte("fourth")}}},
			},
		}, state)
	})
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
