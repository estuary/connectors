package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// This is representative of the v1 connector state.
type oldState struct {
	Resources         map[boilerplate.StateKey]resourceState `json:"bindingStateV1,omitempty"`
	StreamResumeToken bson.Raw                               `json:"stream_resume_token,omitempty"`
}

func TestDecodeState(t *testing.T) {
	makeStateJson := func(t *testing.T, resumeToken bson.Raw, resources map[boilerplate.StateKey]resourceState) json.RawMessage {
		t.Helper()

		s := oldState{
			Resources:         resources,
			StreamResumeToken: resumeToken,
		}

		j, err := json.Marshal(s)
		require.NoError(t, err)

		return j
	}

	testResources := map[boilerplate.StateKey]resourceState{
		"something": {
			Backfill: backfillState{Done: true, LastId: bson.RawValue{Value: []byte("first")}},
		},
		"other": {
			Backfill: backfillState{Done: true, LastId: bson.RawValue{Value: []byte("second")}},
		},
	}

	testBindings := []bindingInfo{
		{
			resource: resource{Database: "db", Collection: "coll1"},
			index:    0,
			stateKey: "something",
		},
		{
			resource: resource{Database: "db", Collection: "coll2"},
			index:    1,
			stateKey: "other",
		},
	}

	testToken := bson.Raw(`{"_data": "82661403CF0000004C2B0429296E1404"}`)

	tests := []struct {
		name            string
		globalStream    bool
		oldResumeToken  bson.Raw
		oldResources    map[boilerplate.StateKey]resourceState
		bindings        []bindingInfo
		wantState       captureState
		wantGlobalToken *bson.Raw
	}{
		{
			name:            "no resources",
			globalStream:    false,
			oldResumeToken:  nil,
			oldResources:    make(map[boilerplate.StateKey]resourceState),
			bindings:        []bindingInfo{},
			wantState:       captureState{},
			wantGlobalToken: nil,
		},
		{
			name:           "resources all backfilled",
			globalStream:   false,
			oldResumeToken: testToken,
			oldResources:   testResources,
			bindings: []bindingInfo{
				{
					resource: resource{Database: "db", Collection: "coll1"},
					index:    0,
					stateKey: "something.v1",
				},
				{
					resource: resource{Database: "db", Collection: "coll2"},
					index:    1,
					stateKey: "other.v1",
				},
			},
			wantState:       captureState{},
			wantGlobalToken: nil,
		},
		{
			name:           "no persisted stream_resume_token",
			globalStream:   false,
			oldResumeToken: nil,
			oldResources:   testResources,
			bindings:       testBindings,
			wantState: captureState{
				Resources: testResources,
			},
			wantGlobalToken: nil,
		},
		{
			name:           "global resume token migration",
			globalStream:   true,
			oldResumeToken: testToken,
			oldResources:   testResources,
			bindings:       testBindings,
			wantState: captureState{
				Resources:            testResources,
				DatabaseResumeTokens: map[string]bson.Raw{},
			},
			wantGlobalToken: &testToken,
		},
		{
			name:           "database resume token migration",
			globalStream:   false,
			oldResumeToken: testToken,
			oldResources:   testResources,
			bindings:       testBindings,
			wantState: captureState{
				Resources: testResources,
				DatabaseResumeTokens: map[string]bson.Raw{
					"db": testToken,
				},
			},
			wantGlobalToken: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotState, gotGlobalToken, err := decodeState(tt.globalStream, makeStateJson(t, tt.oldResumeToken, tt.oldResources), tt.bindings)
			require.NoError(t, err)
			require.Equal(t, tt.wantState, gotState)
			require.Equal(t, tt.wantGlobalToken, gotGlobalToken)
		})
	}
}

func TestPopulateDatabaseResumeTokens(t *testing.T) {
	db1 := "test_db_1"
	db2 := "test_db_2"
	db3 := "test_db_3"

	col1 := "collectionOne"
	col2 := "collectionTwo"
	col3 := "collectionThree"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		require.NoError(t, client.Database(db1).Drop(ctx))
		require.NoError(t, client.Database(db2).Drop(ctx))
		require.NoError(t, client.Database(db3).Drop(ctx))
	}
	cleanup()

	addDoc := func(t *testing.T, db string, collection string, val string) {
		t.Helper()

		_, err := client.Database(db).Collection(collection).InsertOne(ctx, map[string]any{"_id": val})
		require.NoError(t, err)
	}

	makeSk := func(db, col string) boilerplate.StateKey {
		return boilerplate.StateKey(url.QueryEscape(db + "/" + col))
	}

	setup := func(t *testing.T) json.RawMessage {
		t.Helper()

		cs, err := client.Watch(ctx, mongo.Pipeline{})
		require.NoError(t, err)

		// Get an initial resume token, which will be a post-batch resume token, so we can get a resume
		// token based on an item ID later.
		cs.TryNext(ctx)
		require.NoError(t, cs.Err())
		require.NoError(t, cs.Close(ctx))
		initialToken := cs.ResumeToken()
		require.NoError(t, cs.Close(ctx))

		// Add some documents to seed the initial resume token.
		addDoc(t, db1, col1, "collection_one_first_value")
		addDoc(t, db2, col2, "collection_two_first_value")
		addDoc(t, db3, col3, "collection_three_first_value")

		// Get the initial resume token for the global stream.
		cs, err = client.Watch(ctx, mongo.Pipeline{}, options.ChangeStream().SetResumeAfter(initialToken))
		require.NoError(t, err)

		seenDocs := 0
		for {
			for cs.TryNext(ctx) {
				seenDocs += 1
			}
			require.NoError(t, cs.Err())
			if seenDocs == 3 {
				break
			}
		}
		globalToken := cs.ResumeToken()
		require.NoError(t, cs.Close(ctx))

		// We are only testing the change stream migration and resumption, so initialize the state to
		// make it look like the backfills are all done.
		initialState := oldState{
			Resources: map[boilerplate.StateKey]resourceState{
				makeSk(db1, col1): {Backfill: backfillState{Done: true}},
				makeSk(db2, col2): {Backfill: backfillState{Done: true}},
				makeSk(db3, col3): {Backfill: backfillState{Done: true}},
			},
			StreamResumeToken: globalToken,
		}

		stateJson, err := json.Marshal(initialState)
		require.NoError(t, err)

		return stateJson
	}

	t.Run("doesn't re-capture documents", func(t *testing.T) {
		t.Cleanup(cleanup)

		capture := &st.CaptureSpec{
			Driver:       &driver{},
			EndpointSpec: &cfg,
			Checkpoint:   setup(t),
			Validator:    &st.SortedCaptureValidator{},
			Sanitizers:   commonSanitizers(),
			Bindings: []*flow.CaptureSpec_Binding{
				makeBinding(t, db1, col1),
				makeBinding(t, db2, col2),
				makeBinding(t, db3, col3),
			},
		}

		advanceCapture(ctx, t, capture)

		cupaloy.SnapshotT(t, capture.Summary())
	})

	t.Run("captures new documents", func(t *testing.T) {
		t.Cleanup(cleanup)

		capture := &st.CaptureSpec{
			Driver:       &driver{},
			EndpointSpec: &cfg,
			Checkpoint:   setup(t),
			Validator:    &st.SortedCaptureValidator{},
			Sanitizers:   commonSanitizers(),
			Bindings: []*flow.CaptureSpec_Binding{
				makeBinding(t, db1, col1),
				makeBinding(t, db2, col2),
				makeBinding(t, db3, col3),
			},
		}

		// These documents will be emitted as part of the migration process.
		addDoc(t, db1, col1, "collection_one_second_value")
		addDoc(t, db2, col2, "collection_two_second_value")
		addDoc(t, db3, col3, "collection_three_second_value")

		advanceCapture(ctx, t, capture)

		// These will be emitted from change stream resumption.
		addDoc(t, db1, col1, "collection_one_third_value")
		addDoc(t, db2, col2, "collection_two_third_value")
		addDoc(t, db3, col3, "collection_three_third_value")

		advanceCapture(ctx, t, capture)

		cupaloy.SnapshotT(t, capture.Summary())
	})

	t.Run("captures new documents from pbrt resumption", func(t *testing.T) {
		t.Cleanup(cleanup)

		capture := &st.CaptureSpec{
			Driver:       &driver{},
			EndpointSpec: &cfg,
			Checkpoint:   setup(t),
			Validator:    &st.SortedCaptureValidator{},
			Sanitizers:   commonSanitizers(),
			Bindings: []*flow.CaptureSpec_Binding{
				makeBinding(t, db1, col1),
				makeBinding(t, db2, col2),
				makeBinding(t, db3, col3),
			},
		}

		advanceCapture(ctx, t, capture)

		// These will be emitted from change stream resumption.
		addDoc(t, db1, col1, "collection_one_second_value")
		addDoc(t, db2, col2, "collection_two_second_value")
		addDoc(t, db3, col3, "collection_three_second_value")

		advanceCapture(ctx, t, capture)

		cupaloy.SnapshotT(t, capture.Summary())
	})

	t.Run("migrates with uncaptured database change", func(t *testing.T) {
		t.Cleanup(cleanup)

		capture := &st.CaptureSpec{
			Driver:       &driver{},
			EndpointSpec: &cfg,
			Checkpoint:   setup(t),
			Validator:    &st.SortedCaptureValidator{},
			Sanitizers:   commonSanitizers(),
			Bindings: []*flow.CaptureSpec_Binding{
				makeBinding(t, db1, col1),
				makeBinding(t, db2, col2),
				// makeBinding(t, db3, col3),
			},
		}

		// The state migrate will complete with this document being received from the change stream,
		// although it is not applicable to any of the captured databases.
		addDoc(t, db3, col3, "collection_three_second_value")

		advanceCapture(ctx, t, capture)

		// These documents will be captured after the migration has completed.
		addDoc(t, db1, col1, "collection_one_second_value")
		addDoc(t, db2, col2, "collection_two_second_value")

		advanceCapture(ctx, t, capture)

		cupaloy.SnapshotT(t, capture.Summary())
	})

	t.Run("migrates with an empty database", func(t *testing.T) {
		t.Cleanup(cleanup)
		t.Cleanup(func() {
			require.NoError(t, client.Database("other_db").Drop(ctx))
		})

		capture := &st.CaptureSpec{
			Driver:       &driver{},
			EndpointSpec: &cfg,
			Checkpoint:   setup(t),
			Validator:    &st.SortedCaptureValidator{},
			Sanitizers:   commonSanitizers(),
			Bindings: []*flow.CaptureSpec_Binding{
				makeBinding(t, db1, col1),
				makeBinding(t, db2, col2),
				makeBinding(t, db3, col3),
				makeBinding(t, "other_db", "other_collection"),
			},
		}

		advanceCapture(ctx, t, capture)

		// These will be emitted from change stream resumption.
		addDoc(t, db1, col1, "collection_one_second_value")
		addDoc(t, db2, col2, "collection_two_second_value")
		addDoc(t, db3, col3, "collection_three_second_value")
		addDoc(t, "other_db", "other_collection", "some_other_value")

		advanceCapture(ctx, t, capture)

		cupaloy.SnapshotT(t, capture.Summary())
	})
}

func makeBinding(t *testing.T, database string, collection string) *flow.CaptureSpec_Binding {
	t.Helper()

	return &flow.CaptureSpec_Binding{
		ResourceConfigJson: resourceSpecJson(t, resource{Collection: collection, Database: database}),
		ResourcePath:       []string{database, collection},
		Collection:         flow.CollectionSpec{Name: flow.Collection(fmt.Sprintf("acmeCo/test/%s", collection))},
		StateKey:           url.QueryEscape(database + "/" + collection),
	}
}
