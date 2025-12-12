package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

// TestCaptureFromView tests capturing from a MongoDB view in snapshot mode.
// Views should be treated as sharded collections to ensure proper sorting,
// since they can combine _id fields from multiple underlying collections.
// Note: Views do not support change streams, so only snapshot and incremental modes are valid.
func TestCaptureFromView(t *testing.T) {
	database := "testDb"
	sourceCollection := "sourceCollection"
	viewName := "testView"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		// Drop the view first (views must be dropped before the underlying collection)
		_ = client.Database(database).Collection(viewName).Drop(ctx)
		require.NoError(t, client.Database(database).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	// Create a source collection with some data
	addTestTableData(ctx, t, client, database, sourceCollection, 5, 0, stringPkVals, "onlyColumn")

	// Create a view based on the source collection
	err := client.Database(database).RunCommand(ctx, bson.D{
		{Key: "create", Value: viewName},
		{Key: "viewOn", Value: sourceCollection},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$project", Value: bson.D{
				{Key: "_id", Value: 1},
				{Key: "onlyColumn", Value: 1},
			}}},
		}},
	}).Err()
	require.NoError(t, err)

	// Test capturing from the view in snapshot mode
	binding := makeBinding(t, database, viewName, captureModeSnapshot, "_id")
	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.OrderedCaptureValidator{NormalizeJSON: true},
		Sanitizers:   commonSanitizers(),
		Bindings:     []*flow.CaptureSpec_Binding{binding},
	}

	// Capture from the view
	advanceCapture(ctx, t, cs)

	// Add more data to the underlying collection
	addTestTableData(ctx, t, client, database, sourceCollection, 5, 5, stringPkVals, "onlyColumn")

	// Re-capture after backdating state
	backdateState(t, cs, boilerplate.StateKey(binding.StateKey))
	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

// TestCaptureFromViewIncremental tests capturing from a view in incremental mode.
func TestCaptureFromViewIncremental(t *testing.T) {
	database := "testDb"
	sourceCollection := "incrementalSource"
	viewName := "incrementalView"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		_ = client.Database(database).Collection(viewName).Drop(ctx)
		require.NoError(t, client.Database(database).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	// Create a source collection with data
	col := client.Database(database).Collection(sourceCollection)
	for idx := 0; idx < 5; idx++ {
		_, err := col.InsertOne(ctx, map[string]any{
			"_id":   idx,
			"value": fmt.Sprintf("value %d", idx),
		})
		require.NoError(t, err)
	}

	// Create a view based on the source collection
	err := client.Database(database).RunCommand(ctx, bson.D{
		{Key: "create", Value: viewName},
		{Key: "viewOn", Value: sourceCollection},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$project", Value: bson.D{
				{Key: "_id", Value: 1},
				{Key: "value", Value: 1},
			}}},
		}},
	}).Err()
	require.NoError(t, err)

	binding := makeBinding(t, database, viewName, captureModeIncremental, "_id")
	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.OrderedCaptureValidator{NormalizeJSON: true},
		Sanitizers:   commonSanitizers(),
		Bindings:     []*flow.CaptureSpec_Binding{binding},
	}

	// Initial capture (empty)
	advanceCapture(ctx, t, cs)

	// Backdate and capture the existing data
	backdateState(t, cs, boilerplate.StateKey(binding.StateKey))
	advanceCapture(ctx, t, cs)

	// Add more incremental data
	for idx := 5; idx < 10; idx++ {
		_, err := col.InsertOne(ctx, map[string]any{
			"_id":   idx,
			"value": fmt.Sprintf("value %d", idx),
		})
		require.NoError(t, err)
	}

	// Capture should not pick up new data yet
	advanceCapture(ctx, t, cs)

	// But new data is captured at the next scheduled time
	backdateState(t, cs, boilerplate.StateKey(binding.StateKey))
	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

// TestCaptureFromViewWithMultipleSources tests capturing from a view that
// aggregates data from multiple collections. This is the key use case where
// treating views as sharded is important, since _id values may come from
// different underlying collections.
func TestCaptureFromViewWithMultipleSources(t *testing.T) {
	database := "testDb"
	collection1 := "collection1"
	collection2 := "collection2"
	viewName := "unionView"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		_ = client.Database(database).Collection(viewName).Drop(ctx)
		require.NoError(t, client.Database(database).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	// Create two collections with data
	col1 := client.Database(database).Collection(collection1)
	col2 := client.Database(database).Collection(collection2)

	for idx := 0; idx < 5; idx++ {
		_, err := col1.InsertOne(ctx, map[string]any{
			"_id":    fmt.Sprintf("col1_%d", idx),
			"source": "collection1",
			"value":  fmt.Sprintf("value from col1: %d", idx),
		})
		require.NoError(t, err)

		_, err = col2.InsertOne(ctx, map[string]any{
			"_id":    fmt.Sprintf("col2_%d", idx),
			"source": "collection2",
			"value":  fmt.Sprintf("value from col2: %d", idx),
		})
		require.NoError(t, err)
	}

	// Create a view that unions both collections
	err := client.Database(database).RunCommand(ctx, bson.D{
		{Key: "create", Value: viewName},
		{Key: "viewOn", Value: collection1},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$unionWith", Value: bson.D{
				{Key: "coll", Value: collection2},
			}}},
			bson.D{{Key: "$project", Value: bson.D{
				{Key: "_id", Value: 1},
				{Key: "source", Value: 1},
				{Key: "value", Value: 1},
			}}},
		}},
	}).Err()
	require.NoError(t, err)

	// Test capturing from the view in snapshot mode
	binding := makeBinding(t, database, viewName, captureModeSnapshot, "_id")
	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.OrderedCaptureValidator{NormalizeJSON: true},
		Sanitizers:   commonSanitizers(),
		Bindings:     []*flow.CaptureSpec_Binding{binding},
	}

	// Capture from the view
	advanceCapture(ctx, t, cs)

	// Add more data to both underlying collections
	for idx := 5; idx < 10; idx++ {
		_, err := col1.InsertOne(ctx, map[string]any{
			"_id":    fmt.Sprintf("col1_%d", idx),
			"source": "collection1",
			"value":  fmt.Sprintf("value from col1: %d", idx),
		})
		require.NoError(t, err)

		_, err = col2.InsertOne(ctx, map[string]any{
			"_id":    fmt.Sprintf("col2_%d", idx),
			"source": "collection2",
			"value":  fmt.Sprintf("value from col2: %d", idx),
		})
		require.NoError(t, err)
	}

	// Re-capture after backdating state
	backdateState(t, cs, boilerplate.StateKey(binding.StateKey))
	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

// TestIsShardedCollectionWithView tests that the isShardedCollection function
// correctly handles views by treating them as sharded.
func TestIsShardedCollectionWithView(t *testing.T) {
	database := "testDb"
	sourceCollection := "sourceForView"
	viewName := "testViewForShardCheck"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		_ = client.Database(database).Collection(viewName).Drop(ctx)
		require.NoError(t, client.Database(database).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	// Create a source collection
	_, err := client.Database(database).Collection(sourceCollection).InsertOne(ctx, bson.M{"test": "data"})
	require.NoError(t, err)

	// Create a view
	err = client.Database(database).RunCommand(ctx, bson.D{
		{Key: "create", Value: viewName},
		{Key: "viewOn", Value: sourceCollection},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		}},
	}).Err()
	require.NoError(t, err)

	// Create a capture instance to test isShardedCollection
	d := &driver{}
	mongoClient, err := d.Connect(ctx, cfg)
	require.NoError(t, err)
	defer mongoClient.Disconnect(ctx)

	capture := &capture{client: mongoClient}

	// Test that a regular collection is not considered sharded (in a non-sharded cluster)
	isSharded, err := capture.isShardedCollection(ctx, database, sourceCollection)
	require.NoError(t, err)
	require.False(t, isSharded, "regular collection should not be sharded in test environment")

	// Test that a view is treated as sharded
	isSharded, err = capture.isShardedCollection(ctx, database, viewName)
	require.NoError(t, err)
	require.True(t, isSharded, "view should be treated as sharded")
}

// TestIsShardedCollectionWithNonExistentCollection tests that isShardedCollection
// handles non-existent collections gracefully.
func TestIsShardedCollectionWithNonExistentCollection(t *testing.T) {
	database := "testDb"
	nonExistentCollection := "doesNotExist"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		require.NoError(t, client.Database(database).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	// Create a capture instance to test isShardedCollection
	d := &driver{}
	mongoClient, err := d.Connect(ctx, cfg)
	require.NoError(t, err)
	defer mongoClient.Disconnect(ctx)

	capture := &capture{client: mongoClient}

	// Test that a non-existent collection returns false without error
	isSharded, err := capture.isShardedCollection(ctx, database, nonExistentCollection)
	require.NoError(t, err)
	require.False(t, isSharded, "non-existent collection should be treated as not sharded")
}
