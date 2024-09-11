package main

import (
	"context"
	"testing"

	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestDiscover(t *testing.T) {
	ctx := context.Background()
	client, cfg := testClient(t)

	var database = "test"
	var col1 = "collection1"
	var col2 = "collection2"
	var view = "testView"

	cleanup := func() {
		require.NoError(t, client.Database(database).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	var db = client.Database(database)
	require.NoError(t, db.CreateCollection(ctx, col1))
	require.NoError(t, db.CreateCollection(ctx, col2))
	// Create a view, this leads to `system.views` being created but neither the
	// view itself, nor the `system.views` collections must show up in the
	// discovered results
	require.NoError(t, db.CreateView(ctx, view, col1, mongo.Pipeline{}))

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
	}

	cs.VerifyDiscover(ctx, t, nil...)
}

func TestDiscoverMultipleDatabases(t *testing.T) {
	ctx := context.Background()
	client, cfg := testClient(t)

	var databases = []string{"foo", "bar", "baz"}
	var col1 = "collection1"
	var col2 = "collection2"
	var view = "testView"

	cleanup := func() {
		for _, d := range databases {
			require.NoError(t, client.Database(d).Drop(ctx))
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	for _, d := range databases {
		var db = client.Database(d)
		require.NoError(t, db.CreateCollection(ctx, col1))
		require.NoError(t, db.CreateCollection(ctx, col2))
		require.NoError(t, db.CreateView(ctx, view, col1, mongo.Pipeline{}))
	}

	cfg.Database = " foo, bar "

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
	}

	cs.VerifyDiscover(ctx, t, nil...)
}

func TestDiscoverAllDatabases(t *testing.T) {
	ctx := context.Background()
	client, cfg := testClient(t)

	var databases = []string{"foo", "bar", "baz"}
	var col1 = "collection1"
	var col2 = "collection2"
	var view = "testView"

	cleanup := func() {
		for _, d := range databases {
			require.NoError(t, client.Database(d).Drop(ctx))
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	for _, d := range databases {
		var db = client.Database(d)
		require.NoError(t, db.CreateCollection(ctx, col1))
		require.NoError(t, db.CreateCollection(ctx, col2))
		require.NoError(t, db.CreateView(ctx, view, col1, mongo.Pipeline{}))
	}

	cfg.Database = ""

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
	}

	cs.VerifyDiscover(ctx, t, nil...)
}

func TestDiscoverBatchCollections(t *testing.T) {
	ctx := context.Background()
	client, cfg := testClient(t)
	cfg.Database = ""
	cfg.BatchAndChangeStream = true

	var db = "db"
	var col = "collection"
	var view = "view"
	var timeseries = "timeseries"

	cleanup := func() {
		require.NoError(t, client.Database(db).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	require.NoError(t, client.Database(db).CreateCollection(ctx, col))
	require.NoError(t, client.Database(db).CreateView(ctx, view, col, mongo.Pipeline{}))
	require.NoError(t, client.Database(db).CreateCollection(ctx, timeseries, &options.CreateCollectionOptions{TimeSeriesOptions: &options.TimeSeriesOptions{TimeField: "ts"}}))

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
	}

	cs.VerifyDiscover(ctx, t, nil...)
}
