package main

import (
	"context"
	"fmt"
	"testing"

	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestDiscover(t *testing.T) {
	ctx := context.Background()
	client, cfg := testClient(t)

	var database = "test"
	var col1 = "collection1"
	var col2 = "collection2"
	var view = "testView"

	cleanup := func() {
		dropCollection(ctx, t, client, database, col1)
		dropCollection(ctx, t, client, database, col2)
		dropCollection(ctx, t, client, database, view)
	}
	cleanup()
	t.Cleanup(cleanup)

	stringPkVals := func(idx int) any {
		return fmt.Sprintf("pk val %d", idx)
	}

	numberPkVals := func(idx int) any {
		if idx%2 == 0 {
			return idx
		}

		return float64(idx) + 0.5
	}

	// Create a view, this leads to `system.views` being created
	// but neither the view itself, nor the `system.views` collections must not
	// show up in the discovered results
	var db = client.Database(database)
	err := db.CreateView(ctx, view, col1, mongo.Pipeline{})
	require.NoError(t, err)

	addTestTableData(ctx, t, client, database, col1, 5, 0, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database, col2, 5, 0, numberPkVals, "firstColumn", "secondColumn")

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings:     bindings(t, database, col1),
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
			dropCollection(ctx, t, client, d, col1)
			dropCollection(ctx, t, client, d, col2)
			dropCollection(ctx, t, client, d, view)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	stringPkVals := func(idx int) any {
		return fmt.Sprintf("pk val %d", idx)
	}

	numberPkVals := func(idx int) any {
		if idx%2 == 0 {
			return idx
		}

		return float64(idx) + 0.5
	}

	// Create a view, this leads to `system.views` being created
	// but neither the view itself, nor the `system.views` collections must not
	// show up in the discovered results
	var bs []*flow.CaptureSpec_Binding
	for _, d := range databases {
		var db = client.Database(d)
		err := db.CreateView(ctx, view, col1, mongo.Pipeline{})
		require.NoError(t, err)

		addTestTableData(ctx, t, client, d, col1, 5, 0, stringPkVals, "onlyColumn")
		addTestTableData(ctx, t, client, d, col2, 5, 0, numberPkVals, "firstColumn", "secondColumn")

		bs = append(bs, bindings(t, d, col1)...)
	}

	cfg.Database = " foo, bar "

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings:     bs,
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
			dropCollection(ctx, t, client, d, col1)
			dropCollection(ctx, t, client, d, col2)
			dropCollection(ctx, t, client, d, view)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	stringPkVals := func(idx int) any {
		return fmt.Sprintf("pk val %d", idx)
	}

	numberPkVals := func(idx int) any {
		if idx%2 == 0 {
			return idx
		}

		return float64(idx) + 0.5
	}

	// Create a view, this leads to `system.views` being created
	// but neither the view itself, nor the `system.views` collections must not
	// show up in the discovered results
	var bs []*flow.CaptureSpec_Binding
	for _, d := range databases {
		var db = client.Database(d)
		err := db.CreateView(ctx, view, col1, mongo.Pipeline{})
		require.NoError(t, err)

		addTestTableData(ctx, t, client, d, col1, 5, 0, stringPkVals, "onlyColumn")
		addTestTableData(ctx, t, client, d, col2, 5, 0, numberPkVals, "firstColumn", "secondColumn")

		bs = append(bs, bindings(t, d, col1)...)
	}

	cfg.Database = ""

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings:     bs,
	}

	cs.VerifyDiscover(ctx, t, nil...)
}
