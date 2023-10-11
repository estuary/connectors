package main

import (
	"context"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/mongo"
	"github.com/stretchr/testify/require"
	st "github.com/estuary/connectors/source-boilerplate/testing"
)

func TestDiscover(t *testing.T) {
	ctx := context.Background()
	client, cfg := testClient(t)

	var database = "test"
	var col1 = "collection1"
	var col2 = "collection2"

	cleanup := func() {
		dropCollection(ctx, t, client, database, col1)
		dropCollection(ctx, t, client, database, col2)
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
	err := db.CreateView(ctx, "testView", col1, mongo.Pipeline{})
	require.NoError(t, err)

	addTestTableData(ctx, t, client, database, col1, 5, 0, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database, col2, 5, 0, numberPkVals, "firstColumn", "secondColumn")

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:  &st.SortedCaptureValidator{},
		Sanitizers: commonSanitizers(),
		Bindings:   bindings(t, database, col1),
	}

	cs.VerifyDiscover(ctx, t, nil...)
}
