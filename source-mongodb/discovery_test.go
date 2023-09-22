package main

import (
	"context"
	"fmt"
	"testing"

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
