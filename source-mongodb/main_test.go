package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestCapture(t *testing.T) {
	database1 := "testDb1"
	database2 := "testDb2"
	col1 := "collectionOne"
	col2 := "collectionTwo"
	col3 := "collectionThree"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		dropCollection(ctx, t, client, database1, col1)
		dropCollection(ctx, t, client, database1, col2)
		dropCollection(ctx, t, client, database2, col3)
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

	binaryPkVals := func(idx int) any {
		return []byte(fmt.Sprintf("pk val %d", idx))
	}

	addTestTableData(ctx, t, client, database1, col1, 5, 0, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database1, col2, 5, 0, numberPkVals, "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, database2, col3, 5, 0, binaryPkVals, "firstColumn", "secondColumn", "thirdColumn")

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings: []*flow.CaptureSpec_Binding{
			makeBinding(t, database1, col1),
			makeBinding(t, database1, col2),
			makeBinding(t, database2, col3),
		},
	}

	// Run the capture, stopping it before it has completed the entire backfill.
	count := 0
	captureCtx, cancel := context.WithCancel(context.Background())
	cs.Capture(captureCtx, t, func(msg json.RawMessage) {
		count++
		if count == 10 {
			cancel()
		}
	})

	// Run the capture again and let it finish the backfill, resuming from the previous checkpoint.
	advanceCapture(ctx, t, cs)

	// Run the capture one more time, first adding more data that will be picked up from stream
	// shards, to verify resumption from stream checkpoints.
	addTestTableData(ctx, t, client, database1, col1, 5, 5, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database1, col2, 5, 5, numberPkVals, "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, database2, col3, 5, 5, binaryPkVals, "firstColumn", "secondColumn", "thirdColumn")

	advanceCapture(ctx, t, cs)

	// Delete some records
	deleteData(ctx, t, client, database1, col1, stringPkVals(0))
	deleteData(ctx, t, client, database1, col2, numberPkVals(0))
	deleteData(ctx, t, client, database2, col3, binaryPkVals(0))

	advanceCapture(ctx, t, cs)

	// Update records
	updateData(ctx, t, client, database1, col1, stringPkVals(1), "onlyColumn_new")
	updateData(ctx, t, client, database1, col2, numberPkVals(1), "thirdColumn_new")
	updateData(ctx, t, client, database2, col3, binaryPkVals(1), "forthColumn_new")

	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestCaptureExclusiveCollectionFilter(t *testing.T) {
	database1 := "testDb1"
	database2 := "testDb2"
	col1 := "collectionOne"
	col2 := "collectionTwo"
	col3 := "collectionThree"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		dropCollection(ctx, t, client, database1, col1)
		dropCollection(ctx, t, client, database1, col2)
		dropCollection(ctx, t, client, database2, col3)
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

	binaryPkVals := func(idx int) any {
		return []byte(fmt.Sprintf("pk val %d", idx))
	}

	addTestTableData(ctx, t, client, database1, col1, 5, 0, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database1, col2, 5, 0, numberPkVals, "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, database2, col3, 5, 0, binaryPkVals, "firstColumn", "secondColumn", "thirdColumn")

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings: []*flow.CaptureSpec_Binding{
			makeBinding(t, database1, col1),
			// makeBinding(t, database1, col2), Note: Binding disabled for this collection.
			makeBinding(t, database2, col3),
		},
	}

	// Do the initial backfill.
	advanceCapture(ctx, t, cs)

	// Add data to the MongoDB collections.
	addTestTableData(ctx, t, client, database1, col1, 5, 5, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database1, col2, 5, 5, numberPkVals, "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, database2, col3, 5, 5, binaryPkVals, "firstColumn", "secondColumn", "thirdColumn")

	// Read change stream documents.
	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

func commonSanitizers() map[string]*regexp.Regexp {
	sanitizers := make(map[string]*regexp.Regexp)
	sanitizers[`"<TOKEN>"`] = regexp.MustCompile(`"[A-Za-z0-9+/=]{32,}"`)

	return sanitizers
}

func resourceSpecJson(t *testing.T, r resource) json.RawMessage {
	t.Helper()

	out, err := json.Marshal(r)
	require.NoError(t, err)

	return out
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

func advanceCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) {
	t.Helper()
	captureCtx, cancelCapture := context.WithCancel(ctx)

	const shutdownDelay = 100 * time.Millisecond
	var shutdownWatchdog *time.Timer
	cs.Capture(captureCtx, t, func(data json.RawMessage) {
		if shutdownWatchdog == nil {
			shutdownWatchdog = time.AfterFunc(shutdownDelay, func() {
				log.WithField("delay", shutdownDelay.String()).Debug("capture shutdown watchdog expired")
				cancelCapture()
			})
		}
		shutdownWatchdog.Reset(shutdownDelay)
	})

	for _, e := range cs.Errors {
		require.NoError(t, e)
	}
}
