package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	database := "test"
	col1 := "collectionOne"
	col2 := "collectionTwo"
	col3 := "collectionThree"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		dropCollection(ctx, t, client, database, col1)
		dropCollection(ctx, t, client, database, col2)
		dropCollection(ctx, t, client, database, col3)
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

	addTestTableData(ctx, t, client, database, col1, 5, 0, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database, col2, 5, 0, numberPkVals, "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, database, col3, 5, 0, binaryPkVals, "firstColumn", "secondColumn", "thirdColumn")

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		// Values returned from individual segment queries will appear to be a random order, but
		// this is because of how DynamoDB hashes values from the partition key. The returned
		// orderings are deterministic per segment, but the concurrent processing of segments makes
		// the overall ordering non-deterministic.
		Validator:  &st.SortedCaptureValidator{},
		Sanitizers: commonSanitizers(),
		Bindings:   bindings(t, database, col1, col2, col3),
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
	addTestTableData(ctx, t, client, database, col1, 5, 5, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database, col2, 5, 5, numberPkVals, "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, database, col3, 5, 5, binaryPkVals, "firstColumn", "secondColumn", "thirdColumn")

	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

func commonSanitizers() map[string]*regexp.Regexp {
	sanitizers := make(map[string]*regexp.Regexp)
	for k, v := range st.DefaultSanitizers {
		sanitizers[k] = v
	}
	sanitizers[`<UUID>`] = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

	return sanitizers
}

func resourceSpecJson(t *testing.T, r resource) json.RawMessage {
	t.Helper()

	out, err := json.Marshal(r)
	require.NoError(t, err)

	return out
}

func bindings(t *testing.T, database string, collections ...string) []*flow.CaptureSpec_Binding {
	t.Helper()

	out := []*flow.CaptureSpec_Binding{}
	for _, col := range collections {
		out = append(out, &flow.CaptureSpec_Binding{
			ResourceConfigJson: resourceSpecJson(t, resource{Collection: col, Database: database}),
			ResourcePath:       []string{col},
			Collection:         flow.CollectionSpec{Name: flow.Collection(fmt.Sprintf("acmeCo/test/%s", col))},
		})
	}

	return out
}

func advanceCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) {
	t.Helper()
	captureCtx, cancelCapture := context.WithCancel(ctx)

	const shutdownDelay = 100 * time.Millisecond
	var shutdownWatchdog *time.Timer
	log.Info("advanceCapture")
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
