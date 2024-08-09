package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
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

func TestCaptureSplitLargeDocuments(t *testing.T) {
	database := "testDb"
	col := "testCollection"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		dropCollection(ctx, t, client, database, col)
	}
	cleanup()
	t.Cleanup(cleanup)

	require.NoError(t, client.Database(database).CreateCollection(ctx, col, &options.CreateCollectionOptions{ChangeStreamPreAndPostImages: bson.D{{Key: "enabled", Value: true}}}))

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.OrderedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings:     []*flow.CaptureSpec_Binding{makeBinding(t, database, col)},
	}

	collection := client.Database(database).Collection(col)

	// Do the initial backfill to get into "streaming mode" to start capturing changes.
	advanceCapture(ctx, t, cs)

	// Insert a small document and update it.
	val := map[string]string{"_id": "smallDocument", "foo": "bar", "baz": "qux"}
	_, err := collection.InsertOne(ctx, val)
	require.NoError(t, err)
	val["foo"] = "bar_updated"
	res, err := collection.UpdateOne(ctx, bson.D{{Key: "_id", Value: val["_id"]}}, bson.D{{Key: "$set", Value: val}})
	require.NoError(t, err)
	require.Equal(t, 1, int(res.ModifiedCount))

	// Insert a huge document and update it. The output sanitizers will replace
	// the very long values with `<TOKEN>` based on the simple token
	// sanitization logic.
	val = map[string]string{
		"_id":  "hugeDocument",
		"key1": strings.Repeat("value1", 200000),
		"key2": strings.Repeat("value2", 200000),
		"key3": strings.Repeat("value3", 200000),
		"key4": strings.Repeat("value4", 200000),
		"key5": strings.Repeat("value5", 200000),
		"key6": strings.Repeat("value6", 200000),
		"key7": strings.Repeat("value7", 200000),
		"key8": strings.Repeat("value8", 200000),
		"key9": strings.Repeat("value9", 200000),
	}
	_, err = collection.InsertOne(ctx, val)
	require.NoError(t, err)
	val["key1"] = "updated"
	val["key9"] = "also updated"
	res, err = collection.UpdateOne(ctx, bson.D{{Key: "_id", Value: val["_id"]}}, bson.D{{Key: "$set", Value: val}})
	require.NoError(t, err)
	require.Equal(t, 1, int(res.ModifiedCount))

	// Another small document.
	val = map[string]string{"_id": "otherSmallDocument", "some": "other", "values": "here"}
	_, err = collection.InsertOne(ctx, val)
	require.NoError(t, err)
	val["some"] = "other_updated"
	res, err = collection.UpdateOne(ctx, bson.D{{Key: "_id", Value: val["_id"]}}, bson.D{{Key: "$set", Value: val}})
	require.NoError(t, err)
	require.Equal(t, 1, int(res.ModifiedCount))

	// Read change stream documents, waiting until we have received all 6
	// expected change stream documents, not counting checkpoints. Reading the
	// huge document and its updates from the change stream takes a non-trivial
	// amount of time.
	count := 0
	captureCtx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(5*time.Second, cancel) // don't wait forever though
	cs.Capture(captureCtx, t, func(msg json.RawMessage) {
		if !strings.Contains(string(msg), "bindingStateV1") {
			count++
		}
		if count == 6 {
			cancel()
		}
	})

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
