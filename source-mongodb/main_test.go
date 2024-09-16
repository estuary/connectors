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
	boilerplate "github.com/estuary/connectors/source-boilerplate"
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
	batchCol1 := "batchCollection1"
	batchCol2 := "batchCollection2"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		require.NoError(t, client.Database(database1).Drop(ctx))
		require.NoError(t, client.Database(database2).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	addTestTableData(ctx, t, client, database1, col1, 5, 0, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database1, col2, 5, 0, numberPkVals, "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, database2, col3, 5, 0, binaryPkVals, "firstColumn", "secondColumn", "thirdColumn")
	addTestTableData(ctx, t, client, database1, batchCol1, 5, 0, stringPkVals, "batchColDb1")
	addTestTableData(ctx, t, client, database2, batchCol2, 5, 0, stringPkVals, "batchColDb2")

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings: []*flow.CaptureSpec_Binding{
			makeBinding(t, database1, col1, "", ""), // empty mode defaults to captureModeChangeStream
			makeBinding(t, database1, col2, captureModeChangeStream, ""),
			makeBinding(t, database1, batchCol1, captureModeSnapshot, "_id"),
			makeBinding(t, database2, col3, captureModeChangeStream, ""),
			makeBinding(t, database2, batchCol2, captureModeSnapshot, "_id"),
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

	// Run the capture one more time, first adding more data that will be picked up from change
	// streams (but not batch collections), to verify resumption from stream checkpoints.
	addTestTableData(ctx, t, client, database1, col1, 5, 5, stringPkVals, "onlyColumn")
	addTestTableData(ctx, t, client, database1, col2, 5, 5, numberPkVals, "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, database2, col3, 5, 5, binaryPkVals, "firstColumn", "secondColumn", "thirdColumn")
	addTestTableData(ctx, t, client, database1, batchCol1, 5, 5, stringPkVals, "batchColDb1")
	addTestTableData(ctx, t, client, database2, batchCol2, 5, 5, stringPkVals, "batchColDb2")

	advanceCapture(ctx, t, cs)

	// Delete some records
	deleteData(ctx, t, client, database1, col1, stringPkVals(0))
	deleteData(ctx, t, client, database1, col2, numberPkVals(0))
	deleteData(ctx, t, client, database2, col3, binaryPkVals(0))
	deleteData(ctx, t, client, database1, batchCol1, stringPkVals(0))
	deleteData(ctx, t, client, database2, batchCol2, stringPkVals(0))

	advanceCapture(ctx, t, cs)

	// Update records
	updateData(ctx, t, client, database1, col1, stringPkVals(1), "onlyColumn_new")
	updateData(ctx, t, client, database1, col2, numberPkVals(1), "thirdColumn_new")
	updateData(ctx, t, client, database2, col3, binaryPkVals(1), "forthColumn_new")
	updateData(ctx, t, client, database1, batchCol1, stringPkVals(1), "batchColDb1_new")
	updateData(ctx, t, client, database2, batchCol2, stringPkVals(1), "batchColDb2_new")

	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestCaptureBatchSnapshotResumption(t *testing.T) {
	database := "testDb"
	collection := "collection"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		require.NoError(t, client.Database(database).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	binding := makeBinding(t, database, collection, captureModeSnapshot, "_id")
	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.OrderedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings:     []*flow.CaptureSpec_Binding{binding},
	}

	// Initially empty collection
	advanceCapture(ctx, t, cs)

	// Simulate re-capturing the same collection after enough time has passed,
	// this time with some documents.
	addTestTableData(ctx, t, client, database, collection, 5, 0, stringPkVals, "onlyColumn")
	backdateState(t, cs, boilerplate.StateKey(binding.StateKey))
	advanceCapture(ctx, t, cs)

	// Re-capture again with even more added documents.
	addTestTableData(ctx, t, client, database, collection, 5, 5, stringPkVals, "onlyColumn")
	backdateState(t, cs, boilerplate.StateKey(binding.StateKey))
	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestBatchIncrementalResumption(t *testing.T) {
	database := "testDb"
	defaultCursorCollection := "defaultCursorCollection"
	customCursorCollection := "customCursorCollection"
	timeseriesCollection := "timeseries"
	refTime := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		require.NoError(t, client.Database(database).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	defaultBinding := makeBinding(t, database, defaultCursorCollection, captureModeIncremental, "_id")
	customBinding := makeBinding(t, database, customCursorCollection, captureModeIncremental, "updatedAt")
	timeseriesBinding := makeBinding(t, database, timeseriesCollection, captureModeIncremental, "ts")
	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.OrderedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings:     []*flow.CaptureSpec_Binding{defaultBinding, customBinding, timeseriesBinding},
	}

	// Initially empty collection
	advanceCapture(ctx, t, cs)

	// Add some data and capture it.
	for idx := 0; idx < 5; idx++ {
		_, err := client.Database(database).Collection(defaultCursorCollection).InsertOne(ctx, map[string]any{
			"_id":   idx,
			"value": fmt.Sprintf("value %d", idx),
		})
		require.NoError(t, err)
		_, err = client.Database(database).Collection(customCursorCollection).InsertOne(ctx, map[string]any{
			"_id":       idx,
			"value":     fmt.Sprintf("value %d", idx),
			"updatedAt": refTime.Add(time.Duration(idx) * time.Second),
		})
		require.NoError(t, err)
		_, err = client.Database(database).Collection(timeseriesCollection).InsertOne(ctx, map[string]any{
			"_id":   idx,
			"value": fmt.Sprintf("value %d", idx),
			"ts":    refTime.Add(time.Duration(idx) * time.Second),
		})
		require.NoError(t, err)
	}
	backdateState(t, cs, boilerplate.StateKey(defaultBinding.StateKey))
	backdateState(t, cs, boilerplate.StateKey(customBinding.StateKey))
	backdateState(t, cs, boilerplate.StateKey(timeseriesBinding.StateKey))
	advanceCapture(ctx, t, cs)

	// Run again before the next incremental period is due and data is not re-captured.
	advanceCapture(ctx, t, cs)

	// Run again after the next incremental period is due and data is still not re-captured.
	backdateState(t, cs, boilerplate.StateKey(defaultBinding.StateKey))
	backdateState(t, cs, boilerplate.StateKey(customBinding.StateKey))
	backdateState(t, cs, boilerplate.StateKey(timeseriesBinding.StateKey))
	advanceCapture(ctx, t, cs)

	// Deletions and updates "earlier" than the cursor are not captured.
	for _, col := range []string{defaultCursorCollection, customCursorCollection, timeseriesCollection} {
		res, err := client.Database(database).Collection(col).DeleteOne(ctx, bson.M{"_id": 0})
		require.NoError(t, err)
		require.Equal(t, int64(1), res.DeletedCount)
		_, err = client.Database(database).Collection(col).UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"value": "new value"}})
		require.NoError(t, err)
	}
	backdateState(t, cs, boilerplate.StateKey(defaultBinding.StateKey))
	backdateState(t, cs, boilerplate.StateKey(customBinding.StateKey))
	backdateState(t, cs, boilerplate.StateKey(timeseriesBinding.StateKey))
	advanceCapture(ctx, t, cs)

	// Add some new incremental data.
	for idx := 5; idx < 10; idx++ {
		_, err := client.Database(database).Collection(defaultCursorCollection).InsertOne(ctx, map[string]any{
			"_id":   idx,
			"value": fmt.Sprintf("value %d", idx),
		})
		require.NoError(t, err)
		_, err = client.Database(database).Collection(customCursorCollection).InsertOne(ctx, map[string]any{
			"_id":       idx,
			"value":     fmt.Sprintf("value %d", idx),
			"updatedAt": refTime.Add(time.Duration(idx) * time.Second),
		})
		require.NoError(t, err)
		_, err = client.Database(database).Collection(timeseriesCollection).InsertOne(ctx, map[string]any{
			"_id":   idx,
			"value": fmt.Sprintf("value %d", idx),
			"ts":    refTime.Add(time.Duration(idx) * time.Second),
		})
		require.NoError(t, err)
	}

	// The new incremental data is not captured yet.
	advanceCapture(ctx, t, cs)

	// But the new data is captured at the next scheduled time.
	backdateState(t, cs, boilerplate.StateKey(defaultBinding.StateKey))
	backdateState(t, cs, boilerplate.StateKey(customBinding.StateKey))
	backdateState(t, cs, boilerplate.StateKey(timeseriesBinding.StateKey))
	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestCaptureSplitLargeDocuments(t *testing.T) {
	database := "testDb"
	col := "testCollection"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		require.NoError(t, client.Database(database).Drop(ctx))
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
		Bindings:     []*flow.CaptureSpec_Binding{makeBinding(t, database, col, captureModeChangeStream, "")},
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
		require.NoError(t, client.Database(database1).Drop(ctx))
		require.NoError(t, client.Database(database2).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

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
			makeBinding(t, database1, col1, captureModeChangeStream, ""),
			// makeBinding(t, database1, col2, captureModeChangeStream), Note: Binding disabled for this collection.
			makeBinding(t, database2, col3, captureModeChangeStream, ""),
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
	sanitizers[`"last_poll_start":"<LAST_POLLED_TS>"`] = regexp.MustCompile(`"last_poll_start":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z"`)

	return sanitizers
}

func resourceSpecJson(t *testing.T, r resource) json.RawMessage {
	t.Helper()

	out, err := json.Marshal(r)
	require.NoError(t, err)

	return out
}

func makeBinding(t *testing.T, database string, collection string, mode captureMode, cursor string) *flow.CaptureSpec_Binding {
	t.Helper()

	return &flow.CaptureSpec_Binding{
		ResourceConfigJson: resourceSpecJson(t, resource{Collection: collection, Database: database, Mode: mode, Cursor: cursor}),
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

func stringPkVals(idx int) any {
	return fmt.Sprintf("pk val %d", idx)
}

func numberPkVals(idx int) any {
	if idx%2 == 0 {
		return idx
	}

	return float64(idx) + 0.5
}

func binaryPkVals(idx int) any {
	return []byte(fmt.Sprintf("pk val %d", idx))
}

func backdateState(t *testing.T, cs *st.CaptureSpec, sk boilerplate.StateKey) {
	// Trigger another poll of the batch snapshot binding by manipulating its
	// checkpoint to have a "last polled time" more than 24 hours in the past.
	var state captureState
	require.NoError(t, json.Unmarshal(cs.Checkpoint, &state))
	resState := state.Resources[boilerplate.StateKey(sk)]
	backDate := resState.Backfill.LastPollStart.Add(-48 * time.Hour)
	resState.Backfill.LastPollStart = &backDate
	state.Resources[boilerplate.StateKey(sk)] = resState
	cp, err := json.Marshal(state)
	require.NoError(t, err)
	cs.Checkpoint = cp
}
