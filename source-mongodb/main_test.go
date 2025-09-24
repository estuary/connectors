package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	"golang.org/x/sync/errgroup"
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

	// Create the timeseries collection with the appropriate options.
	require.NoError(t, client.Database(database).CreateCollection(ctx, timeseriesCollection, options.CreateCollection().SetTimeSeriesOptions(options.TimeSeries().
		SetTimeField("ts").
		SetGranularity("seconds"))),
	)

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
	// Timeseries collections do not support updates in the conventional sense, so that is not tested here.
	for _, col := range []string{defaultCursorCollection, customCursorCollection} {
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

// TestCaptureStressChangeStreamCorrectness is based on the sqlcapture
// testStressCorrectness test. It is simpler in some ways since MongoDB doesn't
// have a concept of a "precise" backfill so backfilling is not part of the
// test, but also more complicated in that it exercises processing several
// change streams concurrently.
//
// The dataset for this test is a collection of (id, counter) tuples which will
// progress through the following states:
//
//	(id, 1) ->  (id, 2) ->  (id, 3) -> (id, 4) -> deleted
//	     ->  (id, 6) ->  (id, 7) -> (id, 8)
//
// The specific ordering of changes is randomly generated on every test run and
// the capture introduces more variation depending on the precise timing of the
// pull batches from change stream cursors. However it is still possible to
// verify many correctness properties by observing the output event stream. For
// instance, when the capture completes, we know exactly how many IDs should
// exist and that they should all be in the final state with counter=8. If this
// is not the case then some data has been lost.
func TestCaptureStressChangeStreamCorrectness(t *testing.T) {
	const loadGenTime = 90 * time.Second // run the load generator for this long
	const numActiveIDs = 1000

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	dbsAndCollections := [][]string{
		{"stressdb1", "stressdb1collection1"},
		{"stressdb1", "stressdb1collection2"},
		{"stressdb2", "stressdb2collection1"},
		{"stressdb2", "stressdb2collection2"},
		{"stressdb3", "stressdb3collection1"},
	}

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		for _, dbcol := range dbsAndCollections {
			require.NoError(t, client.Database(dbcol[0]).Collection(dbcol[1]).Drop(ctx))
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	// Because the Full Document lookup option looks up the _current_ document
	// and not necessarily the document at the time of the change event, the
	// test collection will use pre-images and the "counter" value from the
	// pre-image will be used to verify that events are captured correctly.
	for _, dbcol := range dbsAndCollections {
		require.NoError(
			t,
			client.Database(dbcol[0]).CreateCollection(ctx, dbcol[1], &options.CreateCollectionOptions{ChangeStreamPreAndPostImages: bson.D{{Key: "enabled", Value: true}}}),
		)
	}

	bindings := make([]*flow.CaptureSpec_Binding, 0, len(dbsAndCollections))
	for _, dbcol := range dbsAndCollections {
		bindings = append(bindings, makeBinding(t, dbcol[0], dbcol[1], captureModeChangeStream, ""))
	}

	validator := &correctnessInvariantsCaptureValidator{}
	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    validator,
		Sanitizers:   commonSanitizers(),
		Bindings:     bindings,
	}
	cs.Reset()

	// The initial backfill is a no-op, so only change events are captured.
	advanceCapture(ctx, t, cs)

	var loadgenDeadline = time.Now().Add(loadGenTime)

	var mu sync.Mutex
	bindingExpectedIDs := make(map[string]int) // updated concurrently by the goroutines below

	group, groupCtx := errgroup.WithContext(ctx)

	for _, dbcol := range dbsAndCollections {
		dbcol := dbcol
		col := client.Database(dbcol[0]).Collection(dbcol[1])

		group.Go(func() error {
			ll := log.WithFields(log.Fields{"db": dbcol[0], "collection": dbcol[1]})

			ll.WithField("runtime", loadGenTime.String()).Info("load generator started")
			var eventCount int
			var nextID int
			var activeIDs = make(map[int]int) // Map from ID to counter value
			for {
				// There should always be N active IDs (until we reach the end of the test)
				for len(activeIDs) < numActiveIDs && time.Now().Before(loadgenDeadline) {
					if nextID%1000 == 0 {
						ll.WithField("id", nextID).Info("load generator progress")
					}
					activeIDs[nextID] = 1
					nextID++
				}
				// Thus if we run out of active IDs we must be done
				if len(activeIDs) == 0 {
					break
				}

				// Randomly select an entry from the active set
				var selected int
				var sampleIndex int
				for id := range activeIDs {
					if sampleIndex == 0 || rand.Intn(sampleIndex) == 0 {
						selected = id
					}
					sampleIndex++
				}
				var counter = activeIDs[selected]

				// Issue an update/insert/delete depending on the counter value
				switch counter {
				case 1, 6:
					_, err := col.InsertOne(groupCtx, map[string]any{"_id": selected, "counter": counter})
					require.NoError(t, err)
				case 5:
					_, err := col.DeleteOne(groupCtx, bson.D{{Key: "_id", Value: selected}})
					require.NoError(t, err)
				default:
					_, err := col.UpdateOne(groupCtx, bson.D{{Key: "_id", Value: selected}}, bson.D{{Key: "$set", Value: bson.D{{Key: "counter", Value: counter}}}})
					require.NoError(t, err)
				}
				eventCount++

				// Increment the counter state or delete the entry if we're done with it.
				if counter := activeIDs[selected]; counter >= 8 {
					delete(activeIDs, selected)
				} else {
					activeIDs[selected] = counter + 1
				}
			}

			ll.WithField("id count", nextID).WithField("event count", eventCount).Info("load generator finished")
			mu.Lock()
			bindingExpectedIDs[validator.bindingIdentifier(dbcol[0], dbcol[1])] = nextID
			mu.Unlock()

			return nil
		})
	}

	go func() {
		group.Wait()
	}()

	// Let the load generator run a while to build up a backlog of change stream
	// events. After that, small tailing batches will be read for the remainder
	// of the test.
	time.Sleep(20 * time.Second)

	// Repeatedly run the capture in parallel with the ongoing database load,
	// killing and restarting it regularly, until the load generator is done.
loop:
	for {
		select {
		case <-groupCtx.Done():
			require.NoError(t, group.Wait())
			break loop
		default:
			var captureCtx, cancelCapture = context.WithCancel(ctx)
			time.AfterFunc(5*time.Second, cancelCapture)
			cs.Capture(captureCtx, t, nil)
		}
	}

	// Run the capture once more and verify the overall results this time.
	validator.BindingExpectedIDs = bindingExpectedIDs
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

type correctnessInvariantsCaptureValidator struct {
	BindingExpectedIDs map[string]int               // binding -> number of expected ids
	states             map[string]map[string]string // binding -> event id -> prior state
	violations         *strings.Builder
}

var correctnessInvariantsStateTransitions = map[string]string{
	// Typical state transitions that one would expect.
	"New:Create(1)":     "1",
	"1:Update(2)":       "2",
	"2:Update(3)":       "3",
	"3:Update(4)":       "4",
	"4:Delete()":        "Deleted",
	"Deleted:Create(6)": "6",
	"6:Update(7)":       "7",
	"7:Update(8)":       "Finished",

	// Change events with a `nil` Full Document are discarded, and this is
	// possible if there is a later change event that deletes the document. So
	// technically a deletion may be observed from any prior state.
	"1:Delete()": "Deleted",
	"2:Delete()": "Deleted",
	"3:Delete()": "Deleted",

	// MongoDB change streams appear to do some kind of on-line compaction,
	// where rapid updates to the same document will be condensed into a single
	// update. This makes it appear as though intermediate updates are
	// "skipped", although the final state is the same.
	"1:Update(3)": "3",
	"1:Update(4)": "4",
	"2:Update(4)": "4",
	"6:Update(8)": "8",
}

func (v *correctnessInvariantsCaptureValidator) Output(collection string, data json.RawMessage) {
	var event struct {
		ID      string `json:"_id"`
		Counter int    `json:"counter"`
		Meta    struct {
			Operation string `json:"op"`
			Before    struct {
				Counter int `json:"counter"`
			} `json:"before"`
			Source struct {
				Database   string `json:"db"`
				Collection string `json:"collection"`
			} `json:"source"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		fmt.Fprintf(v.violations, "error parsing change event: %v\n", err)
		return
	}

	var binding = v.bindingIdentifier(event.Meta.Source.Database, event.Meta.Source.Collection)

	// Compute a string represent the change event and validate that it is a
	// valid transition compared to the previous change event for this binding.
	var change string
	switch event.Meta.Operation {
	case "c":
		change = fmt.Sprintf("Create(%d)", event.Counter)
	case "u":
		if event.Meta.Before.Counter+1 != event.Counter {
			log.WithField("event", event).Trace("full document lookup counter exceeded pre-image counter by more than 1")
		}
		change = fmt.Sprintf("Update(%d)", event.Meta.Before.Counter+1)
	case "d":
		change = "Delete()"
	default:
		change = fmt.Sprintf("UnknownOperation(%q)", event.Meta.Operation)
	}

	if v.states[binding] == nil {
		v.states[binding] = make(map[string]string)
	}

	var prevState = v.states[binding][event.ID]
	if prevState == "" {
		prevState = "New"
	} else if prevState == "Error" {
		return // Ignore documents once they enter an error state
	}
	var edge = prevState + ":" + change

	if nextState, ok := correctnessInvariantsStateTransitions[edge]; ok {
		log.WithFields(log.Fields{
			"binding": binding,
			"id":      event.ID,
			"edge":    edge,
			"prev":    prevState,
			"next":    nextState,
		}).Trace("validated transition")
		v.states[binding][event.ID] = nextState
	} else {
		fmt.Fprintf(v.violations, "id %s of binding %s: invalid state transition %q\n", event.ID, binding, edge)
		v.states[binding][event.ID] = "Error"
	}

}

func (v *correctnessInvariantsCaptureValidator) Checkpoint(data json.RawMessage) {}

func (v *correctnessInvariantsCaptureValidator) SourcedSchema(collection string, schema json.RawMessage) {
}

func (v *correctnessInvariantsCaptureValidator) Reset() {
	v.violations = new(strings.Builder)
	v.states = make(map[string]map[string]string)
}

func (v *correctnessInvariantsCaptureValidator) Summarize(w io.Writer) error {
	fmt.Fprintf(w, "# ================================\n")
	fmt.Fprintf(w, "# Invariant Violations\n")
	fmt.Fprintf(w, "# ================================\n")

	for binding, numExpectedIDs := range v.BindingExpectedIDs {
		for id := 0; id < numExpectedIDs; id++ {
			id := strconv.Itoa(id)
			var state = v.states[binding][id]
			log.WithFields(log.Fields{
				"binding": binding,
				"id":      id,
				"state":   state,
			}).Trace("final state")
			if state != "Finished" && state != "Error" {
				fmt.Fprintf(v.violations, "id %s of binding %s in state %q (expected \"Finished\")\n", id, binding, state)
			}
		}
	}

	if str := v.violations.String(); len(str) == 0 {
		fmt.Fprintf(w, "no invariant violations observed\n\n")
	} else {
		fmt.Fprintf(w, "%s\n", str)
	}
	return nil
}

func (v *correctnessInvariantsCaptureValidator) bindingIdentifier(database, collection string) string {
	return database + "." + collection
}

// TestNestedCursorFields tests nested cursor field lookups like "a.b".
func TestNestedCursorFields(t *testing.T) {
	const (
		database   = "testDb"
		collection = "nestedCursorCollection"
	)
	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		require.NoError(t, client.Database(database).Drop(ctx))
	}
	cleanup()
	t.Cleanup(cleanup)

	binding := makeBinding(t, database, collection, captureModeIncremental, "a.b")
	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		Validator:    &st.OrderedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings:     []*flow.CaptureSpec_Binding{binding},
	}

	col := client.Database(database).Collection(collection)

	advanceCapture(ctx, t, cs)

	for idx := 0; idx < 5; idx++ {
		doc := map[string]any{
			"_id": fmt.Sprintf("%d", idx),
			"a": map[string]any{
				"b": idx,
			},
			"data": fmt.Sprintf("test data %d", idx),
		}
		_, err := col.InsertOne(ctx, doc)
		require.NoError(t, err)
	}

	backdateState(t, cs, boilerplate.StateKey(binding.StateKey))
	advanceCapture(ctx, t, cs)

	for idx := 5; idx < 10; idx++ {
		doc := map[string]any{
			"_id": fmt.Sprintf("%d", idx),
			"a": map[string]any{
				"b": idx,
			},
			"data": fmt.Sprintf("test data %d", idx),
		}
		_, err := col.InsertOne(ctx, doc)
		require.NoError(t, err)
	}

	backdateState(t, cs, boilerplate.StateKey(binding.StateKey))
	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}
