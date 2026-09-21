package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/metadata"
)

func TestPullStream(t *testing.T) {
	ctx := context.Background()
	client, _ := testClient(t)

	testDb := "testDb"
	testColl1 := "testColl1"
	testColl2 := "testColl2"
	testColl3 := "testColl3"

	cp := "checkpoint"
	testChangeStreamBatchSize := int32(5)

	bindings := []bindingInfo{
		{resource: resource{Database: testDb, Collection: testColl1}, index: 0},
		{resource: resource{Database: testDb, Collection: testColl2}, index: 1},
	}

	cleanup := func() {
		require.NoError(t, client.Database(testDb).Drop(ctx))
	}

	insertDoc := func(t *testing.T, collection string, id int) {
		_, err := client.Database(testDb).Collection(collection).InsertOne(ctx, bson.D{{Key: "_id", Value: id}})
		require.NoError(t, err)
	}

	insertDocs := func(t *testing.T, collection string, ids ...int) {
		docs := make([]interface{}, len(ids))
		for i, id := range ids {
			docs[i] = bson.D{{Key: "_id", Value: id}}
		}
		_, err := client.Database(testDb).Collection(collection).InsertMany(ctx, docs)
		require.NoError(t, err)
	}

	tests := []struct {
		name            string
		setup           func(t *testing.T)
		fullDocRequired map[string]bool
		pullTimes       int
		wantSent        []string
		wantEventCount  int
		wantErr         string // if non-empty, expect processBatch to return an error containing this string
	}{
		{
			name: "one document",
			setup: func(t *testing.T) {
				insertDoc(t, testColl1, 1)
			},
			pullTimes:      1,
			wantSent:       []string{cp, "1", cp},
			wantEventCount: 1,
		},
		{
			name: "multiple documents",
			setup: func(t *testing.T) {
				insertDocs(t, testColl1, 1, 2, 3)
			},
			pullTimes:      1,
			wantSent:       []string{cp, "1", "2", "3", cp},
			wantEventCount: 3,
		},
		{
			name: "multiple collections",
			setup: func(t *testing.T) {
				insertDoc(t, testColl1, 1)
				insertDoc(t, testColl2, 2)
				insertDoc(t, testColl3, 3) // not a captured collection
			},
			pullTimes:      3,
			wantSent:       []string{cp, "1", cp, "2", cp},
			wantEventCount: 3,
		},
		{
			name: "multiple batches - only one is retrieved",
			setup: func(t *testing.T) {
				insertDocs(t, testColl1, 1, 2, 3, 4, 5, 6, 7, 8, 9)
			},
			pullTimes:      1,
			wantSent:       []string{cp, "1", "2", "3", "4", "5", cp},
			wantEventCount: 5,
		},
		{
			name: "fullDocument required mode",
			setup: func(t *testing.T) {
				require.NoError(t, client.Database(testDb).CreateCollection(ctx, testColl1, &options.CreateCollectionOptions{ChangeStreamPreAndPostImages: bson.D{{Key: "enabled", Value: true}}}))
				insertDoc(t, testColl1, 1)
				_, err := client.Database(testDb).Collection(testColl1).UpdateOne(ctx, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "updated", Value: true}}}})
				require.NoError(t, err)
			},
			fullDocRequired: map[string]bool{testDb: true},
			pullTimes:       2,
			wantSent:        []string{cp, "1", cp, "1", cp},
			wantEventCount:  2,
		},
		{
			name: "fullDocument missing on tracked binding with fullDocRequired errors",
			setup: func(t *testing.T) {
				// Collection WITHOUT changeStreamPreAndPostImages but fullDocRequired is set.
				// With whenAvailable, update events on collections without the flag will have
				// null fullDocument since there is no stored post-image to return.
				insertDoc(t, testColl1, 1)
				_, err := client.Database(testDb).Collection(testColl1).UpdateOne(ctx,
					bson.D{{Key: "_id", Value: 1}},
					bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: true}}}})
				require.NoError(t, err)
			},
			fullDocRequired: map[string]bool{testDb: true},
			pullTimes:       5,
			wantErr:         "changeStreamPreAndPostImages",
		},
		{
			name: "fullDocument missing on untracked collection is silently ignored",
			setup: func(t *testing.T) {
				// testColl1 is tracked and has changeStreamPreAndPostImages enabled.
				// testColl3 is NOT tracked and does NOT have the flag. With whenAvailable,
				// an update on testColl3 produces a null fullDocument event in the
				// database-level change stream. This should be silently ignored since
				// testColl3 is not a tracked binding.
				require.NoError(t, client.Database(testDb).CreateCollection(ctx, testColl1, &options.CreateCollectionOptions{ChangeStreamPreAndPostImages: bson.D{{Key: "enabled", Value: true}}}))
				insertDoc(t, testColl3, 1)
				_, err := client.Database(testDb).Collection(testColl3).UpdateOne(ctx,
					bson.D{{Key: "_id", Value: 1}},
					bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: true}}}})
				require.NoError(t, err)
				insertDoc(t, testColl1, 1)
			},
			fullDocRequired: map[string]bool{testDb: true},
			pullTimes:       3,
			wantSent:        []string{cp, "1", cp},
			wantEventCount:  3, // insert on coll3 (skipped: untracked) + update on coll3 (skipped: untracked, no fullDoc) + insert on coll1 (emitted)
		},
		{
			name: "split fragments",
			setup: func(t *testing.T) {
				require.NoError(t, client.Database(testDb).CreateCollection(ctx, testColl1, &options.CreateCollectionOptions{ChangeStreamPreAndPostImages: bson.D{{Key: "enabled", Value: true}}}))

				val := map[string]string{
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
				_, err := client.Database(testDb).Collection(testColl1).InsertOne(ctx, val)
				require.NoError(t, err)

				val["key1"] = "updated"
				val["key9"] = "also updated"
				res, err := client.Database(testDb).Collection(testColl1).UpdateOne(ctx, bson.D{{Key: "_id", Value: val["_id"]}}, bson.D{{Key: "$set", Value: val}})
				require.NoError(t, err)
				require.Equal(t, 1, int(res.ModifiedCount))

				insertDoc(t, testColl1, 2)
			},
			pullTimes:      3,
			wantSent:       []string{cp, "hugeDocument", cp, "hugeDocument", "2", cp},
			wantEventCount: 4, // insert + 2 split fragments (skipped) + update (emitted) + insert
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanup()
			t.Cleanup(cleanup)

			transcoder, err := NewTranscoder(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { transcoder.Stop() })

			srv := &testServer{}

			fullDocRequired := tt.fullDocRequired
			if fullDocRequired == nil {
				fullDocRequired = map[string]bool{}
			}

			c := capture{
				client:     client,
				output:     &boilerplate.PullOutput{Connector_CaptureServer: srv},
				transcoder: transcoder,
				trackedChangeStreamBindings: map[string]bindingInfo{
					resourceId(testDb, testColl1): bindings[0],
					resourceId(testDb, testColl2): bindings[1],
				},
				fullDocRequired:      fullDocRequired,
				state:                captureState{DatabaseResumeTokens: map[string]bson.Raw{}},
				lastEventClusterTime: map[string]primitive.Timestamp{},
			}
			streams, err := c.initializeStreams(ctx, bindings, nil, true, true, false, map[string][]string{}, fullDocRequired)
			require.NoError(t, err)
			require.Equal(t, 1, len(streams))

			stream := streams[0]
			stream.ms.SetBatchSize(testChangeStreamBatchSize)

			// Create channel for batches with buffer size 4
			batches := make(chan streamBatch, 4)

			// Start producer
			producerCtx, cancelProducer := context.WithCancel(ctx)
			defer cancelProducer()

			producerDone := make(chan struct{})
			go func() {
				defer close(producerDone)
				c.produceStreamBatches(producerCtx, stream, batches)
			}()

			// Wait for initial batch to complete (establishes resume token).
			for batch := range batches {
				require.NoError(t, batch.err)
				_, err := c.processBatch(ctx, stream, batch)
				require.NoError(t, err)
				break // First batch establishes resume token
			}

			c.processedStreamEvents = 0
			c.emittedStreamDocs = 0

			// Run test setup (insert documents)
			tt.setup(t)

			// Pull and process batches until we've seen pullTimes batch completions.
			var processErr error
			batchesCompleted := 0
			for batch := range batches {
				require.NoError(t, batch.err)

				_, err := c.processBatch(ctx, stream, batch)
				if err != nil {
					processErr = err
					break
				}

				batchesCompleted++
				if batchesCompleted >= tt.pullTimes {
					break
				}
			}

			// Stop producer
			cancelProducer()
			<-producerDone

			if tt.wantErr != "" {
				require.Error(t, processErr)
				require.Contains(t, processErr.Error(), tt.wantErr)
				return
			}
			require.NoError(t, processErr)

			// MongoDB may non-deterministically batch change events together or
			// separately, and map iteration order in processBatch can vary the
			// emission order across bindings. When the exact sequence doesn't match,
			// fall back to verifying we got the right documents regardless of order.
			if !slices.Equal(tt.wantSent, srv.sent) {
				// Fall back to checking sorted doc IDs when batching is non-deterministic.
				wantDocs := filterDocs(tt.wantSent)
				gotDocs := filterDocs(srv.sent)
				slices.Sort(wantDocs)
				slices.Sort(gotDocs)
				require.Equal(t, wantDocs, gotDocs, "sent docs mismatch (exact was: %v)", srv.sent)
			}
			require.Equal(t, tt.wantEventCount, c.processedStreamEvents)
		})
	}
}

var _ pc.Connector_CaptureServer = (*testServer)(nil)

type testServer struct {
	sent []string
	docs []json.RawMessage
}

func (t *testServer) Send(m *pc.Response) error {
	type captured struct {
		Id string `json:"_id"`
	}

	if m.Checkpoint != nil {
		t.sent = append(t.sent, "checkpoint")
	} else if m.Captured != nil {
		var c captured
		if err := json.Unmarshal(m.Captured.DocJson, &c); err != nil {
			return err
		}
		t.sent = append(t.sent, c.Id)
		t.docs = append(t.docs, m.Captured.DocJson)
	} else {
		panic(fmt.Sprintf("unhandled message: %v", m))
	}

	return nil
}

func (t *testServer) Context() context.Context     { panic("unimplemented") }
func (t *testServer) Recv() (*pc.Request, error)   { panic("unimplemented") }
func (t *testServer) RecvMsg(m any) error          { panic("unimplemented") }
func (t *testServer) SendHeader(metadata.MD) error { panic("unimplemented") }
func (t *testServer) SendMsg(m any) error          { panic("unimplemented") }
func (t *testServer) SetHeader(metadata.MD) error  { panic("unimplemented") }
func (t *testServer) SetTrailer(metadata.MD)       { panic("unimplemented") }

func filterDocs(sent []string) []string {
	var docs []string
	for _, s := range sent {
		if s != "checkpoint" {
			docs = append(docs, s)
		}
	}
	return docs
}

func TestIsMidSplitEvent(t *testing.T) {
	splitEvent := func(v any) bson.D {
		return bson.D{
			{Key: "operationType", Value: "update"},
			{Key: "splitEvent", Value: v},
		}
	}

	tests := []struct {
		name  string
		event bson.D
		want  bool
	}{
		{
			name:  "not a split event",
			event: bson.D{{Key: "operationType", Value: "insert"}},
			want:  false,
		},
		{
			name:  "first fragment of three",
			event: splitEvent(bson.D{{Key: "fragment", Value: int32(1)}, {Key: "of", Value: int32(3)}}),
			want:  true,
		},
		{
			name:  "middle fragment of three",
			event: splitEvent(bson.D{{Key: "fragment", Value: int32(2)}, {Key: "of", Value: int32(3)}}),
			want:  true,
		},
		{
			name:  "final fragment of three",
			event: splitEvent(bson.D{{Key: "fragment", Value: int32(3)}, {Key: "of", Value: int32(3)}}),
			want:  false,
		},
		{
			name:  "single fragment is complete",
			event: splitEvent(bson.D{{Key: "fragment", Value: int32(1)}, {Key: "of", Value: int32(1)}}),
			want:  false,
		},
		{
			name:  "unreadable splitEvent is treated as mid-split",
			event: splitEvent("nonsense"),
			want:  true,
		},
		{
			name:  "splitEvent missing 'of' is treated as mid-split",
			event: splitEvent(bson.D{{Key: "fragment", Value: int32(1)}}),
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := bson.Marshal(tt.event)
			require.NoError(t, err)
			require.Equal(t, tt.want, isMidSplitEvent(bson.Raw(raw)))
		})
	}
}

// TestSplitEventCheckpointSafety asserts the invariant that a resume token is
// never checkpointed for a non-final fragment of a split event. Resuming from
// such a token makes MongoDB deliver the *next* fragment, and the fragments
// already consumed live only in the transcoder's memory, so a restart from that
// position leaves the transcoder unable to reassemble the event.
func TestSplitEventCheckpointSafety(t *testing.T) {
	ctx := context.Background()
	client, _ := testClient(t)

	testDb := "testDb"
	testColl1 := "testColl1"

	// Force the PBRT checkpoint branch to be eligible on every batch, which is
	// the path that used to checkpoint a mid-split token.
	prevInterval := pbrtCheckpointInterval
	pbrtCheckpointInterval = 0
	t.Cleanup(func() { pbrtCheckpointInterval = prevInterval })

	require.NoError(t, client.Database(testDb).Drop(ctx))
	t.Cleanup(func() { require.NoError(t, client.Database(testDb).Drop(ctx)) })

	bindings := []bindingInfo{
		{resource: resource{Database: testDb, Collection: testColl1}, index: 0},
	}

	transcoder, err := NewTranscoder(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { transcoder.Stop() })

	c := capture{
		client:     client,
		output:     &boilerplate.PullOutput{Connector_CaptureServer: &testServer{}},
		transcoder: transcoder,
		trackedChangeStreamBindings: map[string]bindingInfo{
			resourceId(testDb, testColl1): bindings[0],
		},
		fullDocRequired:      map[string]bool{},
		state:                captureState{DatabaseResumeTokens: map[string]bson.Raw{}},
		lastEventClusterTime: map[string]primitive.Timestamp{},
	}

	require.NoError(t, client.Database(testDb).CreateCollection(ctx, testColl1,
		&options.CreateCollectionOptions{ChangeStreamPreAndPostImages: bson.D{{Key: "enabled", Value: true}}}))

	streams, err := c.initializeStreams(ctx, bindings, nil, true, true, false, map[string][]string{}, map[string]bool{})
	require.NoError(t, err)
	require.Equal(t, 1, len(streams))
	stream := streams[0]

	batches := make(chan streamBatch, 4)
	producerCtx, cancelProducer := context.WithCancel(ctx)
	defer cancelProducer()

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		c.produceStreamBatches(producerCtx, stream, batches)
	}()
	defer func() {
		cancelProducer()
		<-producerDone
	}()

	// First batch establishes the resume token.
	for batch := range batches {
		require.NoError(t, batch.err)
		_, err := c.processBatch(ctx, stream, batch)
		require.NoError(t, err)
		break
	}

	// A document large enough that an update carrying both fullDocument and
	// fullDocumentBeforeChange exceeds the 16MB event limit and is split.
	val := map[string]string{"_id": "hugeDocument"}
	for i := 1; i <= 9; i++ {
		val[fmt.Sprintf("key%d", i)] = strings.Repeat(fmt.Sprintf("value%d", i), 200000)
	}
	_, err = client.Database(testDb).Collection(testColl1).InsertOne(ctx, val)
	require.NoError(t, err)

	val["key1"] = "updated"
	res, err := client.Database(testDb).Collection(testColl1).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: val["_id"]}}, bson.D{{Key: "$set", Value: val}})
	require.NoError(t, err)
	require.Equal(t, 1, int(res.ModifiedCount))

	// Resume tokens which must never be checkpointed, and every token which
	// actually was. The fragment position is deliberately re-derived from the
	// raw event here rather than by calling isMidSplitEvent, so that a wrong
	// predicate cannot make this assertion agree with it.
	unsafeTokens := make(map[string]bool)
	checkpointedTokens := make(map[string]bool)
	var sawCompletedSplit bool
	var batchesAfterSplit int

	// produceStreamBatches never closes its channel, so the loop needs its own
	// deadline: without one, a change stream which never splits would block here
	// until the package-wide test timeout took down every test in the package.
	loopCtx, cancelLoop := context.WithTimeout(ctx, 60*time.Second)
	defer cancelLoop()

collect:
	for {
		var batch streamBatch
		select {
		case batch = <-batches:
		case <-loopCtx.Done():
			break collect
		}

		require.NoError(t, batch.err)

		for _, ev := range batch.events {
			fragment, fragmentOk := ev.raw.Lookup("splitEvent", "fragment").Int32OK()
			of, ofOk := ev.raw.Lookup("splitEvent", "of").Int32OK()
			if !fragmentOk || !ofOk {
				continue
			}
			if fragment < of {
				unsafeTokens[string(ev.resumeToken)] = true
			} else {
				sawCompletedSplit = true
			}
		}

		_, err := c.processBatch(ctx, stream, batch)
		require.NoError(t, err)

		if tok, ok := c.state.DatabaseResumeTokens[testDb]; ok {
			checkpointedTokens[string(tok)] = true
		}

		// Keep going past the split so that the checkpoints taken once the
		// event completes are collected too.
		if sawCompletedSplit {
			batchesAfterSplit++
			if batchesAfterSplit >= 3 {
				break collect
			}
		}
	}

	require.True(t, sawCompletedSplit, "change stream never delivered a complete split event")
	require.NotEmpty(t, unsafeTokens, "change stream never delivered a non-final fragment")

	for tok := range unsafeTokens {
		require.False(t, checkpointedTokens[tok],
			"checkpointed a resume token for a non-final fragment of a split event")
	}
}

// TestSplitEventResumedPartway reproduces a capture resuming from a checkpoint
// taken partway through a split event, which is the state a capture wedged
// itself in before mid-split positions stopped being checkpointed. The event's
// remaining fragments cannot be reassembled by a transcoder which never saw the
// first one, so the stream is repositioned to the event itself and captures it
// whole. A reopen which fails, for any reason, fails the capture rather than
// skipping the event.
func TestSplitEventResumedPartway(t *testing.T) {
	ctx := context.Background()
	client, _ := testClient(t)

	testDb := "testDb"
	testColl1 := "testColl1"

	prevInterval := pbrtCheckpointInterval
	pbrtCheckpointInterval = 0
	t.Cleanup(func() { pbrtCheckpointInterval = prevInterval })

	require.NoError(t, client.Database(testDb).Drop(ctx))
	t.Cleanup(func() { require.NoError(t, client.Database(testDb).Drop(ctx)) })

	bindings := []bindingInfo{
		{resource: resource{Database: testDb, Collection: testColl1}, index: 0},
	}

	newCapture := func(srv *testServer, tokens map[string]bson.Raw) capture {
		transcoder, err := NewTranscoder(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { transcoder.Stop() })

		return capture{
			client:     client,
			output:     &boilerplate.PullOutput{Connector_CaptureServer: srv},
			transcoder: transcoder,
			trackedChangeStreamBindings: map[string]bindingInfo{
				resourceId(testDb, testColl1): bindings[0],
			},
			fullDocRequired:      map[string]bool{},
			state:                captureState{DatabaseResumeTokens: tokens},
			lastEventClusterTime: map[string]primitive.Timestamp{},
		}
	}

	// Collects batches from a stream until stop says to finish, or the deadline
	// passes. produceStreamBatches never closes its channel, so a deadline is
	// what keeps a stream which does not split from hanging the package.
	collect := func(c *capture, stream *changeStream, stop func(streamBatch) bool) error {
		batches := make(chan streamBatch, 4)
		producerCtx, cancelProducer := context.WithCancel(ctx)
		producerDone := make(chan struct{})
		go func() {
			defer close(producerDone)
			c.produceStreamBatches(producerCtx, stream, batches)
		}()
		defer func() {
			cancelProducer()
			<-producerDone
		}()

		deadline, cancelDeadline := context.WithTimeout(ctx, 60*time.Second)
		defer cancelDeadline()

		for {
			select {
			case batch := <-batches:
				if batch.err != nil {
					return batch.err
				}
				if stop(batch) {
					return nil
				}
			case <-deadline.Done():
				return nil
			}
		}
	}

	require.NoError(t, client.Database(testDb).CreateCollection(ctx, testColl1,
		&options.CreateCollectionOptions{ChangeStreamPreAndPostImages: bson.D{{Key: "enabled", Value: true}}}))

	// Observe a split event, and remember the resume token of its first
	// fragment: that is the poisoned position a capture could previously
	// persist. Events are only inspected here, never transcoded.
	observer := newCapture(&testServer{}, map[string]bson.Raw{})
	streams, err := observer.initializeStreams(ctx, bindings, nil, true, true, false, map[string][]string{}, map[string]bool{})
	require.NoError(t, err)
	require.Equal(t, 1, len(streams))

	val := map[string]string{"_id": "hugeDocument"}
	for i := 1; i <= 9; i++ {
		val[fmt.Sprintf("key%d", i)] = strings.Repeat(fmt.Sprintf("value%d", i), 200000)
	}
	_, err = client.Database(testDb).Collection(testColl1).InsertOne(ctx, val)
	require.NoError(t, err)

	val["key1"] = "updated"
	_, err = client.Database(testDb).Collection(testColl1).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: val["_id"]}}, bson.D{{Key: "$set", Value: val}})
	require.NoError(t, err)

	// Captured after the split event, and expected to survive either outcome.
	_, err = client.Database(testDb).Collection(testColl1).InsertOne(ctx, bson.D{{Key: "_id", Value: "afterTheSplit"}})
	require.NoError(t, err)

	var firstFragmentToken bson.Raw
	require.NoError(t, collect(&observer, streams[0], func(batch streamBatch) bool {
		for _, ev := range batch.events {
			fragment, of, ok := splitEventFragment(ev.raw)
			if ok && fragment == 1 && of > 1 {
				firstFragmentToken = ev.resumeToken
				return true
			}
		}
		return false
	}))
	require.NotNil(t, firstFragmentToken, "change stream never delivered a split event")

	// resumePartway resumes from partway through that split event with a
	// transcoder which has never seen its first fragment. A non-nil reopen
	// replaces the stream's own, standing in for a reopen which fails.
	resumePartway := func(t *testing.T, reopen func(context.Context, primitive.Timestamp) (*mongo.ChangeStream, error)) (*testServer, error) {
		srv := &testServer{}
		c := newCapture(srv, map[string]bson.Raw{testDb: firstFragmentToken})
		resumed, err := c.initializeStreams(ctx, bindings, nil, true, true, false, map[string][]string{}, map[string]bool{})
		require.NoError(t, err)
		if reopen != nil {
			resumed[0].reopenAt = reopen
		}

		collectErr := collect(&c, resumed[0], func(batch streamBatch) bool {
			_, err := c.processBatch(ctx, resumed[0], batch)
			require.NoError(t, err, "capture failed on a split event it could not reassemble")
			return slices.Contains(srv.sent, "afterTheSplit")
		})

		return srv, collectErr
	}

	t.Run("repositioned to recover the event", func(t *testing.T) {
		srv, err := resumePartway(t, nil)
		require.NoError(t, err)
		require.Contains(t, srv.sent, "hugeDocument", "the split event was not recovered")
		require.Contains(t, srv.sent, "afterTheSplit", "capture did not carry on past the split event")

		// The event must be recovered whole, not merely present. Its post-image
		// carries the updated value, and its pre-image arrives in a later
		// fragment, so finding both proves every fragment was reassembled.
		type capturedDoc struct {
			ID   string `json:"_id"`
			Key1 string `json:"key1"`
			Meta struct {
				Op     string          `json:"op"`
				Before json.RawMessage `json:"before"`
			} `json:"_meta"`
		}
		var recovered capturedDoc
		var found bool
		for _, doc := range srv.docs {
			// Declared per iteration: encoding/json leaves fields absent from
			// the payload untouched, so a reused value could assert against a
			// previous document.
			var candidate capturedDoc
			require.NoError(t, json.Unmarshal(doc, &candidate))
			if candidate.ID == "hugeDocument" {
				recovered, found = candidate, true
				break
			}
		}
		require.True(t, found, "no recovered document for the split event")
		require.Equal(t, "u", recovered.Meta.Op)
		require.Equal(t, "updated", recovered.Key1, "recovered document does not carry the post-image of the event")
		require.NotEmpty(t, recovered.Meta.Before, "recovered document is missing the pre-image, which arrives in a later fragment")
	})

	t.Run("fails when the oplog no longer reaches the event", func(t *testing.T) {
		// Skipping the event would be silent data loss. Failing is consistent
		// with how any other position the oplog no longer reaches is handled.
		historyLost := func(context.Context, primitive.Timestamp) (*mongo.ChangeStream, error) {
			return nil, mongo.CommandError{Code: 286, Name: "ChangeStreamHistoryLost", Message: "Resume of change stream was not possible"}
		}

		srv, err := resumePartway(t, historyLost)
		require.Error(t, err, "expected the capture to fail rather than skip the event")
		require.Contains(t, err.Error(), "repositioning change stream")
		require.NotContains(t, srv.sent, "hugeDocument")
	})

	t.Run("fails on a transient reopen error", func(t *testing.T) {
		// Anything which is not the oplog having moved on is retryable, and a
		// restart re-runs this recovery from the same checkpoint. Skipping here
		// would discard a real change event over an election or a dropped
		// connection.
		notPrimary := func(context.Context, primitive.Timestamp) (*mongo.ChangeStream, error) {
			return nil, mongo.CommandError{Code: 189, Message: "PrimarySteppedDown"}
		}

		srv, err := resumePartway(t, notPrimary)
		require.Error(t, err, "expected the capture to fail rather than skip the event")
		require.Contains(t, err.Error(), "repositioning change stream")
		require.NotContains(t, srv.sent, "hugeDocument")
	})
}
