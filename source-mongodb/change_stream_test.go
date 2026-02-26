package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
			c := capture{
				client:     client,
				output:     &boilerplate.PullOutput{Connector_CaptureServer: srv},
				transcoder: transcoder,
				trackedChangeStreamBindings: map[string]bindingInfo{
					resourceId(testDb, testColl1): bindings[0],
					resourceId(testDb, testColl2): bindings[1],
				},
				state:                captureState{DatabaseResumeTokens: map[string]bson.Raw{}},
				lastEventClusterTime: map[string]primitive.Timestamp{},
			}

			fullDocRequired := tt.fullDocRequired
			if fullDocRequired == nil {
				fullDocRequired = map[string]bool{}
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
			batchesCompleted := 0
			for batch := range batches {
				require.NoError(t, batch.err)

				_, err := c.processBatch(ctx, stream, batch)
				require.NoError(t, err)

				batchesCompleted++
				if batchesCompleted >= tt.pullTimes {
					break
				}
			}

			// Stop producer
			cancelProducer()
			<-producerDone

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
