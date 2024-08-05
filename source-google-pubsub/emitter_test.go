package main

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"

	"cloud.google.com/go/pubsub"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
)

func TestEmitter(t *testing.T) {
	testSrv := &TestCaptureServer{
		msgsTo: make(chan *pc.Request),
	}

	stream := boilerplate.PullOutput{
		Connector_CaptureServer: testSrv,
	}

	ctx := context.Background()

	// Start up the emitter.
	emitter := newEmitter(&stream)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return emitter.runtimeAckWorker(groupCtx)
	})
	group.Go(func() error {
		return emitter.emitWorker(groupCtx)
	})

	// Emit some messages concurrently with many goroutines.
	numMessages := 50_000
	half := numMessages / 2
	ackdMsgs := make(chan string, numMessages)
	var sent sync.WaitGroup

	for idx := 0; idx < numMessages; idx++ {
		idx := idx
		sent.Add(1)

		go func() {
			m := emitMessage{
				m: &pubsub.Message{
					ID:   strconv.Itoa(idx),
					Data: []byte("{}"),
				},
				subcription: "sub",
				topic:       "topic",
			}

			runtimeAck, err := emitter.emit(ctx, m)
			require.NoError(t, err)

			sent.Done()
			<-runtimeAck       // Simulate ack received from runtime.
			ackdMsgs <- m.m.ID // Simulate ack sent to PubSub.
		}()
	}

	// Wait for all documents to be output from the connector.
	sent.Wait()

	// Get the set of documents that were output from the connector referenced
	// by their IDs.
	gotDocuments := []string{}

	type capturedDoc struct {
		Meta map[string]string `json:"_meta"`
	}

	for _, doc := range testSrv.msgsFrom {
		if doc.Checkpoint != nil {
			continue
		}

		var cap capturedDoc
		require.NoError(t, json.Unmarshal(doc.Captured.DocJson, &cap))
		gotDocuments = append(gotDocuments, cap.Meta["id"])
	}

	// Send a runtime acknowledgement for half of emitted checkpoints.
	testSrv.msgsTo <- &pc.Request{
		Acknowledge: &pc.Request_Acknowledge{
			Checkpoints: uint32(half),
		},
	}

	// Make sure the acknowledged messages match up to the acknowledged runtime
	// commits for just the first half.
	doneCount := 0
	gotDocs := []string{}
	for id := range ackdMsgs {
		doneCount += 1
		gotDocs = append(gotDocs, id)
		if doneCount == half {
			break
		}
	}
	require.ElementsMatch(t, gotDocuments[:half], gotDocs)

	// Send a runtime acknowledgement for the rest of emitted checkpoints.
	testSrv.msgsTo <- &pc.Request{
		Acknowledge: &pc.Request_Acknowledge{
			Checkpoints: uint32(numMessages - half),
		},
	}

	// Make sure the acknowledged messages match up to the acknowledged runtime
	// commits for the second half.
	doneCount = half
	gotDocs = []string{}
	for id := range ackdMsgs {
		doneCount += 1
		gotDocs = append(gotDocs, id)
		if doneCount == numMessages {
			break
		}
	}
	require.ElementsMatch(t, gotDocuments[half:], gotDocs)
}

var _ pc.Connector_CaptureServer = &TestCaptureServer{}

type TestCaptureServer struct {
	msgsTo   chan *pc.Request
	msgsFrom []pc.Response
}

func (t *TestCaptureServer) Send(msg *pc.Response) error {
	t.msgsFrom = append(t.msgsFrom, *msg)
	return nil
}

func (t *TestCaptureServer) Recv() (*pc.Request, error) {
	return <-t.msgsTo, nil
}

func (t *TestCaptureServer) Context() context.Context {
	panic("unimplemented")
}

func (t *TestCaptureServer) RecvMsg(m any) error {
	panic("unimplemented")
}

func (t *TestCaptureServer) SendHeader(metadata.MD) error {
	panic("unimplemented")
}

func (t *TestCaptureServer) SendMsg(m any) error {
	panic("unimplemented")
}

func (t *TestCaptureServer) SetHeader(metadata.MD) error {
	panic("unimplemented")
}

func (t *TestCaptureServer) SetTrailer(metadata.MD) {
	panic("unimplemented")
}
