package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/segmentio/encoding/json"
)

// No state is maintained for this connector, so all checkpoints are an empty
// checkpoint.
var emptyCheckpoint = json.RawMessage("{}")

type runtimeAck chan struct{}

type emitMessage struct {
	m           *pubsub.Message
	binding     int
	subcription string
	topic       string
}

// emitter provides synchronization for emitted documents with their checkpoints
// and runtime acknowledgements of those checkpoints.
//
// When a received PubSub message is requested to be emitted as a captured
// document, the caller of `emit` will receive a `runtimeAck` channel which is
// closed when the connector has received acknowledgement from the runtime of
// the checkpoint corresponding to that emitted document. When the `runtimeAck`
// channel is closed, the message can be acknowledged to PubSub. Inevitably this
// kind of strategy is at-least-once, since the connector could crash between
// emitting the document and sending the PubSub acknowledgement.
type emitter struct {
	mu          sync.Mutex
	stream      *boilerplate.PullOutput
	pendingAcks chan runtimeAck
}

func newEmitter(stream *boilerplate.PullOutput) *emitter {
	return &emitter{
		stream: stream,
		// The buffer size of `pendingAcks` is pretty arbitrary. If the channel
		// fills up, calls to `emit` will block until some acknowledgements are
		// received from the runtime. Since the channel is holding only channels
		// itself it can be fairly large without causing memory issues, and
		// practically may only limit how many documents the runtime batches
		// together into a single checkpoint, since the connector will pause
		// when the channel is full.
		pendingAcks: make(chan runtimeAck, 50_000),
	}
}

func (e *emitter) emit(ctx context.Context, m emitMessage) (runtimeAck, error) {
	// Runtime acknowledgement signals must be sequenced in the same order as
	// output documents, so this operation is done under a lock to prevent races
	// between sequencing the acknowledgement signal and outputting the
	// document.
	e.mu.Lock()
	defer e.mu.Unlock()

	ack := make(runtimeAck)

	// Add the runtime acknowledgement signal channel to the `pendingAcks`
	// channel first. it will be closed when the runtime has acknowledged the
	// durable commit for the emitted document.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e.pendingAcks <- ack:
	}

	// Now output the message.
	if doc, err := makeDoc(m); err != nil {
		return nil, fmt.Errorf("making document: %w", err)
	} else if err := e.stream.DocumentsAndCheckpoint(emptyCheckpoint, true, m.binding, doc); err != nil {
		return nil, fmt.Errorf("emitting document: %w", err)
	}

	return ack, nil
}

// runtimeAckWorker reads acknowledgement messages from the runtime and signals
// that the PubSub message handlers can acknowledge their received messages by
// closing the pending `runtimeAck` channel corresponding to that message.
func (e *emitter) runtimeAckWorker(ctx context.Context) error {
	for {
		request, err := e.stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if err = request.Validate_(); err != nil {
			return fmt.Errorf("validating request: %w", err)
		} else if request.Acknowledge == nil {
			return fmt.Errorf("unexpected message %#v", request)
		}

		n := request.Acknowledge.Checkpoints
		for idx := 0; idx < int(n); idx++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ack := <-e.pendingAcks:
				close(ack)
			default:
				return fmt.Errorf("received runtime acknowledgement with no pending message acknowledgement (n=%d)", n)
			}
		}

	}
}

func makeDoc(m emitMessage) (json.RawMessage, error) {
	msg := m.m

	meta := documentMetadata{
		ID:          msg.ID,
		Subcription: m.subcription,
		Topic:       m.topic,
	}

	if !msg.PublishTime.IsZero() {
		meta.PublishTime = msg.PublishTime.UTC().Format(time.RFC3339Nano)
	}
	if len(msg.Attributes) > 0 {
		meta.Attributes = msg.Attributes
	}
	if msg.OrderingKey != "" {
		meta.OrderingKey = msg.OrderingKey
	}

	var doc map[string]any
	// TODO(whb): For now we only support messages encoded as JSON. Future
	// support for other formats could be considered.
	if err := json.Unmarshal(msg.Data, &doc); err != nil {
		return nil, fmt.Errorf("could not unmarshal message data as a JSON object: %w", err)
	}
	doc["_meta"] = meta

	return json.Marshal(doc)
}
