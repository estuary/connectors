package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
	"unsafe"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	jsonpatch "github.com/evanphx/json-patch/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type fakeCaptureServer struct {
	pc.Connector_CaptureServer
	ctx       context.Context
	requests  chan *pc.Request
	responses chan pc.Response
	onSend    func(*pc.Response) error
}

func newFakeCaptureServer(ctx context.Context) *fakeCaptureServer {
	return &fakeCaptureServer{
		ctx: ctx, requests: make(chan *pc.Request, 16), responses: make(chan pc.Response, 512),
	}
}

func (f *fakeCaptureServer) Context() context.Context { return f.ctx }

func (f *fakeCaptureServer) Send(response *pc.Response) error {
	if f.onSend != nil {
		if err := f.onSend(response); err != nil {
			return err
		}
	}
	// PullOutput reuses Captured responses. Copy before the next Send.
	copy := *response
	if response.Captured != nil {
		captured := *response.Captured
		captured.DocJson = append(json.RawMessage(nil), captured.DocJson...)
		copy.Captured = &captured
	}
	select {
	case <-f.ctx.Done():
		return f.ctx.Err()
	case f.responses <- copy:
		return nil
	}
}

func (f *fakeCaptureServer) Recv() (*pc.Request, error) {
	select {
	case <-f.ctx.Done():
		return nil, io.EOF
	case request, ok := <-f.requests:
		if !ok {
			return nil, io.EOF
		}
		return request, nil
	}
}

type ackCall struct {
	queue    string
	tag      uint64
	multiple bool
}

type fakeAcknowledger struct {
	queue string
	calls chan ackCall
	err   error
}

func (a *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	a.calls <- ackCall{queue: a.queue, tag: tag, multiple: multiple}
	return a.err
}

func (*fakeAcknowledger) Nack(uint64, bool, bool) error { return errors.New("unexpected Nack") }
func (*fakeAcknowledger) Reject(uint64, bool) error     { return errors.New("unexpected Reject") }

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for capture")
		var zero T
		return zero
	}
}

// Classic and quorum checkpoints only pair a document with its acknowledgement.
func requireEmptyCheckpoint(t *testing.T, checkpoint *pc.Response_Checkpoint) {
	t.Helper()
	require.NotNil(t, checkpoint)
	require.True(t, checkpoint.State.MergePatch)
	require.JSONEq(t, `{}`, string(checkpoint.State.UpdatedJson), "classic and quorum checkpoints must not write state")
}

func mergeCheckpoint(t *testing.T, state json.RawMessage, checkpoint *pc.Response_Checkpoint) json.RawMessage {
	t.Helper()
	require.NotNil(t, checkpoint)
	require.True(t, checkpoint.State.MergePatch)
	merged, err := jsonpatch.MergePatch(state, checkpoint.State.UpdatedJson)
	require.NoError(t, err)
	return merged
}

func startTestCapture(t *testing.T, bindings []*bindingInfo, stateJSON json.RawMessage) (*fakeCaptureServer, <-chan error) {
	t.Helper()
	var state captureState
	require.NoError(t, json.Unmarshal(stateJSON, &state))
	for _, binding := range bindings {
		if saved, ok := state.Queues[binding.stateKey]; ok {
			binding.nextOffset = &saved.NextOffset
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	fake := newFakeCaptureServer(ctx)
	done := make(chan error, 1)
	go func() {
		done <- runCapture(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake, NumBindings: len(bindings)}, bindings)
	}()
	require.True(t, receive(t, fake.responses).Opened.ExplicitAcknowledgements)
	return fake, done
}

// Feed the fixture's six messages through the real capture pipeline, withholding
// commits until all documents and their checkpoints have been emitted. Delivery
// tags overlap across the two queues, so each ack must match its checkpoint.
func TestCaptureBackfill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fake := newFakeCaptureServer(ctx)
	stream := &boilerplate.PullOutput{Connector_CaptureServer: fake, NumBindings: 2}
	calls := make(chan ackCall, 6)
	var bindings []*bindingInfo
	for index, queue := range []string{"orders", "events"} {
		deliveries := make(chan amqp.Delivery, 3)
		ack := &fakeAcknowledger{queue: queue, calls: calls}
		for i := 1; i <= 3; i++ {
			id, canary := i, fmt.Sprintf("o%d", i)
			var headers amqp.Table
			if queue == "events" {
				id, canary = 100+i, fmt.Sprintf("e%d", 100+i)
				headers = amqp.Table{"x-stream-offset": int64(i - 1)}
			}
			body := fmt.Sprintf(`{"id":%d,"canary":%q}`, id, canary)
			if queue == "orders" && i == 3 {
				body = "plain text o3"
			}
			deliveries <- amqp.Delivery{
				Acknowledger: ack, DeliveryTag: uint64(i), MessageId: fmt.Sprintf("m%d", id),
				RoutingKey: queue, Headers: headers, Body: []byte(body),
			}
		}
		bindings = append(bindings, &bindingInfo{index: index, queue: queue, stateKey: queue + ".v0", isStream: queue == "events", deliveries: deliveries})
	}
	done := make(chan error, 1)
	go func() { done <- runCapture(ctx, stream, bindings) }()
	ready := receive(t, fake.responses)
	require.NotNil(t, ready.Opened)
	require.True(t, ready.Opened.ExplicitAcknowledgements)

	docs := make(map[string]json.RawMessage)
	var wantAcks []ackCall
	for range 6 {
		captured := receive(t, fake.responses).Captured
		require.NotNil(t, captured)
		var doc struct {
			Meta documentMetadata `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(captured.DocJson, &doc))
		require.NotContains(t, docs, doc.Meta.ID)
		docs[doc.Meta.ID] = captured.DocJson
		checkpoint := receive(t, fake.responses).Checkpoint
		require.NotNil(t, checkpoint)
		require.True(t, checkpoint.State.MergePatch)
		var tag uint64
		if doc.Meta.Queue == "events" {
			require.Equal(t, uint32(1), captured.Binding)
			require.NotNil(t, doc.Meta.Offset)
			tag = uint64(*doc.Meta.Offset + 1)
			require.JSONEq(t, fmt.Sprintf(`{"bindingStateV1":{"events.v0":{"nextOffset":%d}}}`, tag), string(checkpoint.State.UpdatedJson))
		} else {
			require.Equal(t, uint32(0), captured.Binding)
			_, err := fmt.Sscanf(doc.Meta.MessageID, "m%d", &tag)
			require.NoError(t, err)
			requireEmptyCheckpoint(t, checkpoint)
		}
		wantAcks = append(wantAcks, ackCall{queue: doc.Meta.Queue, tag: tag})
	}
	require.Empty(t, calls, "emitting checkpoints must not acknowledge deliveries")
	for i := 1; i <= 3; i++ {
		if i < 3 {
			require.JSONEq(t, fmt.Sprintf(`{"id":%d,"canary":"o%d","_meta":{"queue":"orders","id":"m%d","exchange":"","routingKey":"orders","messageId":"m%d","redelivered":false,"priority":0,"deliveryMode":0}}`, i, i, i, i), string(docs[fmt.Sprintf("m%d", i)]))
		} else {
			require.JSONEq(t, `{"body":"plain text o3","_meta":{"queue":"orders","id":"m3","exchange":"","routingKey":"orders","messageId":"m3","redelivered":false,"priority":0,"deliveryMode":0}}`, string(docs["m3"]))
		}
		require.JSONEq(t, fmt.Sprintf(`{"id":%d,"canary":"e%d","_meta":{"queue":"events","id":"events:%d","exchange":"","routingKey":"events","messageId":"m%d","redelivered":false,"priority":0,"deliveryMode":0,"headers":{"x-stream-offset":%d},"offset":%d}}`, 100+i, 100+i, i-1, 100+i, i-1, i-1), string(docs[fmt.Sprintf("events:%d", i-1)]))
	}

	fake.requests <- &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: 2}}
	for _, want := range wantAcks[:2] {
		require.Equal(t, want, receive(t, calls))
	}
	require.Empty(t, calls, "later checkpoints must remain unacknowledged")
	fake.requests <- &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: 4}}
	for _, want := range wantAcks[2:] {
		require.Equal(t, want, receive(t, calls))
	}
	close(fake.requests)
	require.NoError(t, receive(t, done))
}

func TestCaptureCheckpointAcknowledgedDuringSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fake := newFakeCaptureServer(ctx)
	calls := make(chan ackCall, 1)
	ack := &fakeAcknowledger{queue: "orders", calls: calls}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Body: []byte(`{}`)}
	var acknowledged bool
	fake.onSend = func(response *pc.Response) error {
		if response.Checkpoint != nil && !acknowledged {
			acknowledged = true
			fake.requests <- &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: 1}}
			select {
			case <-calls:
				close(fake.requests)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	err := runCapture(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake}, []*bindingInfo{{queue: "orders", deliveries: deliveries}})
	require.NoError(t, err)
}

func TestCaptureClassicWithStreamHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fake := newFakeCaptureServer(ctx)
	calls := make(chan ackCall, 1)
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: &fakeAcknowledger{calls: calls}, DeliveryTag: 1,
		MessageId: "m1", Body: []byte(`{"id":1}`), Headers: amqp.Table{"x-stream-offset": "producer header"},
	}
	done := make(chan error, 1)
	go func() {
		done <- runCapture(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake}, []*bindingInfo{{queue: "orders", deliveries: deliveries}})
	}()
	require.NotNil(t, receive(t, fake.responses).Opened)
	captured := receive(t, fake.responses).Captured
	require.NotNil(t, captured)
	var doc struct {
		Meta documentMetadata `json:"_meta"`
	}
	require.NoError(t, json.Unmarshal(captured.DocJson, &doc))
	require.Equal(t, "m1", doc.Meta.ID)
	require.Nil(t, doc.Meta.Offset)
	require.Equal(t, "producer header", doc.Meta.Headers["x-stream-offset"])
	requireEmptyCheckpoint(t, receive(t, fake.responses).Checkpoint)
	close(fake.requests)
	require.NoError(t, receive(t, done))
	require.Empty(t, calls)
}

func TestCaptureStreamRequiresOffset(t *testing.T) {
	for _, headers := range []amqp.Table{nil, {"x-stream-offset": "invalid"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		fake := newFakeCaptureServer(ctx)
		calls := make(chan ackCall, 1)
		deliveries := make(chan amqp.Delivery, 1)
		deliveries <- amqp.Delivery{
			Acknowledger: &fakeAcknowledger{calls: calls}, DeliveryTag: 1,
			MessageId: "m101", Body: []byte(`{"id":101}`), Headers: headers,
		}
		err := runCapture(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake}, []*bindingInfo{{queue: "events", isStream: true, deliveries: deliveries}})
		cancel()
		require.ErrorContains(t, err, "x-stream-offset")
		require.ErrorContains(t, err, "events")
		require.NotNil(t, receive(t, fake.responses).Opened)
		require.Empty(t, fake.responses, "a stream offset is required to checkpoint a message")
		require.Empty(t, calls)
	}
}

func TestCaptureStopsWithoutAcknowledgingUncommittedDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fake := newFakeCaptureServer(ctx)
	calls := make(chan ackCall, 1)
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{Acknowledger: &fakeAcknowledger{calls: calls}, DeliveryTag: 1, Body: []byte(`{}`)}
	done := make(chan error, 1)
	go func() {
		done <- runCapture(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake}, []*bindingInfo{{queue: "orders", deliveries: deliveries}})
	}()
	for range 3 {
		receive(t, fake.responses) // Opened, document, checkpoint.
	}
	close(fake.requests)
	require.NoError(t, receive(t, done))
	require.Empty(t, calls)
}

func TestPendingDeliveryReleasesBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newFakeCaptureServer(ctx)
	messages := make(chan capturedMessage, 1)
	pending := make(chan pendingEntry, 1)
	calls := make(chan ackCall, 1)
	messages <- capturedMessage{
		binding: &bindingInfo{queue: "orders", stateKey: "orders"},
		delivery: amqp.Delivery{
			Acknowledger: &fakeAcknowledger{calls: calls}, DeliveryTag: 7,
			MessageId: "large", Body: bytes.Repeat([]byte("x"), 12<<20),
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- runTransactionLoop(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake}, messages, pending)
	}()
	captured := receive(t, fake.responses).Captured
	require.NotNil(t, captured)
	var doc struct {
		Body string `json:"body"`
	}
	require.NoError(t, json.Unmarshal(captured.DocJson, &doc))
	require.Len(t, doc.Body, 12<<20)
	require.NotNil(t, receive(t, fake.responses).Checkpoint)
	entry := receive(t, pending)
	require.LessOrEqual(t, unsafe.Sizeof(entry), uintptr(64), "pending acknowledgements must release emitted message bodies")
	require.Empty(t, calls)
	require.NoError(t, entry.acknowledger.Ack(entry.deliveryTag, false))
	require.Equal(t, ackCall{tag: 7}, receive(t, calls))
	cancel()
	require.ErrorIs(t, receive(t, done), context.Canceled)
}

func TestPendingEntrySize(t *testing.T) {
	require.LessOrEqual(t, unsafe.Sizeof(pendingEntry{}), uintptr(32), "the FIFO must retain only acknowledgement metadata")
}

func TestCaptureInvalidTimestamp(t *testing.T) {
	for _, seconds := range []int64{1704067200000, -70000000000} {
		t.Run(fmt.Sprint(seconds), func(t *testing.T) {
			calls := make(chan ackCall, 1)
			deliveries := make(chan amqp.Delivery, 1)
			deliveries <- amqp.Delivery{
				Acknowledger: &fakeAcknowledger{calls: calls}, DeliveryTag: 1,
				MessageId: "m1", Body: []byte(`{"event":"ping"}`), Timestamp: time.Unix(seconds, 0),
			}
			fake, done := startTestCapture(t, []*bindingInfo{{queue: "orders", stateKey: "orders", deliveries: deliveries}}, json.RawMessage(`{}`))
			captured := receive(t, fake.responses).Captured
			require.NotNil(t, captured)
			var doc struct {
				Event string         `json:"event"`
				Meta  map[string]any `json:"_meta"`
			}
			require.NoError(t, json.Unmarshal(captured.DocJson, &doc))
			require.Equal(t, "ping", doc.Event)
			require.NotContains(t, doc.Meta, "timestamp")
			requireEmptyCheckpoint(t, receive(t, fake.responses).Checkpoint)
			fake.requests <- &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: 1}}
			require.Equal(t, ackCall{tag: 1}, receive(t, calls))
			close(fake.requests)
			require.NoError(t, receive(t, done))
			require.Empty(t, fake.responses)
		})
	}
}

func TestCaptureOutputFailure(t *testing.T) {
	for _, stage := range []string{"document", "checkpoint"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fake := newFakeCaptureServer(ctx)
			failure := errors.New("runtime write failed")
			fake.onSend = func(response *pc.Response) error {
				if stage == "document" && response.Captured != nil || stage == "checkpoint" && response.Checkpoint != nil {
					return failure
				}
				return nil
			}
			calls := make(chan ackCall, 1)
			deliveries := make(chan amqp.Delivery, 1)
			deliveries <- amqp.Delivery{Acknowledger: &fakeAcknowledger{calls: calls}, DeliveryTag: 1, Body: []byte(`{}`)}
			err := runCapture(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake}, []*bindingInfo{{queue: "orders", deliveries: deliveries}})
			require.ErrorIs(t, err, failure)
			require.Empty(t, calls)
		})
	}
}

func TestCaptureConsumerClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fake := newFakeCaptureServer(ctx)
	deliveries := make(chan amqp.Delivery)
	close(deliveries)
	closed := make(chan *amqp.Error, 1)
	closed <- &amqp.Error{Code: 404, Reason: "queue deleted"}
	err := runCapture(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake}, []*bindingInfo{{queue: "orders", deliveries: deliveries, closed: closed}})
	require.ErrorContains(t, err, "queue deleted")
	require.ErrorContains(t, err, "orders")
}

func TestProcessAcknowledgementsErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message ackMessage
		ackErr  error
		want    string
	}{
		{"unknown checkpoint", ackMessage{acknowledged: 2}, nil, "never emitted"},
		{"runtime read", ackMessage{err: errors.New("read failed")}, nil, "read failed"},
		{"AMQP ack", ackMessage{acknowledged: 1}, errors.New("channel closed"), "channel closed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			calls := make(chan ackCall, 1)
			pending := make(chan pendingEntry, 1)
			pending <- pendingEntry{binding: &bindingInfo{queue: "orders"}, acknowledger: &fakeAcknowledger{calls: calls, err: tc.ackErr}, deliveryTag: 1}
			acks := make(chan ackMessage, 1)
			acks <- tc.message
			require.ErrorContains(t, processAcknowledgements(ctx, acks, pending), tc.want)
		})
	}
}

// A preview can commit its final state without sending Acknowledge. Reopen from
// that state with every classic delivery still on the broker: each is captured
// again and acknowledged after its new checkpoint commits, while the stream
// resumes from its saved offset.
func TestCaptureRedeliveryAfterRestart(t *testing.T) {
	state := json.RawMessage(`{}`)
	for run := range 2 {
		calls := make(chan ackCall, 10)
		var bindings []*bindingInfo
		var want []string
		for index, queue := range []string{"orders", "events"} {
			deliveries := make(chan amqp.Delivery, 5)
			for i := 1; i <= 3+2*run; i++ {
				if queue == "events" && run == 1 && i <= 3 {
					continue // Stream consumers resume at the saved offset.
				}
				id := fmt.Sprintf("m%d", i)
				var headers amqp.Table
				if queue == "events" {
					id = fmt.Sprintf("events:%d", i-1)
					headers = amqp.Table{"x-stream-offset": int64(i - 1)}
				}
				deliveries <- amqp.Delivery{
					Acknowledger: &fakeAcknowledger{queue: queue, calls: calls}, DeliveryTag: uint64(i),
					MessageId: fmt.Sprintf("m%d", i), RoutingKey: queue, Headers: headers, Body: []byte(`{}`),
					Redelivered: queue == "orders" && run == 1 && i <= 3,
				}
				want = append(want, id)
			}
			bindings = append(bindings, &bindingInfo{index: index, queue: queue, stateKey: queue + ".v0", isStream: queue == "events", deliveries: deliveries})
		}
		fake, done := startTestCapture(t, bindings, state)
		var ids []string
		var wantAcks []ackCall
		for range want {
			captured := receive(t, fake.responses).Captured
			require.NotNil(t, captured)
			var doc struct {
				Meta documentMetadata `json:"_meta"`
			}
			require.NoError(t, json.Unmarshal(captured.DocJson, &doc))
			ids = append(ids, doc.Meta.ID)
			var tag uint64
			if doc.Meta.Queue == "orders" {
				_, err := fmt.Sscanf(doc.Meta.ID, "m%d", &tag)
				require.NoError(t, err)
				require.Equal(t, run == 1 && tag <= 3, doc.Meta.Redelivered)
			} else {
				require.NotNil(t, doc.Meta.Offset)
				tag = uint64(*doc.Meta.Offset + 1)
			}
			wantAcks = append(wantAcks, ackCall{queue: doc.Meta.Queue, tag: tag})
			state = mergeCheckpoint(t, state, receive(t, fake.responses).Checkpoint)
		}
		require.ElementsMatch(t, want, ids, "uncommitted redeliveries must be captured again")
		require.JSONEq(t, fmt.Sprintf(`{"bindingStateV1":{"events.v0":{"nextOffset":%d}}}`, 3+2*run), string(state))
		require.Empty(t, calls, "deliveries must wait for their checkpoints to commit")
		if run == 1 {
			fake.requests <- &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: uint32(len(wantAcks))}}
			for _, call := range wantAcks {
				require.Equal(t, call, receive(t, calls))
			}
		}
		close(fake.requests)
		require.NoError(t, receive(t, done))
		require.Empty(t, calls)
		require.Empty(t, fake.responses)
	}
}

func TestCaptureStreamOffsetRegression(t *testing.T) {
	for _, offset := range []int64{0, 1, 2, 999, 1000} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := make(chan ackCall, 1)
			channel := &fakeConsumerChannel{t: t, deliveries: make(chan amqp.Delivery, 1)}
			nextOffset := int64(1000)
			binding := &bindingInfo{queue: "events", queueType: "stream", stateKey: "events", nextOffset: &nextOffset}
			require.NoError(t, binding.consume(func() (consumerChannel, error) { return channel, nil }))
			require.Equal(t, int64(1000), channel.args["x-stream-offset"])
			channel.deliveries <- amqp.Delivery{
				Acknowledger: &fakeAcknowledger{calls: calls}, DeliveryTag: 1,
				Headers: amqp.Table{"x-stream-offset": offset},
			}
			fake := newFakeCaptureServer(ctx)
			done := make(chan error, 1)
			go func() {
				done <- runCapture(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake}, []*bindingInfo{binding})
			}()
			require.NotNil(t, receive(t, fake.responses).Opened)
			if offset == 1000 {
				require.NotNil(t, receive(t, fake.responses).Captured)
				require.NotNil(t, receive(t, fake.responses).Checkpoint)
				close(fake.requests)
				require.NoError(t, receive(t, done))
			} else {
				err := receive(t, done)
				require.ErrorContains(t, err, `queue "events"`)
				require.ErrorContains(t, err, fmt.Sprintf("delivered offset %d", offset))
				require.ErrorContains(t, err, "saved next offset 1000")
				require.ErrorContains(t, err, "backfill")
				require.Equal(t, int64(1000), *binding.nextOffset)
			}
			require.Empty(t, calls)
			require.Empty(t, fake.responses)
		})
	}
}

func TestPullInvalidStateAndResources(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    string
		resource string
		want     string
	}{
		{"invalid state JSON", `{`, `{"queue":"events"}`, "parsing capture state"},
		{"negative offset", `{"bindingStateV1":{"events.v0":{"nextOffset":-1}}}`, `{"queue":"events"}`, "negative stream offset"},
		{"fractional offset", `{"bindingStateV1":{"events.v0":{"nextOffset":0.5}}}`, `{"queue":"events"}`, "parsing capture state"},
		{"unknown state field", `{"bindingStateV1":{"events.v0":{"unexpected":1}}}`, `{"queue":"events"}`, "unknown field"},
		{"missing queue", `{}`, `{}`, "missing 'queue'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCaptureServer(context.Background())
			err := driver{}.Pull(&pc.Request_Open{
				StateJson: json.RawMessage(tc.state),
				Capture: &pf.CaptureSpec{
					ConfigJson: json.RawMessage(validConfigJSON),
					Bindings:   []*pf.CaptureSpec_Binding{{ResourceConfigJson: json.RawMessage(tc.resource), StateKey: "events.v0"}},
				},
			}, &boilerplate.PullOutput{Connector_CaptureServer: fake})
			require.ErrorContains(t, err, tc.want)
		})
	}
}
