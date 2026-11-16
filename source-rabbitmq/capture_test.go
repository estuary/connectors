package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestIntegrationCapture(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := buildClient(ctx, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	channel, err := client.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { channel.Close() })
	require.NoError(t, channel.Confirm(false))

	// Exceed prefetch on each queue so completion requires runtime commits to
	// release delivery credit. No management API is available to the capture.
	const count = defaultPrefetchCount + 1
	cfg.ManagementPort = 1
	var bindings []*pf.CaptureSpec_Binding
	queueTypes := []string{"classic", "quorum", "stream"}
	prefix := fmt.Sprintf("test-capture-%d-", time.Now().UnixNano())
	for _, queueType := range queueTypes {
		queue := prefix + queueType
		_, err := channel.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": queueType})
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := channel.QueueDelete(queue, false, false, false)
			require.NoError(t, err)
		})
		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: mustMarshal(t, resource{Queue: queue, QueueType: queueType}),
			ResourcePath:       []string{queue},
			StateKey:           queue + ".v0",
		})
		for i := range count {
			body := fmt.Sprintf(`{"id":%d,"canary":%q}`, i, queueType)
			if queueType == "classic" && i == 2 {
				body = "plain text o3"
			}
			confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
				MessageId: fmt.Sprintf("m%d", i), Body: []byte(body), DeliveryMode: amqp.Persistent,
			})
			require.NoError(t, err)
			confirmed, err := confirmation.WaitContext(ctx)
			require.NoError(t, err)
			require.True(t, confirmed)
		}
	}

	fake := newFakeCaptureServer(ctx)
	configJSON := mustMarshal(t, cfg)
	done := make(chan error, 1)
	go func() {
		done <- driver{}.Pull(&pc.Request_Open{
			Capture: &pf.CaptureSpec{ConfigJson: configJSON, Bindings: bindings},
		}, &boilerplate.PullOutput{Connector_CaptureServer: fake, NumBindings: len(bindings)})
	}()
	require.NotNil(t, receive(t, fake.responses).Opened)
	seen := make(map[string]bool)
	state := json.RawMessage(`{}`)
	for range count * len(queueTypes) {
		captured := receive(t, fake.responses).Captured
		require.NotNil(t, captured)
		require.Less(t, int(captured.Binding), len(bindings))
		queueType := queueTypes[captured.Binding]
		var doc struct {
			ID     int              `json:"id"`
			Canary string           `json:"canary"`
			Body   string           `json:"body"`
			Meta   documentMetadata `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(captured.DocJson, &doc))
		require.Greater(t, len(doc.Meta.MessageID), 1)
		i, err := strconv.Atoi(doc.Meta.MessageID[1:])
		require.NoError(t, err)
		require.GreaterOrEqual(t, i, 0)
		require.Less(t, i, count)
		key := queueType + ":" + strconv.Itoa(i)
		require.NotContains(t, seen, key)
		seen[key] = true
		require.Equal(t, prefix+queueType, doc.Meta.Queue)
		require.Equal(t, prefix+queueType, doc.Meta.RoutingKey)
		if queueType == "classic" && i == 2 {
			require.Equal(t, "plain text o3", doc.Body)
		} else {
			require.Equal(t, i, doc.ID)
			require.Equal(t, queueType, doc.Canary)
		}
		checkpoint := receive(t, fake.responses).Checkpoint
		require.NotNil(t, checkpoint)
		if queueType == "stream" {
			require.NotNil(t, doc.Meta.Offset)
			require.Equal(t, int64(i), *doc.Meta.Offset)
			require.Equal(t, prefix+"stream:"+strconv.Itoa(i), doc.Meta.ID)
			var state captureState
			require.NoError(t, json.Unmarshal(checkpoint.State.UpdatedJson, &state))
			require.Equal(t, int64(i+1), state.Queues[bindings[captured.Binding].StateKey].NextOffset)
		} else {
			require.Nil(t, doc.Meta.Offset)
			require.Equal(t, doc.Meta.MessageID, doc.Meta.ID)
		}
		state = mergeCheckpoint(t, state, checkpoint)
		fake.requests <- &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: 1}}
	}
	close(fake.requests)
	require.NoError(t, receive(t, done))
	require.Empty(t, fake.responses)
	require.JSONEq(t, fmt.Sprintf(`{"bindingStateV1":{%q:{"nextOffset":%d}}}`, prefix+"stream.v0", count), string(state), "classic and quorum checkpoints must not write state")

	for _, queueType := range []string{"classic", "quorum"} {
		_, ok, err := channel.Get(prefix+queueType, false)
		require.NoError(t, err)
		require.False(t, ok, "committed messages must be removed from %s", queueType)
	}

	// Stream acknowledgements must leave the original messages readable.
	reader, err := client.Channel()
	require.NoError(t, err)
	defer reader.Close()
	require.NoError(t, reader.Qos(defaultPrefetchCount, 0, false))
	deliveries, err := reader.Consume(prefix+"stream", "", false, false, false, false, amqp.Table{"x-stream-offset": "first"})
	require.NoError(t, err)
	first := receive(t, deliveries)
	require.Equal(t, "m0", first.MessageId)
	require.Equal(t, int64(0), first.Headers["x-stream-offset"])
}

func TestIntegrationResume(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := buildClient(ctx, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	channel, err := client.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { channel.Close() })
	require.NoError(t, channel.Confirm(false))
	cfg.ManagementPort = 1

	var bindings []*pf.CaptureSpec_Binding
	queueTypes := []string{"classic", "quorum", "stream"}
	prefix := fmt.Sprintf("test-resume-%d-", time.Now().UnixNano())
	for _, queueType := range queueTypes {
		queue := prefix + queueType
		_, err := channel.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": queueType})
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := channel.QueueDelete(queue, false, false, false)
			require.NoError(t, err)
		})
		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: mustMarshal(t, resource{Queue: queue}), ResourcePath: []string{queue}, StateKey: queue + ".v0",
		})
	}

	// The first run commits state without any Acknowledge, as a preview does,
	// so the second run captures those classic and quorum messages again.
	state := json.RawMessage(`{}`)
	for run := range 2 {
		start, end := 0, 3
		if run == 1 {
			start, end = 3, 5
		}
		for _, queueType := range queueTypes {
			for i := start; i < end; i++ {
				confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, "", prefix+queueType, false, false, amqp.Publishing{
					MessageId: fmt.Sprintf("m%d", i), Body: []byte(fmt.Sprintf(`{"id":%d}`, i)), DeliveryMode: amqp.Persistent,
				})
				require.NoError(t, err)
				confirmed, err := confirmation.WaitContext(ctx)
				require.NoError(t, err)
				require.True(t, confirmed)
			}
		}

		fake := newFakeCaptureServer(ctx)
		done := make(chan error, 1)
		open := &pc.Request_Open{
			Capture: &pf.CaptureSpec{ConfigJson: mustMarshal(t, cfg), Bindings: bindings}, StateJson: state,
		}
		go func() {
			done <- driver{}.Pull(open, &boilerplate.PullOutput{Connector_CaptureServer: fake, NumBindings: len(bindings)})
		}()
		require.NotNil(t, receive(t, fake.responses).Opened)
		// Streams resume from the saved offset; unacknowledged classic and
		// quorum messages are redelivered.
		var want int
		for _, queueType := range queueTypes {
			if queueType == "stream" {
				want += end - start
			} else {
				want += end
			}
		}
		seen := make(map[string]bool)
		for len(seen) < want {
			captured := receive(t, fake.responses).Captured
			require.NotNil(t, captured)
			require.Less(t, int(captured.Binding), len(bindings))
			queueType := queueTypes[captured.Binding]
			var doc struct {
				ID   int              `json:"id"`
				Meta documentMetadata `json:"_meta"`
			}
			require.NoError(t, json.Unmarshal(captured.DocJson, &doc))
			require.Less(t, doc.ID, end)
			key := fmt.Sprintf("%s:%d", queueType, doc.ID)
			require.NotContains(t, seen, key)
			seen[key] = true
			require.Equal(t, prefix+queueType, doc.Meta.Queue)
			if queueType == "stream" {
				require.GreaterOrEqual(t, doc.ID, start, "streams must resume from the saved offset")
				require.NotNil(t, doc.Meta.Offset)
				require.Equal(t, int64(doc.ID), *doc.Meta.Offset)
				require.Equal(t, fmt.Sprintf("%sstream:%d", prefix, doc.ID), doc.Meta.ID)
			} else {
				require.Equal(t, fmt.Sprintf("m%d", doc.ID), doc.Meta.ID)
				require.Equal(t, doc.ID < start, doc.Meta.Redelivered, "uncommitted messages must be captured again")
			}
			state = mergeCheckpoint(t, state, receive(t, fake.responses).Checkpoint)
			if run == 1 {
				fake.requests <- &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: 1}}
			}
		}
		require.JSONEq(t, fmt.Sprintf(`{"bindingStateV1":{%q:{"nextOffset":%d}}}`, prefix+"stream.v0", end), string(state))
		close(fake.requests)
		require.NoError(t, receive(t, done))
		require.Empty(t, fake.responses)
		for _, queueType := range []string{"classic", "quorum"} {
			want := 0
			if run == 0 {
				want = end
			}
			// The broker requeues a closed connection's unacknowledged deliveries
			// asynchronously, and passive declarations count only ready messages.
			require.Eventually(t, func() bool {
				queue, err := channel.QueueInspect(prefix + queueType)
				require.NoError(t, err)
				return queue.Messages == want
			}, 10*time.Second, 20*time.Millisecond, "unacknowledged messages must remain queued and acknowledged messages must be removed")
		}
	}
}

func TestIntegrationStreamNumericOffset(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := buildClient(ctx, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { client.CloseDeadline(time.Now().Add(5 * time.Second)) })
	stop := context.AfterFunc(ctx, func() { client.CloseDeadline(time.Now()) })
	defer stop()
	channel, err := client.Channel()
	require.NoError(t, err)
	require.NoError(t, channel.Confirm(false))
	queue := fmt.Sprintf("test-numeric-offset-%d", time.Now().UnixNano())
	_, err = channel.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "stream"})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := channel.QueueDelete(queue, false, false, false)
		require.NoError(t, err)
	})

	// Publish a burst so numeric seeks can land inside a storage chunk.
	const count = 64
	var confirmations []*amqp.DeferredConfirmation
	for i := range count {
		confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
			MessageId: fmt.Sprintf("m%d", i), Body: []byte(`{}`), DeliveryMode: amqp.Persistent,
		})
		require.NoError(t, err)
		confirmations = append(confirmations, confirmation)
	}
	for _, confirmation := range confirmations {
		confirmed, err := confirmation.WaitContext(ctx)
		require.NoError(t, err)
		require.True(t, confirmed)
	}
	for _, start := range []int64{1, 17, 63} {
		reader, err := client.Channel()
		require.NoError(t, err)
		require.NoError(t, reader.Qos(count, 0, false))
		deliveries, err := reader.Consume(queue, "", false, false, false, false, amqp.Table{"x-stream-offset": start})
		require.NoError(t, err)
		for offset := start; offset < count; offset++ {
			delivery := receive(t, deliveries)
			require.Equal(t, offset, delivery.Headers["x-stream-offset"], "numeric AMQP offsets must filter earlier entries within a chunk")
			require.Equal(t, fmt.Sprintf("m%d", offset), delivery.MessageId)
			require.NoError(t, delivery.Ack(false))
		}
		require.NoError(t, reader.Close())
	}
}
