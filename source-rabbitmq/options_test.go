package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestPullPrefetch(t *testing.T) {
	for _, queueType := range []string{"classic", "quorum", "stream"} {
		t.Run(queueType, func(t *testing.T) {
			var args = wireTable()
			if queueType == "stream" {
				args = wireTable("x-stream-offset", byte('S'), uint32(5), []byte("first"))
			}
			// Withhold commits for a full prefetch window, which fills the pending FIFO.
			testPullOptions(t, resource{Queue: "events", QueueType: queueType}, nil, 64, 64, args)
		})
	}
	for _, prefetch := range []int{1, 65535} {
		t.Run(fmt.Sprint(prefetch), func(t *testing.T) {
			testPullOptions(t, resource{Queue: "events", QueueType: "quorum"}, nil, prefetch, 1, wireTable())
		})
	}
}

func TestPullStreamStartOffsets(t *testing.T) {
	for _, tc := range []struct {
		value string
		args  []byte
	}{
		{"", wireTable("x-stream-offset", byte('S'), uint32(5), []byte("first"))},
		{"first", wireTable("x-stream-offset", byte('S'), uint32(5), []byte("first"))},
		{"last", wireTable("x-stream-offset", byte('S'), uint32(4), []byte("last"))},
		{"next", wireTable("x-stream-offset", byte('S'), uint32(4), []byte("next"))},
		{"7D", wireTable("x-stream-offset", byte('S'), uint32(2), []byte("7D"))},
		{"2024-01-01T00:00:00Z", wireTable("x-stream-offset", byte('T'), uint64(1704067200))},
	} {
		t.Run(tc.value, func(t *testing.T) {
			res := resource{Queue: "events", QueueType: "stream", StreamStartOffset: tc.value}
			testPullOptions(t, res, nil, 0, 1, tc.args)
			for _, next := range []int64{0, 23} {
				t.Run(fmt.Sprintf("resume-%d", next), func(t *testing.T) {
					state := mustMarshal(t, captureState{Queues: map[string]queueState{"events.v0": {NextOffset: next}}})
					testPullOptions(t, res, state, 0, 1, wireTable("x-stream-offset", byte('l'), uint64(next)))
				})
			}
		})
	}
}

// Classic checkpoints leave other bindings' saved offsets alone, so a disabled
// stream binding resumes from its offset once it is enabled again.
func TestPullLeavesOtherBindingState(t *testing.T) {
	saved := json.RawMessage(`{"bindingStateV1":{"events.v0":{"nextOffset":42}}}`)
	state := testPullOptions(t, resource{Queue: "orders", QueueType: "classic"}, saved, 0, 2, wireTable())
	require.JSONEq(t, string(saved), string(state))
	testPullOptions(t, resource{Queue: "events", QueueType: "stream"}, state, 0, 1, wireTable("x-stream-offset", byte('l'), uint64(42)))
}

// Run Pull for one binding with state key "<queue>.v0" against an in-memory
// broker, and return the state merged from every checkpoint.
func testPullOptions(t *testing.T, res resource, state json.RawMessage, prefetch, count int, args []byte) json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	require.NoError(t, serverConn.SetDeadline(time.Now().Add(10*time.Second)))
	acks := make(chan uint64, count)
	brokerDone := make(chan struct{})
	wantPrefetch := prefetch
	if wantPrefetch == 0 {
		wantPrefetch = 10
	}
	go func() {
		defer close(brokerDone)
		defer serverConn.Close()
		peer := amqpPeer{t: t, conn: serverConn, prefetchCount: wantPrefetch}
		peer.handshake()
		channel, consumer := peer.consume(res.Queue, args)
		peer.sendMethod(channel, 60, 21, wireFields(consumer))
		for i := 1; i <= count; i++ {
			body := fmt.Sprintf(`{"sequence":%d}`, i)
			headers := wireTable()
			if res.QueueType == "stream" {
				headers = wireTable("x-stream-offset", byte('l'), uint64(100+i))
			}
			peer.sendMethod(channel, 60, 60, wireFields(consumer, uint64(i), byte(0), "", res.Queue))
			peer.sendFrame(2, channel, wireFields(uint16(60), uint16(0), uint64(len(body)), uint16(1<<13|1<<7), headers, fmt.Sprintf("m%d", i)))
			peer.sendFrame(3, channel, []byte(body))
		}
		for i := 1; i <= count; i++ {
			ackChannel, fields := peer.readMethod(60, 80)
			require.Equal(t, channel, ackChannel)
			require.Equal(t, wireFields(uint64(i), byte(0)), fields)
			acks <- uint64(i)
		}
		peer.readMethod(10, 50)
		peer.sendMethod(0, 10, 51, nil)
	}()
	d := driver{connect: func(ctx context.Context, cfg *config) (*amqp.Connection, error) {
		options, err := clientConfig(ctx, cfg)
		require.NoError(t, err)
		return amqp.Open(clientConn, options)
	}}
	cfg := config{Host: "rabbitmq.test", Username: "test", Password: "test", PrefetchCount: prefetch}
	fake := newFakeCaptureServer(ctx)
	done := make(chan error, 1)
	go func() {
		done <- d.Pull(&pc.Request_Open{
			StateJson: state,
			Capture: &pf.CaptureSpec{
				ConfigJson: mustMarshal(t, cfg),
				Bindings:   []*pf.CaptureSpec_Binding{{ResourceConfigJson: mustMarshal(t, res), StateKey: res.Queue + ".v0"}},
			},
		}, &boilerplate.PullOutput{Connector_CaptureServer: fake, NumBindings: 1})
	}()
	require.True(t, receive(t, fake.responses).Opened.ExplicitAcknowledgements)
	merged := state
	if merged == nil {
		merged = json.RawMessage(`{}`)
	}
	for i := 1; i <= count; i++ {
		captured := receive(t, fake.responses).Captured
		require.NotNil(t, captured)
		var doc struct {
			Sequence int              `json:"sequence"`
			Meta     documentMetadata `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(captured.DocJson, &doc))
		require.Equal(t, i, doc.Sequence)
		checkpoint := receive(t, fake.responses).Checkpoint
		require.NotNil(t, checkpoint)
		if res.QueueType == "stream" {
			require.Equal(t, fmt.Sprintf("%s:%d", res.Queue, 100+i), doc.Meta.ID)
			require.JSONEq(t, fmt.Sprintf(`{"bindingStateV1":{%q:{"nextOffset":%d}}}`, res.Queue+".v0", 101+i), string(checkpoint.State.UpdatedJson))
		} else {
			require.Equal(t, fmt.Sprintf("m%d", i), doc.Meta.ID)
			requireEmptyCheckpoint(t, checkpoint)
		}
		merged = mergeCheckpoint(t, merged, checkpoint)
	}
	require.Empty(t, acks, "higher prefetch must not acknowledge any message before commit")
	fake.requests <- &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: uint32(count)}}
	for i := 1; i <= count; i++ {
		require.Equal(t, uint64(i), receive(t, acks))
	}
	close(fake.requests)
	require.NoError(t, receive(t, done))
	receive(t, brokerDone)
	require.Empty(t, fake.responses)
	return merged
}
