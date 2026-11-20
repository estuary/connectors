package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

type captureState struct {
	Queues map[string]queueState `json:"bindingStateV1,omitempty"`
}

// Only stream bindings keep state: the next offset to consume.
type queueState struct {
	NextOffset int64 `json:"nextOffset"`
}

// Classic and quorum checkpoints carry no state; each one only pairs a
// document with the acknowledgement of its delivery.
var emptyCheckpoint = json.RawMessage("{}")

func (s *captureState) Validate() error {
	for key, queue := range s.Queues {
		if queue.NextOffset < 0 {
			return fmt.Errorf("negative stream offset for binding %q", key)
		}
	}
	return nil
}

type bindingInfo struct {
	index         int
	queue         string
	queueType     string
	prefetchCount int
	startOffset   any
	stateKey      string
	isStream      bool
	nextOffset    *int64
	deliveries    <-chan amqp.Delivery
	closed        <-chan *amqp.Error
}

type capturedMessage struct {
	binding  *bindingInfo
	delivery amqp.Delivery
}

func (d driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	ctx := stream.Context()

	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	var state captureState
	if len(open.StateJson) > 0 {
		if err := pf.UnmarshalStrict(open.StateJson, &state); err != nil {
			return fmt.Errorf("parsing capture state: %w", err)
		}
	}

	var bindings []*bindingInfo
	for idx, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		startOffset, err := res.initialStreamOffset()
		if err != nil {
			return err
		}
		info := &bindingInfo{index: idx, queue: res.Queue, queueType: res.QueueType, stateKey: binding.StateKey, prefetchCount: cfg.PrefetchCount, startOffset: startOffset}
		// State for bindings outside this spec is left alone, so a disabled
		// stream binding resumes from its saved offset when re-enabled.
		if saved, ok := state.Queues[info.stateKey]; ok {
			info.nextOffset = &saved.NextOffset
		}
		bindings = append(bindings, info)
	}

	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	connect := d.connect
	if connect == nil {
		connect = buildClient
	}
	client, err := connect(connectCtx, &cfg)
	cancel()
	if err != nil {
		return err
	}
	defer func() { client.CloseDeadline(time.Now().Add(5 * time.Second)) }()
	stopCapture := context.AfterFunc(ctx, func() { client.CloseDeadline(time.Now()) })
	defer stopCapture()

	for _, binding := range bindings {
		setupCtx, cancel := context.WithTimeout(ctx, connectTimeout)
		stop := context.AfterFunc(setupCtx, func() { client.CloseDeadline(time.Now()) })
		err := binding.consume(func() (consumerChannel, error) { return client.Channel() })
		stopped := stop()
		cancel()
		if !stopped {
			return setupCtx.Err()
		}
		if err != nil {
			return err
		}
	}

	return runCapture(ctx, stream, bindings)
}

type consumerChannel interface {
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	NotifyClose(chan *amqp.Error) chan *amqp.Error
}

func (b *bindingInfo) consume(openChannel func() (consumerChannel, error)) error {
	var offset any = "first"
	if b.startOffset != nil {
		offset = b.startOffset
	}
	if b.nextOffset != nil {
		offset = *b.nextOffset
	}
	if b.queueType != "" {
		b.isStream = b.queueType == "stream"
		var args amqp.Table
		if b.isStream {
			args = amqp.Table{"x-stream-offset": offset}
		}
		return b.startConsumer(openChannel, args)
	}

	// Older bindings omit the queue type, and passive declarations don't return
	// queue arguments. Probe with a stream offset and retry classic and quorum
	// queues on a fresh channel after they reject it.
	err := b.startConsumer(openChannel, amqp.Table{"x-stream-offset": offset})
	if err == nil {
		b.isStream = true
		return nil
	}
	if !streamOffsetUnsupported(err) {
		return err
	}
	b.isStream = false
	return b.startConsumer(openChannel, nil)
}

func streamOffsetUnsupported(err error) bool {
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && amqpErr.Code == amqp.PreconditionFailed &&
		strings.HasPrefix(amqpErr.Reason, "PRECONDITION_FAILED - invalid arg 'x-stream-offset' for queue ") &&
		(strings.HasSuffix(amqpErr.Reason, "of queue type rabbit_classic_queue") ||
			strings.HasSuffix(amqpErr.Reason, "of queue type rabbit_quorum_queue"))
}

func (b *bindingInfo) startConsumer(openChannel func() (consumerChannel, error), args amqp.Table) error {
	channel, err := openChannel()
	if err != nil {
		return fmt.Errorf("opening channel for queue %q: %w", b.queue, err)
	}
	b.closed = channel.NotifyClose(make(chan *amqp.Error, 1))
	_, err = channel.QueueDeclarePassive(b.queue, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("queue %q is not accessible: %w", b.queue, err)
	}
	if err := channel.Qos(b.prefetch(), 0, false); err != nil {
		return fmt.Errorf("setting prefetch for queue %q: %w", b.queue, err)
	}

	b.deliveries, err = channel.Consume(b.queue, "", false, false, false, false, args)
	if err != nil {
		return fmt.Errorf("consuming queue %q: %w", b.queue, err)
	}
	return nil
}

func (b *bindingInfo) prefetch() int {
	if b.prefetchCount == 0 {
		return defaultPrefetchCount
	}
	return b.prefetchCount
}

func runCapture(ctx context.Context, stream *boilerplate.PullOutput, bindings []*bindingInfo) error {
	if err := stream.Ready(true); err != nil {
		return err
	}

	messages := make(chan capturedMessage)
	// Each entry is one unacknowledged delivery, which prefetch bounds per
	// queue. A full FIFO backpressures emission.
	var capacity int
	for _, binding := range bindings {
		capacity += binding.prefetch()
	}
	pending := make(chan pendingEntry, max(1, capacity))
	acks := make(chan ackMessage, 16)
	group, groupCtx := errgroup.WithContext(ctx)

	// Recv cannot be cancelled on the stdio transport, so keep the reader out
	// of the errgroup. A blocked read is reclaimed when the process exits.
	go readAcknowledgements(groupCtx, stream, acks)
	group.Go(func() error { return processAcknowledgements(groupCtx, acks, pending) })
	group.Go(func() error { return runTransactionLoop(groupCtx, stream, messages, pending) })
	for _, binding := range bindings {
		group.Go(func() error { return runReceiver(groupCtx, binding, messages) })
	}

	if err := group.Wait(); err != nil && !errors.Is(err, errCaptureStopped) {
		return err
	}
	return nil
}

func runReceiver(ctx context.Context, binding *bindingInfo, messages chan<- capturedMessage) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-binding.deliveries:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				select {
				case err := <-binding.closed:
					if err != nil {
						return fmt.Errorf("consuming queue %q: %w", binding.queue, err)
					}
				default:
				}
				return fmt.Errorf("consumer for queue %q closed", binding.queue)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case messages <- capturedMessage{binding: binding, delivery: delivery}:
			}
		}
	}
}

// A single emitter keeps documents, stream offsets, and the pending FIFO in
// checkpoint order, even when deliveries arrive from different queues.
func runTransactionLoop(ctx context.Context, stream *boilerplate.PullOutput, messages <-chan capturedMessage, pending chan<- pendingEntry) error {
	for {
		var message capturedMessage
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message = <-messages:
		}
		binding, delivery := message.binding, message.delivery
		var offset *int64
		if binding.isStream {
			var err error
			offset, err = streamOffset(delivery.Headers)
			if err != nil {
				return fmt.Errorf("reading stream offset for queue %q: %w", binding.queue, err)
			}
			if offset == nil {
				return fmt.Errorf("missing x-stream-offset header for stream queue %q", binding.queue)
			}
		}
		if offset != nil && binding.nextOffset != nil && *offset < *binding.nextOffset {
			return fmt.Errorf("stream offset regression for queue %q: delivered offset %d is below saved next offset %d; the stream may have been recreated or reset; backfill this binding into a new collection to avoid reusing document IDs", binding.queue, *offset, *binding.nextOffset)
		}

		doc, err := makeDocument(binding.queue, delivery, offset)
		if err != nil {
			return err
		}
		checkpoint := emptyCheckpoint
		if offset != nil {
			next := *offset + 1
			checkpoint, err = json.Marshal(captureState{Queues: map[string]queueState{
				binding.stateKey: {NextOffset: next},
			}})
			if err != nil {
				return fmt.Errorf("serializing stream state: %w", err)
			}
			binding.nextOffset = &next
		}
		if err := stream.Documents(binding.index, doc); err != nil {
			return err
		}
		if err := emitCheckpoint(ctx, stream, pending, pendingEntry{binding: binding, acknowledger: delivery.Acknowledger, deliveryTag: delivery.DeliveryTag}, checkpoint); err != nil {
			return err
		}
	}
}

func emitCheckpoint(ctx context.Context, stream *boilerplate.PullOutput, pending chan<- pendingEntry, entry pendingEntry, checkpoint json.RawMessage) error {
	// Push before emitting the checkpoint: the runtime may acknowledge it
	// as soon as Checkpoint writes it, before that call even returns.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pending <- entry:
	}
	return stream.Checkpoint(checkpoint, true)
}
