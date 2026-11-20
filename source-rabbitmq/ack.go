package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	amqp "github.com/rabbitmq/amqp091-go"
)

var errCaptureStopped = errors.New("capture stopped by runtime")

type pendingEntry struct {
	binding      *bindingInfo
	acknowledger amqp.Acknowledger
	deliveryTag  uint64
}

type ackMessage struct {
	acknowledged uint32
	err          error
}

func readAcknowledgements(ctx context.Context, stream *boilerplate.PullOutput, acks chan<- ackMessage) {
	for {
		request, err := stream.Recv()
		var count uint32
		if err == nil {
			if request == nil || request.Acknowledge == nil {
				err = errors.New("expected runtime acknowledgement")
			} else if err = request.Validate_(); err == nil {
				count = request.Acknowledge.Checkpoints
			}
		}
		select {
		case <-ctx.Done():
			return
		case acks <- ackMessage{acknowledged: count, err: err}:
		}
		if err != nil {
			return
		}
	}
}

// Runtime acknowledgements release newly captured deliveries. Closing the AMQP
// connection without acknowledging leaves classic and quorum messages queued;
// stream acknowledgements replenish credit without deleting messages.
func processAcknowledgements(ctx context.Context, acks <-chan ackMessage, pending <-chan pendingEntry) error {
	for {
		var msg ackMessage
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg = <-acks:
		}
		if errors.Is(msg.err, io.EOF) {
			return errCaptureStopped
		} else if msg.err != nil {
			return fmt.Errorf("reading runtime acknowledgement: %w", msg.err)
		}

		for i := uint32(0); i < msg.acknowledged; i++ {
			var entry pendingEntry
			select {
			case entry = <-pending:
			default:
				return errors.New("runtime acknowledged a checkpoint the connector never emitted")
			}
			// Delivery tags are scoped to their AMQP channel. Ack individually
			// so deliveries outside this checkpoint can never be acknowledged.
			delivery := amqp.Delivery{Acknowledger: entry.acknowledger, DeliveryTag: entry.deliveryTag}
			if err := delivery.Ack(false); err != nil {
				return fmt.Errorf("acknowledging delivery for queue %q: %w", entry.binding.queue, err)
			}
		}
	}
}
