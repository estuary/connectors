package main

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

const classicOffsetRejection = "PRECONDITION_FAILED - invalid arg 'x-stream-offset' for queue 'orders' in vhost '/' of queue type rabbit_classic_queue"

type fakeConsumerChannel struct {
	t          *testing.T
	consumeErr error
	closed     chan *amqp.Error
	deliveries chan amqp.Delivery
	args       amqp.Table
	prefetch   int
	failed     bool
}

func (c *fakeConsumerChannel) NotifyClose(closed chan *amqp.Error) chan *amqp.Error {
	c.closed = closed
	return closed
}

func (c *fakeConsumerChannel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	require.False(c.t, noWait)
	return amqp.Queue{Name: name}, nil
}

func (c *fakeConsumerChannel) Qos(count, size int, global bool) error {
	require.Positive(c.t, count, "prefetch must bound unacknowledged deliveries")
	require.Zero(c.t, size)
	require.False(c.t, global, "streams require per-consumer prefetch")
	c.prefetch = count
	return nil
}

func (c *fakeConsumerChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	if c.failed {
		return nil, amqp.ErrClosed
	}
	require.Positive(c.t, c.prefetch, "prefetch must be set on each new channel")
	require.False(c.t, autoAck, "deliveries must wait for runtime commits")
	require.False(c.t, noWait, "consume must wait for the broker's response")
	c.args = args
	if c.consumeErr != nil {
		c.failed = true
		var amqpErr *amqp.Error
		if errors.As(c.consumeErr, &amqpErr) {
			c.closed <- amqpErr
		}
		close(c.closed)
		close(c.deliveries)
		return nil, c.consumeErr
	}
	return c.deliveries, nil
}

func TestConsumeQueueTypes(t *testing.T) {
	savedOffset := int64(23)
	for _, tc := range []struct {
		name       string
		rejection  error
		nextOffset *int64
	}{
		{"classic", &amqp.Error{Code: amqp.PreconditionFailed, Reason: classicOffsetRejection}, nil},
		{"quorum", &amqp.Error{Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - invalid arg 'x-stream-offset' for queue 'orders' in vhost '/' of queue type rabbit_quorum_queue"}, nil},
		{"stream first", nil, nil},
		{"stream offset", nil, &savedOffset},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var channels []*fakeConsumerChannel
			binding := &bindingInfo{queue: "orders", nextOffset: tc.nextOffset}
			err := binding.consume(func() (consumerChannel, error) {
				require.Less(t, len(channels), 2, "queue-type fallback must be bounded")
				channel := &fakeConsumerChannel{t: t, deliveries: make(chan amqp.Delivery, 1)}
				if len(channels) == 0 {
					channel.consumeErr = tc.rejection
				}
				channels = append(channels, channel)
				return channel, nil
			})
			require.NoError(t, err)
			var offset any = "first"
			if tc.nextOffset != nil {
				offset = *tc.nextOffset
			}
			require.Equal(t, amqp.Table{"x-stream-offset": offset}, channels[0].args)
			if tc.rejection == nil {
				require.Len(t, channels, 1)
				require.True(t, binding.isStream)
			} else {
				require.Len(t, channels, 2)
				require.True(t, channels[0].failed)
				require.Nil(t, channels[1].args)
				require.False(t, binding.isStream)
			}
			active := channels[len(channels)-1]
			require.LessOrEqual(t, active.prefetch, 20, "limit buffered large messages per queue")
			require.Equal(t, (<-chan *amqp.Error)(active.closed), binding.closed)
			require.Equal(t, (<-chan amqp.Delivery)(active.deliveries), binding.deliveries)
			active.deliveries <- amqp.Delivery{Body: []byte("message")}
			require.Equal(t, []byte("message"), receive(t, binding.deliveries).Body)
		})
	}
}

func TestConsumeErrors(t *testing.T) {
	rejection := &amqp.Error{Code: amqp.PreconditionFailed, Reason: classicOffsetRejection}
	for _, tc := range []struct {
		name      string
		firstErr  error
		retryErr  error
		openError bool
	}{
		{"access refused", &amqp.Error{Code: amqp.AccessRefused, Reason: "ACCESS_REFUSED - queue access denied"}, nil, false},
		{"missing queue", &amqp.Error{Code: amqp.NotFound, Reason: "NOT_FOUND - no queue 'orders'"}, nil, false},
		{"unrelated precondition", &amqp.Error{Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - consumer limit reached"}, nil, false},
		{"stream offset invalid", &amqp.Error{Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - invalid arg 'x-stream-offset' for queue 'orders' in vhost '/' of queue type rabbit_stream_queue"}, nil, false},
		{"wrong code", &amqp.Error{Code: amqp.InternalError, Reason: classicOffsetRejection}, nil, false},
		{"untyped error", errors.New(classicOffsetRejection), nil, false},
		{"retry rejected", rejection, rejection, false},
		{"retry channel open failed", rejection, amqp.ErrClosed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opened int
			binding := &bindingInfo{queue: "orders"}
			err := binding.consume(func() (consumerChannel, error) {
				opened++
				require.LessOrEqual(t, opened, 2)
				consumeErr := tc.firstErr
				if opened == 2 {
					if tc.openError {
						return nil, tc.retryErr
					}
					consumeErr = tc.retryErr
				}
				return &fakeConsumerChannel{t: t, consumeErr: consumeErr, deliveries: make(chan amqp.Delivery)}, nil
			})
			if tc.retryErr != nil {
				require.Equal(t, 2, opened)
				require.ErrorIs(t, err, tc.retryErr)
			} else {
				require.Equal(t, 1, opened)
				require.ErrorIs(t, err, tc.firstErr)
			}
			require.ErrorContains(t, err, `queue "orders"`)
			require.Nil(t, binding.deliveries)
		})
	}
}

func TestConsumeDiscoveredQueueTypes(t *testing.T) {
	savedOffset := int64(23)
	for _, queueType := range []string{"classic", "quorum", "stream"} {
		for _, offset := range []*int64{nil, &savedOffset} {
			binding := &bindingInfo{queue: "orders", queueType: queueType, nextOffset: offset}
			channel := &fakeConsumerChannel{t: t, deliveries: make(chan amqp.Delivery)}
			opened := 0
			require.NoError(t, binding.consume(func() (consumerChannel, error) {
				opened++
				return channel, nil
			}))
			require.Equal(t, 1, opened, "discovered types must not depend on a rejected consume")
			require.Equal(t, queueType == "stream", binding.isStream)
			if queueType == "stream" {
				var want any = "first"
				if offset != nil {
					want = *offset
				}
				require.Equal(t, amqp.Table{"x-stream-offset": want}, channel.args)
			} else {
				require.Nil(t, channel.args, "classic and quorum brokers may silently accept stream arguments")
			}
		}
	}
}

func TestConsumePrefetchRetry(t *testing.T) {
	var channels []*fakeConsumerChannel
	binding := &bindingInfo{queue: "orders", prefetchCount: 500, startOffset: "next"}
	require.NoError(t, binding.consume(func() (consumerChannel, error) {
		channel := &fakeConsumerChannel{t: t, deliveries: make(chan amqp.Delivery)}
		if len(channels) == 0 {
			channel.consumeErr = &amqp.Error{Code: amqp.PreconditionFailed, Reason: classicOffsetRejection}
		}
		channels = append(channels, channel)
		return channel, nil
	}))
	require.Len(t, channels, 2)
	require.Equal(t, amqp.Table{"x-stream-offset": "next"}, channels[0].args)
	require.Nil(t, channels[1].args)
	for _, channel := range channels {
		require.Equal(t, 500, channel.prefetch)
	}
}
