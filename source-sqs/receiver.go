package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

const (
	// receiveBatchSize is the ReceiveMessage API's hard cap on messages per call.
	receiveBatchSize = 10

	// receiveReservationBytes is the worst-case body volume of one
	// ReceiveMessage call, 10 messages at the 1 MiB payload maximum.
	// Receivers reserve it against the body-bytes budget before each call
	// and trim to actual size on response, making the budget a strict bound
	// on bodies in memory regardless of the message-size mix.
	receiveReservationBytes = receiveBatchSize * 1024 * 1024

	// receiveCallTimeout bounds a single ReceiveMessage call, guarding
	// against hung connections. It must comfortably exceed the long-poll
	// wait plus the SDK's internal retries.
	receiveCallTimeout = 90 * time.Second
)

// overLimitBackoff paces receive retries while the queue's in-flight quota
// is exhausted. The quota clears as messages are deleted or visibility
// timeouts lapse, so retrying faster than this doesn't help. A variable
// so tests can shorten it.
var overLimitBackoff = 5 * time.Second

// longPollWaitSeconds enables server-side long polling. Calls still return
// immediately when a backlog is present. A var so tests can shorten the
// empty-poll cycle.
var longPollWaitSeconds int32 = 20

// A binding that has gone quiet shrinks its receive fleet so its empty long
// polls hold only a minimal portion of the shared body budget instead of full
// worst-case reservations that would crowd out busy bindings. These are
// vars so tests can tighten them.
var (
	// scoutsPerBinding is how many receivers keep polling an idle binding.
	// One is enough because a long poll returns the moment a message
	// arrives, so coverage is continuous. A stalled scout (hung
	// connection, SDK retries) can delay wake-up by up to the call
	// timeout; we accept that in exchange for the small idle footprint.
	scoutsPerBinding = 1

	// scoutAskMessages is the per-call ask while idle, so a scout's
	// worst-case reservation is one message instead of ten.
	scoutAskMessages = 1

	// idleAfter is how long a binding goes without a message before its
	// fleet parks down to scouts. Keep it well above worst-case commit
	// latency: a FIFO binding whose groups are all locked receives empty
	// responses even though it isn't actually idle.
	idleAfter = 60 * time.Second

	// parkedRecheck is how often a parked receiver re-checks for wake-up.
	parkedRecheck = 500 * time.Millisecond
)

// wake records message arrival, keeping the full receive fleet active.
func (b *bindingInfo) wake() { b.lastMessageAt.Store(time.Now().UnixNano()) }

// isIdle reports whether the binding has gone without messages long enough
// that its receive fleet should shrink to scouts with minimal asks.
func (b *bindingInfo) isIdle() bool {
	return time.Since(time.Unix(0, b.lastMessageAt.Load())) > idleAfter
}

// capturedBatch is one ReceiveMessage batch, transformed and ready for the
// transaction loop to emit.
type capturedBatch struct {
	binding    *bindingInfo
	docs       []json.RawMessage
	handles    []string
	bodyBytes  int64
	receivedAt time.Time
}

// runReceiver long-polls ReceiveMessage and hands transformed batches to the
// transaction loop. Transform runs here, in the receiver goroutine, so its
// CPU cost spreads across the fleet instead of serializing in the
// transaction loop.
//
// The budget semaphore grants in FIFO order, but an empty 20 second long
// poll holds its reservation ~500x longer than a busy receive does, so idle
// bindings would gradually crowd busy ones out of the shared budget. To
// prevent that, receivers past the scout count park entirely while a
// binding is idle, and the scouts ask for one message per call. Any
// received message wakes the full fleet within a recheck interval.
func runReceiver(ctx context.Context, client *sqs.Client, binding *bindingInfo, index int, bodyBytes *semaphore.Weighted, batches chan<- capturedBatch) error {
	for ctx.Err() == nil {
		isIdle := binding.isIdle()
		if isIdle && index >= scoutsPerBinding {
			// Non-scout receivers on an idle binding park here, rechecking
			// until the scout receives a message and wakes the binding.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(parkedRecheck):
			}
			continue
		}
		ask := receiveBatchSize
		if isIdle {
			ask = scoutAskMessages
		}
		askBytes := int64(ask) * 1024 * 1024

		if err := binding.inflight.Acquire(ctx, int64(ask)); err != nil {
			return err
		}
		if err := bodyBytes.Acquire(ctx, askBytes); err != nil {
			binding.inflight.Release(int64(ask))
			return err
		}

		input := &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(binding.queueURL),
			MaxNumberOfMessages: int32(ask),
			WaitTimeSeconds:     longPollWaitSeconds,
			// VisibilityTimeout is omitted, so the queue's configured
			// visibility timeout applies.
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
			MessageAttributeNames:       []string{"All"},
		}
		if binding.isFifo {
			// On a FIFO queue, a receive whose response is lost leaves its
			// messages in flight and their groups locked for the full
			// visibility timeout. A stable ReceiveRequestAttemptId lets a
			// retry recover the same messages and handles instead.
			input.ReceiveRequestAttemptId = aws.String(uuid.NewString())
		}

		callCtx, cancel := context.WithTimeout(ctx, receiveCallTimeout)
		resp, err := client.ReceiveMessage(callCtx, input)
		cancel()
		receivedAt := time.Now()

		if err != nil {
			binding.inflight.Release(int64(ask))
			bodyBytes.Release(askBytes)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// OverLimit means the queue-wide in-flight quota (120k messages,
			// shared across all consumers of the queue) is exhausted. Our
			// own unacked cap keeps this connector's contribution small, so
			// hitting it usually means some other consumer is holding most
			// of the quota. The SDK treats it as a client fault and won't
			// retry it, but it's transient, so back off and retry here
			// instead of failing.
			var overLimit *types.OverLimit
			if errors.As(err, &overLimit) {
				log.WithField("queue", binding.queueURL).
					Warn("queue in-flight message quota exceeded (OverLimit); backing off")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(overLimitBackoff):
				}
				continue
			}
			// The SDK has already retried transient failures, so anything
			// that still fails here fails the task. Nothing is lost;
			// unreceived messages just stay in the queue.
			return fmt.Errorf("receiving from queue %q: %w", binding.queueURL, err)
		}

		var actualBytes int64
		var redelivered int64
		for _, m := range resp.Messages {
			actualBytes += int64(len(aws.ToString(m.Body)))
			if n, err := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]); err == nil && n > 1 {
				redelivered++
			}
		}
		binding.inflight.Release(int64(ask - len(resp.Messages)))
		bodyBytes.Release(askBytes - actualBytes)

		if len(resp.Messages) == 0 {
			continue // The long poll already waited server-side.
		}
		binding.wake()
		binding.stats.received.Add(int64(len(resp.Messages)))
		binding.stats.unacked.Add(int64(len(resp.Messages)))
		binding.stats.redelivered.Add(redelivered)

		docs := make([]json.RawMessage, len(resp.Messages))
		handles := make([]string, len(resp.Messages))
		for i, m := range resp.Messages {
			doc, err := makeDocument(binding.queueURL, m)
			if err != nil {
				return err
			}
			docs[i] = doc
			handles[i] = aws.ToString(m.ReceiptHandle)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case batches <- capturedBatch{
			binding:    binding,
			docs:       docs,
			handles:    handles,
			bodyBytes:  actualBytes,
			receivedAt: receivedAt,
		}:
		}
	}
	return ctx.Err()
}
