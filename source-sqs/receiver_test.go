package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// TestReceiverBacksOffOnOverLimit verifies that an OverLimit receive error
// (the queue-wide in-flight quota, typically exhausted by external consumers
// sharing the queue) backs off and retries instead of failing the task. The
// SDK won't retry it since it's modeled as a client fault, so the receiver
// loop must handle it. Responses are injected via middleware. The first call
// returns OverLimit and the second returns a message, proving the loop kept
// going.
func TestReceiverBacksOffOnOverLimit(t *testing.T) {
	prevBackoff := overLimitBackoff
	overLimitBackoff = 10 * time.Millisecond
	t.Cleanup(func() { overLimitBackoff = prevBackoff })

	var calls atomic.Int32
	inject := middleware.InitializeMiddlewareFunc("InjectResponses", func(
		ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
	) (middleware.InitializeOutput, middleware.Metadata, error) {
		switch calls.Add(1) {
		case 1:
			return middleware.InitializeOutput{}, middleware.Metadata{},
				&types.OverLimit{Message: aws.String("in-flight quota exceeded")}
		case 2:
			return middleware.InitializeOutput{Result: &sqs.ReceiveMessageOutput{
				Messages: []types.Message{{
					MessageId:     aws.String("m-1"),
					ReceiptHandle: aws.String("h-1"),
					Body:          aws.String(`{"x": 1}`),
				}},
			}}, middleware.Metadata{}, nil
		default:
			return middleware.InitializeOutput{Result: &sqs.ReceiveMessageOutput{}},
				middleware.Metadata{}, nil
		}
	})

	client := sqs.New(sqs.Options{
		Region: "us-east-1",
		APIOptions: []func(*middleware.Stack) error{func(stack *middleware.Stack) error {
			return stack.Initialize.Add(inject, middleware.Before)
		}},
	})

	binding := &bindingInfo{
		index:    0,
		queueURL: "https://sqs.test.example/123456789012/test-queue",
		inflight: semaphore.NewWeighted(int64(maxUnackedMessages)),
	}
	binding.wake()
	bodyBytes := semaphore.NewWeighted(receiveReservationBytes)
	batches := make(chan capturedBatch, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runReceiver(ctx, client, binding, 0, bodyBytes, batches) }()

	select {
	case batch := <-batches:
		require.Len(t, batch.docs, 1)
		require.Equal(t, []string{"h-1"}, batch.handles)
	case err := <-done:
		t.Fatalf("receiver exited instead of retrying after OverLimit: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the post-OverLimit batch")
	}
	require.GreaterOrEqual(t, calls.Load(), int32(2))
	require.Equal(t, int64(1), binding.stats.received.Load())

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("receiver did not stop on cancellation")
	}
}

// TestReceiverIdleScouting verifies the idle-binding behavior that protects
// the shared body budget. A binding with no recent messages parks its
// non-scout receivers entirely, scouts ask for one message per call, and a
// received message wakes the binding back to full-size asks.
func TestReceiverIdleScouting(t *testing.T) {
	prevIdle, prevRecheck := idleAfter, parkedRecheck
	idleAfter, parkedRecheck = 50*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { idleAfter, parkedRecheck = prevIdle, prevRecheck })

	var mu sync.Mutex
	var asks []int32
	var calls atomic.Int32
	inject := middleware.InitializeMiddlewareFunc("InjectResponses", func(
		ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
	) (middleware.InitializeOutput, middleware.Metadata, error) {
		input := in.Parameters.(*sqs.ReceiveMessageInput)
		mu.Lock()
		asks = append(asks, input.MaxNumberOfMessages)
		mu.Unlock()
		// The second call returns a message, waking the binding. All other
		// calls return empty.
		if calls.Add(1) == 2 {
			return middleware.InitializeOutput{Result: &sqs.ReceiveMessageOutput{
				Messages: []types.Message{{
					MessageId:     aws.String("m-1"),
					ReceiptHandle: aws.String("h-1"),
					Body:          aws.String(`{"x": 1}`),
				}},
			}}, middleware.Metadata{}, nil
		}
		return middleware.InitializeOutput{Result: &sqs.ReceiveMessageOutput{}},
			middleware.Metadata{}, nil
	})

	client := sqs.New(sqs.Options{
		Region: "us-east-1",
		APIOptions: []func(*middleware.Stack) error{func(stack *middleware.Stack) error {
			return stack.Initialize.Add(inject, middleware.Before)
		}},
	})

	// The zero lastMessageAt makes the binding idle from the start.
	binding := &bindingInfo{
		index:    0,
		queueURL: "https://sqs.test.example/123456789012/test-queue",
		inflight: semaphore.NewWeighted(int64(maxUnackedMessages)),
	}
	require.True(t, binding.isIdle())
	// Extra headroom because delivered batches hold their body bytes until
	// emission, which never happens in this harness.
	bodyBytes := semaphore.NewWeighted(3 * receiveReservationBytes)
	batches := make(chan capturedBatch, 4)

	// A non-scout receiver on an idle binding must park and never call SQS.
	parkedCtx, cancelParked := context.WithCancel(context.Background())
	parkedDone := make(chan error, 1)
	go func() { parkedDone <- runReceiver(parkedCtx, client, binding, scoutsPerBinding, bodyBytes, batches) }()
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, int32(0), calls.Load(), "parked receiver must not call SQS")
	cancelParked()
	require.ErrorIs(t, <-parkedDone, context.Canceled)

	// A scout polls with one-message asks, then wakes the binding when a
	// message arrives, and subsequent asks are full size.
	scoutCtx, cancelScout := context.WithCancel(context.Background())
	scoutDone := make(chan error, 1)
	go func() { scoutDone <- runReceiver(scoutCtx, client, binding, 0, bodyBytes, batches) }()

	select {
	case batch := <-batches:
		require.Equal(t, []string{"h-1"}, batch.handles)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the scout's batch")
	}
	require.False(t, binding.isIdle(), "message arrival must wake the binding")

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(asks) >= 3
	}, 10*time.Second, 5*time.Millisecond)

	cancelScout()
	require.ErrorIs(t, <-scoutDone, context.Canceled)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, int32(scoutAskMessages), asks[0], "idle scout must ask for one message")
	require.Equal(t, int32(scoutAskMessages), asks[1], "still idle before the wake message lands")
	require.Equal(t, int32(receiveBatchSize), asks[2], "awake binding must ask full size")
}
