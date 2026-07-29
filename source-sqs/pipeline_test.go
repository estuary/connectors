package main

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/metadata"
)

// These tests exercise the emit -> acknowledge -> delete-dispatch pipeline in
// isolation, driving the channels the receiver and runtime would otherwise
// feed. They deliberately do not involve SQS. Message-group locking and
// redelivery are server-side behaviors covered by the LocalStack integration
// tests, whereas the invariant tested here, that runtime acknowledgements map
// to the right delete jobs in checkpoint-emission order, lives entirely in
// this connector's channel plumbing.

// fakeCaptureServer is an in-memory pc.Connector_CaptureServer. Emitted
// documents and checkpoints are recorded, and runtime requests are fed via
// recv.
type fakeCaptureServer struct {
	mu          sync.Mutex
	docs        []string // DocJson of each emitted document, in emission order
	checkpoints int
	recv        chan *pc.Request
}

func newFakeCaptureServer() *fakeCaptureServer {
	return &fakeCaptureServer{recv: make(chan *pc.Request)}
}

func (f *fakeCaptureServer) Send(msg *pc.Response) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// PullOutput.Documents reuses a single Response value across calls, so copy
	// out what we need rather than retaining the pointer.
	if msg.Captured != nil {
		f.docs = append(f.docs, string(msg.Captured.DocJson))
	}
	if msg.Checkpoint != nil {
		f.checkpoints++
	}
	return nil
}

func (f *fakeCaptureServer) snapshot() (docs []string, checkpoints int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.docs...), f.checkpoints
}

func (f *fakeCaptureServer) Recv() (*pc.Request, error)   { return <-f.recv, nil }
func (f *fakeCaptureServer) Context() context.Context     { return context.Background() }
func (f *fakeCaptureServer) RecvMsg(any) error            { panic("unimplemented") }
func (f *fakeCaptureServer) SendMsg(any) error            { panic("unimplemented") }
func (f *fakeCaptureServer) SendHeader(metadata.MD) error { panic("unimplemented") }
func (f *fakeCaptureServer) SetHeader(metadata.MD) error  { panic("unimplemented") }
func (f *fakeCaptureServer) SetTrailer(metadata.MD)       { panic("unimplemented") }

func docsOf(ids ...string) []json.RawMessage {
	out := make([]json.RawMessage, len(ids))
	for i, id := range ids {
		out[i] = json.RawMessage(`{"id":"` + id + `"}`)
	}
	return out
}

// TestProcessAcknowledgementsDispatchesInOrder verifies that acknowledged
// checkpoints are popped in emission order and their delete jobs dispatched in
// the same order, including a checkpoint that coalesced multiple batches.
func TestProcessAcknowledgementsDispatchesInOrder(t *testing.T) {
	b := &bindingInfo{index: 0, queueURL: "queue-a"}

	entries := []pendingEntry{
		{emittedAt: time.Now(), jobs: []deleteJob{{binding: b, handles: []string{"h0", "h1"}}}},
		{emittedAt: time.Now(), jobs: []deleteJob{
			{binding: b, handles: []string{"h2"}},
			{binding: b, handles: []string{"h3", "h4"}},
		}},
		{emittedAt: time.Now(), jobs: []deleteJob{{binding: b, handles: []string{"h5"}}}},
	}

	pending := make(chan pendingEntry, len(entries))
	for _, e := range entries {
		pending <- e
	}
	acks := make(chan ackMessage, 4)
	deletes := make(chan deleteJob, 8)
	latency := new(ackLatencyStats)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- processAcknowledgements(ctx, acks, pending, deletes, latency) }()

	// Acknowledge the first checkpoint, then the remaining two together.
	acks <- ackMessage{acknowledged: 1}
	acks <- ackMessage{acknowledged: 2}

	var gotHandles []string
	for len(gotHandles) < 6 {
		select {
		case job := <-deletes:
			gotHandles = append(gotHandles, job.handles...)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for delete jobs; got %v", gotHandles)
		}
	}
	require.Equal(t, []string{"h0", "h1", "h2", "h3", "h4", "h5"}, gotHandles)

	// EOF is a graceful stop.
	acks <- ackMessage{err: io.EOF}
	require.ErrorIs(t, <-done, errCaptureStopped)

	// One latency observation per acknowledged checkpoint.
	require.Equal(t, int64(len(entries)), latency.count.Load())
}

// TestProcessAcknowledgementsRejectsUnknownCheckpoint verifies the guard that
// fails the task if the runtime acknowledges more checkpoints than were
// emitted, a condition that is otherwise near-impossible to provoke against a
// real runtime.
func TestProcessAcknowledgementsRejectsUnknownCheckpoint(t *testing.T) {
	b := &bindingInfo{index: 0, queueURL: "queue-a"}

	pending := make(chan pendingEntry, 1)
	pending <- pendingEntry{emittedAt: time.Now(), jobs: []deleteJob{{binding: b, handles: []string{"h0"}}}}

	acks := make(chan ackMessage, 1)
	deletes := make(chan deleteJob, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- processAcknowledgements(ctx, acks, pending, deletes, new(ackLatencyStats)) }()

	// Acknowledge two checkpoints when only one was ever emitted.
	acks <- ackMessage{acknowledged: 2}

	select {
	case err := <-done:
		require.ErrorContains(t, err, "never emitted")
	case <-time.After(5 * time.Second):
		t.Fatal("processAcknowledgements did not return")
	}
}

// TestTransactionLoopOrdersAndReleasesBudget verifies that the transaction loop
// emits documents and pushes pending handles in receive order (regardless of
// how it coalesces batches into checkpoints), and releases each checkpoint's
// body-bytes reservation once emitted.
func TestTransactionLoopOrdersAndReleasesBudget(t *testing.T) {
	fake := newFakeCaptureServer()
	stream := &boilerplate.PullOutput{Connector_CaptureServer: fake}

	bindingA := &bindingInfo{index: 0, queueURL: "queue-a"}
	bindingB := &bindingInfo{index: 1, queueURL: "queue-b"}

	const perBatch = int64(1024)
	batchesIn := []capturedBatch{
		{binding: bindingA, docs: docsOf("a0", "a1"), handles: []string{"a0", "a1"}, bodyBytes: perBatch},
		{binding: bindingB, docs: docsOf("b0"), handles: []string{"b0"}, bodyBytes: perBatch},
		{binding: bindingA, docs: docsOf("a2"), handles: []string{"a2"}, bodyBytes: perBatch},
	}

	var totalDocs int
	var totalBytes int64
	for _, b := range batchesIn {
		totalDocs += len(b.docs)
		totalBytes += b.bodyBytes
	}

	// Stand in for the receivers having reserved the body budget. The loop is
	// responsible for releasing it after emission.
	bodyBytes := semaphore.NewWeighted(totalBytes)
	require.True(t, bodyBytes.TryAcquire(totalBytes))

	batches := make(chan capturedBatch, len(batchesIn))
	for _, b := range batchesIn {
		batches <- b
	}
	pending := make(chan pendingEntry, len(batchesIn))

	group, groupCtx := errgroup.WithContext(context.Background())
	loopCtx, cancel := context.WithCancel(groupCtx)
	group.Go(func() error {
		return runTransactionLoop(loopCtx, stream, batches, pending, bodyBytes)
	})

	var gotHandles []string
	for len(gotHandles) < 4 {
		select {
		case entry := <-pending:
			for _, job := range entry.jobs {
				gotHandles = append(gotHandles, job.handles...)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for pending entries; got %v", gotHandles)
		}
	}
	require.Equal(t, []string{"a0", "a1", "b0", "a2"}, gotHandles)

	// Each emitted checkpoint releases its batches' reservation. Once all are
	// drained the full budget is available again.
	require.Eventually(t, func() bool {
		if bodyBytes.TryAcquire(totalBytes) {
			bodyBytes.Release(totalBytes)
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	require.ErrorIs(t, group.Wait(), context.Canceled)

	docs, checkpoints := fake.snapshot()
	require.Len(t, docs, totalDocs)
	require.GreaterOrEqual(t, checkpoints, 1)
	require.LessOrEqual(t, checkpoints, len(batchesIn))
}
