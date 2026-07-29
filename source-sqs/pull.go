package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Connector-internal pipeline sizing, benchmarked in BENCHMARKS.md under
// "Measured results". These aren't exposed as user configuration. They're
// vars rather than constants only so tests can tune concurrency down for
// emulators.
//
//   - 40 receivers sustain ~14-20k msg/sec at in-region RTTs (10 msgs per
//     call, ~20-30ms). Each reserves a 10 MiB worst case (10 msgs x 1 MiB
//     max payload) against the body budget, so the receive fleet also fixes
//     worst-case body memory at 400 MiB.
//   - 48 delete workers put the delete ceiling (~17k msg/sec) above the
//     receive ceiling. In measured runs the delete fleet, not Flow, was the
//     first bottleneck once collections had enough journals.
//   - 1,000 unacked messages per binding covers steady-state pipeline
//     occupancy with plenty of slack (by Little's law, rate x ~80ms
//     end-to-end residence is ~650 at measured rates) without letting a
//     large backlog of received-but-uncommitted messages build up. Measured
//     throughput was identical to a 50k cap. Flow-side commit throughput is
//     not a sizing input because the v2 runtime auto-splits journals before
//     they become the bottleneck.
//   - 448 MiB of buffered bodies is 40 receiver reservations plus headroom,
//     the hard worst-case bound on body memory for any message-size mix.
//     AWS documents no total-size cap on a ReceiveMessage response, so a
//     hard guarantee must reserve messages-in-flight x 1 MiB, and that
//     budget floor (target rate x RTT x max message size) works out to
//     ~350 MiB at 14k/s and ~25ms RTT. Steady-state actual usage is a few
//     MB. Peak worst-case connector footprint is ~560 MB, well under the
//     1 GiB task memory limit.
var (
	receiversPerBinding      = 40
	deleteWorkers            = 48
	maxUnackedMessages       = 1_000
	maxBufferedBodyBytes     = 448 * 1024 * 1024
	maxMessagesPerCheckpoint = 1_000
)

// maxBytesPerCheckpoint bounds checkpoint coalescing alongside the message
// count, which alone would admit a ~1 GiB transaction of max-size messages.
// Coalescing appends whole batches, so the effective bound can overshoot by
// up to one batch (10 MiB).
const maxBytesPerCheckpoint = 32 * 1024 * 1024

var emptyCheckpoint = json.RawMessage("{}")

// bindingInfo is the per-queue state shared across pipeline stages.
type bindingInfo struct {
	index    int
	queueURL string
	isFifo   bool

	// visibility is the queue's configured visibility timeout, used by
	// delete workers to skip API calls for handles that are already dead.
	visibility time.Duration

	// lastMessageAt is when a receiver last got a non-empty response in
	// unix nanos. It drives the idle detection that shrinks a quiet
	// binding's receive fleet down to scouts.
	lastMessageAt atomic.Int64

	// inflight caps messages received but not yet deleted, which bounds the
	// redelivery blast radius and leaves headroom under SQS's queue-wide
	// 120k in-flight quota.
	inflight *semaphore.Weighted

	stats bindingStats
}

func (driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	ctx := stream.Context()

	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := buildClient(ctx, &cfg, receiversPerBinding*len(open.Capture.Bindings)+deleteWorkers)
	if err != nil {
		return err
	}

	var bindings []*bindingInfo
	for idx, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		info, err := newBindingInfo(ctx, client, idx, res.QueueURL)
		if err != nil {
			return err
		}
		bindings = append(bindings, info)
	}

	if err := stream.Ready(true); err != nil {
		return err
	}

	// bodyBytes is the second backpressure gate on receivers. The per-binding
	// inflight semaphore is counted in messages and held from receive until
	// delete, which bounds redelivery exposure. This one is process-wide,
	// counted in bytes, and held only from receive until emission, which
	// bounds memory. The two cover different things, so we need both.
	// The budget has to cover at least one receive reservation or receivers
	// could never acquire, so clamp it up rather than deadlock.
	bodyBudget := max(maxBufferedBodyBytes, receiveReservationBytes)
	bodyBytes := semaphore.NewWeighted(int64(bodyBudget))

	totalUnacked := maxUnackedMessages * len(bindings)
	batches := make(chan capturedBatch, 2*receiversPerBinding*len(bindings))
	// Both `pending` and `deletes` are sized to the per-binding unacked
	// caps, so entries can never outnumber unacked messages and sends never
	// block. The ack processor can't afford to stall on a slow delete path:
	// the runtime fails the task if an Acknowledge goes unread for 10s.
	pending := make(chan pendingEntry, totalUnacked)
	deletes := make(chan deleteJob, totalUnacked)

	// The pipeline is four stages connected by channels. Receivers long-poll
	// SQS and hand off transformed batches. The transaction loop
	// turns batches into documents plus a checkpoint and records
	// the covered receipt handles in `pending`. The ack processor pops one
	// entry per runtime acknowledgement and feeds its delete jobs to the
	// delete workers, which remove the committed messages from SQS.
	// Emission doesn't wait on acknowledgement, so many checkpoints can be
	// outstanding at once and throughput isn't gated on commit latency.
	group, groupCtx := errgroup.WithContext(ctx)

	ackLatency := new(ackLatencyStats)
	acks := make(chan ackMessage, 16)
	// Detached rather than in the errgroup since stream.Recv cannot be
	// cancelled. Its blocked read is reclaimed at process exit.
	go readAcknowledgements(stream, acks)
	group.Go(func() error {
		return processAcknowledgements(groupCtx, acks, pending, deletes, ackLatency)
	})

	group.Go(func() error {
		return runTransactionLoop(groupCtx, stream, batches, pending, bodyBytes)
	})

	for _, binding := range bindings {
		for i := range receiversPerBinding {
			group.Go(func() error {
				return runReceiver(groupCtx, client, binding, i, bodyBytes, batches)
			})
		}
	}

	for range deleteWorkers {
		group.Go(func() error {
			return runDeleteWorker(groupCtx, client, deletes)
		})
	}

	group.Go(func() error {
		return runStatsLogger(groupCtx, client, bindings, ackLatency)
	})

	if err := group.Wait(); err != nil && !errors.Is(err, errCaptureStopped) {
		return err
	}
	return nil
}

func newBindingInfo(ctx context.Context, client *sqs.Client, index int, queueURL string) (*bindingInfo, error) {
	name, _, err := parseQueueURL(queueURL)
	if err != nil {
		return nil, err
	}

	// The queue's visibility timeout drives the delete workers'
	// expired-handle skip.
	attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameVisibilityTimeout},
	})
	if err != nil {
		return nil, fmt.Errorf("queue %q is not accessible: %w", queueURL, err)
	}
	visibilitySeconds, _ := strconv.Atoi(attrs.Attributes[string(types.QueueAttributeNameVisibilityTimeout)])

	// The zero lastMessageAt means bindings start idle. That costs a busy
	// queue almost nothing, since a scout's long poll returns as soon as a
	// backlog exists and the fleet is at full asks within one receive. It
	// saves a restarting capture with many quiet queues from holding a
	// minute of worst-case reservations for fleets with nothing to do.
	return &bindingInfo{
		index:      index,
		queueURL:   queueURL,
		isFifo:     isFifoQueueName(name),
		visibility: time.Duration(visibilitySeconds) * time.Second,
		inflight:   semaphore.NewWeighted(int64(maxUnackedMessages)),
	}, nil
}

// runTransactionLoop is the sole emitter of documents and checkpoints. It
// drains whatever batches are already waiting into one checkpoint, bounded
// by count and bytes but never waiting, and pushes each checkpoint's receipt
// handles onto the pending FIFO. Acknowledge handling depends on
// pending-push order matching checkpoint-emission order, and having a
// single goroutine do both is what guarantees that.
func runTransactionLoop(ctx context.Context, stream *boilerplate.PullOutput, batches <-chan capturedBatch, pending chan<- pendingEntry, bodyBytes *semaphore.Weighted) error {
	for {
		var first capturedBatch
		select {
		case <-ctx.Done():
			return ctx.Err()
		case first = <-batches:
		}

		group := []capturedBatch{first}
		messages, groupBytes := len(first.docs), first.bodyBytes

		// Coalesce whatever is already waiting, but never wait for more.
	drain:
		for messages < maxMessagesPerCheckpoint && groupBytes < maxBytesPerCheckpoint {
			select {
			case batch := <-batches:
				group = append(group, batch)
				messages += len(batch.docs)
				groupBytes += batch.bodyBytes
			default:
				break drain
			}
		}

		entry := pendingEntry{jobs: make([]deleteJob, 0, len(group))}
		for _, batch := range group {
			if err := stream.Documents(batch.binding.index, batch.docs...); err != nil {
				return err
			}
			entry.jobs = append(entry.jobs, deleteJob{
				binding:    batch.binding,
				handles:    batch.handles,
				receivedAt: batch.receivedAt,
			})
		}

		// The pending entry must be pushed before the checkpoint is emitted.
		// The ack reader runs concurrently and the runtime can acknowledge a
		// checkpoint the instant it's written, so pushing after would race
		// the pop.
		entry.emittedAt = time.Now()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case pending <- entry:
		}

		if err := stream.Checkpoint(emptyCheckpoint, true); err != nil {
			return err
		}

		bodyBytes.Release(groupBytes)
	}
}
