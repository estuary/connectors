package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
)

// errCaptureStopped signals a graceful shutdown. The runtime closed its side
// of the stream, so no further acknowledgements can arrive and the capture
// should wind down. Anything received but not yet deleted redelivers after
// its visibility timeout.
var errCaptureStopped = errors.New("capture stopped by runtime")

// deleteJob is one DeleteMessageBatch call's worth of receipt handles, at
// most 10 per the API cap. One receive batch produces exactly one job.
type deleteJob struct {
	binding    *bindingInfo
	handles    []string
	receivedAt time.Time
}

// pendingEntry records the receipt handles covered by one emitted checkpoint,
// in checkpoint-emission order.
type pendingEntry struct {
	jobs      []deleteJob
	emittedAt time.Time
}

type ackMessage struct {
	acknowledged uint32
	err          error
}

// readAcknowledgements reads Request.Acknowledge messages from the runtime
// and forwards them to the ack processor.
func readAcknowledgements(stream *boilerplate.PullOutput, acks chan<- ackMessage) {
	for {
		request, err := stream.Recv()
		if err != nil {
			acks <- ackMessage{err: err}
			return
		}
		if err := request.Validate_(); err != nil {
			acks <- ackMessage{err: fmt.Errorf("validating runtime request: %w", err)}
			return
		}
		if request.Acknowledge == nil {
			acks <- ackMessage{err: fmt.Errorf("unexpected runtime request %#v", request)}
			return
		}
		acks <- ackMessage{acknowledged: request.Acknowledge.Checkpoints}
	}
}

// processAcknowledgements pops acknowledged checkpoints off the pending FIFO
// and dispatches their delete jobs. It must never stall because the runtime
// fails the task if a pending Acknowledge isn't consumed within 10 seconds.
func processAcknowledgements(
	ctx context.Context,
	acks <-chan ackMessage,
	pending <-chan pendingEntry,
	deletes chan<- deleteJob,
	latency *ackLatencyStats,
) error {
	for {
		// Wait for an acknowledgement from the runtime indicating
		// documents have been committed.
		var msg ackMessage
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg = <-acks:
		}

		if msg.err == io.EOF {
			return errCaptureStopped
		} else if msg.err != nil {
			return fmt.Errorf("reading runtime acknowledgement: %w", msg.err)
		}

		// Pop msg.acknowledged entries off of the FIFO queue and dispatch
		// jobs to delete the committed message from SQS.
		for i := uint32(0); i < msg.acknowledged; i++ {
			var entry pendingEntry
			select {
			case entry = <-pending:
			default:
				return fmt.Errorf("runtime acknowledged a checkpoint the connector never emitted")
			}
			latency.observe(time.Since(entry.emittedAt))
			for _, job := range entry.jobs {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case deletes <- job:
				}
			}
		}
	}
}

// runDeleteWorker performs DeleteMessageBatch calls for acknowledged
// messages. On cancellation it makes a best-effort pass over jobs already
// buffered in `deletes` so already-committed messages are deleted.
// It stops once the channel is momentarily empty (no new jobs are
// produced after cancellation), with a 5s deadline in case deletes run slow.
func runDeleteWorker(ctx context.Context, client *sqs.Client, deletes <-chan deleteJob) error {
	for {
		select {
		case job := <-deletes:
			if err := deleteBatch(ctx, client, job); err != nil {
				return err
			}
		case <-ctx.Done():
			deadline := time.After(5 * time.Second)
			for {
				select {
				case job := <-deletes:
					// The capture context is gone, so use a short
					// independent timeout for the remaining calls.
					drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := deleteBatch(drainCtx, client, job)
					cancel()
					if err != nil {
						return nil // Shutting down, the drain is best-effort.
					}
				case <-deadline:
					return nil
				default:
					return nil
				}
			}
		}
	}
}

// deleteBatch deletes one job's handles, retrying transient partial failures.
func deleteBatch(ctx context.Context, client *sqs.Client, job deleteJob) error {
	defer func() {
		job.binding.inflight.Release(int64(len(job.handles)))
		job.binding.stats.unacked.Add(-int64(len(job.handles)))
	}()

	// A handle past its visibility deadline is already dead. The message is
	// visible again and will redeliver regardless, so skip the API call.
	if job.binding.visibility > 0 && time.Since(job.receivedAt) > job.binding.visibility {
		job.binding.stats.deleteExpired.Add(int64(len(job.handles)))
		log.WithFields(log.Fields{
			"queue":    job.binding.queueURL,
			"messages": len(job.handles),
		}).Warn("receipt handles expired before deletion. messages will redeliver as duplicates")
		return nil
	}

	handles := job.handles
	for attempt := 0; len(handles) > 0; attempt++ {
		entries := make([]types.DeleteMessageBatchRequestEntry, len(handles))
		for i, handle := range handles {
			entries[i] = types.DeleteMessageBatchRequestEntry{
				Id:            aws.String(strconv.Itoa(i)),
				ReceiptHandle: aws.String(handle),
			}
		}

		resp, err := client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(job.binding.queueURL),
			Entries:  entries,
		})
		if err != nil {
			// A capture that can receive but not delete looks healthy while
			// duplicates pile up without bound, so treat access denial as
			// fatal.
			if isAccessDenied(err) {
				return fmt.Errorf("credentials lack sqs:DeleteMessage on queue %q: %w", job.binding.queueURL, err)
			}
			if ctx.Err() != nil {
				return nil
			}
			job.binding.stats.deleteDropped.Add(int64(len(handles)))
			log.WithFields(log.Fields{
				"queue": job.binding.queueURL,
				"error": err,
			}).Warn("DeleteMessageBatch failed. messages will redeliver as duplicates")
			return nil
		}

		var retry []string
		for _, failed := range resp.Failed {
			idx, err := strconv.Atoi(aws.ToString(failed.Id))
			if err != nil || idx < 0 || idx >= len(handles) {
				continue
			}
			if failed.SenderFault {
				// An invalid or expired handle means the message is already
				// visible again and will redeliver.
				job.binding.stats.deleteExpired.Add(1)
				log.WithFields(log.Fields{
					"queue": job.binding.queueURL,
					"code":  aws.ToString(failed.Code),
				}).Warn("receipt handle rejected. message will redeliver as a duplicate")
				continue
			}
			retry = append(retry, handles[idx])
		}
		job.binding.stats.deleted.Add(int64(len(handles) - len(resp.Failed)))
		handles = retry

		if len(handles) > 0 {
			if attempt >= 2 {
				job.binding.stats.deleteDropped.Add(int64(len(handles)))
				log.WithFields(log.Fields{
					"queue":    job.binding.queueURL,
					"messages": len(handles),
				}).Error("delete retries exhausted. messages will redeliver as duplicates")
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(100 * time.Millisecond << attempt):
			}
		}
	}
	return nil
}
