package main

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	log "github.com/sirupsen/logrus"
)

// statsInterval paces progress logging and queue-depth sampling.
const statsInterval = 30 * time.Second

// bindingStats tracks per-queue pipeline counters. The signal that matters
// most is delete health: a capture that receives but never deletes looks
// healthy while duplicates pile up, so deleted needs to keep up with
// received.
type bindingStats struct {
	received      atomic.Int64 // messages received from SQS
	redelivered   atomic.Int64 // received with ApproximateReceiveCount > 1
	deleted       atomic.Int64 // messages successfully deleted
	deleteExpired atomic.Int64 // handles expired or rejected before deletion
	deleteDropped atomic.Int64 // handles dropped after failed delete calls
	unacked       atomic.Int64 // gauge of received but not yet deleted/dropped
}

// ackLatencyStats aggregates emit-to-acknowledge latency across checkpoints.
// That's the runtime's commit round trip, which caps FIFO per-group
// throughput and determines how deep the pending queue runs.
type ackLatencyStats struct {
	count     atomic.Int64
	sumMicros atomic.Int64
	maxMicros atomic.Int64
}

func (s *ackLatencyStats) observe(d time.Duration) {
	micros := d.Microseconds()
	s.count.Add(1)
	s.sumMicros.Add(micros)
	for {
		prev := s.maxMicros.Load()
		if micros <= prev || s.maxMicros.CompareAndSwap(prev, micros) {
			return
		}
	}
}

// snapshotAndReset returns the interval's checkpoint count and average/max
// latencies, then zeroes the aggregates for the next interval.
func (s *ackLatencyStats) snapshotAndReset() (count int64, avg, max time.Duration) {
	count = s.count.Swap(0)
	sum := s.sumMicros.Swap(0)
	maxMicros := s.maxMicros.Swap(0)
	if count > 0 {
		avg = time.Duration(sum/count) * time.Microsecond
	}
	return count, avg, time.Duration(maxMicros) * time.Microsecond
}

// runStatsLogger periodically logs per-queue progress and a task-level
// connectorStatus rollup, sampling queue depth from GetQueueAttributes.
func runStatsLogger(ctx context.Context, client *sqs.Client, bindings []*bindingInfo, latency *ackLatencyStats) error {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	prevReceived := make([]int64, len(bindings))
	var lastStatus string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		var totalBacklog int64
		var queuesBehind int
		for i, binding := range bindings {
			received := binding.stats.received.Load()
			rate := (received - prevReceived[i]) / int64(statsInterval/time.Second)
			prevReceived[i] = received

			backlog := int64(-1)
			if attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl:       aws.String(binding.queueURL),
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
			}); err == nil {
				backlog, _ = strconv.ParseInt(attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], 10, 64)
			}

			log.WithFields(log.Fields{
				"queue":         binding.queueURL,
				"received":      received,
				"deleted":       binding.stats.deleted.Load(),
				"redelivered":   binding.stats.redelivered.Load(),
				"deleteExpired": binding.stats.deleteExpired.Load(),
				"deleteDropped": binding.stats.deleteDropped.Load(),
				"unacked":       binding.stats.unacked.Load(),
				"msgsPerSec":    rate,
				"queueDepth":    backlog,
			}).Info("sqs capture progress")

			if backlog > 0 {
				totalBacklog += backlog
				queuesBehind++
			}
		}

		ackCount, ackAvg, ackMax := latency.snapshotAndReset()
		log.WithFields(log.Fields{
			"checkpoints":   ackCount,
			"ackLatencyAvg": ackAvg.String(),
			"ackLatencyMax": ackMax.String(),
		}).Info("checkpoint commit+acknowledge latency")

		// Backlog is SQS's ApproximateNumberOfMessages, meaning messages still
		// waiting to be received. Messages the connector already holds in
		// flight are not "behind" and surface as the per-queue unacked
		// field above. The count is approximate and eventually consistent;
		// treat it as a trend, not an exact figure.
		//
		// Emit connectorStatus only when the rollup changes.
		status := fmt.Sprintf("%d messages behind across %d of %d queues", totalBacklog, queuesBehind, len(bindings))
		if status != lastStatus {
			log.WithField("eventType", "connectorStatus").Info(status)
			lastStatus = status
		}
	}
}
