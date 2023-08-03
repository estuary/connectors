package main

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamTypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// streamTable starts streaming change events from the table. It periodically checks if the shard
// topology has changed and re-assigns workers to read from newly added shards.
func (c *capture) streamTable(ctx context.Context, t *table) error {
	var initialShards map[string]streamTypes.Shard
	var workersStop chan struct{}
	var workers *errgroup.Group
	var workersCtx context.Context

	for {
		currentShards, err := c.listShards(ctx, t.streamArn)
		if err != nil {
			return fmt.Errorf("listing shards: %w", err)
		}

		// Re-start workers on a change in shard topology.
		if !reflect.DeepEqual(initialShards, currentShards) {
			// On initial startup the workers will have not have been started yet.
			if workersStop != nil {
				log.WithFields(log.Fields{
					"table":  t.tableName,
					"stream": t.streamArn,
				}).Debug("shard topology changed")

				// Signal the workers to stop.
				close(workersStop)
				go func() { workers.Wait() }()

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-workersCtx.Done(): // Wait for the workers to have all returned.
					if err := workers.Wait(); err != nil {
						return fmt.Errorf("waiting for workers to stop: %w", err)
					}
				}
			}

			if err := c.pruneShards(t.tableName, currentShards); err != nil {
				return fmt.Errorf("pruning shards: %w", err)
			}

			workersStop = make(chan struct{})
			workers, workersCtx = errgroup.WithContext(ctx)

			// Each shard "tree" must be read concurrently. A shard tree consists of a root parent
			// shard, with one or more child shards. Parent shards must be read completely before
			// reading child shards to ensure correct ordering of events on a per-key basis. There
			// is no way to read shards for completely accurate ordering of events across _all_ keys
			// - we can only get correct ordering relative to individual keys by reading shards in
			// this way.
			//
			// TODO(whb): We may need to consider some kind of semaphore for cases where there are a
			// huge number of shards to read from. We do need to read each shard (well, shard tree)
			// concurrently to make progress on every shard. But if there are many shards we might
			// hit memory issues if we get a set of results back from each one and are delayed in
			// emitting it to the runtime.
			for _, shardTree := range buildShardTrees(currentShards) {
				shardTree := shardTree
				workers.Go(func() error {
					return c.readShardTree(workersCtx, t, shardTree, workers, workersStop, false, 0)
				})
			}

			initialShards = currentShards
		}

		// Delay before the next monitoring pass.
		select {
		case <-time.After(t.shardMonitorDelay):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// catchupStreams is similar to streamTable, but only does a single pass of reading all currently
// closed shards to the end, or to the first record newer than horizon.
func (c *capture) catchupStreams(ctx context.Context, t *table, horizon time.Duration) error {
	currentShards, err := c.listShards(ctx, t.streamArn)
	if err != nil {
		return fmt.Errorf("listing shards: %w", err)
	}

	if err := c.pruneShards(t.tableName, currentShards); err != nil {
		return fmt.Errorf("pruning shards: %w", err)
	}

	workers, workersCtx := errgroup.WithContext(ctx)

	for _, shardTree := range buildShardTrees(currentShards) {
		shardTree := shardTree
		workers.Go(func() error {
			return c.readShardTree(workersCtx, t, shardTree, workers, make(chan struct{}), true, horizon)
		})
	}

	return workers.Wait()
}

// pruneShards removes any shards from the capture state that we have completely read and do not
// still exist in the shard listing. This is done to keep the checkpoint from endlessly growing as
// stream shards are cycled through.
func (c *capture) pruneShards(tableName string, activeShards map[string]streamTypes.Shard) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	prune := []string{}
	for id, state := range c.state.Tables[tableName].Shards {
		if _, ok := activeShards[id]; !ok {
			// This is a shard we previously saw but now do not. Was it completely read?
			if state.FinishedReading {
				// Shard is gone but was completely read, so prune it from our list.
				log.WithFields(log.Fields{
					"table":   tableName,
					"shardId": id,
				}).Info("pruning fully-read shard from capture state since it no longer exists")
				prune = append(prune, id)
			} else {
				// Shard is gone but we didn't get to completely read it. This is an error since it
				// means the capture has lost consistency. The binding will need to be reset to run
				// a new backfill for it before resuming streaming.
				return fmt.Errorf("shard %s was not completely read but no longer exists", id)
			}
		}
	}
	if len(prune) > 0 {
		for _, p := range prune {
			delete(c.state.Tables[tableName].Shards, p)
		}

		if err := c.checkpoint(); err != nil {
			return fmt.Errorf("emitting prune shards checkpoint: %w", err)
		}
	}

	return nil
}

func (c *capture) readShardTree(
	ctx context.Context,
	t *table,
	l *shardTree,
	workers *errgroup.Group,
	workersStop <-chan struct{},
	catchup bool,
	horizon time.Duration,
) error {
	// Only read closed shards for catch-up reads. An `EndingSequenceNumber` of `nil` means this
	// shard is still open and we have read as far as we can into this branch of the tree.
	if catchup && l.shard.SequenceNumberRange.EndingSequenceNumber == nil {
		log.WithFields(log.Fields{
			"table":     t.tableName,
			"streamArn": t.streamArn,
			"shardId":   *l.shard.ShardId,
		}).Debug("not reading open shard for catch-up read")
		return nil
	}

	// Read the root shard to the end. If the shard is currently open, this will block until the
	// shard is closed.
	if err := c.readShard(ctx, t, l.shard, workersStop, horizon); err != nil {
		return fmt.Errorf("reading shard tree: %w", err)
	}

	// Bail out if we have been signaled to stop. The root shard may not have been completely read
	// in this case, and we need to stop processing anyway.
	select {
	case <-workersStop:
		return nil
	default:
		// Read all of this shards children. Child shards can be read in parallel after the parent
		// shard is closed and has been completely read.
		for _, child := range l.children {
			child := child
			workers.Go(func() error {
				return c.readShardTree(ctx, t, child, workers, workersStop, catchup, horizon)
			})
		}

		return nil
	}
}

func (c *capture) readShard(
	ctx context.Context,
	t *table,
	shard streamTypes.Shard,
	workersStop <-chan struct{},
	horizon time.Duration,
) error {
	state := c.getSequenceState(t.tableName, *shard.ShardId)
	if state.FinishedReading {
		// Don't re-read already fully read shards.
		return nil
	}

	lastReadSeq := state.LastReadSequence

	iterOutput, err := c.client.stream.GetShardIterator(ctx, t.getShardIteratorInput(shard, lastReadSeq))
	if err != nil {
		return fmt.Errorf("getting shard iterator: %w", err)
	}
	iter := iterOutput.ShardIterator

	log.WithFields(log.Fields{
		"table":     t.tableName,
		"streamArn": t.streamArn,
		"shardId":   *shard.ShardId,
	}).Debug("started reading shard")

	// DynamoDB streams are billed per GetRecords request, and GetRecords returns immediately if
	// there is no data. Limit GetRecords requests to once per second to avoid excessively frequent
	// calls to GetRecords when tailing an open shard where there may be minimal new data available.
	limiter := rate.NewLimiter(rate.Every(time.Second), 1)
	if shard.SequenceNumberRange.EndingSequenceNumber != nil {
		// If this shard is closed, read it to the end as fast as possible.
		limiter.SetLimit(rate.Inf)
	}

	for iter != nil {
		select {
		case <-workersStop:
			// Stop processing if we have been signaled to stop due to a change in shard topology.
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := limiter.Wait(ctx); err != nil {
			return fmt.Errorf("stream limiter wait: %w", err)
		}

		recs, err := c.client.stream.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: iter,
		})
		if err != nil {
			return fmt.Errorf("getting stream records: %w", err)
		}

		reachedHorizon := false
		lastItemIdx := 0
		for _, r := range recs.Records {
			if horizon != 0 && time.Since(*r.Dynamodb.ApproximateCreationDateTime) <= horizon {
				// If this record is more recent than the desired time horizon, we will checkpoint
				// everything prior to this record and then return.
				log.WithFields(log.Fields{
					"table":                       t.tableName,
					"streamArn":                   t.streamArn,
					"shardId":                     *shard.ShardId,
					"horizon":                     horizon,
					"approximateCreationDateTime": r.Dynamodb.ApproximateCreationDateTime.String(),
				}).Debug("read to horizon for shard on catch-up read")
				reachedHorizon = true
				break
			}

			lastItemIdx++
			lastReadSeq = *r.Dynamodb.SequenceNumber
		}

		// Checkpoint new records or the fact that this shard is closed.
		if lastItemIdx > 0 || recs.NextShardIterator == nil {
			newState := shardState{
				LastReadSequence: lastReadSeq,
				FinishedReading:  recs.NextShardIterator == nil,
			}

			if err := c.emitStream(t.bindingIdx, t.tableName, *shard.ShardId, newState, recs.Records[:lastItemIdx], t.keyFields); err != nil {
				return fmt.Errorf("emitting stream documents for table '%s': %w", t.tableName, err)
			}
		}

		if reachedHorizon {
			return nil
		}

		// Advance for the next round. A nil NextShardIterator from the GetRecords response
		// indicates that the shard is closed and will yield no additional records.
		iter = recs.NextShardIterator
	}

	log.WithFields(log.Fields{
		"table":     t.tableName,
		"streamArn": t.streamArn,
		"shardId":   *shard.ShardId,
	}).Info("finished reading shard")

	return nil
}

func (t *table) getShardIteratorInput(shard streamTypes.Shard, lastReadSeq string) *dynamodbstreams.GetShardIteratorInput {
	input := &dynamodbstreams.GetShardIteratorInput{
		ShardId:   shard.ShardId,
		StreamArn: aws.String(t.streamArn),
	}

	if lastReadSeq == "" {
		// Haven't started this shard before, so start at the beginning.
		input.ShardIteratorType = streamTypes.ShardIteratorTypeTrimHorizon
	} else {
		// Pick up where we left off from a previous read.
		input.ShardIteratorType = streamTypes.ShardIteratorTypeAfterSequenceNumber
		input.SequenceNumber = aws.String(lastReadSeq)
	}

	return input
}

func (c *capture) listShards(ctx context.Context, streamArn string) (map[string]streamTypes.Shard, error) {
	shards := make(map[string]streamTypes.Shard)

	var exclusiveStartShardId *string
	for {
		streamDescribe, err := c.client.stream.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(streamArn),
			ExclusiveStartShardId: exclusiveStartShardId,
		})
		if err != nil {
			return nil, fmt.Errorf("describing stream: %w", err)
		}

		if streamDescribe.StreamDescription.StreamStatus != streamTypes.StreamStatusEnabled {
			return nil, fmt.Errorf("stream %s is no longer enabled and now has status %s", streamArn, streamDescribe.StreamDescription.StreamStatus)
		}

		for _, s := range streamDescribe.StreamDescription.Shards {
			shards[*s.ShardId] = s
		}

		exclusiveStartShardId = streamDescribe.StreamDescription.LastEvaluatedShardId
		if exclusiveStartShardId == nil { // Pagination
			break
		}
	}

	return shards, nil
}
