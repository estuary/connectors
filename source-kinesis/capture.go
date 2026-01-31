package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	glueTypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

type capture struct {
	mu     sync.Mutex
	client *kinesis.Client
	stream *boilerplate.PullOutput

	updateState map[boilerplate.StateKey]map[string]*string

	stats map[string]map[string]shardStats

	glueClient      *glue.Client
	glueSchemaCache map[string]struct{} // tracks emitted schema version IDs
}

type shardStats struct {
	docsThisRound int
	millisBehind  int
}

func (c *capture) updateStats(stream string, shardID string, docs int, millisBehind int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stats[stream] == nil {
		c.stats[stream] = make(map[string]shardStats)
	}

	upd := c.stats[stream][shardID]
	upd.docsThisRound += docs
	upd.millisBehind = millisBehind
	c.stats[stream][shardID] = upd
}

func (c *capture) statsLogger(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Minute):
			if err := func() error {
				c.mu.Lock()
				defer c.mu.Unlock()

				fields := make(map[string]any)
				for streamName, shardStats := range c.stats {
					for shardID, stats := range shardStats {
						if stats.docsThisRound == 0 {
							continue
						}

						if fields[streamName] == nil {
							fields[streamName] = make(map[string]any)
						}

						lag := time.Duration(stats.millisBehind) * time.Millisecond
						s := make(map[string]any)
						s["docs"] = stats.docsThisRound
						s["lag"] = lag.String()
						fields[streamName].(map[string]any)[shardID] = s
					}
				}

				if len(fields) == 0 {
					log.Info("all kinesis streams idle")
					return nil
				}
				log.WithFields(fields).Info("processed kinesis stream records")
				maps.Clear(c.stats)

				return nil
			}(); err != nil {
				return err
			}
		}
	}
}

func (c *capture) emitDoc(
	doc json.RawMessage,
	stateKey boilerplate.StateKey,
	bindingIndex int,
	shardId string,
	sequence string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.stream.Documents(bindingIndex, doc); err != nil {
		return err
	}

	if c.updateState[stateKey] == nil {
		c.updateState[stateKey] = make(map[string]*string)
	}
	c.updateState[stateKey][shardId] = &sequence

	return nil
}

func (c *capture) emitState() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.updateState) == 0 {
		return nil
	}

	cp := captureState{
		Streams: c.updateState,
	}

	if cpJson, err := json.Marshal(cp); err != nil {
		return err
	} else if err := c.stream.Checkpoint(cpJson, true); err != nil {
		return err
	}

	maps.Clear(c.updateState)

	return nil
}

func (c *capture) pruneShards(
	stateKey boilerplate.StateKey,
	streamState map[string]*string,
	allShards map[string]types.Shard,
) error {
	deleteCheckpoint := make(map[string]*string)
	for shardId := range streamState {
		if _, ok := allShards[shardId]; !ok {
			delete(streamState, shardId)
			deleteCheckpoint[shardId] = nil
			log.WithFields(log.Fields{
				"stateKey": stateKey,
				"shardId":  shardId,
			}).Info("removing shard from checkpoint state since it no longer exists")
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(deleteCheckpoint) == 0 {
		// pruneShards will always emit a checkpoint, although it may be empty.
		// This is really for allowing our test suite to work correctly with
		// shutdown timeouts, otherwise no output would be observed from the
		// connector at all in some cases.
		return c.stream.Checkpoint([]byte("{}"), true)
	}

	cp := captureState{
		Streams: map[boilerplate.StateKey]map[string]*string{
			stateKey: deleteCheckpoint,
		},
	}

	if cpJson, err := json.Marshal(cp); err != nil {
		return err
	} else if err := c.stream.Checkpoint(cpJson, true); err != nil {
		return err
	}

	return nil
}

type shardToRead struct {
	parents []string
	shardId string
}

// shardCompletionEvent is either a child shard resulting from a completed shard
// read, or a notification of the completed shard read itself. These events are
// used to track if a stream has any active shards left.
type shardCompletionEvent interface {
	isShardCompletionEvent()
}

type childShardCompletionEvent struct {
	child types.ChildShard
}

func (c childShardCompletionEvent) isShardCompletionEvent() {}

type shardDoneCompletionEvent struct{}

func (c shardDoneCompletionEvent) isShardCompletionEvent() {}

func (c *capture) readStream(
	ctx context.Context,
	stream kinesisStream,
	stateKey boilerplate.StateKey,
	bindingIndex int,
	initialState map[string]*string,
) error {
	allShards, err := c.listAllShards(ctx, stream.arn)
	if err != nil {
		return fmt.Errorf("listing shards: %w", err)
	}

	// Clear out the state checkpoint of any shards that no longer exist. This
	// is only done the first time the connector starts up, but should keep the
	// checkpoint from growing indefinitely as long as the capture restarts
	// every now and then.
	if err := c.pruneShards(stateKey, initialState, allShards); err != nil {
		return fmt.Errorf("pruning shards: %w", err)
	}

	// Root shards are those that either don't have a parent or the parent no
	// longer exists. These are the shards we'll start reading from, and move on
	// to their children when they are closed.
	rootShards := make([]types.Shard, 0, len(allShards))
	for _, s := range allShards {
		if s.ParentShardId == nil {
			rootShards = append(rootShards, s)
		} else if _, parentExists := allShards[*s.ParentShardId]; !parentExists {
			rootShards = append(rootShards, s)
		}
	}

	group, groupCtx := errgroup.WithContext(ctx)
	readerOutput := make(chan shardCompletionEvent)
	activeReaders := 0
	tracker := newShardTracker()

	for _, shard := range rootShards {
		activeReaders++
		shard := shardToRead{shardId: *shard.ShardId}
		group.Go(func() error {
			return c.readShard(groupCtx, tracker, readerOutput, stream, stateKey, bindingIndex, shard, initialState)
		})
	}

	// As the root shard reads complete, any child shards they produce must be
	// then read.
	for e := range readerOutput {
		switch e := e.(type) {
		case childShardCompletionEvent:
			activeReaders++
			group.Go(func() error {
				shard := shardToRead{parents: e.child.ParentShards, shardId: *e.child.ShardId}
				return c.readShard(groupCtx, tracker, readerOutput, stream, stateKey, bindingIndex, shard, initialState)
			})
		case shardDoneCompletionEvent:
			activeReaders--
			if activeReaders == 0 {
				// This may happen if a stream is closed, but shards remain
				// available to read for 24 hours.
				log.WithField("stream", stream.name).Info("finished reading stream")
				close(readerOutput)
			}
		}
	}

	return group.Wait()
}

func (c *capture) getShardIterator(ctx context.Context, stream kinesisStream, shardId string, startingSequence *string) (*string, error) {
	iteratorInput := &kinesis.GetShardIteratorInput{
		StreamARN:         &stream.arn,
		ShardId:           &shardId,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	}

	if startingSequence != nil {
		iteratorInput.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		iteratorInput.StartingSequenceNumber = startingSequence
	}

	for {
		iterInit, err := c.client.GetShardIterator(ctx, iteratorInput)
		if err != nil {
			var invalidArugmentErr *types.InvalidArgumentException
			if errors.As(err, &invalidArugmentErr) && iteratorInput.ShardIteratorType == types.ShardIteratorTypeAfterSequenceNumber {
				// This error occurs if a stream is deleted and re-created, or
				// if the retention limit of a sequence number is exceeded. In
				// either case the only thing to do is start reading the shard
				// from the beginning, which is actually the "future" relative
				// to an expired sequence.
				iteratorInput.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
				iteratorInput.StartingSequenceNumber = nil
				log.WithError(invalidArugmentErr).Warn("starting sequence was invalid; will attempt to read shard from TRIM_HORIZON")
				continue
			}
			return nil, fmt.Errorf("getting shard iterator: %w", err)
		}

		return iterInit.ShardIterator, nil
	}
}

func (c *capture) readShard(
	ctx context.Context,
	tracker *shardTracker,
	output chan<- shardCompletionEvent,
	stream kinesisStream,
	stateKey boilerplate.StateKey,
	bindingIndex int,
	shard shardToRead,
	state map[string]*string,
) error {
	defer func() {
		output <- shardDoneCompletionEvent{}
	}()

	ll := log.WithFields(log.Fields{
		"stream":       stream.name,
		"kinesisShard": shard.shardId,
	})

	if !tracker.setReading(shard.shardId) {
		// This could happen when two shards are merged, and both parent shards
		// return the same child shard.
		ll.Info("skipping shard since it's already being read")
		return nil
	}

	for _, parent := range shard.parents {
		// All parent shards must be fully read to ensure accurate sequencing of
		// read events. This is pretty straightforward for shards that are split
		// where all children have the same single parent, but more annoying
		// when shards are merged and one child has multiple parents. For the
		// later case, the shard tracker is used to coordinate across shard
		// lineages.
		if waitFn := tracker.waitForFinished(parent); waitFn != nil {
			ll.WithField("parent", parent).Info("waiting for parent shard to finish reading before reading this shard")
			if err := waitFn(ctx); err != nil {
				return err
			}
		}
	}

	readLog := ll
	var lastSequence *string
	if lastSequence = state[shard.shardId]; lastSequence != nil {
		readLog = readLog.WithField("startingSequenceNumber", *lastSequence)
	}
	readLog.Info("started reading kinesis shard")

	iterator, err := c.getShardIterator(ctx, stream, shard.shardId, lastSequence)
	if err != nil {
		return err
	}

	// Respect the kinesis 5 TPS rate limit.
	limiter := rate.NewLimiter(rate.Every(time.Second), 5)
	var didLogNoData bool
	for {
		if err := limiter.Wait(ctx); err != nil {
			return fmt.Errorf("waiting for rate limiter: %w", err)
		}
		res, err := c.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iterator,
			StreamARN:     &stream.arn,
		})
		if err != nil {
			var expiredIteratorErr *types.ExpiredIteratorException
			if errors.As(err, &expiredIteratorErr) {
				ll.Info("shard iterator expired, getting new iterator")
				if iterator, err = c.getShardIterator(ctx, stream, shard.shardId, lastSequence); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("get records: %w", err)
		}

		if len(res.Records) > 0 {
			lastSequence = res.Records[len(res.Records)-1].SequenceNumber
		}

		if err := c.processRecords(ctx, res.Records, stream, stateKey, bindingIndex, shard); err != nil {
			return fmt.Errorf("processing records for stream %s: %w", stream.name, err)
		}
		c.updateStats(stream.name, shard.shardId, len(res.Records), int(*res.MillisBehindLatest))

		if res.NextShardIterator == nil {
			ll.WithField("childShards", len(res.ChildShards)).Info("finished reading shard")
			tracker.setFinished(shard.shardId)
			for _, s := range res.ChildShards {
				output <- childShardCompletionEvent{
					child: s,
				}
			}
			return nil
		}
		iterator = res.NextShardIterator

		if *res.MillisBehindLatest != 0 && len(res.Records) == 0 {
			ll.WithField("MillisBehindLatest", *res.MillisBehindLatest).Info("shard is not caught up but returned no new data")
			didLogNoData = true
		} else if *res.MillisBehindLatest == 0 && len(res.Records) == 0 {
			if didLogNoData {
				ll.Info("shard is caught up")
				didLogNoData = false
			}
		}
	}
}

// getGlueSchemaForRecord parses the AWS Glue Schema Registry header and fetches the schema.
// Returns the payload (data without header), schema definition, data format, whether schema was from cache, and error.
//
// AWS Glue Schema Registry header format:
//   - Byte 0: Header version (expected: 3)
//   - Byte 1: Compression type (0 = none)
//   - Bytes 2-17: Schema version UUID (16 bytes)
//   - Bytes 18+: Payload
//
// Ref: https://github.com/awslabs/aws-glue-schema-registry
// getGlueSchemaForRecord parses a Glue header from data and returns the JSON Schema if this
// is a newly-seen schema version. Returns nil if the schema was already cached.
// The caller should use data[18:] as the payload after this call succeeds.
func (c *capture) getGlueSchemaForRecord(ctx context.Context, data []byte) (json.RawMessage, error) {
	compression := data[1]
	if compression != 0 {
		return nil, fmt.Errorf("compressed glue schema headers are not supported")
	}

	if c.glueClient == nil {
		return nil, fmt.Errorf("got message with glue header, but glue connection previously failed")
	}

	schemaUUID, err := uuid.FromBytes(data[2:18])
	if err != nil {
		return nil, fmt.Errorf("parsing schema version UUID: %w", err)
	}
	schemaVersionID := schemaUUID.String()

	// Check cache first - if cached, no schema to emit
	c.mu.Lock()
	if _, ok := c.glueSchemaCache[schemaVersionID]; ok {
		c.mu.Unlock()
		return nil, nil
	}
	c.mu.Unlock()

	out, err := c.glueClient.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
		SchemaVersionId: aws.String(schemaVersionID),
	})
	if err != nil {
		return nil, err
	}

	schemaDefinition := aws.ToString(out.SchemaDefinition)

	// Cache the schema
	c.mu.Lock()
	c.glueSchemaCache[schemaVersionID] = struct{}{}
	c.mu.Unlock()

	// Convert to JSON Schema based on the data format
	switch out.DataFormat {
	case glueTypes.DataFormatAvro:
		jsonSchemaObj, err := avroToJSONSchema(schemaDefinition)
		if err != nil {
			return nil, fmt.Errorf("converting avro to JSON Schema: %w", err)
		}
		return json.Marshal(jsonSchemaObj)

	case glueTypes.DataFormatJson:
		if !json.Valid([]byte(schemaDefinition)) {
			return nil, fmt.Errorf("invalid JSON Schema from Glue")
		}
		return json.RawMessage(schemaDefinition), nil

	default:
		return nil, fmt.Errorf("unsupported Glue schema format: %s", out.DataFormat)
	}
}

func (c *capture) processRecords(
	ctx context.Context,
	records []types.Record,
	stream kinesisStream,
	stateKey boilerplate.StateKey,
	bindingIndex int,
	shard shardToRead,
) error {
	if len(records) == 0 {
		return nil
	}

	var lastSequence string
	for _, r := range records {
		lastSequence = *r.SequenceNumber

		doc := map[string]json.RawMessage{}

		// Check for Glue Schema Registry header (version byte = 3)
		// Header is 18 bytes: 1 version + 1 compression + 16 UUID
		payload := r.Data
		if len(r.Data) >= 18 && r.Data[0] == 3 {
			jsonSchema, err := c.getGlueSchemaForRecord(ctx, r.Data)
			if err != nil {
				return fmt.Errorf(
					"parsing glue header for record with sequenceNumber %q and partitionKey %q: %w",
					*r.SequenceNumber, *r.PartitionKey, err,
				)
			}
			payload = r.Data[18:]

			if jsonSchema != nil {
				if err := c.stream.SourcedSchema(bindingIndex, jsonSchema); err != nil {
					return fmt.Errorf("emitting sourced schema: %w", err)
				}
			}
		}

		if err := json.Unmarshal(payload, &doc); err != nil {
			return fmt.Errorf(
				"unmarshalling record with sequenceNumber %q and partitionKey %q: %w",
				*r.SequenceNumber, *r.PartitionKey, err,
			)
		}

		meta := map[string]any{
			sequenceNumber: *r.SequenceNumber,
			partitionKey:   *r.PartitionKey,
			sourceProperty: map[string]any{
				streamSource: stream.name,
				shardSource:  shard.shardId,
			},
		}

		// If the Kinesis record happens to already have a property called
		// "_meta", merge it into our synthesized metadata.
		if existingMeta, ok := doc[metaProperty]; ok {
			existingMetaParsed := map[string]json.RawMessage{}
			// A parsing error here is ignored, because it means that the
			// existing "_meta" field isn't an object, and must be completely
			// overwritten by the connector's "_meta" object.
			_ = json.Unmarshal(existingMeta, &existingMetaParsed)

			for k, v := range existingMetaParsed {
				if _, ok := meta[k]; ok {
					// Don't clobber our metadata fields if the Kinesis record
					// has one with the same name. This is means we aren't
					// necessarily doing a full merge patch on nested objects,
					// but that seems unlikely to be a problem in practice.
					continue
				}
				meta[k] = v
			}
		}

		metaBytes, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf(
				"marshalling metadata for record with sequenceNumber %q and partitionKey %q: %w",
				*r.SequenceNumber, *r.PartitionKey, err,
			)
		}

		doc[metaProperty] = metaBytes
		if docBytes, err := json.Marshal(doc); err != nil {
			return err
		} else if err := c.emitDoc(docBytes, stateKey, bindingIndex, shard.shardId, lastSequence); err != nil {
			return err
		}
	}

	return c.emitState()
}

func (c *capture) listAllShards(ctx context.Context, arn string) (map[string]types.Shard, error) {
	allShards := make(map[string]types.Shard)

	input := &kinesis.ListShardsInput{StreamARN: &arn}
	for {
		res, err := c.client.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}

		for _, s := range res.Shards {
			allShards[*s.ShardId] = s
		}

		if res.NextToken == nil {
			break
		}
		input.NextToken = res.NextToken
	}

	return allShards, nil
}
