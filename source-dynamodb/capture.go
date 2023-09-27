package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamTypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
)

// captureState is the persistent state for the entire capture, which consists of one or more table
// states.
type captureState struct {
	Tables map[string]tableState `json:"tables,omitempty"`
}

type tableState struct {
	BackfillFinishedAt      time.Time             `json:"backfillFinishedAt,omitempty"`
	TotalBackfillSegments   int                   `json:"totalBackfillSegments,omitempty"`
	BackfillSegmentProgress map[int]segmentState  `json:"backfillSegmentProgress,omitempty"`
	Shards                  map[string]shardState `json:"shards,omitempty"`
	StreamArn               string                `json:"streamArn,omitempty"`
}

type segmentState struct {
	ExclusiveStartKey []byte    `json:"exclusiveStartKey,omitempty"`
	FinishedAt        time.Time `json:"finishedAt,omitempty"`
}

type shardState struct {
	LastReadSequence string `json:"lastReadSequence,omitempty"`
	FinishedReading  bool   `json:"finishedReading,omitempty"`
}

type capture struct {
	client *client
	stream *boilerplate.PullOutput

	config config

	// Mutex for guarding changes to captureState.
	mu    sync.Mutex
	state captureState
}

func (c *capture) getSegmentState(table string, segment int) segmentState {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.state.Tables[table].BackfillSegmentProgress[segment]
}

func (c *capture) getSequenceState(table string, shardId string) shardState {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.state.Tables[table].Shards[shardId]
}

func (c *capture) initializeTable(ctx context.Context, binding int, tableName string, configuredRcus int) (*table, error) {
	t := &table{
		tableName:  tableName,
		bindingIdx: binding,
	}

	d, hasStream, err := discoverTable(ctx, c.client, tableName)
	if err != nil {
		return nil, fmt.Errorf("discover table %s for capture: %w", tableName, err)
	} else if !hasStream {
		return nil, fmt.Errorf("table %s does not have an enabled stream", tableName)
	}

	// The key fields for an active table must be known for this reason: DynamoDB allows for numeric
	// keys, but makes no distinction between integers and decimals. Flow does not allow decimal
	// collection keys, so numeric DynamoDB keys are converted to string values with number format
	// in their suggested collection schema.
	t.keyFields = d.keyFields

	// If an RCU limit was not configured and the table has a provisioned limit, use the provisioned
	// limit.
	if configuredRcus != 0 {
		t.effectiveRcus = configuredRcus
	} else if d.rcus != 0 {
		t.effectiveRcus = d.rcus
	} else {
		log.WithField("table", tableName).Info("no RCU limit set for the table and no provisioned limit detected; backfills will use unlimited RCUs")
	}

	tableState := c.state.Tables[t.tableName]

	if tableState.Shards == nil {
		// Never emitted a document from reading a shard.
		tableState.Shards = make(map[string]shardState)
	}
	if tableState.BackfillSegmentProgress == nil {
		// Never emitted a document from a backfill.
		tableState.BackfillSegmentProgress = make(map[int]segmentState)
	}

	if tableState.StreamArn == "" {
		// First time seeing this table.

		// Set the number of backfill segments for parallel scans. This must not change after the
		// backfill has started. It can be overridden via advanced configuration but changing that
		// after the backfill has started will have no effect.
		segments := defaultBackfillSegments
		if c.config.Advanced.BackfillSegments != 0 {
			segments = c.config.Advanced.BackfillSegments
		}
		log.WithFields(log.Fields{
			"table":    t.tableName,
			"segments": segments,
		}).Info("setting backfill segments for table")
		tableState.TotalBackfillSegments = segments

		// Get a persistent reference to the stream ARN. It is an error if this ever changes.
		log.WithFields(log.Fields{
			"table":     t.tableName,
			"streamArn": d.streamArn,
		}).Info("setting stream ARN for table")
		tableState.StreamArn = d.streamArn

		// Mark any shards that are closed before the backfill begins as fully read. Shards that
		// have been closed prior to the backfill starting by definition will not have any records
		// added to them while the backfill is on-going. It would not be incorrect to capture the
		// values from these shards, but is unnecessary.
		shards, err := c.listShards(ctx, tableState.StreamArn)
		if err != nil {
			return nil, fmt.Errorf("listing shards: %w", err)
		}

		for shardId, shard := range shards {
			if shard.SequenceNumberRange.EndingSequenceNumber == nil {
				// Shard is still open.
				continue
			}

			log.WithFields(log.Fields{
				"table":     t.tableName,
				"streamArn": tableState.StreamArn,
				"shardId":   shardId,
			}).Info("will not read change events from shard since it was closed prior to starting table backfill")
			tableState.Shards[shardId] = shardState{FinishedReading: true}
		}
	}

	if tableState.StreamArn != d.streamArn {
		// The stream ARN can only change if the stream is disabled and then re-enabled on a table,
		// which would result in a loss of data consistency.
		return nil, fmt.Errorf("stream ARN has changed for table %s: was %s and is now %s", t.tableName, tableState.StreamArn, d.streamArn)
	}

	// Hydrate parameters from the persisted state.
	t.totalBackfillSegments = tableState.TotalBackfillSegments
	t.streamArn = tableState.StreamArn
	t.backfillComplete = !tableState.BackfillFinishedAt.IsZero()

	t.shardMonitorDelay = defaultShardMonitorDelay
	t.scanLimitFromConfig = 0 // The actual scan limit used may be lower if the table has low provisioned RCUs. 0 is unlimited.

	// Overrides from advanced configuration.
	if c.config.Advanced.ScanLimit != 0 {
		t.scanLimitFromConfig = c.config.Advanced.ScanLimit
	}

	c.state.Tables[t.tableName] = tableState

	return t, nil
}

// captureTable runs the capture process for a single table, which will either backfill the table
// while keeping the table streams reasonably well caught-up, or continuously read from the table
// streams in real-time after the backfill is finished. A couple of important things to be aware of
// are that it is not possible to do a sorted scan through a table in a general way for backfilling,
// nor can we do any kind of watermark writes for establishing a consistent point in the table
// streams.
//
// Given these limitations, we can achieve an eventually consistent capture of the table with
// respect to a "last write wins" materialization of the resulting collection. All change events
// from the table are captured intermingled with backfill records.
//
// Consider these scenarios:
//
// - A change event is read from the stream for a key that has already been read during the
// backfill, and the change event is newer than the value read during the backfill: This is the most
// desirable case, where the value in the collection will follow a logical progression of changes.
//
// - A change event is read from the stream for a key that has already been read during the
// backfill, and the change event is older than the value read during the backfill: The key in the
// collection will appear to temporarily "revert" to an older value, but eventually the more recent
// change event will be read from the stream and restore it.
//
// - A change event is read from the stream for a key that has not been read by the backfill: The
// backfill will eventually read the key from the table and will emit an identical record to the
// collection. The record from the backfill will have metadata indicating that it is a "snapshot"
// record.
func (c *capture) captureTable(ctx context.Context, t *table) error {
	for !t.backfillComplete {
		// Catch up streams for the table between periods of backfilling to ensure stream records do
		// not become unavailable after DynamoDB stream's 24-hour retention limit. This will read
		// only "closed" stream shards to the end. Shards are cycled out at regular intervals (every
		// ~4 hours), with open shards getting closed and replaced with new ones.
		//
		// It is impossible to know when we have "caught up" when reading an open shard, since a
		// GetRecords request to an open shard may return no data even if we aren't at the end of
		// it, and there is no guarantee of how many of these "no data" responses are needed to get
		// to the next section of the shard with data. Reading only closed shards allows for knowing
		// when we are at the end of the shard via the next shard iterator being `nil`.
		//
		// The horizon parameter is used to mitigate a possible race condition that can only exist
		// if this capture were split into multiple Flow task shards (not currently supported, see
		// TODO comment further down in this function). The challenge is that there is no way to
		// tell the key range covered by individual backfill segments or stream shards. This means
		// that one Flow task shard might end up processing keys for backfill segments that overlap
		// with keys from stream shards for a different Flow task shard.
		//
		// There is a potential data race where an older backfill record might clobber a newer
		// stream record:
		// 1) Flow task shard A reads value of key `foo` from the table while backfilling.
		// 2) A change to key `foo` in the table occurs. Task shard A will never read this key in
		// the table again during its backfill.
		// 3) Flow task shard B reads the change record of key `foo` from the stream.
		// 4) Flow task shard B commits the change record to Flow.
		// 5) Flow task shard A commits the now stale value it read to Flow.
		//
		// When running these "catch up" reads, stream change records newer than the horizon will be
		// held back to prevent this possible data race. The assumption is that the Flow task shard
		// responsible for backfilling the table will not be delayed in actually committing the
		// values read to Flow for longer than the specified horizon.
		log.WithFields(log.Fields{
			"table": t.tableName,
		}).Info("catching up streams for table")
		if err := c.catchupStreams(ctx, t, backfillStreamingHorizon); err != nil {
			return fmt.Errorf("catching up streams for table '%s': %w", t.tableName, err)
		}

		// Alternate catching up streams with backfilling from the table. The backfill runs for the
		// duration provided by backfillDuration. Alternating between backfilling a streaming is a
		// little simpler than backfilling continuously concurrently with streaming because of the
		// need to manage the horizon time for streaming while the backfill is in progress.
		log.WithFields(log.Fields{
			"table":            t.tableName,
			"segments":         t.totalBackfillSegments,
			"rcus":             t.effectiveRcus,
			"backfillDuration": backfillDuration.String(),
		}).Info("backfilling table")
		if err := c.backfill(ctx, t, backfillDuration); err != nil {
			return fmt.Errorf("backfilling table '%s': %w", t.tableName, err)
		}
	}

	// TODO(whb): If we were ever going to allow these Flow consumer shards to be split, we would
	// need to coordinate that ALL Flow task shards were done backfilling prior to starting this
	// continuous stream. This is because of the possible data race described above in the backfill
	// loop.
	log.WithFields(log.Fields{
		"table": t.tableName,
	}).Info("streaming from table")
	if err := c.streamTable(ctx, t); err != nil {
		return fmt.Errorf("streaming from table '%s': %w", t.tableName, err)
	}

	return nil
}

// checkpoint emits the current state of the connector to the output stream. Synchronization must be
// handled by the caller.
func (c *capture) checkpoint() error {
	if cp, err := json.Marshal(c.state); err != nil {
		return fmt.Errorf("preparing checkpoint update: %w", err)
	} else if err := c.stream.Checkpoint(cp, false); err != nil {
		return fmt.Errorf("emitting checkpoint: %w", err)
	}

	return nil
}

var dynamoOpsToChangeOps = map[streamTypes.OperationType]string{
	streamTypes.OperationTypeInsert: "c",
	streamTypes.OperationTypeRemove: "d",
	streamTypes.OperationTypeModify: "u",
}

func (c *capture) emitBackfill(
	binding int,
	tableName string,
	segment int,
	segmentState segmentState,
	records []map[string]types.AttributeValue,
	keyFields []string,
) error {
	docs := make([]json.RawMessage, 0, len(records))

	for _, r := range records {
		doc, err := decodeAttributes(r, keyFields)
		if err != nil {
			return err
		}

		doc["_meta"] = backfillItemMeta{
			Snapshot: true,
			Op:       dynamoOpsToChangeOps[streamTypes.OperationTypeInsert],
		}

		raw, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("serializing document: %w", err)
		}

		docs = append(docs, raw)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.state.Tables[tableName].BackfillSegmentProgress[segment] = segmentState

	if err := c.stream.Documents(binding, docs...); err != nil {
		return fmt.Errorf("outputting backfill documents: %w", err)
	} else if err := c.checkpoint(); err != nil {
		return fmt.Errorf("outputting backfill checkpoint: %w", err)
	}

	return nil
}

func (c *capture) emitStream(
	binding int,
	tableName string,
	shardId string,
	shardState shardState,
	records []streamTypes.Record,
	keyFields []string,
) error {
	docs := make([]json.RawMessage, 0, len(records))

	for _, r := range records {
		doc, err := decodeStreamRecordAttributes(r.Dynamodb.NewImage, keyFields)
		if err != nil {
			return err
		}

		meta := streamRecordMeta{
			EventId:                     r.EventID,
			Op:                          dynamoOpsToChangeOps[r.EventName],
			ApproximateCreationDateTime: r.Dynamodb.ApproximateCreationDateTime,
		}

		if r.UserIdentity != nil {
			meta.UserIdentity = &userIdentity{
				PrincipalId: r.UserIdentity.PrincipalId,
				Type:        r.UserIdentity.Type,
			}
		}

		var beforeRecord map[string]any
		if r.Dynamodb.OldImage != nil {
			beforeRecord, err = decodeStreamRecordAttributes(r.Dynamodb.OldImage, keyFields)
			if err != nil {
				return err
			}

			meta.Before = beforeRecord
		}

		// Deletes are streamed as empty records with the partition & sort keys only
		// available in the prior record image.
		if len(doc) == 0 {
			// Sanity check: Discovery requires the streamViewType be set to
			// NEW_AND_OLD_IMAGES, so this should never happen.
			if beforeRecord == nil {
				log.WithFields(log.Fields{
					"eventId":                     *r.EventID,
					"approximateCreationDateTime": r.Dynamodb.ApproximateCreationDateTime.String(),
					"operationType":               r.EventName,
				},
				).Error("delete event")
				return fmt.Errorf("received delete event with nil OldImage, check the logs for more details")
			}

			// Add the key values (only) from the old image to the output document.
			for _, key := range keyFields {
				doc[key] = beforeRecord[key]
			}
		}

		doc["_meta"] = meta

		raw, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("serializing document: %w", err)
		}

		docs = append(docs, raw)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.state.Tables[tableName].Shards[shardId] = shardState

	if err := c.stream.Documents(binding, docs...); err != nil {
		return fmt.Errorf("outputting stream documents: %w", err)
	} else if err := c.checkpoint(); err != nil {
		return fmt.Errorf("outputting stream checkpoint: %w", err)
	}

	return nil
}

type backfillItemMeta struct {
	Snapshot bool   `json:"snapshot"`
	Op       string `json:"op"`
}

type streamRecordMeta struct {
	EventId                     *string        `json:"eventId,omitempty"`
	Op                          string         `json:"op"`
	UserIdentity                *userIdentity  `json:"userIdentity,omitempty"`
	ApproximateCreationDateTime *time.Time     `json:"approximateCreationDateTime,omitempty"`
	Before                      map[string]any `json:"before,omitempty"`
}

// This is for distinguishing between deletion records as a result of explicit deletions vs. things
// expiring from TTL. See
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html
type userIdentity struct {
	PrincipalId *string `json:"principalId,omitempty"`
	Type        *string `json:"type,omitempty"`
}

func decodeStreamRecordAttributes(attrs map[string]streamTypes.AttributeValue, keyFields []string) (map[string]any, error) {
	item, err := attributevalue.FromDynamoDBStreamsMap(attrs)
	if err != nil {
		return nil, fmt.Errorf("converting from streams attribute value: %w", err)
	}

	return decodeAttributes(item, keyFields)
}

func decodeAttributes(attrs map[string]types.AttributeValue, keyFields []string) (map[string]any, error) {
	// Unmarshal the map of field name: types.AttributeValue to a map of field name: `any`.
	// attributevalue.UnmarshalMap decodes a types.AttributeValue to an `any` by converting the
	// DynamoDB value to its corresponding native Go type and value, which works quite well for
	// building a JSON document.
	record := make(map[string]any)

	if err := attributevalue.UnmarshalMap(attrs, &record); err != nil {
		return nil, fmt.Errorf("unmarshalling attributevalue: %w", err)
	}

	for key, val := range record {
		// Convert any key fields that are DynamoDB numeric to strings.
		if key == keyFields[0] || (len(keyFields) == 2 && key == keyFields[1]) {
			// attributevalue.UnmarshalMap decodes all N types into a float64.
			if actual, ok := val.(float64); ok {
				record[key] = strconv.FormatFloat(actual, 'f', -1, 64)
			}
		}
	}

	return record, nil
}
