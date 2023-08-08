package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

func (c *capture) backfill(ctx context.Context, t *table, dur time.Duration) error {
	var scanLimit *int32
	if t.scanLimitFromConfig != 0 {
		// Stick with the scan limit set by the advanced configuration, if present.
		scanLimit = aws.Int32(int32(t.scanLimitFromConfig))
	} else if t.effectiveRcus > 100 || t.effectiveRcus == 0 {
		// For tables with a significant amount of provisioned capacity (or unlimited) do not
		// restrict the number of returned items. There is still a 1 MB limit to the amount of data
		// that a single scan request can return.
		scanLimit = nil
	} else {
		// Tables with low provisioned RCUs use a lower limit to even out the load on the table.
		scanLimit = aws.Int32(lowRcuScanLimit)
	}

	// Try to match the read capacity units consumed with either the configured RCU limit or the
	// provisioned RCUs for the table.
	rcuLimiter := rate.NewLimiter(rate.Limit(t.effectiveRcus), 1)
	if t.effectiveRcus == 0 {
		// If no RCU limit was set or determined from the table there will be no limit.
		rcuLimiter.SetLimit(rate.Inf)
	}

	// Initialize the active segments list. We cycle through reads of segments using a FIFO queue to
	// more evenly distribute reads across table partitions, as opposed to something like repeatedly
	// reading the same segment over and over. Individual table partitions have a hard RCU cap so it
	// is best to distribute read load across the table's partitions.
	t.activeSegments = make(chan int, t.totalBackfillSegments)
	for segment := 0; segment < t.totalBackfillSegments; segment++ {
		if c.getSegmentState(t.tableName, segment).FinishedAt.IsZero() {
			t.activeSegments <- segment
		}
	}

	// Arrange for stopCh to be closed after the provided duration to signal the backfill workers to
	// stop.
	stopCh := make(chan struct{})
	stopTimer := time.AfterFunc(dur, func() { close(stopCh) })
	defer stopTimer.Stop()

	// Run the backfill workers. They will continuously pull segements from t.activeSegements and
	// run backfill scans, returning only if they run out of segments or stopCh is closed.
	eg, groupCtx := errgroup.WithContext(ctx)
	for idx := 0; idx < backfillConcurrency; idx++ {
		eg.Go(func() error {
			return c.backfillWorker(groupCtx, t, scanLimit, rcuLimiter, stopCh)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	// All backfill workers may have returned because they ran out of work to do, meaning the
	// backfill is done. If there are still activeSegments, the workers must have returned because
	// they were signalled by stopCh and the backfill is as of yet incomplete.
	if len(t.activeSegments) > 0 {
		return nil
	}

	// Approximate completion time of the table backfill.
	finished := time.Now()

	// Checkpoint that the table is done backfilling.
	c.mu.Lock()
	defer c.mu.Unlock()

	tableState := c.state.Tables[t.tableName]
	tableState.BackfillFinishedAt = finished
	c.state.Tables[t.tableName] = tableState
	t.backfillComplete = true

	if err := c.checkpoint(); err != nil {
		return fmt.Errorf("checkpointing backfill complete: %w", err)
	}

	log.WithFields(log.Fields{
		"table":              t.tableName,
		"backfillFinishedAt": finished.String(),
	}).Info("backfill complete")

	return nil
}

func (c *capture) backfillWorker(ctx context.Context, t *table, scanLimit *int32, limiter *rate.Limiter, stopCh <-chan struct{}) error {
	// Initialized as a dummy value; this will be adjusted (increased) based on actual requests.
	rcusPerRequest := 1

	for {
		var segment int
		select {
		case <-stopCh:
			// Signalled to stop after the specified backfill duration.
			return nil
		default:
			select {
			case segment = <-t.activeSegments:
			default:
				// No remaining active segments to be backfilled, so this worker is done.
				return nil
			}
		}

		log.WithFields(log.Fields{
			"table":   t.tableName,
			"segment": segment,
		}).Debug("backfilling segment")

		state := c.getSegmentState(t.tableName, segment)

		startKey, err := decodeKey(t.keyFields, state.ExclusiveStartKey)
		if err != nil {
			return fmt.Errorf("unpacking resume key for segment %d of table %s: %w", segment, t.tableName, err)
		}

		scanned, err := c.doScan(ctx, t, scanLimit, limiter, rcusPerRequest, segment, startKey)
		if err != nil {
			return err
		}

		newState := segmentState{}
		if scanned.LastEvaluatedKey == nil {
			newState.FinishedAt = time.Now()
			// Retain the most recent start key rather than setting it to nil, mostly for the
			// benefit of test snapshots.
			newState.ExclusiveStartKey = state.ExclusiveStartKey
		} else {
			newStartKey, err := encodeKey(t.keyFields, scanned.LastEvaluatedKey)
			if err != nil {
				return fmt.Errorf("encoding resume key for segment %d of table %s: %w", segment, t.tableName, err)
			}
			newState.ExclusiveStartKey = newStartKey
		}

		if err := c.emitBackfill(t.bindingIdx, t.tableName, segment, newState, scanned.Items, t.keyFields); err != nil {
			return fmt.Errorf("emitting backfill documents for segment %d of table %s: %w", segment, t.tableName, err)
		}

		if len(scanned.Items) > 0 {
			// Adjust RCU usage prediction if we got anything from this request.
			rcusPerRequest = int(*scanned.ConsumedCapacity.CapacityUnits)

			// Adjust limiter bucket size if needed.
			if rcusPerRequest > limiter.Burst() {
				log.WithFields(log.Fields{
					"table":         t.tableName,
					"segment":       segment,
					"previousBurst": limiter.Burst(),
					"newBurst":      rcusPerRequest,
				}).Debug("increasing rcu limiter burst size")
				limiter.SetBurst(rcusPerRequest)
			}
		}

		if scanned.LastEvaluatedKey != nil {
			// There is more data available for this segment, so put it back into the queue.
			t.activeSegments <- segment
		} else {
			log.WithFields(log.Fields{
				"table":   t.tableName,
				"segment": segment,
			}).Info("backfill segment complete")
		}
	}
}

func (c *capture) doScan(
	ctx context.Context,
	t *table,
	scanLimit *int32,
	limiter *rate.Limiter,
	rcusPerRequest int,
	segment int,
	startKey map[string]types.AttributeValue,
) (*dynamodb.ScanOutput, error) {
	var (
		// The DynamoDB client library does retry internally, but occasionally we need to do
		// additional retries for throughput exceeded & other unhandled, transient exceptions.
		maxAttempts = 10
	)

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := limiter.WaitN(ctx, rcusPerRequest); err != nil {
			return nil, fmt.Errorf("backfill limiter wait: %w", err)
		}

		scanned, err := c.client.db.Scan(ctx, &dynamodb.ScanInput{
			TableName:              aws.String(t.tableName),
			ConsistentRead:         aws.Bool(true),
			ExclusiveStartKey:      startKey,
			Limit:                  scanLimit,
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
			Segment:                aws.Int32(int32(segment)),
			TotalSegments:          aws.Int32(int32(t.totalBackfillSegments)),
		})
		if err != nil {
			var throughputError *types.ProvisionedThroughputExceededException
			if errors.As(err, &throughputError) {
				log.WithFields(log.Fields{
					"table":   t.tableName,
					"segment": segment,
					"attempt": attempt,
					"err":     err.Error(),
				}).Info("retrying a throughput exceeded exception")
				continue
			}

			// See https://github.com/aws/aws-sdk-go-v2/issues/1825
			if strings.Contains(err.Error(), "use of closed network connection") {
				log.WithFields(log.Fields{
					"table":   t.tableName,
					"segment": segment,
					"attempt": attempt,
					"err":     err.Error(),
				}).Info("retrying a closed network connection error")
				continue
			}

			return nil, fmt.Errorf("scanning table %q segment %d: %w", t.tableName, segment, err)
		}

		return scanned, nil
	}

	return nil, fmt.Errorf("failed to scan segment %d of table %q after %d attempts", segment, t.tableName, maxAttempts)
}

func encodeKey(keyFields []string, av map[string]types.AttributeValue) ([]byte, error) {
	fields := make(map[string]any)
	for k, v := range av {
		fields[k] = v
	}

	return sqlcapture.EncodeRowKey(keyFields, fields, nil, encodeKeyFDB)
}

func decodeKey(keyFields []string, key []byte) (map[string]types.AttributeValue, error) {
	if key == nil {
		return nil, nil
	}

	out := make(map[string]types.AttributeValue)

	tuple, err := sqlcapture.UnpackTuple(key, decodeKeyFDB)
	if err != nil {
		return nil, fmt.Errorf("unpacking tuple: %w", err)
	}

	if len(tuple) != len(keyFields) {
		return nil, fmt.Errorf("key tuple fields count does not match table fields count: %d vs %d", len(tuple), len(keyFields))
	}

	for idx, field := range keyFields {
		switch v := tuple[idx].(type) {
		case types.AttributeValueMemberS:
			out[field] = &v
		case types.AttributeValueMemberN:
			out[field] = &v
		case types.AttributeValueMemberB:
			out[field] = &v
		default:
			return nil, fmt.Errorf("invalid tuple element type %T (%#v)", tuple[idx], tuple[idx])
		}
	}

	return out, nil
}

const numericKey = "N"

func encodeKeyFDB(key, ktype any) (tuple.TupleElement, error) {
	switch av := key.(types.AttributeValue).(type) {
	case *types.AttributeValueMemberS:
		return av.Value, nil
	case *types.AttributeValueMemberN:
		return tuple.Tuple{numericKey, av.Value}, nil
	case *types.AttributeValueMemberB:
		return av.Value, nil
	default:
		return nil, fmt.Errorf("invalid attribute value type %T (%#v)", key, key)
	}
}

func decodeKeyFDB(t tuple.TupleElement) (interface{}, error) {
	switch t := t.(type) {
	case tuple.Tuple:
		if len(t) == 2 && t[0] == numericKey {
			if sv, ok := t[1].(string); ok {
				return types.AttributeValueMemberN{Value: sv}, nil
			}
		}
		return nil, fmt.Errorf("failed to decode fdb tuple %#v", t)
	case string:
		return types.AttributeValueMemberS{Value: t}, nil
	case []byte:
		return types.AttributeValueMemberB{Value: t}, nil
	default:
		return nil, fmt.Errorf("invalid tuple type %T (%#v)", t, t)
	}
}
