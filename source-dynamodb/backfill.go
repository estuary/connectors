package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
		startKey := state.ExclusiveStartKey.inner

		scanned, err := c.doScan(ctx, t, scanLimit, limiter, rcusPerRequest, segment, startKey)
		if err != nil {
			return fmt.Errorf("backfill worker doScan: %w", err)
		}

		newState := segmentState{}
		if scanned.LastEvaluatedKey == nil {
			newState.FinishedAt = time.Now()
			// Retain the most recent start key rather than setting it to nil, mostly for the
			// benefit of test snapshots.
			newState.ExclusiveStartKey = state.ExclusiveStartKey
		} else {
			newState.ExclusiveStartKey = keyAttributeWrapper{
				inner: scanned.LastEvaluatedKey,
			}
		}

		if err := c.emitBackfill(t.bindingIdx, t.tableName, segment, newState, scanned.Items, t.keyFields); err != nil {
			return fmt.Errorf("emitting backfill documents for segment %d: %w", segment, err)
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

// keyAttributeWrapper makes it possible to serialize a DynamoDB attribute value as JSON for
// checkpointing, and deserialize that checkpoint JSON back into a DynamoDB attribute value. The
// primary reason this is so weird is because of the possibility for binary type keys: The AWS SDK
// handles these as []byte, but JSON serialization encodes them as base64 strings. So we need a way
// to tell the difference between a regular string value and a string value that actually represents
// binary data as base64. The attribute type and its value (always a string) is encoded/decoded
// using attributeJson. DynamoDB keys can only be strings, numbers, or binary, so we handle them all
// with this wrapper.
type keyAttributeWrapper struct {
	inner map[string]types.AttributeValue
}

type attributeJson struct {
	AttrType types.ScalarAttributeType `json:"attrType"`
	Value    string                    `json:"value"`
}

func (w keyAttributeWrapper) MarshalJSON() ([]byte, error) {
	out := make(map[string]attributeJson)

	for name, val := range w.inner {
		switch av := val.(type) {
		case *types.AttributeValueMemberS:
			out[name] = attributeJson{
				AttrType: types.ScalarAttributeTypeS,
				Value:    av.Value,
			}
		case *types.AttributeValueMemberN:
			out[name] = attributeJson{
				AttrType: types.ScalarAttributeTypeN,
				Value:    av.Value,
			}
		case *types.AttributeValueMemberB:
			out[name] = attributeJson{
				AttrType: types.ScalarAttributeTypeB,
				// Standard JSON encoding would do this conversion anyway, but doing it here allows
				// for a consistent type of string for `attributeJson.Value`.
				Value: base64.StdEncoding.EncodeToString(av.Value),
			}
		default:
			return nil, fmt.Errorf("invalid attribute value type for %s: %T", name, val)

		}
	}

	return json.Marshal(out)
}

func (w *keyAttributeWrapper) UnmarshalJSON(data []byte) error {
	v := make(map[string]attributeJson)
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	w.inner = make(map[string]types.AttributeValue)

	for name, val := range v {
		switch val.AttrType {
		case types.ScalarAttributeTypeS:
			w.inner[name] = &types.AttributeValueMemberS{
				Value: val.Value,
			}
		case types.ScalarAttributeTypeN:
			w.inner[name] = &types.AttributeValueMemberN{
				Value: val.Value,
			}
		case types.ScalarAttributeTypeB:
			// Get the bytes back out of the base64 string.
			bytes, err := base64.StdEncoding.DecodeString(val.Value)
			if err != nil {
				return fmt.Errorf("value for %s could not be base64 decoded", name)
			}

			w.inner[name] = &types.AttributeValueMemberB{
				Value: bytes,
			}
		default:
			return fmt.Errorf("invalid attribute value type for %s: %s", name, val.AttrType)
		}
	}

	return nil
}
