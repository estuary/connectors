package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	// This is the maximum allowed by DynamoDB for a BatchGetItem request.
	loadBatchSize = 100

	// This is the maximum allowed by DynamoDB for a BatchWriteItem request.
	storeBatchSize = 25
)

type transactor struct {
	client   *client
	bindings []binding
	be       *m.BindingEvents

	// Allows for correlating a table name back into a binding number. This is needed for loads
	// since DynamoDB allows for "get" batches to include different tables, and the results will
	// have the table names for each returned record. This is better than grouping "get" batches by
	// table since load requests are received randomly by binding.
	tablesToBindings map[string]int

	// truncateAfterBackfill permits deleting the items published before a
	// completed backfill.
	truncateAfterBackfill bool
	// truncations maps a binding index to the boundary of a completed
	// backfill, for deletion after this transaction's stores.
	truncations map[int]time.Time
}

type binding struct {
	tableName    string
	path         []string
	fields       []mappedType
	docField     string
	deltaUpdates bool
	// keyFields holds the DynamoDB key schema's attribute names, in schema
	// order (partition key, then an optional sort key), used to build
	// DeleteRequest keys when truncating stale items.
	keyFields []string
	// publishedAtField is the attribute name of the selected field that
	// projects /_meta/uuid as a date-time, used to find items published
	// before a completed backfill. Empty if the binding excludes that
	// field, in which case publishedAtErr explains why.
	publishedAtField string
	publishedAtErr   error
}

func (b binding) convertKey(ts tuple.Tuple) (map[string]types.AttributeValue, error) {
	return b.convert(ts, nil)
}

func (b binding) convert(ts tuple.Tuple, doc json.RawMessage) (map[string]types.AttributeValue, error) {
	vals := make(map[string]any)

	fieldsIdx := 0
	do := func(v tuple.TupleElement) error {
		f := b.fields[fieldsIdx]
		c, err := f.converter(v)
		if err != nil {
			return fmt.Errorf("converting field %s: %w", f.field, err)
		}

		vals[f.field] = c
		fieldsIdx++

		return nil
	}

	for _, t := range ts {
		if err := do(t); err != nil {
			return nil, err
		}
	}

	if b.docField != "" && doc != nil {
		if err := do(doc); err != nil {
			return nil, err
		}
	}

	attrs, err := attributevalue.MarshalMap(vals)
	if err != nil {
		return nil, fmt.Errorf("marshalMap: %w", err)
	}

	return attrs, nil
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error { return nil }

func (t *transactor) Flush(_ context.Context, completes map[int]time.Time) error {
	t.truncations = make(map[int]time.Time, len(completes))
	for binding, boundary := range completes {
		b := t.bindings[binding]
		if b.deltaUpdates {
			continue
		} else if !t.truncateAfterBackfill {
			log.WithFields(log.Fields{
				"table":    b.path,
				"boundary": boundary,
			}).Info("items published before the backfill were not deleted because feature flags disable it")
			continue
		} else if b.publishedAtField == "" {
			t.be.Warn(b.tableName, fmt.Sprintf("Items published before the backfill of table %s were not deleted because %s.", b.tableName, b.publishedAtErr))
			continue
		}
		t.truncations[binding] = boundary
	}
	return nil
}

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	ctx := it.Context()

	// Processing load requests as the arrive, so make sure the previous transaction has fully
	// committed before evaluating any loads.
	it.WaitForAcknowledged()

	batches := make(chan map[string]types.KeysAndAttributes)
	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return t.loadWorker(groupCtx, loaded, batches)
	})

	batch := make(map[string]types.KeysAndAttributes)
	batchSize := 0

	sendBatch := func(b map[string]types.KeysAndAttributes) error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case batches <- b:
			return nil
		}
	}

	for it.Next() {
		b := t.bindings[it.Binding]

		key, err := b.convertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting key for table '%s': %w", b.tableName, err)
		}

		if _, ok := batch[b.tableName]; !ok {
			batch[b.tableName] = types.KeysAndAttributes{
				ConsistentRead: aws.Bool(true),
				// ProjectionExpression is for handling strange cases where the root document field
				// has been projected as a field that clashes with a reserved word, or contains a
				// dot. See:
				// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html#Expressions.ExpressionAttributeNames.AttributeNamesContainingSpecialCharacters
				ProjectionExpression:     aws.String("#flow_document"),
				ExpressionAttributeNames: map[string]string{"#flow_document": b.docField},
			}
		}

		keyAndAttrs := batch[b.tableName]
		keyAndAttrs.Keys = append(keyAndAttrs.Keys, key)
		batch[b.tableName] = keyAndAttrs
		batchSize++

		if batchSize == loadBatchSize {
			if err := sendBatch(batch); err != nil {
				return err
			}
			batch = make(map[string]types.KeysAndAttributes)
			batchSize = 0
		}
	}

	if batchSize > 0 {
		if err := sendBatch(batch); err != nil {
			return err
		}
	}

	close(batches)
	return group.Wait()
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	batches := make(chan map[string][]types.WriteRequest)
	group, groupCtx := errgroup.WithContext(ctx)
	round := it.Round
	// stored counts the items DynamoDB accepted per binding, for the
	// transaction health report.
	stored := make([]atomic.Int64, len(t.bindings))

	group.Go(func() error {
		return t.storeWorker(groupCtx, batches, stored)
	})

	batch := make(map[string][]types.WriteRequest)
	batchSize := 0

	sendBatch := func(b map[string][]types.WriteRequest) error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case batches <- b:
			return nil
		}
	}

	for it.Next(false) {
		b := t.bindings[it.Binding]

		item, err := b.convert(append(it.Key, it.Values...), it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting values for table '%s': %w", b.tableName, err)
		}

		batch[b.tableName] = append(batch[b.tableName], types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
		batchSize++

		if batchSize == storeBatchSize {
			if err := sendBatch(batch); err != nil {
				return nil, err
			}
			batch = make(map[string][]types.WriteRequest)
			batchSize = 0
		}
	}

	if batchSize > 0 {
		if err := sendBatch(batch); err != nil {
			return nil, err
		}
	}

	close(batches)
	if err := group.Wait(); err != nil {
		return nil, err
	}
	for i, b := range t.bindings {
		if n := stored[i].Load(); n > 0 {
			t.be.ReportRowStats(round, b.path, m.TotalRowStats(n))
		}
	}

	// Truncations run after the stores, so the scan sees each item's
	// post-store timestamp and doesn't need to delete rows this transaction
	// just refreshed.
	for binding, boundary := range t.truncations {
		b := t.bindings[binding]
		deleted, err := truncateStaleItems(ctx, t.client, b, boundary)
		if err != nil {
			return nil, fmt.Errorf("truncating table %q after backfill: %w", b.tableName, err)
		}
		log.WithFields(log.Fields{
			"table":    b.path,
			"boundary": boundary,
			"deleted":  deleted,
		}).Info("truncated items published before the backfill")
		t.be.ReportRowStats(round, b.path, m.ExactRowStats(0, 0, 0).WithTruncated(deleted))
	}
	clear(t.truncations)

	return nil, nil
}

func (t *transactor) Destroy() {}

func (t *transactor) loadWorker(ctx context.Context, loaded func(i int, doc json.RawMessage) error, batches <-chan map[string]types.KeysAndAttributes) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch := <-batches:
			if batch == nil {
				// Channel is closed and no more items will be sent for this transaction.
				return nil
			}

			if err := unprocessedRetry.Do(ctx, "BatchGetItem", func(ctx context.Context) error {
				res, err := t.client.db.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
					RequestItems: batch,
				})
				if err != nil {
					return fmt.Errorf("load worker BatchGetItem: %w", err)
				}

				for table, records := range res.Responses {
					b := t.tablesToBindings[table]

					for _, record := range records {
						doc := make(map[string]any)
						if err := attributevalue.UnmarshalMap(record, &doc); err != nil {
							return fmt.Errorf("unmarshalling attributevalue: %w", err)
						}

						// Sanity check: There should be only one field in the response per the
						// ProjectionExpression.
						if len(doc) != 1 {
							return fmt.Errorf("expected 1 field in load response but got %d fields", len(record))
						}

						for _, d := range doc {
							j, err := json.Marshal(d)
							if err != nil {
								return fmt.Errorf("marshalling document to JSON bytes: %w", err)
							}

							if err := loaded(b, json.RawMessage(j)); err != nil {
								return err
							}
						}
					}
				}

				if len(res.UnprocessedKeys) == 0 {
					return nil
				}
				batch = res.UnprocessedKeys

				var count int
				for _, ka := range batch {
					count += len(ka.Keys)
				}
				return &unprocessedError{operation: "BatchGetItem", count: count}
			}); err != nil {
				return err
			}
		}
	}
}

func (t *transactor) storeWorker(ctx context.Context, batches <-chan map[string][]types.WriteRequest, stored []atomic.Int64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch := <-batches:
			if batch == nil {
				// Channel is closed and no more items will be sent for this transaction.
				return nil
			}

			if err := unprocessedRetry.Do(ctx, "BatchWriteItem", func(ctx context.Context) error {
				res, err := t.client.db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: batch,
				})
				if err != nil {
					log.WithField("batch", batch).Error("failed batch write")
					return err
				}

				// Everything not handed back as unprocessed was written.
				for table, reqs := range batch {
					stored[t.tablesToBindings[table]].Add(int64(len(reqs) - len(res.UnprocessedItems[table])))
				}

				if len(res.UnprocessedItems) == 0 {
					return nil
				}
				batch = res.UnprocessedItems

				var count int
				for _, reqs := range batch {
					count += len(reqs)
				}
				return &unprocessedError{operation: "BatchWriteItem", count: count}
			}); err != nil {
				return err
			}
		}
	}
}

// truncateStaleItems deletes items from the table whose selected
// publication-time field (see publishedAtField) predates boundary. An item
// with no value for that field at all is left alone. It scans the whole table
// because DynamoDB has no secondary index on the publication time by default.
func truncateStaleItems(ctx context.Context, client *client, b binding, boundary time.Time) (int64, error) {
	exprNames := make(map[string]string, len(b.keyFields)+1)
	projExpr := make([]string, len(b.keyFields), len(b.keyFields)+1)
	for i, f := range b.keyFields {
		ph := fmt.Sprintf("#k%d", i)
		exprNames[ph] = f
		projExpr[i] = ph
	}
	exprNames["#pub"] = b.publishedAtField
	projExpr = append(projExpr, "#pub")

	exprValues := map[string]types.AttributeValue{
		":boundary": &types.AttributeValueMemberS{Value: filterBoundary(boundary)},
	}

	// A single deleteWorker processes one batch of DeleteRequests at a
	// time, so at most one is ever in flight, the same way storeWorker
	// limits stores to one in-flight batch. Scanning ahead for the next
	// page overlaps with it rather than waiting for it to finish first.
	batches := make(chan []types.WriteRequest)
	group, groupCtx := errgroup.WithContext(ctx)
	var deleted atomic.Int64

	group.Go(func() error {
		return deleteWorker(groupCtx, client, b.tableName, batches, &deleted)
	})

	sendBatch := func(reqs []types.WriteRequest) error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case batches <- reqs:
			return nil
		}
	}

	var exclusiveStartKey map[string]types.AttributeValue
	for {
		out, err := client.db.Scan(groupCtx, &dynamodb.ScanInput{
			TableName:                 aws.String(b.tableName),
			ConsistentRead:            aws.Bool(true),
			ProjectionExpression:      aws.String(strings.Join(projExpr, ", ")),
			FilterExpression:          aws.String("#pub < :boundary"),
			ExpressionAttributeNames:  exprNames,
			ExpressionAttributeValues: exprValues,
			ExclusiveStartKey:         exclusiveStartKey,
		})
		if err != nil {
			return deleted.Load(), fmt.Errorf("scanning table %q: %w", b.tableName, err)
		}

		var staleKeys []map[string]types.AttributeValue
		for _, item := range out.Items {
			if !b.isStale(item, boundary) {
				continue
			}
			key := make(map[string]types.AttributeValue, len(b.keyFields))
			for _, f := range b.keyFields {
				key[f] = item[f]
			}
			staleKeys = append(staleKeys, key)
		}

		for i := 0; i < len(staleKeys); i += storeBatchSize {
			end := min(i+storeBatchSize, len(staleKeys))

			reqs := make([]types.WriteRequest, 0, end-i)
			for _, key := range staleKeys[i:end] {
				reqs = append(reqs, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: key}})
			}

			if err := sendBatch(reqs); err != nil {
				return deleted.Load(), err
			}
		}

		if out.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = out.LastEvaluatedKey
	}

	close(batches)
	if err := group.Wait(); err != nil {
		return deleted.Load(), err
	}

	return deleted.Load(), nil
}

// deleteWorker issues one BatchWriteItem of DeleteRequests at a time, the
// same way storeWorker issues PutRequests, so truncateStaleItems never has
// more than one delete in flight.
func deleteWorker(ctx context.Context, client *client, tableName string, batches <-chan []types.WriteRequest, deleted *atomic.Int64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case reqs := <-batches:
			if reqs == nil {
				// Channel is closed and no more batches will be sent.
				return nil
			}

			n, err := deleteBatchWithRetry(ctx, client, tableName, reqs)
			deleted.Add(n)
			if err != nil {
				return err
			}
		}
	}
}

// filterBoundary is the boundary time rounded to the next second.
func filterBoundary(boundary time.Time) string {
	ceil := boundary.Truncate(time.Second)
	if !ceil.Equal(boundary) {
		ceil = ceil.Add(time.Second)
	}
	return ceil.UTC().Format(time.RFC3339)
}

// isStale reports whether item's publication time predates boundary. An
// item with no value for the field at all is left alone: it was written
// before the field was selected, or before this feature existed, and
// there's no publication time to compare. An item whose value can't be
// read as the expected date-time string is also left alone, rather than
// risk deleting live data.
func (b binding) isStale(item map[string]types.AttributeValue, boundary time.Time) bool {
	av, ok := item[b.publishedAtField]
	if !ok {
		return false
	}
	s, ok := av.(*types.AttributeValueMemberS)
	if !ok {
		log.WithField("table", b.tableName).Warn("skipping truncation of an item whose published-at field is not a string")
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, s.Value)
	if err != nil {
		log.WithFields(log.Fields{"table": b.tableName, "value": s.Value}).WithError(err).
			Warn("skipping truncation of an item whose published-at field could not be parsed")
		return false
	}
	return parsed.Before(boundary)
}

// deleteBatchWithRetry issues a BatchWriteItem of DeleteRequests for a single
// table and retries any UnprocessedItems, the same way storeWorker retries
// unprocessed PutRequests.
func deleteBatchWithRetry(ctx context.Context, client *client, tableName string, reqs []types.WriteRequest) (int64, error) {
	var deleted int64
	batch := map[string][]types.WriteRequest{tableName: reqs}

	err := unprocessedRetry.Do(ctx, "BatchWriteItem (truncate)", func(ctx context.Context) error {
		res, err := client.db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: batch,
		})
		if err != nil {
			return fmt.Errorf("BatchWriteItem for truncation: %w", err)
		}

		deleted += int64(len(batch[tableName]) - len(res.UnprocessedItems[tableName]))

		if len(res.UnprocessedItems) == 0 {
			return nil
		}
		batch = res.UnprocessedItems
		return &unprocessedError{operation: "BatchWriteItem", count: len(batch[tableName])}
	})

	return deleted, err
}
