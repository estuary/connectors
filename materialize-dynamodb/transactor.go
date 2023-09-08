package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	// This is the maximum allowed by DynamoDB for a BatchGetItem request.
	loadBatchSize = 100

	// This is the maximum allowed by DynamoDB for a BatchWriteItem request.
	storeBatchSize = 25

	// Initial backoff to use if a batch request returns successfully with unprocessed items/keys.
	// Will increase exponentially as additional requests continue to return unprocessed items/keys.
	initialBackoff = 10 * time.Millisecond

	// Maximum amount of time to wait between retry attempts of unprocessed items/keys.
	maxBackoff = 1 * time.Second

	// Maximum number of retry attempts before failing with an error.
	maxAttempts = 30
)

type transactor struct {
	client   *client
	bindings []binding

	// Allows for correlating a table name back into a binding number. This is needed for loads
	// since DynamoDB allows for "get" batches to include different tables, and the results will
	// have the table names for each returned record. This is better than grouping "get" batches by
	// table since load requests are received randomly by binding.
	tablesToBindings map[string]int
}

type binding struct {
	tableName string
	fields    []mappedType
	docField  string
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

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
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

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	ctx := it.Context()

	batches := make(chan map[string][]types.WriteRequest)
	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return t.storeWorker(groupCtx, batches)
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

	for it.Next() {
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
	return nil, group.Wait()
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

			for attempt := 1; ; attempt++ {
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
					break
				}

				// BatchGetItem returns without error if at least one of the requests was successful.
				// The items that the get request was not successful for are returned as UnprocessedKeys,
				// and must be retried. Items are unprocessed if getting them would exceed rate limits
				// for the table.
				batch = res.UnprocessedKeys

				if err := delay(ctx, attempt, "load"); err != nil {
					return err
				}
			}
		}
	}
}

func (t *transactor) storeWorker(ctx context.Context, batches <-chan map[string][]types.WriteRequest) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch := <-batches:
			if batch == nil {
				// Channel is closed and no more items will be sent for this transaction.
				return nil
			}

			for attempt := 1; ; attempt++ {
				res, err := t.client.db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: batch,
				})
				if err != nil {
					return err
				}

				if len(res.UnprocessedItems) == 0 {
					break
				}

				// BatchWriteItem returns without error if at least one of the request items was stored
				// successfully. The remaining items are returned as UnprocessedItems, and must be retried.
				// Items are unprocessed if storing them would exceed rate limits for the table.
				batch = res.UnprocessedItems

				if err := delay(ctx, attempt, "store"); err != nil {
					return err
				}
			}
		}
	}
}

func delay(ctx context.Context, attempt int, key string) error {
	if attempt > maxAttempts {
		return fmt.Errorf("%s worker failed after %d retry attempts", key, maxAttempts)
	}

	d := time.Duration(1<<attempt) * initialBackoff
	if d > maxBackoff {
		d = maxBackoff
	}

	entry := log.WithFields(log.Fields{
		"attempt":  attempt,
		"waitTime": d.String(),
	})
	msg := fmt.Sprintf("%s worker waiting to retry unprocessed items", key)

	if attempt < 15 {
		entry.Debug(msg)
	} else {
		entry.Info(msg)
	}

	// Retry with exponential backoff if there were unprocessed items in the batch.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
