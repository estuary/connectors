package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	idField    = "_id"
	idFieldAlt = "_flow" + idField

	batchByteLimit = 5 * 1024 * 1024

	// MongoDB docs recommend limiting this to "10's" of values, see
	// https://www.mongodb.com/docs/manual/reference/operator/query/in/#-in
	loadBatchSize = 100

	concurrentLoadWorkers = 5

	// The default batchWriteLimit is 100,000 documents. Practically speaking we will be limited to
	// less than that to keep connector memory usage reasonable.
	storeBatchSize = 10_000
)

type transactor struct {
	cfg      *config
	client   *mongo.Client
	bindings []*binding
}

type binding struct {
	collection   *mongo.Collection
	deltaUpdates bool
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (boilerplate.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	ctx := it.Context()
	it.WaitForAcknowledged()

	var mu sync.Mutex
	lockedAndLoaded := func(binding int, doc json.RawMessage) error {
		// Prevent concurrent load workers from interleaving |loaded| responses.
		mu.Lock()
		defer mu.Unlock()
		return loaded(binding, doc)
	}

	sendBatches := make(chan loadBatch)

	group, groupCtx := errgroup.WithContext(ctx)
	for idx := 0; idx < concurrentLoadWorkers; idx++ {
		group.Go(func() error {
			return t.loadWorker(groupCtx, lockedAndLoaded, sendBatches)
		})
	}

	sendBatch := func(binding int, batch []string) error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case sendBatches <- loadBatch{binding: binding, keys: batch}:
			return nil
		}
	}

	batches := make([][]string, len(t.bindings))
	// Approximate the size of the batch in memory based on the length of the packed key as a
	// protection against huge document keys running the connector out of memory.
	batchSizes := make([]int, len(t.bindings))

	for it.Next() {
		key := fmt.Sprintf("%x", it.PackedKey) // Hex-encode

		batches[it.Binding] = append(batches[it.Binding], key)
		batchSizes[it.Binding] += len(it.PackedKey)

		if len(batches[it.Binding]) == loadBatchSize || batchSizes[it.Binding] >= batchByteLimit {
			if err := sendBatch(it.Binding, batches[it.Binding]); err != nil {
				return err
			}
			batches[it.Binding] = nil
			batchSizes[it.Binding] = 0
		}
	}

	// Drain residual batch items.
	for bindingIdx, batch := range batches {
		if len(batch) > 0 {
			if err := sendBatch(bindingIdx, batch); err != nil {
				return err
			}
		}
	}

	close(sendBatches)
	return group.Wait()
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var ctx = it.Context()

	sendBatches := make(chan storeBatch)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return t.storeWorker(groupCtx, sendBatches)
	})

	var batch []mongo.WriteModel
	// Approximate the size of the batch in memory based on the length of the document JSON as a
	// protection against huge documents running the connector out of memory.
	batchSize := 0
	lastBinding := -1

	sendBatch := func() error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case sendBatches <- storeBatch{binding: lastBinding, models: batch}:
			batch = nil
			batchSize = 0
			return nil
		}
	}

	for it.Next() {
		if lastBinding != -1 && (lastBinding != it.Binding || len(batch) == storeBatchSize || batchSize >= batchByteLimit) {
			if err := sendBatch(); err != nil {
				return nil, err
			}
		}

		key := fmt.Sprintf("%x", it.PackedKey) // Hex-encode

		if it.Delete && t.cfg.HardDelete {
			if it.Exists {
				del := &mongo.DeleteOneModel{Filter: map[string]string{idField: key}}
				batch = append(batch, del)
				batchSize += len(key)
			} else {
				// Ignore items which do not exist and are already deleted
				continue
			}
		} else {
			var doc bson.M
			if err := json.Unmarshal(it.RawJSON, &doc); err != nil {
				return nil, fmt.Errorf("bson unmarshalling json doc: %w", err)
			}
			if idVal, ok := doc[idField]; ok {
				// Preserve the original value of a collection field with a name
				// that collides with the MongoDB _id field by materializing it
				// with an alternate name.
				doc[idFieldAlt] = idVal
				delete(doc, idField)
			}

			// In case of delta updates, we don't want to set the _id. We want MongoDB to generate a new
			// _id for each record we insert
			if !t.bindings[it.Binding].deltaUpdates {
				doc[idField] = key
			}

			var m mongo.WriteModel
			if it.Exists {
				m = &mongo.ReplaceOneModel{
					Filter:      bson.D{{Key: idField, Value: bson.D{{Key: "$eq", Value: key}}}},
					Replacement: doc,
				}
			} else {
				m = &mongo.InsertOneModel{Document: doc}
			}

			batch = append(batch, m)
			batchSize += len(it.RawJSON)
		}

		lastBinding = it.Binding
	}

	// Drain the last batch.
	if len(batch) > 0 {
		if err := sendBatch(); err != nil {
			return nil, err
		}
	}

	close(sendBatches)
	return nil, group.Wait()
}

func (t *transactor) Destroy() {}

type loadBatch struct {
	binding int
	keys    []string
}

func (t *transactor) loadWorker(
	ctx context.Context,
	loaded func(i int, doc json.RawMessage) error,
	batches <-chan loadBatch,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-batches:
			if !ok {
				return nil
			}

			if err := func() error { // Closure for the deferred cur.Close()
				collection := t.bindings[batch.binding].collection
				cur, err := collection.Find(ctx, bson.D{{
					Key:   idField,
					Value: bson.D{{Key: "$in", Value: batch.keys}},
				}})
				if err != nil {
					return fmt.Errorf("finding document in collection: %w", err)
				}
				defer cur.Close(ctx)

				for cur.Next(ctx) {
					var doc bson.M
					if err = cur.Decode(&doc); err != nil {
						return fmt.Errorf("decoding document in collection %s: %w", collection.Name(), err)
					}

					js, err := json.Marshal(sanitizedLoadedDocument(doc))
					if err != nil {
						return fmt.Errorf("encoding document in collection %s as json: %w", collection.Name(), err)
					} else if err := loaded(batch.binding, js); err != nil {
						return fmt.Errorf("sending loaded: %w", err)
					}
				}

				return nil
			}(); err != nil {
				return err
			}
		}
	}
}

type storeBatch struct {
	binding int
	models  []mongo.WriteModel
}

func (t *transactor) storeWorker(
	ctx context.Context,
	batches <-chan storeBatch,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-batches:
			if !ok {
				return nil
			}

			var expectModified, expectInserted, expectDeleted int64
			for _, m := range batch.models {
				switch m.(type) {
				case *mongo.ReplaceOneModel:
					expectModified++
				case *mongo.InsertOneModel:
					expectInserted++
				case *mongo.DeleteOneModel:
					expectDeleted++
				default:
					return fmt.Errorf("invalid model type: %T", m)
				}
			}

			collection := t.bindings[batch.binding].collection

			// Ordered operations are not needed since each key can only be seen a single time in a
			// Flow transaction. Turning this off supposedly allows for optimizations by the server.
			res, err := collection.BulkWrite(ctx, batch.models, options.BulkWrite().SetOrdered(false))
			if err != nil {
				return fmt.Errorf("bulk write for collection %s: %w", collection.Name(), err)
			}

			// Sanity check the result of the bulk write operation. For updated documents, we check
			// MatchedCount instead of ModifiedCount since certain Flow reduction strategies can
			// result in identical documents being stored, and MongoDB does not report an attempted
			// "replace" with an identical document as an update when it's part of a bulk write
			// operation.
			if res.MatchedCount != expectModified || res.InsertedCount != expectInserted || res.DeletedCount != expectDeleted {
				logrus.WithFields(logrus.Fields{
					"deleted":        res.DeletedCount,
					"inserted":       res.InsertedCount,
					"matched":        res.MatchedCount,
					"modified":       res.ModifiedCount,
					"upserted":       res.UpsertedCount,
					"expectModified": expectModified,
					"expectInserted": expectInserted,
					"expectDeleted":  expectDeleted,
				}).Warn("bulk write counts")

				return fmt.Errorf(
					"unexpected bulkWrite counts for MongoDB collection %s.%s: got %d matched vs %d expected, %d inserted vs %d expected, %d deleted vs %d expected",
					collection.Database().Name(),
					collection.Name(),
					res.MatchedCount,
					expectModified,
					res.InsertedCount,
					expectInserted,
					res.DeletedCount,
					expectDeleted,
				)
			}
		}
	}
}

func sanitizedLoadedDocument(doc map[string]interface{}) map[string]interface{} {
	if idValAlt, ok := doc[idFieldAlt]; ok {
		// Reverse the renaming of a collection's _id field to _flow_id by
		// putting the original value back as _id and removing the alternate
		// field.
		doc[idField] = idValAlt
		delete(doc, idFieldAlt)
	} else {
		// Otherwise we need to remove the _id property from loaded documents
		// because the Flow collection schemas may forbid that property. If we
		// left it in, it could cause validation errors on loaded documents.
		delete(doc, idField)
	}

	return sanitizeDocumentInner(doc)
}

func sanitizeDocumentInner(doc map[string]interface{}) map[string]interface{} {
	for key, value := range doc {
		switch v := value.(type) {
		case float64:
			if math.IsNaN(v) {
				doc[key] = "NaN"
			}
		case map[string]interface{}:
			doc[key] = sanitizeDocumentInner(v)
		}
	}

	return doc
}
