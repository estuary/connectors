package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	pm "github.com/estuary/flow/go/protocols/materialize"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type transactor struct {
	client   *mongo.Client
	bindings []*binding
}

type binding struct {
	collection *mongo.Collection
	res        resource
}

var idField = "_id"

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	it.WaitForAcknowledged()

	for it.Next() {
		var key = fmt.Sprintf("%x", it.PackedKey) // Hex-encode.
		var collection = t.bindings[it.Binding].collection

		var ctx = context.Background()
		if err := func() error { // Closure for the deferred cur.Close()
			var cur, err = collection.Find(ctx, bson.D{{Key: idField, Value: bson.D{{Key: "$eq", Value: key}}}})
			if err != nil {
				return fmt.Errorf("finding document in collection: %w", err)
			}
			defer cur.Close(ctx)
			if !cur.Next(ctx) {
				return nil
			}

			var doc bson.M

			if err = cur.Decode(&doc); err != nil {
				return fmt.Errorf("decoding document in collection %s: %w", collection.Name(), err)
			}
			doc = sanitizeDocument(doc)

			js, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("encoding document in collection %s as json: %w", collection.Name(), err)
			}

			if err := loaded(it.Binding, js); err != nil {
				return fmt.Errorf("sending loaded: %w", err)
			}

			return nil
		}(); err != nil {
			return err
		}
	}

	return it.Err()
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	var ctx = it.Context()

	for it.Next() {
		var key = fmt.Sprintf("%x", it.PackedKey) // Hex-encode.
		binding := t.bindings[it.Binding]

		var doc bson.M
		if err := json.Unmarshal(it.RawJSON, &doc); err != nil {
			return nil, fmt.Errorf("bson unmarshalling json doc: %w", err)
		}
		// In case of delta updates, we don't want to set the _id. We want
		// MongoDB to generate a new _id for each record we insert
		if !binding.res.DeltaUpdates {
			doc[idField] = key
		}

		if it.Exists {
			var opts = options.Replace()
			_, err := binding.collection.ReplaceOne(ctx, bson.D{{Key: idField, Value: bson.D{{Key: "$eq", Value: key}}}}, doc, opts)
			if err != nil {
				return nil, fmt.Errorf("upserting document into collection %s: %w", binding.collection.Name(), err)
			}
		} else {
			_, err := binding.collection.InsertOne(ctx, doc, options.InsertOne())
			if err != nil {
				return nil, fmt.Errorf("inserting document into collection %s: %w", binding.collection.Name(), err)
			}
		}
	}

	return nil, nil
}

func (t *transactor) Destroy() {
}

func sanitizeDocument(doc map[string]interface{}) map[string]interface{} {
	for key, value := range doc {
		switch v := value.(type) {
		case float64:
			if math.IsNaN(v) {
				doc[key] = "NaN"
			}
		case map[string]interface{}:
			doc[key] = sanitizeDocument(v)
		}
	}

	return doc
}
