package main

import (
	"encoding/json"
	"context"
	"os"
	"fmt"
	"math"

	pm "github.com/estuary/flow/go/protocols/materialize"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type transactor struct {
	bindings []*binding
}

type binding struct {
	collection *mongo.Collection
}

var idField = "_id"

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		var key = string(it.Key.Pack())
		var collection = t.bindings[it.Binding].collection

		var ctx = context.Background()
		var cur, err = collection.Find(ctx, bson.D{{idField, bson.D{{"$eq", key}}}})
		defer cur.Close(ctx)
		if err != nil {
			return fmt.Errorf("finding document in collection: %w", err)
		}
		if !cur.Next(ctx) {
			continue
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
	}

	return it.Err()
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	var ctx = it.Context()

	for it.Next() {
		binding := t.bindings[it.Binding]

		var doc bson.M
		if err := json.Unmarshal(it.RawJSON, &doc); err != nil {
			return nil, fmt.Errorf("bson unmarshalling json doc: %w", err)
		}
		// When we specify the _id field below in the `ReplaceOne` call, the value
		// of _id for the upserted document will be taken from there, so we can
		// safely remove this field from the document here to avoid a conflict.
		delete(doc, idField)

		var opts = options.Replace().SetUpsert(true)
		_, err := binding.collection.ReplaceOne(ctx, bson.D{{idField, bson.D{{"$eq", it.Key.String()}}}}, doc, opts)
		if err != nil {
			return nil, fmt.Errorf("upserting document into collection: %w", err)
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
