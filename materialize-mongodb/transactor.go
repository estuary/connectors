package main

import (
	"encoding/json"
	"context"
	"fmt"
	"math"

	pm "github.com/estuary/flow/go/protocols/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type transactor struct {
	materialization string
	fenceCollection *mongo.Collection
	fence *fenceRecord

	client *mongo.Client
	bindings []*binding
}

type binding struct {
	collection *mongo.Collection
	res resource
}

type fenceRecord struct {
	Materialization string `bson:"materialization"`
	Fence int `bson:"fence"`
}

var idField = "_id"
var fenceCollectionName = "flow_checkpoints"


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

	sess, err := t.client.StartSession(options.Session())
	if err != nil {
		return nil, fmt.Errorf("starting session: %w", err)
	}

	tx := mongo.NewSessionContext(ctx, sess)

	if err = sess.StartTransaction(); err != nil {
		return nil, fmt.Errorf("starting transaction: %w", err)
	}

	for it.Next() {
		binding := t.bindings[it.Binding]

		var doc bson.M
		if err := json.Unmarshal(it.RawJSON, &doc); err != nil {
			return nil, fmt.Errorf("bson unmarshalling json doc: %w", err)
		}

		if binding.res.DeltaUpdates == true {
			_, err := binding.collection.InsertOne(tx, doc, options.InsertOne())
			if err != nil {
				return nil, fmt.Errorf("inserting document into collection %s: %w", binding.collection.Name(), err)
			}
		} else {
			// When we specify the _id field below in the `ReplaceOne` call, the value
			// of _id for the upserted document will be taken from there, so we can
			// safely remove this field from the document here to avoid a conflict.
			delete(doc, idField)

			var opts = options.Replace().SetUpsert(true)
			_, err := binding.collection.ReplaceOne(tx, bson.D{{idField, bson.D{{"$eq", it.Key.String()}}}}, doc, opts)
			if err != nil {
				return nil, fmt.Errorf("upserting document into collection %s: %w", binding.collection.Name(), err)
			}
		}
	}

	var filter = bson.D{{"materialization", bson.D{{"$eq", t.fence.Materialization}}}, {"fence", bson.D{{"$eq", t.fence.Fence}}}}
	var fence fenceRecord
	if err = t.fenceCollection.FindOne(tx, filter, options.FindOne()).Decode(&fence); err != nil {
		return nil, fmt.Errorf("finding fence: %w", err)
	}

	return func(ctx context.Context, runtimeCheckpoint []byte, runtimeAckCh <-chan struct{}) (*pf.DriverCheckpoint, pf.OpFuture) {
		return nil, pf.RunAsyncOperation(func() error {
			defer sess.EndSession(ctx)
			return sess.CommitTransaction(context.Background())
		})
	}, nil


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
