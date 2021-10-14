package mattest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/estuary/protocols/catalog"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type BasicData struct {
	Key1    int     `json:"key1" yaml:"key1,required"`
	Key2    bool    `json:"key2" yaml:"key2,required"`
	Boolean bool    `json:"boolean" yaml:"boolean"`
	Integer int     `json:"integer" yaml:"integer"`
	Number  float32 `json:"number" yaml:"number"`
	String  string  `json:"string" yaml:"string"`
	// Diffrent string  `json:"different" yaml:"different"`
}

func (d *BasicData) Keys() tuple.Tuple {
	return tuple.Tuple{d.Key1, d.Key2}
}

func (d *BasicData) SetKeys(keys tuple.Tuple) {
	d.Key1 = keys[0].(int)
	d.Key2 = keys[1].(bool)
}

func (d *BasicData) Values() tuple.Tuple {
	return tuple.Tuple{d.Boolean, d.Integer, d.Number, d.String}
	// return tuple.Tuple{d.Boolean, d.Integer, d.Number, d.String, d.Diffrent}
}

func (d *BasicData) JSON() []byte {
	b, _ := json.Marshal(d)
	return b
}

type BasicTester struct {
	image    string
	config   ConfigMap
	resource ConfigMap
}

func NewBasicTester(image string, config ConfigMap, resource ConfigMap) MaterializeTester {
	return &BasicTester{
		image:    image,
		config:   config,
		resource: resource,
	}
}

func (t *BasicTester) GenerateTestCatalog() (*TestCatalog, error) {
	return &TestCatalog{
		Collections: map[string]Collection{
			"coltest": {
				Schema: BuildSchema(BasicData{}),
				Keys:   []string{"/key1", "/key2"},
			},
		},
		Materializations: map[string]Materialization{
			"mattest": {
				Endpoint: EndpointFlowSync{
					Image:  t.image,
					Config: t.config,
				},
				Bindings: []Binding{
					{
						Source:   "coltest",
						Resource: t.resource,
					},
				},
			},
		},
	}, nil
}

func (t *BasicTester) RunTest(ctx context.Context, catalog *catalog.BuiltCatalog, client pm.DriverClient) error {

	// Perform the setup and open the transactions stream
	stream, err := SetupSingleMaterializationTransactions(ctx, catalog, client)
	if err != nil {
		return err
	}

	materialization := catalog.Materializations[0]

	// Send the Open request.
	if err := stream.Send(&pm.TransactionRequest{
		Open: &pm.TransactionRequest_Open{
			Materialization: &materialization,
			Version:         "1.0",
			KeyBegin:        0,
			KeyEnd:          0xFFFF,
		},
	}); err != nil {
		return fmt.Errorf("open error: %v", err)
	}

	// Received the Opened response
	opened, err := stream.Recv()
	if err != nil || opened.Opened == nil {
		return fmt.Errorf("opened error: %v", err)
	}
	log.WithFields(log.Fields{"checkpoint": string(opened.Opened.FlowCheckpoint)}).Info("connection opened")

	var commitWait sync.WaitGroup
	var store = NewTestStore()

	var returnToStore = make(chan *TestStore, 1)
	returnToStore <- NewTestStore()

	rand.Seed(time.Now().UnixNano())

	// Perform 10 load/prepare/store/commit loops.
	for i := 0; i < 10; i++ {

		// Build a cache of documents to load and then store, some of them will exist and others won't.
		var cache = NewTestStore()
		var exists []bool

		// Send up to 3 load requests
		for j := rand.Intn(3); j >= 0; j-- {
			// Load up to 10 documents. Pick at random from our store or fake data.
			for k := rand.Intn(10); k >= 0; k-- {
				// If nothing is in the store or at random pick a random ID to load.
				if store.Len() == 0 || rand.Intn(2) == 0 {
					var data BasicData
					gofakeit.Struct(&data)
					cache.Push(&data)
					exists = append(exists, false)
				} else {
					// Pop an element from the store to the cache to load on.
					cache.Push(store.Pop())
					exists = append(exists, true)
				}
			}

			// Generate and send the Load request.
			var packedKeys [][]byte
			for _, data := range cache.Range() {
				packedKeys = append(packedKeys, data.Keys().Pack())
			}
			var arena pf.Arena
			var arenaPackedKeys = arena.AddAll(packedKeys...)
			if err := stream.Send(&pm.TransactionRequest{
				Load: &pm.TransactionRequest_Load{
					Arena:      arena,
					PackedKeys: arenaPackedKeys,
				},
			}); err != nil {
				return fmt.Errorf("load: %v", err)
			}
			log.Infof("loaded request with %d keys", cache.Len())
		}

		// Wait until previous commit has completed.
		commitWait.Wait()

		// Previously committed data is now available to put into the store again.
		store.Push((<-returnToStore).Range()...)

		// Write the Prepared request with the checkpoint information.
		var checkpoint = fmt.Sprintf("checkpoint loop %d", i)
		if err := stream.Send(&pm.TransactionRequest{
			Prepare: &pm.TransactionRequest_Prepare{
				FlowCheckpoint: []byte(checkpoint),
			},
		}); err != nil {
			return fmt.Errorf("prepare: %v", err)
		}
		log.Infof("prepare request with checkpoint: %s", checkpoint)

		// Loop through loaded requests until we are prepared.
		var next *pm.TransactionResponse
		for {
			next, err = stream.Recv()
			if err != nil {
				return fmt.Errorf("loaded/prepared: %v", err)
			}
			if next.Loaded != nil {
				log.Infof("loaded response with %d documents", len(next.Loaded.DocsJson))
				// Compare the loaded item with the item from the cache to ensure they match.
				for _, arena := range next.Loaded.DocsJson {
					var item BasicData
					if err = json.Unmarshal(next.Loaded.Arena[arena.Begin:arena.End], &item); err != nil {
						return fmt.Errorf("unmarshaling loaded doc: %v", err)
					}
					cacheItem := cache.Find(item.Keys(), false)
					if cacheItem == nil {
						return fmt.Errorf("unable to find loaded doc in cache")
					}
					if *cacheItem.(*BasicData) != item {
						return fmt.Errorf("loaded document does not match cache")
					}
				}
			} else if next.Prepared != nil {
				log.Infof("prepared response with checkpoint: %s", string(next.Prepared.DriverCheckpointJson))
				break
			} else {
				return fmt.Errorf("unexpected message: %#v", next)
			}
		}

		// Send the Store requests with our data.
		var docs [][]byte
		var keys [][]byte
		var values [][]byte
		for _, data := range cache.Range() {
			// Randomize the data, but keep the keys the same
			dataKeys := data.Keys()
			gofakeit.Struct(&data)
			data.SetKeys(dataKeys)
			docs = append(docs, data.JSON())
			keys = append(keys, data.Keys().Pack())
			values = append(values, data.Values().Pack())
		}
		var storeRequest = pm.TransactionRequest_Store{}
		storeRequest.DocsJson = storeRequest.Arena.AddAll(docs...)
		storeRequest.PackedKeys = storeRequest.Arena.AddAll(keys...)
		storeRequest.PackedValues = storeRequest.Arena.AddAll(values...)
		storeRequest.Exists = exists
		if err = stream.Send(&pm.TransactionRequest{
			Store: &storeRequest,
		}); err != nil {
			return fmt.Errorf("store error: %v", err)
		}

		// Send the Commit request.
		if err = stream.Send(&pm.TransactionRequest{
			Commit: &pm.TransactionRequest_Commit{},
		}); err != nil {
			return fmt.Errorf("commit error: %v", err)
		}

		// Commit and loop around to immediately start loading again.
		commitWait.Add(1) // Signal that this committed has completed.
		go func(cache *TestStore) {
			defer commitWait.Done()
			committed, err := stream.Recv()
			if err != nil || committed.Committed == nil {
				log.Fatalf("committed response: %#v, error: %v", committed, err)
			}
			log.Info("committed response")

			// Once committed, return the cached/modified items back to the store. They will
			// sit in this channel until the next loading cycle has completed.
			returnToStore <- cache
		}(cache)

	}

	// Wait until the final commit has completed.
	commitWait.Wait()
	// Return the last cached items to the store.
	store.Push((<-returnToStore).Range()...)

	log.Info("testing completed, validating data")

	// Load everything from the store
	var packedKeys [][]byte
	for _, item := range store.Range() {
		packedKeys = append(packedKeys, item.Keys().Pack())
	}
	var arena pf.Arena
	var arenaPackedKeys = arena.AddAll(packedKeys...)

	if err := stream.Send(&pm.TransactionRequest{
		Load: &pm.TransactionRequest_Load{
			Arena:      arena,
			PackedKeys: arenaPackedKeys,
		},
	}); err != nil {
		return fmt.Errorf("final load: %v", err)
	}

	// Write the final Prepared request with the checkpoint information.
	if err := stream.Send(&pm.TransactionRequest{
		Prepare: &pm.TransactionRequest_Prepare{
			FlowCheckpoint: []byte("checkpoint done"),
		},
	}); err != nil {
		return fmt.Errorf("final prepare: %v", err)
	}

	// Loop through loaded requests until we are prepared.
	var next *pm.TransactionResponse
	for {
		next, err = stream.Recv()
		if err != nil {
			return fmt.Errorf("loaded/prepared: %v", err)
		}
		if next.Loaded != nil {
			log.Infof("loaded final response with %d documents", len(next.Loaded.DocsJson))
			// Compare the loaded item with the item from the cache to ensure they match.
			for _, arena := range next.Loaded.DocsJson {
				var item BasicData
				if err = json.Unmarshal(next.Loaded.Arena[arena.Begin:arena.End], &item); err != nil {
					return fmt.Errorf("unmarshaling loaded doc: %v", err)
				}
				storeItem := store.Find(item.Keys(), true)
				if storeItem == nil {
					return fmt.Errorf("unable to find loaded doc in cache")
				}
				if *storeItem.(*BasicData) != item {
					return fmt.Errorf("loaded document does not match cache")
				}
			}
		} else if next.Prepared != nil {
			log.Infof("prepared response with checkpoint: %s", string(next.Prepared.DriverCheckpointJson))
			break
		} else {
			return fmt.Errorf("unexpected message: %#v", next)
		}
	}

	// Send the final commit request.
	if err = stream.Send(&pm.TransactionRequest{
		Commit: &pm.TransactionRequest_Commit{},
	}); err != nil {
		return fmt.Errorf("commit error: %v", err)
	}
	log.Info("final commit request")

	committed, err := stream.Recv()
	if err != nil || committed.Committed == nil {
		log.Fatalf("committed response: %#v, error: %v", committed, err)
	}
	log.Info("final committed response")

	if itemsRemaining := store.Len(); itemsRemaining != 0 {
		log.Errorf("unaccounted for items: %d", itemsRemaining)
	} else {
		log.Info("no missing items detected")
	}

	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("close: %v", err)
	}

	log.Info("validation completed")

	return nil

}
