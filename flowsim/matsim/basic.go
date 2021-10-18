package matsim

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/estuary/connectors/flowsim/testcat"
	"github.com/estuary/connectors/flowsim/testdata"
	"github.com/estuary/flow/go/materialize"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// BasicConfig configures the options for the basic test.
type BasicConfig struct {
	Config
	Loops     int `long:"loops" default:"5" description:"Number of load/prepare/store/commit loops."`
	StartPow  int `long:"start" default:"8" description:"Number of docs to start with in 2^start and each loop will double"`
	BatchSize int `long:"batch" default:"1000" description:"Batch size to split requests into."`
}

// This executes a basic test against a materialization that does not require delta updates. It will perform
// the configured number of loops. It starts by loading 2^StartPow docs and then storing that many documents.
// It will pick at random inserting new documents and updating previously stored documents with new values to ensure
// that it is performing inserts as well as updates. Loads and Stores will be sent in BatchSize blocks of documents.
// Once it has completed it will load all of the documents that were inserted/updated to ensure they match the expected
// values. The documents will be provided using testDataGenerator.
func (c *BasicConfig) Execute(args []string) error {

	if err := c.Config.ParseConfig(); err != nil {
		return fmt.Errorf("could not parse config: %w", err)
	}

	// Build/Compile the TestCatalog into a Flow BuiltCatalog.
	catalog, err := testcat.BuildCatalog(c.ctx, c.NewTestCatalog())
	if err != nil {
		return fmt.Errorf("build catalog: %v", err)
	}

	// There will only be one materialization for this test.
	materialization := &catalog.Materializations[0]

	// Perform the connector setup (spec, validate, apply) and open the transactions stream.
	stream, err := SetupConnectorOpenTransactions(c.ctx, materialization, materialize.AdaptServerToClient(c.driverServer), false)
	if err != nil {
		return fmt.Errorf("setup connector: %v", err)
	}

	// Send the Open request.
	if err := stream.Send(&pm.TransactionRequest{
		Open: &pm.TransactionRequest_Open{
			Materialization: materialization,
			Version:         "1.0",
			KeyBegin:        0,
			KeyEnd:          0xFFFF,
		},
	}); err != nil {
		return fmt.Errorf("open error: %v", err)
	}

	// Received the Opened response.
	opened, err := stream.Recv()
	if err != nil || opened.Opened == nil {
		return fmt.Errorf("opened error: %v received: %#v", err, opened)
	}
	log.WithFields(log.Fields{"checkpoint": string(opened.Opened.FlowCheckpoint)}).Info("connection opened")

	// The store will contain our documents for caching and comparison at the end.
	var store = testdata.NewStore()
	var commitWait sync.WaitGroup

	// After a loop is committed, this channel will return committed items to the store.
	var returnToStore = make(chan *testdata.Store, 1)
	returnToStore <- testdata.NewStore() // preload with an empty store for the first loop.

	start := time.Now()
	rand.Seed(time.Now().UnixNano())

	// This is the main load/prepare/store/commit loop.
	for loop := 0; loop < c.Loops; loop++ {

		// The number of documents we will parse this pass
		docCount := 2 << (loop + c.StartPow)

		var passStart = time.Now()

		// Build a passStore of documents to load and then store, some of them will exist and others won't.
		var passStore = testdata.NewStore()
		// We need to track which documents exist and which don't for the Store request later.
		var exists []bool

		// Build a store of documents to load.
		for j := 0; j < docCount; j++ {
			// If nothing is in the store or at random pick a get TestData to load in the batch.
			if store.Len() == 0 || rand.Intn(2) == 0 {
				passStore.Push(c.newTestData())
				exists = append(exists, false)
			} else {
				// Pop an element from the store to the batch to load on.
				passStore.Push(store.Pop())
				exists = append(exists, true)
			}
		}

		// Send the load requests in in batches of batchSize.
		if err = SendLoad(stream, passStore.Range(), c.BatchSize); err != nil {
			return fmt.Errorf("load request: %v", err)
		}

		log.Infof("load: requests: %d keys: %d duration: %v", (docCount/c.BatchSize)+1, passStore.Len(), time.Since(passStart))

		// Wait until previous commit has completed if it's still running.
		commitWait.Wait()

		// Previously committed passStore is now available to put into the store again as the commit on it is complete.
		store.Push((<-returnToStore).Range()...)

		// Write the Prepared request with the checkpoint information.
		var checkpoint = fmt.Sprintf(`"checkpoint loop %d"`, loop)
		if err = SendPrepare(stream, []byte(checkpoint)); err != nil {
			return fmt.Errorf("prepare request: %v", err)
		}
		log.Infof("prepare: checkpoint: %s", checkpoint)

		// Loop through loaded requests until we are prepared.
		loadedStart := time.Now()
		var loadedMsgs int
		var loadedDocs int
		prepared, err := HandleLoadedPrepared(stream, func(loaded *pm.TransactionResponse_Loaded) error {
			loadedMsgs++
			loadedDocs += len(loaded.DocsJson)
			// For each document loaded, compare it to the passStore to make sure it's there and matches our expectations.
			for _, doc := range loaded.DocsJson {
				var item = c.newTestData()
				if err = json.Unmarshal(loaded.Arena[doc.Begin:doc.End], &item); err != nil {
					return fmt.Errorf("unmarshaling loaded doc: %v", err)
				}
				passItem := passStore.Find(item.Keys(), false)
				if passItem == nil {
					return fmt.Errorf("unable to find loaded doc in cache")
				}
				if !passItem.Equal(item) {
					return fmt.Errorf("loaded document does not match cache")
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("loaded/prepared: %w", err)
		}
		log.Infof("loaded: requests: %d docs: %d duration: %v", loadedMsgs, loadedDocs, time.Since(loadedStart))
		log.Infof("prepared: checkpoint: %s", string(prepared.DriverCheckpointJson))

		// Randomize the values, but keep the keys the same.
		for _, item := range passStore.Range() {
			item.UpdateValues()
		}

		storeStart := time.Now()

		// Send the Store requests with our data.
		if err = SendStore(stream, passStore.Range(), exists, c.BatchSize); err != nil {
			return fmt.Errorf("store error: %v", err)
		}
		log.Infof("store: duration: %v", time.Since(storeStart))

		// Send the Commit request.
		if err = SendCommit(stream); err != nil {
			return fmt.Errorf("commit error: %v", err)
		}

		commitStart := time.Now()
		log.Infof("commit: loop: %d", loop)

		// Commit and loop around to immediately start loading again.
		commitWait.Add(1)
		go func(loop int, passStore *testdata.Store) {
			defer commitWait.Done() // Signal that this committed has completed.
			if err = AwaitCommitted(stream); err != nil {
				log.Fatalf("committed: %v", err)
			}
			log.Infof("committed: loop: %d duration: %v pass duration: %v", loop, time.Since(commitStart), time.Since(passStart))

			// Once committed, return the cached/modified items back to the store. They will
			// sit in this channel until the next loading cycle has completed.
			returnToStore <- passStore
		}(loop, passStore)

	}

	// Wait until the final commit has completed.
	commitWait.Wait()
	// Return the last pass items to the store for comparison.
	store.Push((<-returnToStore).Range()...)

	log.Info("testing completed, verifying documents.")

	// Load everything from the store that we have sent thus far.
	if err = SendLoad(stream, store.Range(), c.BatchSize); err != nil {
		return fmt.Errorf("load: %v", err)
	}

	// Trigger loads via prepare.
	if err = SendPrepare(stream, []byte(`"checkpoint done"`)); err != nil {
		return fmt.Errorf("prepare: %v", err)
	}

	// Verify all the loads.
	loadedStart := time.Now()
	var loadedMsgs int
	var loadedDocs int
	if _, err = HandleLoadedPrepared(stream, func(loaded *pm.TransactionResponse_Loaded) error {
		loadedMsgs++
		loadedDocs += len(loaded.DocsJson)
		// Compare the loaded item with the item from the cache to ensure they match.
		for _, doc := range loaded.DocsJson {
			var item = c.newTestData()
			if err = json.Unmarshal(loaded.Arena[doc.Begin:doc.End], &item); err != nil {
				return fmt.Errorf("unmarshaling loaded doc: %v", err)
			}
			// Find (and remove) the item from the store.
			storeItem := store.Find(item.Keys(), true)
			if storeItem == nil {
				return fmt.Errorf("unable to find loaded doc in store")
			}
			if !storeItem.Equal(item) {
				return fmt.Errorf("loaded document does not match store")
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("loaded/prepared: %w", err)
	}
	log.Infof("loaded: requests: %d docs: %d duration: %v", loadedMsgs, loadedDocs, time.Since(loadedStart))

	// Send the commit and await the committed.
	if err = SendCommit(stream); err != nil {
		return fmt.Errorf("commit: %v", err)
	}
	if err = AwaitCommitted(stream); err != nil {
		return fmt.Errorf("committed: %v", err)
	}

	// Verify there is nothing left in the store.
	if itemsRemaining := store.Len(); itemsRemaining != 0 {
		log.Errorf("unaccounted for items: %d", itemsRemaining)
	} else {
		log.Info("no missing items detected.")
	}

	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("close: %v", err)
	}

	log.Infof("validation completed: duration: %v", time.Since(start))

	return nil

}
