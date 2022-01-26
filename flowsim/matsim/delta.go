package matsim

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/estuary/connectors/flowsim/testcat"
	"github.com/estuary/connectors/flowsim/testdata"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type DeltaConfig struct {
	Config
	Loops     int    `long:"loops" default:"5" description:"Number of load/prepare/store/commit loops."`
	Start     int    `long:"start" default:"500" description:"Number of docs to start with and each loop will double"`
	BatchSize int    `long:"batch" default:"1000" description:"Batch size to split requests into."`
	Output    string `long:"output" default:"" description:"Where to output delta data (blank = stdout)"`
}

// The Delta command runs a connector in Delta mode. It only performs store requests. In addition it also
// writes newline delimeted JSON of the stored documents. You can specify an output file, otherwise it uses
// stdout which can be piped to a file.
func (c *DeltaConfig) Execute(args []string) error {

	if err := c.Config.ParseConfig(); err != nil {
		return fmt.Errorf("could not parse config: %w", err)
	}

	// Build/Compile the TestCatalog into a Flow BuiltCatalog.
	var testCatalog, err = c.NewTestCatalog()
	if err != nil {
		return fmt.Errorf("test catalog: %v", err)
	}
	materializationSpecs, err := testcat.BuildMaterializationSpecs(c.ctx, testCatalog)
	if err != nil {
		return fmt.Errorf("build materialization specs: %v", err)
	}

	// There will only be one materialization for this test.
	materialization := materializationSpecs[0]

	// Perform the setup and open the transactions stream
	stream, err := SetupConnectorOpenTransactions(c.ctx, materialization, pm.AdaptServerToClient(c.driverServer), true)
	if err != nil {
		return err
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
		return fmt.Errorf("opened error: %v response: %#v", err, opened)
	}
	log.WithFields(log.Fields{"checkpoint": string(opened.Opened.FlowCheckpoint)}).Info("connection opened")

	// The store will contain our documents for caching and comparison at the end.
	var store = testdata.NewStore()
	var commitWait sync.WaitGroup

	// After a loop is committed, this channel will return committed items to the store.
	var returnToStore = make(chan *testdata.Store, 1)
	returnToStore <- testdata.NewStore() // preload with an empty store for the first loop.

	start := time.Now()

	// Setup the output file and json encoding.
	var output io.Writer = os.Stdout
	if c.Output != "" {
		if output, err = os.Create(c.Output); err != nil {
			return fmt.Errorf("could not open output file: %w", err)
		}
		defer func() {
			_ = output.(*os.File).Close()
		}()
	}
	var outputEncoder = json.NewEncoder(output)
	outputEncoder.SetEscapeHTML(false)

	// Main prepare/store/commit loop.
	for loop := 0; loop < c.Loops; loop++ {
		// The number of documents we will store this pass.
		docCount := (1 << loop) * c.Start

		var passStart = time.Now()

		// Wait until previous commit has completed if it's still running.
		commitWait.Wait()
		if err := Acknowledge(stream); err != nil {
			return err
		}

		// Write the Prepared request with the checkpoint information.
		var checkpoint = fmt.Sprintf(`"checkpoint loop %d"`, loop)
		if err = SendPrepare(stream, []byte(checkpoint)); err != nil {
			return fmt.Errorf("prepare request: %v", err)
		}
		log.Infof("prepare: checkpoint: %s", checkpoint)

		// Loop through loaded requests until we are prepared.
		prepared, err := HandleLoadedPrepared(stream, func(loaded *pm.TransactionResponse_Loaded) error {
			return fmt.Errorf("unexpected loaded message: %#v", loaded)
		})
		if err != nil {
			return fmt.Errorf("loaded/prepared: %w", err)
		}
		log.Infof("prepared: checkpoint: %s", string(prepared.DriverCheckpointJson))

		store.Push((<-returnToStore).Range()...)
		// Build a passStore of documents to load and then store, some of them will exist and others won't.
		var passStore = testdata.NewStore()
		var exists []bool

		for i := 0; i < docCount; i++ {
			// If nothing is in the store or at random pick a get TestData to update in the batch.
			if store.Len() == 0 || rand.Intn(2) == 0 {
				item := c.newTestData()
				passStore.Push(item)
				exists = append(exists, false)
			} else {
				// Pop an element from the store to the batch to update.
				item := store.Pop()
				item.UpdateValues()
				passStore.Push(item)
				exists = append(exists, true)
			}
		}

		storeStart := time.Now()

		// Send the Store requests.
		if err = SendStore(stream, passStore.Range(), exists, c.BatchSize); err != nil {
			return fmt.Errorf("store error: %v", err)
		}
		log.Infof("store: duration: %v", time.Since(storeStart))

		// Send the Commit request.
		if err = SendCommit(stream); err != nil {
			return fmt.Errorf("commit error: %v", err)
		}

		commitStart := time.Now()
		log.Infof("commit: loop:%d", loop)

		// Commit and loop around to immediately start loading again.
		commitWait.Add(1)
		go func(loop int, store *testdata.Store) {
			defer commitWait.Done() // Signal that this committed has completed.
			if err = AwaitCommitted(stream); err != nil {
				log.Fatalf("committed: %v", err)
			}
			log.Infof("committed: loop: %d duration: %v pass duration: %v", loop, time.Since(commitStart), time.Since(passStart))

			// Once committed, return the cached/modified items back to the store. They will
			// sit in this channel until the next loading cycle has completed.
			returnToStore <- store
		}(loop, passStore)
	}

	// Wait until the final commit has completed.
	commitWait.Wait()

	store.Push((<-returnToStore).Range()...)

	// Once committed, encode to the output location
	log.Warnf("Manual verification of test results required:")
	for _, item := range store.Range() {
		outputEncoder.Encode(item)
	}

	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("close: %v", err)
	}

	log.Infof("validation completed: duration: %v", time.Since(start))

	return nil
}
