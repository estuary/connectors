package matsim

import (
	"errors"
	"fmt"
	"time"

	"github.com/estuary/connectors/flowsim/testcat"
	"github.com/estuary/flow/go/protocols/materialize"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type FenceConfig struct {
	Config
}

// The fence command opens a transaction stream with a full key range and then opens another stream to the same
// materialization with an overlapping key range. It then attempts to commit on the first stream and expects
// an error for overlapping fence constraints.
func (c *FenceConfig) Execute(args []string) error {

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

	materialization := materializationSpecs[0]

	// Perform the connector setup (spec, validate, apply) and open the first transactions stream.
	stream1, err := SetupConnectorOpenTransactions(c.ctx, materialization, materialize.AdaptServerToClient(c.driverServer), false)
	if err != nil {
		return err
	}

	// Send the Open request with our key range.
	if err := stream1.Send(&pm.TransactionRequest{
		Open: &pm.TransactionRequest_Open{
			Materialization: materialization,
			Version:         "1.0",
			KeyBegin:        0,
			KeyEnd:          0xFFFF,
		},
	}); err != nil {
		return fmt.Errorf("stream1: open error: %v", err)
	}

	// Received the Opened response
	if opened, err := stream1.Recv(); err != nil || opened.Opened == nil {
		return fmt.Errorf("stream1: opened error: %v response: %#v", err, opened)
	} else {
		log.WithFields(log.Fields{"checkpoint": string(opened.Opened.FlowCheckpoint)}).Info("stream1: connection opened")
	}

	// Perform the connector setup (spec, validate, apply) and open the second, overlapping transactions stream.
	stream2, err := SetupConnectorOpenTransactions(c.ctx, materialization, pm.AdaptServerToClient(c.driverServer), false)
	if err != nil {
		return err
	}

	// Send the Open request with an overlapping key range.
	if err := stream2.Send(&pm.TransactionRequest{
		Open: &pm.TransactionRequest_Open{
			Materialization: materialization,
			Version:         "1.0",
			KeyBegin:        0,
			KeyEnd:          0xFFFF,
		},
	}); err != nil {
		return fmt.Errorf("stream2: open error: %v", err)
	}

	// Received the Opened response from the second stream.
	if opened, err := stream2.Recv(); err != nil || opened.Opened == nil {
		return fmt.Errorf("stream2: opened error: %v received: %#v", err, opened)
	} else {
		log.WithFields(log.Fields{"checkpoint": string(opened.Opened.FlowCheckpoint)}).Info("stream2: connection opened")
	}

	// Send prepared to stream1.
	var checkpoint = []byte(`"checkpoint fence"`)
	if err = SendPrepare(stream1, checkpoint); err != nil {
		return fmt.Errorf("prepare request: %v", err)
	}
	log.Infof("stream1: prepare: checkpoint: %s", checkpoint)

	// Loop through loaded requests until we are prepared.
	loadedStart := time.Now()
	var loadedMsgs int
	var loadedDocs int
	prepared, err := HandleLoadedPrepared(stream1, func(loaded *pm.TransactionResponse_Loaded) error {
		loadedMsgs++
		loadedDocs += len(loaded.DocsJson)
		return nil
	})
	if err != nil {
		return fmt.Errorf("loaded/prepared: %w", err)
	}
	log.Infof("stream1: loaded: requests: %d docs: %d duration: %v", loadedMsgs, loadedDocs, time.Since(loadedStart))
	log.Infof("stream1: prepared: checkpoint: %s", string(prepared.DriverCheckpointJson))

	// Send the Commit request which should result in a failure as it's fenced off.
	commitStart := time.Now()
	if err = SendCommit(stream1); err != nil {
		return fmt.Errorf("commit error: %v", err)
	}
	log.Info("stream1: commit: awaiting failure")

	// This committed should be an error.
	err = AwaitCommitted(stream1)
	log.Infof("stream1: committed: duration: %v ", time.Since(commitStart))
	if err == nil {
		return errors.New("committed: expected failure but commit returned no error")
	}

	log.Infof("stream1: commit: error: %v", err)

	// Close streams.
	if err := stream1.CloseSend(); err != nil {
		return fmt.Errorf("close: %v", err)
	}

	if err := stream2.CloseSend(); err != nil {
		return fmt.Errorf("close: %v", err)
	}

	log.Info("validation completed")

	return nil

}
