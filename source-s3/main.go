package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/estuary/connectors/go-types/parser"
	log "github.com/sirupsen/logrus"
)

func main() {
	var parserSpec, err = parser.GetSpec()
	if err != nil {
		panic(err)
	}
	var spec = airbyte.Spec{
		SupportsIncremental:           true,
		SupportedDestinationSyncModes: airbyte.AllDestinationSyncModes,
		ConnectionSpecification:       configJSONSchema(parserSpec),
	}
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

func doCheck(args airbyte.CheckCmd) error {
	var result = &airbyte.ConnectionStatus{
		Status: airbyte.StatusSucceeded,
	}
	var _, err = discoverCatalog(args.ConfigFile)
	if err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func doDiscover(args airbyte.DiscoverCmd) error {
	var catalog, err = discoverCatalog(args.ConfigFile)
	if err != nil {
		return err
	}
	log.Infof("Discover completed with %d streams", len(catalog.Streams))
	var encoder = airbyte.NewStdoutEncoder()
	return encoder.Encode(catalog)
}

func discoverCatalog(configFile airbyte.ConfigFile) (*airbyte.Catalog, error) {
	var ctx = context.Background()
	var config, client, err = parseConfigAndConnect(ctx, configFile)
	if err != nil {
		return nil, err
	}

	streams, err := discoverStreams(ctx, &config, client)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	var catalog = &airbyte.Catalog{
		Streams: streams,
	}
	return catalog, nil
}

func doRead(args airbyte.ReadCmd) error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	var config, client, err = parseConfigAndConnect(ctx, args.ConfigFile)
	if err != nil {
		return err
	}
	var catalog airbyte.ConfiguredCatalog
	if err = args.CatalogFile.Parse(&catalog); err != nil {
		return fmt.Errorf("parsing configured catalog: %w", err)
	}

	if err = catalog.Validate(); err != nil {
		return fmt.Errorf("configured catalog is invalid: %w", err)
	}

	var stateMap = make(map[string]streamState)
	var stateMessage = airbyte.Message{
		Type:  airbyte.MessageTypeState,
		State: &airbyte.State{},
	}
	if err = args.StateFile.Parse(&stateMap); err != nil {
		return fmt.Errorf("parsing state file: %w", err)
	}

	var nStreams = len(catalog.Streams)

	// We'll re-use this same message instance for all records we print
	var recordMessage = airbyte.Message{
		Type:   airbyte.MessageTypeRecord,
		Record: &airbyte.Record{},
	}
	// We're all set to start printing data to stdout
	var encoder = json.NewEncoder(os.Stdout)

	var chunker = txnChunker{}

	var completedStreams = 0
	var mutex = sync.Mutex{}
	var onResult = func(next readResult) error {
		mutex.Lock()
		defer mutex.Unlock()

		if next.err == StreamCompleted {
			completedStreams++
			if completedStreams == nStreams {
				log.WithField("nStreams", nStreams).Info("All streams completed")
				cancelFunc()
			}
			return nil
		} else if next.err != nil {
			// time to bail
			var errMessage = airbyte.NewLogMessage(airbyte.LogLevelFatal, "read failed due to error: %v", next.err)
			// Printing the error may fail, but we'll ignore that error and return the original
			_ = encoder.Encode(errMessage)
			cancelFunc()
			return next.err
		}

		// It's expected that some readReuslts might *only* contain a state update, but no record
		// data. This happens when we've reached the end of a file and need to update the state to
		// indicate that it's completed.
		if len(next.record) > 0 {
			recordMessage.Record.Stream = next.streamID
			recordMessage.Record.Data = next.record
			recordMessage.Record.EmittedAt = time.Now().UTC().UnixNano() / int64(time.Millisecond)
			if err := encoder.Encode(recordMessage); err != nil {
				cancelFunc()
				return err
			}
		}

		updateState(stateMap, &next)

		// Should we emit a state checkpoint?
		if chunker.shouldEmitState(&next) {
			if err := writeState(&stateMessage, stateMap, encoder); err != nil {
				cancelFunc()
				return fmt.Errorf("failed to write state: %w", err)
			}
			chunker.reset()
		}
		return nil
	}

	log.WithField("streamCount", nStreams).Info("Starting to read stream(s)")
	for _, stream := range catalog.Streams {
		var state = copyStreamState(stateMap[stream.Stream.Name])
		capture, err := NewStream(&config, client, stream.Stream.Name, state)
		if err != nil {
			cancelFunc()
			return err
		}
		go capture.Start(ctx, onResult)
	}

	<-ctx.Done()
	log.Info("Finished all reads")
	return err
}

func writeState(message *airbyte.Message, state map[string]streamState, encoder *json.Encoder) error {
	var serialized, err = json.Marshal(state)
	if err != nil {
		return err
	}
	message.State.Data = json.RawMessage(serialized)
	return encoder.Encode(message)
}

func updateState(state map[string]streamState, rr *readResult) {
	var ss, exists = state[rr.streamID]
	if !exists {
		ss = make(streamState)
		state[rr.streamID] = ss
	}

	ss[rr.relativeKey] = rr.state
}

// txnChunker decides when we should emit State messages, based on thresholds for the number of
// records or bytes. This is to prevent individual transactions from becoming too large when the
// connector is used with Flow or other systems that can checkpoint whenever a State message is emitted.
type txnChunker struct {
	records int
	bytes   int
}

// shouldEmitState returns true if the readResult contains a record that puts us over any of the
// thresholds, or if it indicates the completion of a file.
func (t *txnChunker) shouldEmitState(rr *readResult) bool {
	var size = len(rr.record)
	if size > 0 {
		t.records++
		t.bytes = t.bytes + size
	}

	return t.records >= recordsPerTxn ||
		t.bytes >= bytesPerTxn ||
		rr.state.Complete
}

func (t *txnChunker) reset() {
	t.records = 0
	t.bytes = 0
}

// These values were chosen somewhat arbitrarily, with the goal of preventing any single transaction
// from becoming too large or slow, while also allowing us to amortize the cost of serializing and
// persisting state checkpoints.
const recordsPerTxn = 256
const bytesPerTxn = 1024 * 1024 * 2
