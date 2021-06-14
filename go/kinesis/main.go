package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/connectors/go/airbyte"
	"github.com/estuary/connectors/go/shardrange"
	log "github.com/sirupsen/logrus"
)

func main() {
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

// TODO: update docs link to kinesis connector-specific docs after they are written
var spec = airbyte.Spec{
	SupportsIncremental:           true,
	SupportedDestinationSyncModes: airbyte.AllDestinationSyncModes,
	ConnectionSpecification:       configJSONSchema,
}

func doCheck(args airbyte.CheckCmd) error {
	var result = &airbyte.ConnectionStatus{
		Status: airbyte.StatusSucceeded,
	}
	var _, err = tryListingStreams(args.ConfigFile)
	if err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func tryListingStreams(configFile airbyte.ConfigFile) ([]string, error) {
	var _, client, err = parseConfigAndConnect(configFile)
	if err != nil {
		return nil, err
	}
	var ctx = context.Background()
	return listAllStreams(ctx, client)
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

func discoverCatalog(config airbyte.ConfigFile) (*airbyte.Catalog, error) {
	var _, client, err = parseConfigAndConnect(config)
	if err != nil {
		return nil, err
	}
	var ctx = context.Background()
	streamNames, err := listAllStreams(ctx, client)

	var schema = airbyte.UnknownSchema()

	var catalog = &airbyte.Catalog{
		Streams: make([]airbyte.Stream, len(streamNames)),
	}
	for i, name := range streamNames {
		catalog.Streams[i] = airbyte.Stream{
			Name:                name,
			JSONSchema:          schema,
			SupportedSyncModes:  []airbyte.SyncMode{airbyte.SyncModeIncremental},
			SourceDefinedCursor: true,
		}
	}
	return catalog, nil
}

func updateState(state *airbyte.State, source *recordSource, sequenceNumber string) {
	var streamMap, ok = state.Data[source.stream]
	if !ok {
		streamMap = make(map[string]interface{})
		state.Data[source.stream] = streamMap
	}
	streamMap.(map[string]interface{})[source.shardID] = sequenceNumber
}

func copyStreamState(state *airbyte.Message, stream string) (map[string]string, error) {
	var dest = make(map[string]string)
	// Is there an entry for this stream
	if ss, ok := state.State.Data[stream]; ok {
		// Does the entry for this stream have the right type
		if typedSS, ok := ss.(map[string]interface{}); ok {
			for k, v := range typedSS {
				if vstr, ok := v.(string); ok {
					dest[k] = vstr
				} else {
					return nil, fmt.Errorf("found a non-string value in state map for stream: '%s'", stream)
				}
			}
		} else {
			return nil, fmt.Errorf("invalid state for stream '%s', expected values to be maps of string to string", stream)
		}
	}
	return dest, nil
}

func doRead(args airbyte.ReadCmd) error {
	var config, client, err = parseConfigAndConnect(args.ConfigFile)
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

	var stateMessage = airbyte.Message{
		Type: airbyte.MessageTypeState,
		State: &airbyte.State{
			Data: make(map[string]interface{}),
		},
	}

	if err = args.StateFile.Parse(&stateMessage.State.Data); err != nil {
		return fmt.Errorf("parsing state file: %w", err)
	}

	var dataCh = make(chan readResult, 8)
	var ctx, cancelFunc = context.WithCancel(context.Background())

	log.WithField("streamCount", len(catalog.Streams)).Info("Starting to read stream(s)")

	for _, stream := range catalog.Streams {
		streamState, err := copyStreamState(&stateMessage, stream.Stream.Name)
		if err != nil {
			cancelFunc()
			return fmt.Errorf("invalid state for stream %s: %w", stream.Stream.Name, err)
		}
		go readStream(ctx, config, client, stream.Stream.Name, streamState, dataCh)
	}

	// We'll re-use this same message instance for all records we print
	var recordMessage = airbyte.Message{
		Type:   airbyte.MessageTypeRecord,
		Record: &airbyte.Record{},
	}
	// We're all set to start printing data to stdout
	var encoder = json.NewEncoder(os.Stdout)
	for {
		var next = <-dataCh
		if next.err != nil {
			// time to bail
			var errMessage = airbyte.NewLogMessage(airbyte.LogLevelFatal, "read failed due to error: %v", next.err)
			// Printing the error may fail, but we'll ignore that error and return the original
			_ = encoder.Encode(errMessage)
			cancelFunc()
			return next.err
		}
		recordMessage.Record.Stream = next.source.stream
		for _, record := range next.records {
			recordMessage.Record.Data = record
			recordMessage.Record.EmittedAt = time.Now().UTC().UnixNano() / int64(time.Millisecond)
			if err := encoder.Encode(recordMessage); err != nil {
				cancelFunc()
				return err
			}
		}
		updateState(stateMessage.State, next.source, next.sequenceNumber)
		if err := encoder.Encode(stateMessage); err != nil {
			cancelFunc()
			return err
		}
	}
}

func parseConfigAndConnect(configFile airbyte.ConfigFile) (config Config, client *kinesis.Kinesis, err error) {
	err = configFile.ConfigFile.Parse(&config)
	if err != nil {
		err = fmt.Errorf("parsing config file: %w", err)
		return
	}
	// If the partition range was not included in the configuration, then we'll assume the full
	// range.
	if config.ShardRange == nil {
		log.Info("Assuming full partition range since no partitionRange was included in the configuration")
		var fullRange = shardrange.NewFullRange()
		config.ShardRange = &fullRange
	}
	client, err = connect(&config)
	if err != nil {
		err = fmt.Errorf("failed to connect: %w", err)
	}
	return
}
