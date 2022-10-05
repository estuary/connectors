package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/flow/go/protocols/airbyte"
	log "github.com/sirupsen/logrus"
)

func main() {
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

// TODO: update docs link to kinesis connector-specific docs after they are written
var spec = airbyte.Spec{
	SupportsIncremental:           true,
	SupportedDestinationSyncModes: airbyte.AllDestinationSyncModes,
	ConnectionSpecification:       json.RawMessage(configJSONSchema),
	DocumentationURL:              "https://go.estuary.dev/source-kinesis",
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
	return encoder.Encode(airbyte.Message{
		Type:    airbyte.MessageTypeCatalog,
		Catalog: catalog,
	})
}

func discoverCatalog(config airbyte.ConfigFile) (*airbyte.Catalog, error) {
	var _, client, err = parseConfigAndConnect(config)
	if err != nil {
		return nil, err
	}

	var ctx = context.Background()
	streamNames, err := listAllStreams(ctx, client)
	if err != nil {
		return nil, err
	}

	streams := discoverStreams(ctx, client, streamNames)

	return &airbyte.Catalog{Streams: streams}, nil
}

func updateState(state map[string]map[string]string, source *recordSource, sequenceNumber string) {
	var streamMap, ok = state[source.stream]
	if !ok {
		streamMap = make(map[string]string)
		state[source.stream] = streamMap
	}
	streamMap[source.shardID] = sequenceNumber
}

func copyStreamState(state map[string]map[string]string, stream string) (map[string]string, error) {
	var dest = make(map[string]string)
	// Is there an entry for this stream
	if ss, ok := state[stream]; ok {
		for k, v := range ss {
			dest[k] = v
		}
	}
	return dest, nil
}

func doRead(args airbyte.ReadCmd) error {
	return readStreamsTo(context.Background(), args, os.Stdout)
}

func readStreamsTo(ctx context.Context, args airbyte.ReadCmd, output io.Writer) error {
	var _, client, err = parseConfigAndConnect(args.ConfigFile)
	if err != nil {
		return err
	}
	var catalog airbyte.ConfiguredCatalog
	if err = args.CatalogFile.Parse(&catalog); err != nil {
		return fmt.Errorf("parsing configured catalog: %w", err)
	}

	var stateMap = make(stateMap)
	if len(args.StateFile) > 0 {
		if err = args.StateFile.Parse(&stateMap); err != nil {
			return fmt.Errorf("parsing state file: %w", err)
		}
	}

	var dataCh = make(chan readResult, 8)
	ctx, cancelFunc := context.WithCancel(ctx)

	log.WithField("streamCount", len(catalog.Streams)).Info("Starting to read stream(s)")

	var shardRange = catalog.Range
	if shardRange.IsZero() {
		log.Info("using full shard range since no range was given in the catalog")
		shardRange = airbyte.NewFullRange()
	}

	var stopAt *time.Time
	if !catalog.Tail {
		var t = time.Now().UTC()
		log.Infof("reading in non-tailing mode until: %v", t)
		stopAt = &t
	} else {
		log.Info("reading indefinitely because tail==true")
	}
	var waitGroup = new(sync.WaitGroup)
	for _, stream := range catalog.Streams {
		streamState, err := copyStreamState(stateMap, stream.Stream.Name)
		if err != nil {
			cancelFunc()
			return fmt.Errorf("invalid state for stream %s: %w", stream.Stream.Name, err)
		}
		waitGroup.Add(1)
		go readStream(ctx, shardRange, client, stream.Stream.Name, streamState, dataCh, stopAt, waitGroup)
	}

	go closeChannelWhenDone(dataCh, waitGroup)

	// We'll re-use this same message instance for all records we print
	var recordMessage = airbyte.Message{
		Type:   airbyte.MessageTypeRecord,
		Record: &airbyte.Record{},
	}
	// We're all set to start printing data to stdout
	var encoder = json.NewEncoder(output)
	for next := range dataCh {
		if next.err != nil {
			// time to bail
			var errMessage = airbyte.NewLogMessage(airbyte.LogLevelFatal, "read failed due to error: %v", next.err)
			// Printing the error may fail, but we'll ignore that error and return the original
			_ = encoder.Encode(errMessage)
			err = next.err
			break
		}
		recordMessage.Record.Stream = next.source.stream
		for _, record := range next.records {
			recordMessage.Record.Data = record
			recordMessage.Record.EmittedAt = time.Now().UTC().UnixNano() / int64(time.Millisecond)
			if err = encoder.Encode(recordMessage); err != nil {
				break
			}
		}
		updateState(stateMap, next.source, next.sequenceNumber)

		stateRaw, err := json.Marshal(stateMap)
		if err != nil {
			break
		}
		if err = encoder.Encode(airbyte.Message{
			Type:  airbyte.MessageTypeState,
			State: &airbyte.State{Data: json.RawMessage(stateRaw)},
		}); err != nil {
			break
		}

		if err != nil {
			break
		}
	}
	cancelFunc()
	return err
}

func closeChannelWhenDone(dataCh chan readResult, waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	log.Info("All reads have completed")
	close(dataCh)
}

func parseConfigAndConnect(configFile airbyte.ConfigFile) (config Config, client *kinesis.Kinesis, err error) {
	if err = configFile.ConfigFile.Parse(&config); err != nil {
		err = fmt.Errorf("parsing config file: %w", err)
		return
	}
	if client, err = connect(&config); err != nil {
		err = fmt.Errorf("failed to connect: %w", err)
	}
	return
}

type stateMap map[string]map[string]string

func (s stateMap) Validate() error {
	return nil // No-op.
}
