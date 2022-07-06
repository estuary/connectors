package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/connectors/schema_inference"
	"github.com/estuary/flow/go/protocols/airbyte"
	log "github.com/sirupsen/logrus"
)

// How many documents to read from each stream. A higher number gives us more
// opportunity to spot variation in the document stream at the expense of having
// to process more possibly-redundant documents.
const DISCOVER_MAX_DOCS_PER_STREAM = 100

// Provides a default schema to use when we encounter an issue with schema inference.
var DISCOVER_FALLBACK_SCHEMA = json.RawMessage(`{"type":"object"}`)

func discoverStreams(ctx context.Context, client *kinesis.Kinesis, streamNames []string) []airbyte.Stream {
	var waitGroup = new(sync.WaitGroup)
	var streams = make([]airbyte.Stream, len(streamNames))

	for i, name := range streamNames {
		waitGroup.Add(1)

		go func(i int, name string) {
			defer waitGroup.Done()

			schema, err := discoverSchema(ctx, client, name)
			if err != nil {
				// If we encounter an error discovering the schema, output a
				// very basic schema rather than failing discovery. If this
				// looks wrong to the user, they can investigate, but ultimately
				// not every stream will contain json documents.
				log.WithField("stream", name).Error(err.Error())
				schema = DISCOVER_FALLBACK_SCHEMA
			}

			streams[i] = airbyte.Stream{
				Name:                name,
				JSONSchema:          schema,
				SupportedSyncModes:  []airbyte.SyncMode{airbyte.SyncModeIncremental},
				SourceDefinedCursor: true,
			}
		}(i, name)
	}

	waitGroup.Wait()

	return streams
}

func discoverSchema(ctx context.Context, client *kinesis.Kinesis, streamName string) (json.RawMessage, error) {
	schema, err := schema_inference.Run(ctx, streamName, func(ctx context.Context, logEntry *log.Entry) (<-chan json.RawMessage, uint, error) {
		return peekAtStream(ctx, client, streamName, DISCOVER_MAX_DOCS_PER_STREAM)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to infer schema for stream `%s`: %w", streamName, err)
	}

	return schema, nil
}

func peekAtStream(ctx context.Context, client *kinesis.Kinesis, streamName string, peekAtMost uint) (<-chan json.RawMessage, uint, error) {
	var (
		err         error
		cancel      context.CancelFunc
		dataCh      chan readResult      = make(chan readResult, 8)
		shardRange  airbyte.Range        = airbyte.NewFullRange()
		stopAt      time.Time            = time.Now()
		streamState map[string]string    = make(map[string]string)
		waitGroup   *sync.WaitGroup      = new(sync.WaitGroup)
		docs        chan json.RawMessage = make(chan json.RawMessage, peekAtMost)
		docCount    uint                 = 0
	)

	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	waitGroup.Add(1)
	go readStream(ctx, shardRange, client, streamName, streamState, dataCh, &stopAt, waitGroup)
	go closeChannelWhenDone(dataCh, waitGroup)
	defer close(docs)

outerLoop:
	for {
		select {
		case <-ctx.Done():
			break outerLoop
		case next, more := <-dataCh:
			if !more {
				break outerLoop
			} else if next.err != nil {
				// We'll log any errors to stderr, as any problems we encounter now are not
				// necessarily fatal errors for discovery.
				log.Errorf("peekAtStream failed due to error: %v", next.err)
				return nil, 0, next.err
			}

			for _, record := range next.records {
				select {
				case docs <- record:
					// Ok, keep going
				default:
					// We've filled up our buffer for this stream
					break outerLoop
				}
				docCount++

				if docCount >= peekAtMost {
					// We've peeked at enough documents
					break outerLoop
				}
			}
		}
	}

	return docs, docCount, err
}
