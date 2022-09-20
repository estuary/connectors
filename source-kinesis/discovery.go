package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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

// How long should we allow reading from each stream to run. Note: empty streams
// will wait for documents until this timeout triggers.
const DISCOVER_READ_TIMEOUT = time.Second * 10

// How long should we allow schema inference to run.
const DISCOVER_INFER_TIMEOUT = time.Second * 5

// Provides a default schema to use when we encounter an issue with schema inference.
var DISCOVER_FALLBACK_SCHEMA = schema_inference.Schema(`{"type":"object"}`)

func discoverStreams(ctx context.Context, client *kinesis.Kinesis, streamNames []string) []airbyte.Stream {
	var waitGroup = new(sync.WaitGroup)
	var streams = make(chan airbyte.Stream, len(streamNames))

	for i, name := range streamNames {
		waitGroup.Add(1)

		go func(i int, name string) {
			defer waitGroup.Done()

			schema, err := runInference(ctx, client, name)
			if err == schema_inference.NO_DOCUMENTS_FOUND {
				// An empty stream is a world full of possibilities.
				schema = DISCOVER_FALLBACK_SCHEMA
			} else if err != nil {
				// If we encounter an error discovering the schema we skip the
				// stream. If the user expects it to be there, they'll need to
				// investigate what is causing the stream to be causing errors.
				// Otherwise, this simply omits unreadable streams from the
				// discover output.
				log.WithField("stream", name).Error(err.Error())
				return
			}

			streams <- airbyte.Stream{
				Name:                name,
				JSONSchema:          schema,
				SupportedSyncModes:  []airbyte.SyncMode{airbyte.SyncModeIncremental},
				SourceDefinedCursor: true,
			}
		}(i, name)
	}

	waitGroup.Wait()
	close(streams)

	return collect(streams, len(streamNames))
}

func runInference(ctx context.Context, client *kinesis.Kinesis, streamName string) (schema_inference.Schema, error) {
	var logEntry = log.WithField("stream", streamName)
	var peekCtx, cancelPeek = context.WithTimeout(ctx, DISCOVER_READ_TIMEOUT)
	defer cancelPeek()

	docCh, docCount, err := peekAtStream(peekCtx, client, streamName, DISCOVER_MAX_DOCS_PER_STREAM)

	if err != nil {
		return nil, fmt.Errorf("schema discovery for stream `%s` failed: %w", streamName, err)
	} else if docCount == 0 {
		return nil, schema_inference.NO_DOCUMENTS_FOUND
	}

	logEntry.WithField("count", docCount).Info("Got documents for stream")

	var inferCtx, cancelInfer = context.WithTimeout(ctx, DISCOVER_INFER_TIMEOUT)
	defer cancelInfer()

	return schema_inference.Run(inferCtx, logEntry, docCh)
}

func peekAtStream(ctx context.Context, client *kinesis.Kinesis, streamName string, peekAtMost uint) (<-chan schema_inference.Document, uint, error) {
	var (
		err         error
		cancel      context.CancelFunc
		dataCh      chan readResult      = make(chan readResult, 8)
		shardRange  airbyte.Range        = airbyte.NewFullRange()
		stopAt      time.Time            = time.Now()
		streamState map[string]string    = make(map[string]string)
		waitGroup   *sync.WaitGroup      = new(sync.WaitGroup)
		docs        chan json.RawMessage = make(chan schema_inference.Document, peekAtMost)
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

func collect(ch <-chan airbyte.Stream, size int) []airbyte.Stream {
	var collected = make([]airbyte.Stream, 0, size)
	for stream := range ch {
		collected = append(collected, stream)
	}
	sort.SliceStable(collected, func(i, j int) bool {
		return collected[i].Name < collected[j].Name
	})
	return collected
}
