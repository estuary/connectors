package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/flow/go/protocols/airbyte"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// How long should we allow reading from each stream to run. Note: empty streams
// will wait for documents until this timeout triggers.
const DISCOVER_READ_TIMEOUT = time.Second * 10

// How long should we allow schema inference to run.
const DISCOVER_INFER_TIMEOUT = time.Second * 5

// How many documents to read from each stream. A higher number gives us more
// opportunity to spot variation in the document stream at the expense of having
// to process more possibly-redundant documents.
const DISCOVER_MAX_DOCS_PER_STREAM = 100

// Provides a default schema to use when we encounter an issue with schema inference.
var FALLBACK_SCHEMA = json.RawMessage(`{"type":"object"}`)

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
				schema = &FALLBACK_SCHEMA
			}

			streams[i] = airbyte.Stream{
				Name:                name,
				JSONSchema:          *schema,
				SupportedSyncModes:  []airbyte.SyncMode{airbyte.SyncModeIncremental},
				SourceDefinedCursor: true,
			}
		}(i, name)
	}

	waitGroup.Wait()

	return streams
}

func discoverSchema(ctx context.Context, client *kinesis.Kinesis, streamName string) (*json.RawMessage, error) {
	var logEntry = log.WithField("stream", streamName)
	var peekCtx, cancelPeek = context.WithTimeout(ctx, DISCOVER_READ_TIMEOUT)
	defer cancelPeek()

	var docCh, docCount, err = peekAtStream(peekCtx, client, streamName, DISCOVER_MAX_DOCS_PER_STREAM)
	if err != nil {
		return nil, fmt.Errorf("schema discovery for stream `%s` failed: %w", streamName, err)
	} else if docCount == 0 {
		return nil, fmt.Errorf("no documents discovered for stream: %v", streamName)
	}

	logEntry.WithField("count", docCount).Info("Got documents for stream")

	var inferCtx, cancelInfer = context.WithTimeout(ctx, DISCOVER_INFER_TIMEOUT)
	defer cancelInfer()

	var schema *json.RawMessage
	schema, err = runInference(inferCtx, logEntry, docCh)
	if err != nil {
		return nil, fmt.Errorf("failed to infer schema for stream `%s`: %w", streamName, err)
	}

	return schema, nil
}

func runInference(ctx context.Context, logEntry *log.Entry, docsCh chan json.RawMessage) (*json.RawMessage, error) {
	var (
		err      error
		errGroup *errgroup.Group
		schema   = new(json.RawMessage)
		cmd      *exec.Cmd
		stdin    io.WriteCloser
		stdout   io.ReadCloser
	)

	errGroup, ctx = errgroup.WithContext(ctx)
	cmd = exec.CommandContext(ctx, "flow-schema-inference", "analyze")

	stdin, err = cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to open stdin: %w", err)
	}
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to open stdout: %w", err)
	}

	errGroup.Go(func() error {
		defer stdin.Close()
		encoder := json.NewEncoder(stdin)

		for doc := range docsCh {
			if err := encoder.Encode(doc); err != nil {
				return fmt.Errorf("failed to encode document: %w", err)
			}
		}
		logEntry.Info("runInference: Done sending documents")

		return nil
	})

	errGroup.Go(func() error {
		defer stdout.Close()
		decoder := json.NewDecoder(stdout)

		if err := decoder.Decode(schema); err != nil {
			return fmt.Errorf("failed to decode schema, %w", err)
		}
		logEntry.Debug("runInference: Done reading schema")

		return nil
	})

	logEntry.Info("runInference: launching")
	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to run flow-schema-inference: %w", err)
	}

	errGroup.Go(cmd.Wait)

	err = errGroup.Wait()
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func peekAtStream(ctx context.Context, client *kinesis.Kinesis, streamName string, peekAtMost uint) (chan json.RawMessage, uint, error) {
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
