package schema_inference

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// How long should we allow reading from each stream to run. Note: empty streams
// will wait for documents until this timeout triggers.
const DISCOVER_READ_TIMEOUT = time.Second * 10

// How long should we allow schema inference to run.
const DISCOVER_INFER_TIMEOUT = time.Second * 5

type Schema = json.RawMessage
type Document = json.RawMessage
type DocumentSource = func(ctx context.Context, logEntry *log.Entry) (docCh <-chan Document, count uint, err error)

func Run(ctx context.Context, streamName string, peekFunc DocumentSource) (Schema, error) {
	var logEntry = log.WithField("stream", streamName)
	var peekCtx, cancelPeek = context.WithTimeout(ctx, DISCOVER_READ_TIMEOUT)
	defer cancelPeek()

	docCh, docCount, err := peekFunc(peekCtx, logEntry)
	if err != nil {
		return nil, fmt.Errorf("schema discovery for stream `%s` failed: %w", streamName, err)
	} else if docCount == 0 {
		return nil, fmt.Errorf("no documents discovered for stream: %v", streamName)
	}

	logEntry.WithField("count", docCount).Info("Got documents for stream")

	var inferCtx, cancelInfer = context.WithTimeout(ctx, DISCOVER_INFER_TIMEOUT)
	defer cancelInfer()

	return infer(inferCtx, logEntry, docCh)
}

func infer(ctx context.Context, logEntry *log.Entry, docsCh <-chan Document) (Schema, error) {
	var (
		err      error
		errGroup *errgroup.Group
		schema   = json.RawMessage{}
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

		if err := decoder.Decode(&schema); err != nil {
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
