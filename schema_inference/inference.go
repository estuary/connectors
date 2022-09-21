package schema_inference

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var NO_DOCUMENTS_FOUND = fmt.Errorf("no documents discovered for stream")

type Schema = json.RawMessage
type Document = json.RawMessage
type DocumentSource = func(ctx context.Context, logEntry *log.Entry) (docCh <-chan Document, count uint, err error)

func Run(ctx context.Context, logEntry *log.Entry, docsCh <-chan Document) (Schema, error) {
	var (
		err      error
		errGroup *errgroup.Group
		schema   = Schema{}
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

		var count int
		for doc := range docsCh {
			count++
			if err := encoder.Encode(doc); err != nil {
				return fmt.Errorf("failed to encode document: %w", err)
			}
		}
		logEntry.WithField("count", count).Info("runInference: Done sending documents")

		return nil
	})

	errGroup.Go(func() error {
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

	if err := errGroup.Wait(); err != nil {
		return nil, err
	} else if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return schema, nil
}
