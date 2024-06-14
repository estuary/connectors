package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/estuary/connectors/filesink"
	m "github.com/estuary/connectors/go/protocols/materialize"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
)

const (
	// Iceberg tables qualify for automatic compaction by AWS Glue (if enabled) if the
	// write.target-file-size-bytes is in the range of 128MB to 512MB, and the default
	// write.target-file-size-bytes is 512 MB, so we'll shoot for that as a maximum file size to
	// write.
	fileSizeLimit = 512 * 1024 * 1024
)

type binding struct {
	path       []string
	pqSchema   enc.ParquetSchema
	includeDoc bool
	stateKey   string
}

type connectorState struct {
	BindingStates map[string]*bindingState `json:"bindingStates,omitempty"`
}

func (cs connectorState) Validate() error { return nil }

type bindingState struct {
	// We'll only append files to tables which have the snapshot property checkpoint equal to
	// PreviousCheckpoint. When a file is appended, in that same transaction to the file
	// "checkpoint" is updated to the current checkpoint. Runtime checkpoints are hashed to minimize
	// storage overhead of table snapshot files.
	PreviousCheckpoint string   `json:"previousCheckpoint,omitempty"`
	CurrentCheckpoint  string   `json:"currentCheckpoint,omitempty"`
	FilePaths          []string `json:"fileKeys,omitempty"`
}

func hashCheckpoint(cp *protocol.Checkpoint) (string, error) {
	mcp, err := cp.Marshal()
	if err != nil {
		return "", fmt.Errorf("marshalling checkpoint: %w", err)
	}

	return fmt.Sprintf("%016x", xxhash.Sum64(mcp)), nil
}

type transactor struct {
	catalog        *glueCatalog
	bindings       []binding
	bucket         string
	prefix         string
	store          *filesink.S3Store
	state          connectorState
	uploadInterval time.Duration
}

func (t *transactor) AckDelay() time.Duration {
	return t.uploadInterval
}

func (t *transactor) UnmarshalState(state json.RawMessage) error {
	if err := pf.UnmarshalStrict(state, &t.state); err != nil {
		return err
	}

	if t.state.BindingStates == nil {
		t.state.BindingStates = make(map[string]*bindingState)
	}

	for _, b := range t.bindings {
		if _, ok := t.state.BindingStates[b.stateKey]; !ok {
			t.state.BindingStates[b.stateKey] = &bindingState{}
		}
	}

	return nil

}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var ctx = it.Context()
	var encoder *enc.ParquetEncoder
	var group errgroup.Group
	var states = t.state.BindingStates

	startFile := func(b binding) {
		// Start uploading a new file, either because the binding changed or because the prior file
		// got sufficiently large.
		r, w := io.Pipe()

		key := path.Join(t.prefix, uuid.New().String()+".parquet")
		s3path := fmt.Sprintf("s3://%s/%s", t.bucket, key)
		states[b.stateKey].FilePaths = append(states[b.stateKey].FilePaths, s3path)

		group.Go(func() error {
			ll := log.WithFields(log.Fields{
				"path":  s3path,
				"table": pathToFQN(b.path),
			})

			ll.Info("started uploading file")
			if err := t.store.PutStream(ctx, r, key); err != nil {
				r.CloseWithError(err)
				return fmt.Errorf("uploading file: %w", err)
			}
			ll.Info("finished uploading file")

			return nil
		})

		encoder = enc.NewParquetEncoder(w, b.pqSchema, enc.WithParquetCompression(enc.Snappy))
	}

	finishFile := func() error {
		if encoder == nil {
			return nil
		} else if err := encoder.Close(); err != nil {
			return fmt.Errorf("closing encoder: %w", err)
		} else if err := group.Wait(); err != nil {
			return fmt.Errorf("group.Wait(): %w", err)
		}

		encoder = nil
		return nil
	}

	lastBinding := -1
	for it.Next() {
		b := t.bindings[it.Binding]

		if lastBinding != -1 && lastBinding != it.Binding {
			if err := finishFile(); err != nil {
				return nil, fmt.Errorf("finishFile after binding change: %w", err)
			}
		}
		lastBinding = it.Binding

		if encoder == nil {
			startFile(b)
		}

		row := make([]any, 0, len(it.Key)+len(it.Values)+1)
		row = append(row, it.Key.ToInterface()...)
		row = append(row, it.Values.ToInterface()...)
		if b.includeDoc {
			row = append(row, it.RawJSON)
		}

		if err := encoder.Encode(row); err != nil {
			return nil, fmt.Errorf("encoding row: %w", err)
		}

		if encoder.Written() > fileSizeLimit {
			if err := finishFile(); err != nil {
				return nil, fmt.Errorf("finishFile on file size limit: %w", err)
			}
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("store iterator error: %w", err)
	}

	if err := finishFile(); err != nil {
		return nil, fmt.Errorf("final finishFile: %w", err)
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		// The driver checkpoint for this transaction will include the list of files upload for each
		// binding, the previously committed runtime checkpoint, and current runtime checkpoint. We
		// will append files to tables that still are still checkpointed with the previous
		// checkpoint in Acknowledge.
		currentCp, err := hashCheckpoint(runtimeCheckpoint)
		if err != nil {
			return nil, m.FinishedOperation(err)
		}

		for _, bs := range t.state.BindingStates {
			bs.PreviousCheckpoint, bs.CurrentCheckpoint = bs.CurrentCheckpoint, currentCp
		}

		checkpointJSON, err := json.Marshal(t.state)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
	}, nil
}

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	for _, b := range t.bindings {
		state := t.state.BindingStates[b.stateKey]

		if len(state.FilePaths) == 0 {
			continue // no data for this binding
		}

		ll := log.WithField("table", pathToFQN(b.path))

		ll.Info("starting appendFiles for table")
		if err := t.catalog.appendFiles(b.path, state.FilePaths, state.PreviousCheckpoint, state.CurrentCheckpoint); err != nil {
			return nil, fmt.Errorf("appendFiles for %s: %w", b.path, err)
		}
		ll.Info("finished appendFiles for table")

		state.FilePaths = nil // reset for next txn
	}

	return nil, nil
}

func (t *transactor) Destroy() {}
