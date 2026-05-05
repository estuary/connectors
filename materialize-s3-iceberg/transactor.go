package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/estuary/connectors/filesink"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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

func tablePath(bucket, prefix, namespace, table string, style LocationStyle) string {
	return fmt.Sprintf("s3://%s/", path.Join(bucket, prefix, createLocationSuffix(namespace, table, style)))
}

// filePath returns path information for a new file, including both the object
// key and full s3Path in s3:// format.
func filePath(catalogTablePath string) (fileKey string, s3Path string) {
	s3Path = strings.TrimSuffix(catalogTablePath, "/") + "/data/" + uuid.New().String() + ".parquet"
	parts := strings.Split(strings.TrimPrefix(s3Path, "s3://"), "/")
	fileKey = strings.Join(parts[1:], "/")

	return
}

type binding struct {
	path             []string
	pqSchema         writer.ParquetSchema
	stateKey         string
	catalogTablePath string
	mapped           *boilerplate.MappedBinding[config, resource, mappedType]
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
	FileKeys           []string `json:"fileKeys"`
}

func hashCheckpoint(cp *protocol.Checkpoint) (string, error) {
	mcp, err := cp.Marshal()
	if err != nil {
		return "", fmt.Errorf("marshalling checkpoint: %w", err)
	}

	return fmt.Sprintf("%016x", xxhash.Sum64(mcp)), nil
}

type transactor struct {
	materialization string
	catalog         *catalog
	bindings        []binding
	bucket          string
	prefix          string
	store           *filesink.S3Store
	state           connectorState
}

var _ m.Transactor = &transactor{}

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
	var pqw *writer.ParquetWriter
	var group errgroup.Group
	var states = t.state.BindingStates

	startFile := func(b binding) {
		// Start uploading a new file, either because the binding changed or because the prior file
		// got sufficiently large.
		r, w := io.Pipe()

		key, s3Path := filePath(b.catalogTablePath)
		states[b.stateKey].FileKeys = append(states[b.stateKey].FileKeys, s3Path)

		group.Go(func() error {
			if err := t.store.PutStream(ctx, r, key); err != nil {
				r.CloseWithError(err)
				return fmt.Errorf("uploading file: %w", err)
			}

			return nil
		})

		pqw = writer.NewParquetWriter(w, b.pqSchema, writer.WithParquetCompression(writer.Snappy))
	}

	finishFile := func() error {
		if pqw == nil {
			return nil
		} else if err := pqw.Close(); err != nil {
			return fmt.Errorf("closing parquet writer: %w", err)
		} else if err := group.Wait(); err != nil {
			return fmt.Errorf("group.Wait(): %w", err)
		}

		pqw = nil
		return nil
	}

	lastBinding := -1
	for it.Next(false) {
		b := t.bindings[it.Binding]

		if lastBinding != -1 && lastBinding != it.Binding {
			if err := finishFile(); err != nil {
				return nil, fmt.Errorf("finishFile after binding change: %w", err)
			}
		}
		lastBinding = it.Binding

		if pqw == nil {
			startFile(b)
		}

		row, err := b.mapped.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting row: %w", err)
		}

		if err := pqw.Write(row); err != nil {
			return nil, fmt.Errorf("writing row: %w", err)
		}

		if pqw.Written() > fileSizeLimit {
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

		for _, b := range t.bindings {
			bindingState := t.state.BindingStates[b.stateKey]

			if len(bindingState.FileKeys) == 0 {
				continue // no data for this binding
			}

			bindingState.PreviousCheckpoint, bindingState.CurrentCheckpoint = bindingState.CurrentCheckpoint, currentCp
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
		bindingState := t.state.BindingStates[b.stateKey]

		if len(bindingState.FileKeys) == 0 {
			continue // no data for this binding
		}

		ll := log.WithFields(log.Fields{
			"table":              pathToFQN(b.path),
			"previousCheckpoint": bindingState.PreviousCheckpoint,
			"currentCheckpoint":  bindingState.CurrentCheckpoint,
		})

		ll.Info("starting appendFiles for table")
		appendCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		if err := t.catalog.appendFiles(appendCtx, t.materialization, b.path, bindingState.FileKeys, bindingState.PreviousCheckpoint, bindingState.CurrentCheckpoint); err != nil {
			return nil, fmt.Errorf("appendFiles for %s: %w", b.path, err)
		}
		ll.Info("finished appendFiles for table")

		bindingState.FileKeys = nil // reset for next txn
	}

	checkpointJSON, err := json.Marshal(t.state)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint json: %w", err)
	}

	return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, range_ pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) Destroy() {}
