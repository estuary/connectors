package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
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
	// partitionSpec is nil for unpartitioned tables; partitionCols carries the
	// per-column metadata used to compute partition values for each row.
	partitionSpec *iceberg.PartitionSpec
	partitionCols []partitionColumn
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
	// PartitionedFiles is populated only for tables with a partition spec; it
	// carries the per-file partition values and counts needed to rebuild
	// DataFiles in Acknowledge. FileKeys is left empty for partitioned tables.
	PartitionedFiles []fileEntry `json:"partitionedFiles,omitempty"`
}

func (bs *bindingState) hasData() bool {
	return len(bs.FileKeys) > 0 || len(bs.PartitionedFiles) > 0
}

// fileEntry describes a single uploaded parquet file for a partitioned table.
type fileEntry struct {
	S3Path      string `json:"s3Path"`
	RecordCount int64  `json:"recordCount"`
	FileSize    int64  `json:"fileSize"`
	// Partition maps partition field ID (as a string, since JSON object keys
	// must be strings) to the canonical partition value, stored as raw JSON and
	// decoded per the field's result type in partitionData.
	Partition map[string]json.RawMessage `json:"partition,omitempty"`
}

// partitionData decodes the stored partition values into the canonical iceberg
// Go types keyed by partition field ID, suitable for iceberg.NewDataFileBuilder.
// A nil value (SQL NULL partition) is preserved as nil.
func (fe fileEntry) partitionData(cols []partitionColumn) (map[int]any, error) {
	out := make(map[int]any, len(cols))
	for _, col := range cols {
		raw, ok := fe.Partition[strconv.Itoa(col.fieldID)]
		if !ok || string(raw) == "null" {
			out[col.fieldID] = nil
			continue
		}
		v, err := decodePartitionValue(raw, col.transform.ResultType(col.sourceType))
		if err != nil {
			return nil, fmt.Errorf("partition field %q: %w", col.name, err)
		}
		out[col.fieldID] = v
	}
	return out, nil
}

func decodePartitionValue(raw json.RawMessage, resultType iceberg.Type) (any, error) {
	switch resultType {
	case iceberg.PrimitiveTypes.Int32:
		var v int32
		return v, json.Unmarshal(raw, &v)
	case iceberg.PrimitiveTypes.Int64:
		var v int64
		return v, json.Unmarshal(raw, &v)
	case iceberg.PrimitiveTypes.Float64:
		var v float64
		return v, json.Unmarshal(raw, &v)
	case iceberg.PrimitiveTypes.Bool:
		var v bool
		return v, json.Unmarshal(raw, &v)
	case iceberg.PrimitiveTypes.String:
		var v string
		return v, json.Unmarshal(raw, &v)
	case iceberg.PrimitiveTypes.Binary:
		var v []byte
		return v, json.Unmarshal(raw, &v)
	case iceberg.PrimitiveTypes.Date:
		var v iceberg.Date
		return v, json.Unmarshal(raw, &v)
	case iceberg.PrimitiveTypes.TimestampTz:
		var v iceberg.Timestamp
		return v, json.Unmarshal(raw, &v)
	default:
		return nil, fmt.Errorf("unsupported partition result type %s", resultType)
	}
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
	// openFile tracks the parquet file currently being written so finishFile can
	// record its metadata. For partitioned tables, partitionData holds the single
	// partition tuple that every row in the file belongs to.
	type openFile struct {
		b             binding
		s3Path        string
		partitionData map[int]any
	}

	var ctx = it.Context()
	var pqw *writer.ParquetWriter
	var open *openFile
	var group errgroup.Group
	var states = t.state.BindingStates

	startFile := func(b binding, partitionData map[int]any) {
		// Start uploading a new file, either because the binding changed, the partition changed, or
		// because the prior file got sufficiently large.
		r, w := io.Pipe()

		key, s3Path := filePath(b.catalogTablePath)
		// Partitioned tables track files (with their partition tuples) in
		// PartitionedFiles when the file is finished; unpartitioned tables only
		// need the file key, recorded up front.
		if b.partitionSpec == nil {
			states[b.stateKey].FileKeys = append(states[b.stateKey].FileKeys, s3Path)
		}

		group.Go(func() error {
			if err := t.store.PutStream(ctx, r, key); err != nil {
				r.CloseWithError(err)
				return fmt.Errorf("uploading file: %w", err)
			}

			return nil
		})

		pqw = writer.NewParquetWriter(w, b.pqSchema, writer.WithParquetCompression(writer.Snappy))
		open = &openFile{b: b, s3Path: s3Path, partitionData: partitionData}
	}

	finishFile := func() error {
		if pqw == nil {
			return nil
		}
		if err := pqw.Close(); err != nil {
			return fmt.Errorf("closing parquet writer: %w", err)
		}

		if open.b.partitionSpec != nil {
			md, err := pqw.FileMetadata()
			if err != nil {
				return fmt.Errorf("reading parquet metadata: %w", err)
			}
			if md.NumRows > 0 {
				part := make(map[string]json.RawMessage, len(open.partitionData))
				for fid, v := range open.partitionData {
					rawv, err := json.Marshal(v)
					if err != nil {
						return fmt.Errorf("marshaling partition value: %w", err)
					}
					part[strconv.Itoa(fid)] = rawv
				}
				bs := states[open.b.stateKey]
				bs.PartitionedFiles = append(bs.PartitionedFiles, fileEntry{
					S3Path:      open.s3Path,
					RecordCount: md.NumRows,
					FileSize:    int64(pqw.Written()),
					Partition:   part,
				})
			}
		}

		if err := group.Wait(); err != nil {
			return fmt.Errorf("group.Wait(): %w", err)
		}

		pqw = nil
		open = nil
		return nil
	}

	lastBinding := -1
	var currentPartKey string
	var havePartKey bool
	for it.Next(false) {
		b := t.bindings[it.Binding]

		if lastBinding != -1 && lastBinding != it.Binding {
			if err := finishFile(); err != nil {
				return nil, fmt.Errorf("finishFile after binding change: %w", err)
			}
			havePartKey = false
		}
		lastBinding = it.Binding

		row, err := b.mapped.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting row: %w", err)
		}

		var partKey string
		var partitionData map[int]any
		if b.partitionSpec != nil {
			if partKey, partitionData, err = partitionValues(row, b.partitionCols); err != nil {
				return nil, fmt.Errorf("computing partition: %w", err)
			}
			// A change in partition since the last row means the open file must
			// be flushed so each file holds a single partition.
			if pqw != nil && (!havePartKey || partKey != currentPartKey) {
				if err := finishFile(); err != nil {
					return nil, fmt.Errorf("finishFile after partition change: %w", err)
				}
				havePartKey = false
			}
		}

		if pqw == nil {
			startFile(b, partitionData)
			currentPartKey = partKey
			havePartKey = true
		}

		if err := pqw.Write(row); err != nil {
			return nil, fmt.Errorf("writing row: %w", err)
		}

		if pqw.Written() > fileSizeLimit {
			if err := finishFile(); err != nil {
				return nil, fmt.Errorf("finishFile on file size limit: %w", err)
			}
			havePartKey = false
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

			if !bindingState.hasData() {
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

		if !bindingState.hasData() {
			continue // no data for this binding
		}

		ll := log.WithFields(log.Fields{
			"table":              pathToFQN(b.path),
			"previousCheckpoint": bindingState.PreviousCheckpoint,
			"currentCheckpoint":  bindingState.CurrentCheckpoint,
		})

		ll.Info("starting append for table")
		if b.partitionSpec != nil {
			if err := t.catalog.appendDataFiles(ctx, t.materialization, b.path, bindingState.PartitionedFiles, b.partitionCols, bindingState.PreviousCheckpoint, bindingState.CurrentCheckpoint); err != nil {
				return nil, fmt.Errorf("appendDataFiles for %s: %w", b.path, err)
			}
		} else {
			if err := t.catalog.appendFiles(ctx, t.materialization, b.path, bindingState.FileKeys, bindingState.PreviousCheckpoint, bindingState.CurrentCheckpoint); err != nil {
				return nil, fmt.Errorf("appendFiles for %s: %w", b.path, err)
			}
		}
		ll.Info("finished append for table")

		bindingState.FileKeys = nil         // reset for next txn
		bindingState.PartitionedFiles = nil // reset for next txn
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
