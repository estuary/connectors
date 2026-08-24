package sqlcapture

// This file provides mock implementations of the Database and ReplicationStream
// interfaces for unit tests and benchmarks within this package. Methods whose
// behavior tests typically need to control have a corresponding hook function
// field, and every hook has a benign default so tests only configure the parts
// they exercise.

import (
	"context"
	"io"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/invopop/jsonschema"
	"github.com/segmentio/encoding/json"
	"google.golang.org/grpc/metadata"
)

// mockDatabase is a configurable in-memory implementation of the Database interface.
type mockDatabase struct {
	// Stream is returned by ReplicationStream calls. When nil, a fresh
	// mockReplicationStream with default behavior is returned instead.
	Stream ReplicationStream

	// FallbackKey is returned by FallbackCollectionKey calls.
	FallbackKey []string

	// ScanTableChunkFunc handles ScanTableChunk calls. The default reports the
	// table as fully backfilled without emitting any rows.
	ScanTableChunkFunc func(ctx context.Context, info *DiscoveryInfo, state *TableState, backfillFilter string, callback func(event ChangeEvent) error) (bool, []byte, error)

	// ListTablesFunc handles ListTables calls. The default lists no tables.
	ListTablesFunc func(ctx context.Context) ([]TableID, error)

	// DiscoverTableDetailsFunc handles DiscoverTableDetails calls. The default
	// returns an empty map.
	DiscoverTableDetailsFunc func(ctx context.Context, tables []TableID) (map[StreamID]*DiscoveryInfo, error)

	// EstimatedRowCountsFunc handles EstimatedRowCounts calls. The default
	// reports a count of zero for every requested table.
	EstimatedRowCountsFunc func(ctx context.Context, tables []TableID) (map[TableID]int, error)

	// ShouldBackfillFunc handles ShouldBackfill calls. The default is to
	// backfill every table.
	ShouldBackfillFunc func(streamID StreamID) bool
}

func (db *mockDatabase) Close(ctx context.Context) error { return nil }

func (db *mockDatabase) ReplicationStream(ctx context.Context, startCursor json.RawMessage) (ReplicationStream, error) {
	if db.Stream != nil {
		return db.Stream, nil
	}
	return &mockReplicationStream{}, nil
}

func (db *mockDatabase) ScanTableChunk(ctx context.Context, info *DiscoveryInfo, state *TableState, backfillFilter string, callback func(event ChangeEvent) error) (bool, []byte, error) {
	if db.ScanTableChunkFunc != nil {
		return db.ScanTableChunkFunc(ctx, info, state, backfillFilter, callback)
	}
	return true, nil, nil
}

func (db *mockDatabase) ListTables(ctx context.Context) ([]TableID, error) {
	if db.ListTablesFunc != nil {
		return db.ListTablesFunc(ctx)
	}
	return nil, nil
}

func (db *mockDatabase) DiscoverTableDetails(ctx context.Context, tables []TableID) (map[StreamID]*DiscoveryInfo, error) {
	if db.DiscoverTableDetailsFunc != nil {
		return db.DiscoverTableDetailsFunc(ctx, tables)
	}
	return make(map[StreamID]*DiscoveryInfo), nil
}

func (db *mockDatabase) EstimatedRowCounts(ctx context.Context, tables []TableID) (map[TableID]int, error) {
	if db.EstimatedRowCountsFunc != nil {
		return db.EstimatedRowCountsFunc(ctx, tables)
	}
	var counts = make(map[TableID]int, len(tables))
	for _, table := range tables {
		counts[table] = 0
	}
	return counts, nil
}

func (db *mockDatabase) ShouldBackfill(streamID StreamID) bool {
	if db.ShouldBackfillFunc != nil {
		return db.ShouldBackfillFunc(streamID)
	}
	return true
}

func (db *mockDatabase) TranslateDBToJSONType(column ColumnInfo, isPrimaryKey bool) (*jsonschema.Schema, error) {
	return &jsonschema.Schema{Type: "string"}, nil
}

func (db *mockDatabase) SourceMetadataSchema(writeSchema bool) *jsonschema.Schema {
	return &jsonschema.Schema{Type: "object"}
}

func (db *mockDatabase) HistoryMode() bool                         { return false }
func (db *mockDatabase) RediscoveryInterval() time.Duration        { return 0 }
func (db *mockDatabase) ReplicationPollingInterval() time.Duration { return 0 }
func (db *mockDatabase) FallbackCollectionKey() []string           { return db.FallbackKey }
func (db *mockDatabase) RequestTxIDs(schema, table string)         {}
func (db *mockDatabase) SetupPrerequisites(ctx context.Context) []error {
	return nil
}
func (db *mockDatabase) SetupTablePrerequisites(ctx context.Context, tables []TableID) map[TableID]error {
	return nil
}
func (db *mockDatabase) ReplicationDiagnostics(ctx context.Context) error { return nil }
func (db *mockDatabase) PeriodicChecks(ctx context.Context) error         { return nil }

// mockReplicationStream is a configurable in-memory implementation of the
// ReplicationStream interface.
type mockReplicationStream struct {
	// ActivateTableFunc handles ActivateTable calls. The default is a no-op.
	ActivateTableFunc func(ctx context.Context, streamID StreamID, keyColumns []string, info *DiscoveryInfo, metadata json.RawMessage) error

	// StreamToFenceFunc handles StreamToFence calls. The default returns
	// immediately without emitting any events.
	StreamToFenceFunc func(ctx context.Context, fenceAfter time.Duration, callback func(event DatabaseEvent) error) error
}

func (s *mockReplicationStream) ActivateTable(ctx context.Context, streamID StreamID, keyColumns []string, info *DiscoveryInfo, metadata json.RawMessage) error {
	if s.ActivateTableFunc != nil {
		return s.ActivateTableFunc(ctx, streamID, keyColumns, info, metadata)
	}
	return nil
}

func (s *mockReplicationStream) StartReplication(ctx context.Context, discovery map[StreamID]*DiscoveryInfo) error {
	return nil
}

func (s *mockReplicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event DatabaseEvent) error) error {
	if s.StreamToFenceFunc != nil {
		return s.StreamToFenceFunc(ctx, fenceAfter, callback)
	}
	return nil
}

func (s *mockReplicationStream) Acknowledge(ctx context.Context, cursor json.RawMessage) error {
	return nil
}

func (s *mockReplicationStream) Close(ctx context.Context) error { return nil }

// mockCommitEvent is a minimal CommitEvent implementation for use with
// StreamToFence hooks.
type mockCommitEvent struct {
	Cursor json.RawMessage
}

func (*mockCommitEvent) IsDatabaseEvent() {}
func (*mockCommitEvent) IsCommitEvent()   {}

func (e *mockCommitEvent) String() string { return "mockCommitEvent(" + string(e.Cursor) + ")" }

func (e *mockCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	return append(buf, e.Cursor...), nil
}

// mockChangeEvent is a minimal ChangeEvent implementation for use with
// StreamToFence hooks. Serialization just appends the prebuilt document.
type mockChangeEvent struct {
	ID     StreamID
	RowKey []byte
	Doc    json.RawMessage
}

func (*mockChangeEvent) IsDatabaseEvent() {}
func (*mockChangeEvent) IsChangeEvent()   {}

func (e *mockChangeEvent) String() string     { return "mockChangeEvent(" + e.ID.String() + ")" }
func (e *mockChangeEvent) StreamID() StreamID { return e.ID }
func (e *mockChangeEvent) GetRowKey() []byte  { return e.RowKey }

func (e *mockChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
	return append(buf, e.Doc...), nil
}

// newMockPullOutput returns a PullOutput whose underlying stream accepts all
// messages at zero cost, for tests and benchmarks which exercise code paths
// that emit documents or checkpoints without caring about the bytes produced.
func newMockPullOutput() *boilerplate.PullOutput {
	return &boilerplate.PullOutput{Connector_CaptureServer: mockOutputStream{}}
}

// mockOutputStream is a no-op implementation of the capture output stream
// underlying a boilerplate.PullOutput. Sends are discarded and receives
// report EOF.
type mockOutputStream struct{}

func (mockOutputStream) Send(*pc.Response) error      { return nil }
func (mockOutputStream) Recv() (*pc.Request, error)   { return nil, io.EOF }
func (mockOutputStream) Context() context.Context     { return context.Background() }
func (mockOutputStream) SendMsg(any) error            { return nil }
func (mockOutputStream) RecvMsg(any) error            { return io.EOF }
func (mockOutputStream) SendHeader(metadata.MD) error { return nil }
func (mockOutputStream) SetHeader(metadata.MD) error  { return nil }
func (mockOutputStream) SetTrailer(metadata.MD)       {}
