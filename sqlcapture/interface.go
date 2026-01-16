package sqlcapture

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/invopop/jsonschema"
	"github.com/segmentio/encoding/json"
)

type StreamID struct {
	Schema string
	Table  string
}

func (s StreamID) String() string {
	return fmt.Sprintf("%s.%s", s.Schema, s.Table)
}

// EncodeKey returns an unambiguous string representation of the StreamID
// suitable for use as a map key in serialized cursors. The schema and table
// components are encoded so that periods can be used as the separator.
func (s StreamID) EncodeKey() string {
	var encodeComponent = func(str string) string {
		str = strings.ReplaceAll(str, "%", "%25")
		str = strings.ReplaceAll(str, ".", "%2E")
		return str
	}
	return encodeComponent(s.Schema) + "." + encodeComponent(s.Table)
}

// ParseStreamIDKey parses a string key (produced by EncodeKey) back into a StreamID.
func ParseStreamIDKey(key string) (StreamID, error) {
	parts := strings.SplitN(key, ".", 2)
	if len(parts) != 2 {
		return StreamID{}, fmt.Errorf("invalid stream ID key %q: expected schema.table format", key)
	}
	schema, err := url.PathUnescape(parts[0])
	if err != nil {
		return StreamID{}, fmt.Errorf("invalid stream ID key %q: %w", key, err)
	}
	table, err := url.PathUnescape(parts[1])
	if err != nil {
		return StreamID{}, fmt.Errorf("invalid stream ID key %q: %w", key, err)
	}
	return StreamID{Schema: schema, Table: table}, nil
}

// This is a temporary hack to allow us to plumb through a feature flag setting for
// the gradual rollout of the "no longer lowercase stream IDs" behavior. This will
// eventually be the standard for all connectors if we can do it without breaking
// anything, but for now we want to be cautious and make it an opt-in.
//
// In an abstract sense we shouldn't be using a global variable for this, but as a
// practical matter there's only ever one capture running at a time so this is fine.
var LowercaseStreamIDs = true

// JoinStreamID combines a namespace and a stream name into a dotted name like "public.foo_table".
func JoinStreamID(namespace, stream string) StreamID {
	if LowercaseStreamIDs {
		namespace = strings.ToLower(namespace)
		stream = strings.ToLower(stream)
	}
	return StreamID{Schema: namespace, Table: stream}
}

// ChangeOp encodes a change operation type.
// It's compatible with Debezium's change event representation.
// TODO(johnny): Factor into a shared package.
type ChangeOp string

const (
	// InsertOp is an INSERT operation.
	InsertOp ChangeOp = "c"
	// UpdateOp is an UPDATE operation.
	UpdateOp ChangeOp = "u"
	// DeleteOp is a DELETE operation.
	DeleteOp ChangeOp = "d"
)

// SourceCommon is common source metadata for data capture events.
// It's a subset of the corresponding Debezium message definition.
// Our design goal is a high signal-to-noise representation which can be
// trivially converted to a Debezium equivalent if desired. See:
//
//	https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events
//
// TODO(johnny): Factor into a shared package.
type SourceCommon struct {
	Millis   int64  `json:"ts_ms,omitempty" jsonschema:"description=Unix timestamp (in millis) at which this event was recorded by the database."`
	Schema   string `json:"schema" jsonschema:"description=Database schema (namespace) of the event."`
	Snapshot bool   `json:"snapshot,omitempty" jsonschema:"description=Snapshot is true if the record was produced from an initial table backfill and unset if produced from the replication log."`
	Table    string `json:"table" jsonschema:"description=Database table of the event."`

	// Implementor's note: `snapshot` is a mildly contentious name, because it
	// could imply involvement of a database snapshot that is not, in fact, used.
	// A better term might be "backfill", but this is an established term in the
	// CDC ecosystem and it's water under the bridge now.

	// Fields which are part of the generalized Debezium representation
	// but are not included here:
	// * `version` string of the connector.
	// * `connector` is the connector name.
	// * `database` is the logical database name.
	// * `name` string of the capture. Debezium connectors use strings like
	//   "PostgreSQL_server", and it's expected that this string composes with
	//   `schema` and `table` to produce an identifier under which an Avro schema is
	//   registered within a schema registry, like "PostgreSQL_server.inventory.customers.Value".
	//
	// All of `version`, `connector`, and `database` are defined by the catalog specification
	// and are available through logs. They seem noisy and low-signal here.
}

// StreamID combines the Schema and Table properties using JoinStreamID()
func (sc SourceCommon) StreamID() StreamID {
	return JoinStreamID(sc.Schema, sc.Table)
}

// SourceMetadata is source-specific metadata about data capture events.
type SourceMetadata interface {
	Common() SourceCommon
}

// ChangeEvent represents an Insert/Update/Delete operation on a specific row in the database.
type ChangeEvent interface {
	IsDatabaseEvent() // Tag method
	IsChangeEvent()   // Tag method

	String() string                    // Returns a string representation of the event, suitable for logging.
	StreamID() StreamID                // Returns the stream ID of the event.
	GetRowKey() []byte                 // Returns the serialized row key for the change event.
	AppendJSON([]byte) ([]byte, error) // Serializes the change event to JSON as an output document and appends to the provided buffer.
}

// OldChangeEvent is the old struct used to represent a change event. It implements ChangeEvent.
type OldChangeEvent struct {
	Operation ChangeOp
	RowKey    []byte
	Source    SourceMetadata
	Before    map[string]interface{}
	After     map[string]interface{}
}

func (OldChangeEvent) IsDatabaseEvent() {}
func (OldChangeEvent) IsChangeEvent()   {}

func (e *OldChangeEvent) String() string {
	switch e.Operation {
	case InsertOp:
		return fmt.Sprintf("Insert(%s)", e.Source.Common().StreamID())
	case UpdateOp:
		return fmt.Sprintf("Update(%s)", e.Source.Common().StreamID())
	case DeleteOp:
		return fmt.Sprintf("Delete(%s)", e.Source.Common().StreamID())
	}
	return fmt.Sprintf("UnknownChange(%s)", e.Source.Common().StreamID())
}

func (e *OldChangeEvent) StreamID() StreamID {
	return e.Source.Common().StreamID()
}

func (e *OldChangeEvent) GetRowKey() []byte {
	return e.RowKey
}

// MarshalJSON implements serialization of a ChangeEvent to JSON.
//
// Note that this implementation is destructive, but that's okay because
// emitting the serialized JSON is the last thing we ever do with a change.
func (e *OldChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
	var record map[string]any
	var meta = struct {
		Operation ChangeOp       `json:"op"`
		Source    SourceMetadata `json:"source"`
		Before    map[string]any `json:"before,omitempty"`
	}{
		Operation: e.Operation,
		Source:    e.Source,
		Before:    nil,
	}
	switch e.Operation {
	case InsertOp:
		record = e.After // Before is never used.
	case UpdateOp:
		meta.Before, record = e.Before, e.After
	case DeleteOp:
		record = e.Before // After is never used.
	}
	if record == nil {
		record = make(map[string]any)
	}
	record["_meta"] = &meta

	// Marshal to JSON and append to the provided buffer.
	return json.Append(buf, record, json.EscapeHTML|json.SortMapKeys)
}

// CommitEvent represents a transaction commit.
type CommitEvent interface {
	IsDatabaseEvent() // Tag method
	IsCommitEvent()   // Tag method
	String() string   // Returns a string representation of the event, suitable for logging.

	AppendJSON([]byte) ([]byte, error) // Serializes the stream cursor as a JSON value and appends to the provided buffer.
}

// OldFlushEvent informs the generic sqlcapture logic about transaction boundaries.
type OldFlushEvent struct {
	// The cursor value at which the current transaction was committed.
	Cursor json.RawMessage
}

func (OldFlushEvent) IsDatabaseEvent() {}
func (OldFlushEvent) IsCommitEvent()   {}

func (evt *OldFlushEvent) String() string {
	return fmt.Sprintf("OldFlushEvent(%s)", evt.Cursor)
}

func (evt *OldFlushEvent) AppendJSON(buf []byte) ([]byte, error) {
	return append(buf, evt.Cursor...), nil
}

// MetadataEvent informs the generic sqlcapture logic about changes to
// the per-table metadata JSON.
type MetadataEvent struct {
	StreamID StreamID
	Metadata json.RawMessage
}

// KeepaliveEvent informs the generic sqlcapture logic that the replication stream
// is still active and processing WAL entries which don't require other events.
type KeepaliveEvent struct{}

// TableDropEvent informs the generic sqlcapture logic that the table in question
// has been dropped and will not be producing any further changes.
type TableDropEvent struct {
	StreamID StreamID
	Cause    string // Informational description of what happened
}

// A DatabaseEvent can be a ChangeEvent, FlushEvent, MetadataEvent, or KeepaliveEvent.
type DatabaseEvent interface {
	IsDatabaseEvent()
	String() string
}

func (*MetadataEvent) IsDatabaseEvent()  {}
func (*KeepaliveEvent) IsDatabaseEvent() {}
func (*TableDropEvent) IsDatabaseEvent() {}

func (evt *MetadataEvent) String() string  { return fmt.Sprintf("MetadataEvent(%s)", evt.StreamID) }
func (*KeepaliveEvent) String() string     { return "KeepaliveEvent" }
func (evt *TableDropEvent) String() string { return fmt.Sprintf("TableDropEvent(%s)", evt.StreamID) }

// A TableID represents the schema/table name of a table.
//
// Unlike a StreamID it is guaranteed to not to be normalized, meaning that it
// can be given back to the database in queries as needed.
type TableID struct {
	Schema string
	Table  string
}

// Database represents the operations which must be performed on a specific database
// during the course of a capture in order to perform discovery, backfill preexisting
// data, and process replicated change events.
type Database interface {
	// Close shuts down the database connection.
	Close(ctx context.Context) error
	// ReplicationStream constructs a new ReplicationStream object, from which
	// a neverending sequence of change events can be read.
	ReplicationStream(ctx context.Context, startCursor json.RawMessage) (ReplicationStream, error)

	// ScanTableChunk fetches a chunk of rows from the specified table, resuming from the `state.Scanned` cursor
	// position if non-nil.
	// The `backfillComplete` boolean will be true after scanning the final chunk of the table.
	ScanTableChunk(ctx context.Context, info *DiscoveryInfo, state *TableState, callback func(event ChangeEvent) error) (backfillComplete bool, nextResumeCursor []byte, err error)
	// DiscoverTables queries the database for the latest information about tables available for capture.
	DiscoverTables(ctx context.Context) (map[StreamID]*DiscoveryInfo, error)
	// TranslateDBToJSONType returns JSON schema information about the provided database column type.
	TranslateDBToJSONType(column ColumnInfo, isPrimaryKey bool) (*jsonschema.Schema, error)
	// Returns the JSON schema of the source-specific metadata object
	SourceMetadataSchema(writeSchema bool) *jsonschema.Schema
	// ShouldBackfill returns true if a given table's contents should be backfilled.
	ShouldBackfill(streamID StreamID) bool
	// HistoryMode returns whether history mode (non-associative reduction of events) is enabled
	HistoryMode() bool

	// The collection key which should be suggested if discovery fails to find suitable
	// primary key. This should be some property or combination of properties in the
	// source metadata which encodes the database change sequence.
	FallbackCollectionKey() []string

	// Request that replication events for the specified table include transaction IDs.
	// This method is part of a temporary hack which includes XID/GTID metadata only for
	// collections which expect the properties (as inferred based on projections), and
	// should be removed in the future once this hack is no longer necessary.
	RequestTxIDs(schema, table string)

	// SetupPrerequisites verifies that various database requirements (things like
	// "Is CDC enabled on this DB?" and "Does the user have replication access?")
	// are met, and possibly attempts to perform some setup. It may return multiple
	// errors if multiple distinct problems are identified, so that the user can be
	// informed about all of them at once.
	SetupPrerequisites(ctx context.Context) []error
	// SetupTablePrerequisites is like SetupPrerequisites but for any table-specific
	// verification or setup that needs to be performed. It receives the full list of
	// tables and returns a map of any errors encountered, keyed by table.
	SetupTablePrerequisites(ctx context.Context, tables []TableID) map[TableID]error

	// Called when replication appears to not be progressing as it should, this
	// function provides a hook for database-specific diagnostic information to
	// be logged.
	ReplicationDiagnostics(ctx context.Context) error

	// Called periodically just so we can make sure everything looks good on the
	// DB and log warnings as necessary.
	PeriodicChecks(ctx context.Context) error

	// Returns estimated sizes (in rows) for the specified tables. Handles caching internally.
	EstimatedRowCounts(ctx context.Context, tables []TableID) (map[TableID]int, error)
}

// ReplicationStream represents the process of receiving change events
// from a database, managing keepalives and status updates, and translating
// these changes into a stream of ChangeEvents.
//
// Each table must be 'Activated' before replicated change events will be
// received for that table. It is permitted and necessary to activate some
// tables before starting replication.
type ReplicationStream interface {
	ActivateTable(ctx context.Context, streamID StreamID, keyColumns []string, info *DiscoveryInfo, metadata json.RawMessage) error
	StartReplication(ctx context.Context, discovery map[StreamID]*DiscoveryInfo) error

	// StreamToFence yields via the provided callback all change events between the
	// current stream position and a new "fence" position which is guaranteed to be
	// no earlier in the WAL than the point at which the StreamToFence call began.
	//
	// If fenceAfter is greater than zero, the fence should not be established until
	// after the specified amount of time has passed. This should be used to guarantee
	// that indefinite streaming doesn't busy-loop on an idle database.
	StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event DatabaseEvent) error) error

	Acknowledge(ctx context.Context, cursor json.RawMessage) error
	Close(ctx context.Context) error
}

// DiscoveryInfo holds metadata about a specific table in the database, and
// is used during discovery to automatically generate catalog information.
type DiscoveryInfo struct {
	Name        string                // The table's name.
	Schema      string                // The schema (a namespace, in normal parlance) which contains the table.
	Columns     map[string]ColumnInfo // Information about each column of the table.
	PrimaryKey  []string              // An ordered list of the column names which together form the table's primary key.
	FallbackKey bool                  // True if the 'Primary Key' is actually a unique secondary index chosen as a fallback.
	ColumnNames []string              // The names of all columns, in the table's natural order.
	BaseTable   bool                  // True if the table type is 'BASE TABLE' and false for views or other not-physical-table entities.
	OmitBinding bool                  // True if the table should be omitted from discovery catalog generation.

	UseSchemaInference bool // True if generated JSON schemas for this table should request schema inference.
	EmitSourcedSchemas bool // True if generated JSON schemas for this table should be emitted as SourcedSchema events.

	// UnpredictableKeyOrdering will be true when the connector is unable to guarantee
	// (for a particular table) that serialized RowKey values will accurately reproduce
	// (when compared bytewise lexicographically) the database sort ordering of the same
	// rows. This might happen, for instance, if the database is applying a text collation
	// whose ordering rules are undocumented and incomprehensible in their edge cases.
	//
	// In such circumstances, the connector must avoid the standard "filtering" behavior
	// (in which replication events are omitted for portions of the table which haven't
	// been reached yet) because we can't actually answer the question of whether some
	// arbitrary key lies before or after the current backfill cursor.
	UnpredictableKeyOrdering bool

	// ExtraDetails may hold additional details about a table, in cases where there are
	// things a specific connector needs to know about a table for its own use.
	ExtraDetails any
}

// ColumnInfo holds metadata about a specific column of some table in the
// database, and is used during discovery to automatically generate catalog
// information.
type ColumnInfo struct {
	Name        string      // The name of the column.
	Index       int         // The ordinal position of this column in a row.
	TableName   string      // The name of the table to which this column belongs.
	TableSchema string      // The schema of the table to which this column belongs.
	IsNullable  bool        // True if the column can contain nulls.
	DataType    interface{} // The datatype of this column. May be a string name or a more complex struct.
	Description *string     // Stored description of the column, if any.
	OmitColumn  bool        // True if the column should be omitted from discovery JSON schema generation.
}
