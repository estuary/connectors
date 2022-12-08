package sqlcapture

import (
	"context"
	"encoding/json"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/invopop/jsonschema"
)

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

// SourceMetadata is source-specific metadata about data capture events.
type SourceMetadata interface {
	Common() SourceCommon
	Cursor() string // TODO(wgd): Maybe json.RawMessage?
}

// ChangeEvent represents an Insert/Update/Delete operation on a specific
// row in the database.
type ChangeEvent struct {
	Operation ChangeOp
	Source    SourceMetadata
	Before    map[string]interface{}
	After     map[string]interface{}
}

// FlushEvent informs the generic sqlcapture logic about transaction
// boundaries.
type FlushEvent struct {
	Source SourceMetadata
}

// MetadataEvent informs the generic sqlcapture logic about changes to
// the per-table metadata JSON.
type MetadataEvent struct {
	StreamID string
	Metadata json.RawMessage
}

// A DatabaseEvent can be a ChangeEvent, FlushEvent, or MetadataEvent.
type DatabaseEvent interface {
	isDatabaseEvent()
	String() string
}

func (*ChangeEvent) isDatabaseEvent()   {}
func (*FlushEvent) isDatabaseEvent()    {}
func (*MetadataEvent) isDatabaseEvent() {}

func (*ChangeEvent) String() string   { return "ChangeEvent" }
func (*FlushEvent) String() string    { return "FlushEvent" }
func (*MetadataEvent) String() string { return "MetadataEvent" }

// KeyFields returns suitable fields for extracting the event primary key.
func (e *ChangeEvent) KeyFields() map[string]interface{} {
	if e.Operation == DeleteOp {
		return e.Before
	}
	return e.After
}

// Database represents the operations which must be performed on a specific database
// during the course of a capture in order to perform discovery, backfill preexisting
// data, and process replicated change events.
type Database interface {
	// Close shuts down the database connection.
	Close(ctx context.Context) error
	// ReplicationStream constructs a new ReplicationStream object, from which
	// a neverending sequence of change events can be read.
	ReplicationStream(ctx context.Context, startCursor string) (ReplicationStream, error)
	// WriteWatermark writes the provided string into the 'watermarks' table.
	WriteWatermark(ctx context.Context, watermark string) error
	// WatermarksTable returns the name of the table to which WriteWatermarks writes UUIDs.
	WatermarksTable() string
	// ScanTableChunk fetches a chunk of rows from the specified table, resuming from `resumeKey` if non-nil.
	ScanTableChunk(ctx context.Context, info *DiscoveryInfo, keyColumns []string, resumeKey []interface{}) ([]*ChangeEvent, error)
	// DiscoverTables queries the database for information about tables available for capture.
	DiscoverTables(ctx context.Context) (map[string]*DiscoveryInfo, error)
	// TranslateDBToJSONType returns JSON schema information about the provided database column type.
	TranslateDBToJSONType(column ColumnInfo) (*jsonschema.Schema, error)
	// Returns an empty instance of the source-specific metadata (used for JSON schema generation).
	EmptySourceMetadata() SourceMetadata
	// EncodeRowKeyForFDB converts a key as necessary to produce a TupleElement,
	// which is encoded as part of a FoundationDB serialized tuple.
	// Make sure the conversion is partial-order-preserving.
	EncodeKeyFDB(key interface{}) (tuple.TupleElement, error)
	// DecodeKeyFDB decodes the result of `EncodeKeyFDB` to its original form.
	DecodeKeyFDB(t tuple.TupleElement) (interface{}, error)
	// ShouldBackfill returns true if a given table's contents should be backfilled.
	ShouldBackfill(streamID string) bool
}

// ReplicationStream represents the process of receiving change events
// from a database, managing keepalives and status updates, and translating
// these changes into a stream of ChangeEvents.
//
// Each table must be 'Activated' before replicated change events will be
// received for that table. It is permitted and necessary to activate some
// tables before starting replication.
type ReplicationStream interface {
	ActivateTable(streamID string, info *DiscoveryInfo, metadata json.RawMessage) error

	StartReplication(ctx context.Context) error
	Events() <-chan DatabaseEvent
	Acknowledge(ctx context.Context, cursor string) error
	Close(ctx context.Context) error
}

// DiscoveryInfo holds metadata about a specific table in the database, and
// is used during discovery to automatically generate catalog information.
type DiscoveryInfo struct {
	Name        string                // The PostgreSQL table name.
	Schema      string                // The PostgreSQL schema (a namespace, in normal parlance) which contains the table.
	Columns     map[string]ColumnInfo // Information about each column of the table.
	PrimaryKey  []string              // An ordered list of the column names which together form the table's primary key.
	ColumnNames []string              // The names of all columns, in the table's natural order.
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
}
