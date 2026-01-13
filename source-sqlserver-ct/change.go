package main

import (
	"fmt"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/segmentio/encoding/json"
)

// sqlserverCTCommitEvent is a cursor checkpoint event containing per-table version numbers.
type sqlserverCTCommitEvent struct {
	Versions map[sqlcapture.StreamID]int64
}

func (sqlserverCTCommitEvent) IsDatabaseEvent() {}
func (sqlserverCTCommitEvent) IsCommitEvent()   {}

func (evt *sqlserverCTCommitEvent) String() string {
	return fmt.Sprintf("Commit(versions=%v)", evt.Versions)
}

func (evt *sqlserverCTCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	// Convert StreamID keys to strings for JSON serialization.
	//
	// TODO(wgd): This could probably be made much more efficient by iterating
	// over the map and serializing keys/values directly, but that's not really
	// worth the effort since we already optimize the frequency of checkpoint
	// serialization in sqlcapture.
	var stringKeyVersions = make(map[string]int64, len(evt.Versions))
	for k, v := range evt.Versions {
		stringKeyVersions[k.String()] = v
	}
	return json.Append(buf, stringKeyVersions, 0)
}

// A jsonTranscoder is something which can transcode a SQL Server client library value into JSON.
type jsonTranscoder interface {
	TranscodeJSON(buf []byte, v any) ([]byte, error)
}

type jsonTranscoderFunc func(buf []byte, v any) ([]byte, error)

func (f jsonTranscoderFunc) TranscodeJSON(buf []byte, v any) ([]byte, error) {
	return f(buf, v)
}

// An fdbTranscoder is something which can transcode a SQL Server client library value into an FDB tuple value.
type fdbTranscoder interface {
	TranscodeFDB(buf []byte, v any) ([]byte, error)
}

type fdbTranscoderFunc func(buf []byte, v any) ([]byte, error)

func (f fdbTranscoderFunc) TranscodeFDB(buf []byte, v any) ([]byte, error) { return f(buf, v) }

type sqlserverChangeEvent struct {
	Shared *sqlserverChangeSharedInfo
	Meta   sqlserverChangeMetadata `json:"_meta"`
	RowKey []byte
	Values []any
}

type sqlserverChangeSharedInfo struct {
	StreamID          sqlcapture.StreamID // StreamID of the table this change event came from.
	Shape             *encrow.Shape       // Shape of the document values, used to serialize them to JSON.
	Transcoders       []jsonTranscoder    // Transcoders for column values, in DB column order.
	DeleteNullability []bool              // Whether nil values of a particular column should be included in delete events.
}

func (sqlserverChangeEvent) IsDatabaseEvent() {}
func (sqlserverChangeEvent) IsChangeEvent()   {}

func (e *sqlserverChangeEvent) String() string {
	switch e.Meta.Operation {
	case sqlcapture.InsertOp:
		return fmt.Sprintf("Insert(%s)", e.Shared.StreamID)
	case sqlcapture.UpdateOp:
		return fmt.Sprintf("Update(%s)", e.Shared.StreamID)
	case sqlcapture.DeleteOp:
		return fmt.Sprintf("Delete(%s)", e.Shared.StreamID)
	}
	return fmt.Sprintf("UnknownChange(%s)", e.Shared.StreamID)
}

func (e *sqlserverChangeEvent) StreamID() sqlcapture.StreamID {
	return e.Shared.StreamID
}

func (e *sqlserverChangeEvent) GetRowKey() []byte {
	return e.RowKey
}

// AppendJSON appends the JSON representation of the change event to the provided buffer.
func (e *sqlserverChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
	var shape = e.Shared.Shape

	// The number of byte values should be one less than the shape's arity, because the last field of
	// the shape is the metadata. Note that this also means there can never be an empty shape here.
	if len(e.Values) != shape.Arity-1 {
		return nil, fmt.Errorf("incorrect row arity: expected %d but got %d", shape.Arity, len(e.Values))
	}

	var err error
	var subsequentField bool
	buf = append(buf, '{')
	for idx, vidx := range shape.Swizzle {
		var val any
		if vidx < len(e.Values) {
			val = e.Values[vidx]
			if e.Meta.Operation == sqlcapture.DeleteOp && val == nil && !e.Shared.DeleteNullability[vidx] {
				// For delete events, non-key columns come from a LEFT OUTER JOIN with the source
				// table, which returns NULL for all columns since the row no longer exists. We skip
				// these columns entirely rather than emitting null values, since the nulls don't
				// represent actual data from the deleted row.
				continue
			}
		}
		if subsequentField {
			buf = append(buf, ',')
		}
		subsequentField = true
		buf = append(buf, shape.Prefixes[idx]...)
		if vidx < len(e.Values) {
			buf, err = e.Shared.Transcoders[vidx].TranscodeJSON(buf, val)
		} else {
			buf, err = e.Meta.AppendJSON(buf) // The last value index is the metadata
		}
		if err != nil {
			return nil, fmt.Errorf("error encoding field %q: %w", shape.Names[idx], err)
		}
	}
	return append(buf, '}'), nil
}

type sqlserverChangeMetadata struct {
	Operation sqlcapture.ChangeOp   `json:"op"`
	Source    sqlserverCTSourceInfo `json:"source"`
}

// AppendJSON appends the JSON representation of the change metadata to the provided buffer.
func (meta *sqlserverChangeMetadata) AppendJSON(buf []byte) ([]byte, error) {
	return json.Append(buf, meta, 0)
}

// sqlserverCTSourceInfo is source metadata for Change Tracking capture events.
type sqlserverCTSourceInfo struct {
	sqlcapture.SourceCommon

	Version int64  `json:"version,omitempty" jsonschema:"description=The Change Tracking version at which this event occurred. Only set for replication events, not backfills."`
	Tag     string `json:"tag,omitempty" jsonschema:"description=Optional 'Source Tag' property as defined in the endpoint configuration."`
}

func (source *sqlserverCTSourceInfo) Common() sqlcapture.SourceCommon {
	return source.SourceCommon
}

// AppendJSON appends the JSON representation of the change metadata to the provided buffer.
func (source *sqlserverCTSourceInfo) AppendJSON(buf []byte) ([]byte, error) {
	return json.Append(buf, source, 0)
}

func (db *sqlserverDatabase) backfillJSONTranscoder(columnType any) jsonTranscoder {
	return jsonTranscoderFunc(func(buf []byte, v any) ([]byte, error) {
		if translated, err := db.translateRecordField(columnType, v); err != nil {
			return nil, fmt.Errorf("error translating value %v for JSON serialization: %w", v, err)
		} else {
			return json.Append(buf, translated, json.EscapeHTML|json.SortMapKeys)
		}
	})
}

func (db *sqlserverDatabase) backfillFDBTranscoder(columnType any) fdbTranscoder {
	return fdbTranscoderFunc(func(buf []byte, v any) ([]byte, error) {
		if translated, err := encodeKeyFDB(v, columnType); err != nil {
			return nil, fmt.Errorf("error translating value %v for FDB serialization: %w", v, err)
		} else {
			return sqlcapture.AppendFDB(buf, translated)
		}
	})
}

func (db *sqlserverDatabase) replicationJSONTranscoder(columnType any) jsonTranscoder {
	// Replication transcoders are the same as backfill transcoders because Change Tracking
	// uses ordinary SQL queries for both backfill and replication (via CHANGETABLE joins).
	return db.backfillJSONTranscoder(columnType)
}

func (db *sqlserverDatabase) replicationFDBTranscoder(columnType any) fdbTranscoder {
	// Replication transcoders are the same as backfill transcoders because Change Tracking
	// uses ordinary SQL queries for both backfill and replication (via CHANGETABLE joins).
	return db.backfillFDBTranscoder(columnType)
}
