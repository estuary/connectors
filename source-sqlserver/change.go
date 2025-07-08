package main

import (
	"encoding/base64"
	"fmt"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/segmentio/encoding/json"
)

type sqlserverCommitEvent struct {
	CommitLSN LSN
}

func (sqlserverCommitEvent) IsDatabaseEvent() {}
func (sqlserverCommitEvent) IsCommitEvent()   {}

func (evt *sqlserverCommitEvent) String() string {
	return fmt.Sprintf("Commit(%X)", evt.CommitLSN)
}

func (evt *sqlserverCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	// Since base64 strings never require escaping we can just splat it between quotes.
	buf = append(buf, '"')
	buf = base64.StdEncoding.AppendEncode(buf, evt.CommitLSN)
	return append(buf, '"'), nil
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
	StreamID    sqlcapture.StreamID // StreamID of the table this change event came from.
	Shape       *encrow.Shape       // Shape of the document values, used to serialize them to JSON.
	Transcoders []jsonTranscoder    // Transcoders for column values, in DB column order.
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
	Operation sqlcapture.ChangeOp `json:"op"`
	Source    sqlserverSourceInfo `json:"source"`
}

// AppendJSON appends the JSON representation of the change metadata to the provided buffer.
func (meta *sqlserverChangeMetadata) AppendJSON(buf []byte) ([]byte, error) {
	return json.Append(buf, meta, 0)
}

// sqlserverSourceInfo is source metadata for data capture events.
type sqlserverSourceInfo struct {
	sqlcapture.SourceCommon

	LSN        LSN    `json:"lsn" jsonschema:"description=The LSN at which a CDC event occurred. Only set for CDC events, not backfills."`
	SeqVal     []byte `json:"seqval" jsonschema:"description=Sequence value used to order changes to a row within a transaction. Only set for CDC events, not backfills."`
	UpdateMask any    `json:"updateMask,omitempty" jsonschema:"description=A bit mask with a bit corresponding to each captured column identified for the capture instance. Only set for CDC events, not backfills."`
}

func (source *sqlserverSourceInfo) Common() sqlcapture.SourceCommon {
	return source.SourceCommon
}

// AppendJSON appends the JSON representation of the change metadata to the provided buffer.
func (source *sqlserverSourceInfo) AppendJSON(buf []byte) ([]byte, error) {
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
	return db.backfillJSONTranscoder(columnType)
}

func (db *sqlserverDatabase) replicationFDBTranscoder(columnType any) fdbTranscoder {
	return db.backfillFDBTranscoder(columnType)
}
