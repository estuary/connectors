package main

import (
	"fmt"
	"strconv"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/invopop/jsonschema"
	"github.com/segmentio/encoding/json"
)

type mysqlCommitEvent struct {
	Position mysql.Position
}

func (mysqlCommitEvent) IsDatabaseEvent() {}
func (mysqlCommitEvent) IsCommitEvent()   {}

func (evt *mysqlCommitEvent) String() string {
	return fmt.Sprintf("Commit(%s:%d)", evt.Position.Name, evt.Position.Pos)
}

func (evt *mysqlCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	var cursorString = fmt.Sprintf("%s:%d", evt.Position.Name, evt.Position.Pos)
	return json.AppendEscape(buf, cursorString, 0), nil
}

// A jsonTranscoder is something which can transcode a MySQL value into JSON.
type jsonTranscoder interface {
	TranscodeJSON(buf []byte, v any) ([]byte, error)
}

// An fdbTranscoder is something which can transcode a MySQL value into an FDB tuple value.
type fdbTranscoder interface {
	TranscodeFDB(buf []byte, v any) ([]byte, error)
}

type jsonTranscoderFunc func(buf []byte, v any) ([]byte, error)

func (f jsonTranscoderFunc) TranscodeJSON(buf []byte, v any) ([]byte, error) {
	if v == nil {
		// A nil byte slice should always be serialized as JSON null.
		return append(buf, `null`...), nil
	}
	return f(buf, v)
}

type fdbTranscoderFunc func(buf []byte, v any) ([]byte, error)

func (f fdbTranscoderFunc) TranscodeFDB(buf []byte, v any) ([]byte, error) {
	if v == nil {
		// In general row keys don't contain nil values, but it is possible in
		// certain corner cases (mainly involving overriding the row key selection
		// to something other than the primary key). In that case, we should always
		// translate a nil byte slice to the nil tuple element 0x00.
		return append(buf, 0x00), nil
	}
	return f(buf, v)
}

type mysqlChangeEvent struct {
	Info *mysqlChangeSharedInfo // Information about the source table structure which is shared across events.

	Meta   mysqlChangeMetadata // Document metadata object representing the _meta property
	RowKey []byte
	Values []any
}

type mysqlChangeSharedInfo struct {
	StreamID    sqlcapture.StreamID // StreamID of the table this change event came from.
	Shape       *encrow.Shape       // Shape of the row values, used to serialize them to JSON.
	Transcoders []jsonTranscoder    // Transcoders for column values, in DB column order.
}

type mysqlChangeMetadata struct {
	Info *mysqlChangeSharedInfo `json:"-"` // Information about the source table structure which is shared across events. Copied here to simplify before-value serialization.

	Operation sqlcapture.ChangeOp `json:"op"`
	Source    mysqlSourceInfo     `json:"source"`
	Before    []any               `json:"before,omitempty"`
}

// mysqlSourceInfo is source metadata for data capture events.
type mysqlSourceInfo struct {
	sqlcapture.SourceCommon
	Cursor mysqlChangeEventCursor `json:"cursor" jsonschema:"description=Cursor value representing the current position in the binlog."`
	TxID   string                 `json:"txid,omitempty" jsonschema:"description=The global transaction identifier associated with a change by MySQL. Only set if GTIDs are enabled."`
}

type mysqlChangeEventCursor struct {
	BinlogFile   string // Cursor representing the binlog file containing this event
	BinlogOffset uint64 // Cursor representing the estimated binlog offset of the rows change event
	RowIndex     int    // Index of the current row within the binlog event
}

func (c mysqlChangeEventCursor) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{Type: "string"}
}

func (c mysqlChangeEventCursor) MarshalJSON() ([]byte, error) {
	var buf = make([]byte, 0, 64)
	buf = json.AppendEscape(buf, c.BinlogFile, 0)
	buf[len(buf)-1] = ':'   // Turn the closing quote into a colon separator
	if c.BinlogOffset > 0 { // All valid offsets are >= 4, offset == 0 only occurs in backfills
		buf = strconv.AppendInt(buf, int64(c.BinlogOffset), 10)
		buf = append(buf, ':')
	}
	buf = strconv.AppendInt(buf, int64(c.RowIndex), 10)
	return append(buf, '"'), nil
}

func (s *mysqlSourceInfo) Common() sqlcapture.SourceCommon {
	return s.SourceCommon
}

func (mysqlChangeEvent) IsDatabaseEvent() {}
func (mysqlChangeEvent) IsChangeEvent()   {}

func (e *mysqlChangeEvent) String() string {
	switch e.Meta.Operation {
	case sqlcapture.InsertOp:
		return fmt.Sprintf("Insert(%s)", e.Info.StreamID)
	case sqlcapture.UpdateOp:
		return fmt.Sprintf("Update(%s)", e.Info.StreamID)
	case sqlcapture.DeleteOp:
		return fmt.Sprintf("Delete(%s)", e.Info.StreamID)
	}
	return fmt.Sprintf("UnknownChange(%s)", e.Info.StreamID)
}

func (e *mysqlChangeEvent) StreamID() sqlcapture.StreamID {
	return e.Info.StreamID
}

func (e *mysqlChangeEvent) GetRowKey() []byte {
	return e.RowKey
}

// MarshalJSON implements serialization of a ChangeEvent to JSON.
//
// Note that this implementation is destructive, but that's okay because
// emitting the serialized JSON is the last thing we ever do with a change.
func (e *mysqlChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
	var shape = e.Info.Shape

	// The number of column values should be one less than the shape's arity, because the last field of
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
			if val == rowValueOmitted {
				// The `rowValueOmitted` value is a special sentinel used to indicate that a
				// particular field should be omitted from the output document entirely.
				continue
			}
		}
		if subsequentField {
			buf = append(buf, ',')
		}
		subsequentField = true
		buf = append(buf, shape.Prefixes[idx]...)
		if vidx < len(e.Values) {
			buf, err = e.Info.Transcoders[vidx].TranscodeJSON(buf, val)
		} else {
			buf, err = e.Meta.AppendJSON(buf) // The last value index is the metadata
		}
		if err != nil {
			return nil, fmt.Errorf("error encoding field %q: %w", shape.Names[idx], err)
		}
	}
	return append(buf, '}'), nil
}

// AppendJSON appends the JSON representation of the change metadata to the provided buffer.
//
// In theory it should be equivalent to `json.Append(buf, meta, 0)`, but we're using a
// hand-crafted sequence of appends to avoid any reflection overheads and dispatch the
// encoding of the "before" values appropriately.
func (meta *mysqlChangeMetadata) AppendJSON(buf []byte) ([]byte, error) {
	var err error
	buf = append(buf, `{"op":"`...)
	buf = append(buf, meta.Operation...)
	buf = append(buf, `","source":`...)
	buf, err = meta.Source.AppendJSON(buf)
	if err != nil {
		return nil, fmt.Errorf("error encoding source metadata: %w", err)
	}
	if meta.Before != nil {
		buf = append(buf, `,"before":`...)
		buf, err = meta.AppendBeforeJSON(buf)
		if err != nil {
			return nil, fmt.Errorf("error encoding before values: %w", err)
		}
	}
	return append(buf, '}'), nil
}

// AppendBeforeJSON appends the JSON representation of the change event's before-state to
// the provided buffer.
//
// In theory it should be roughly equivalent to `json.Append(buf, <before>, 0)` for some
// appropriately constructed before-state values map, but we're using a modified version
// of the `encrow.Shape` encoding method for maximum efficiency and to implement omitted
// values correctly.
func (meta *mysqlChangeMetadata) AppendBeforeJSON(buf []byte) ([]byte, error) {
	var shape = meta.Info.Shape

	// The number of row values should be one less than the shape's arity, because the last field of
	// the shape is the metadata. However because this is the /_meta/before value we omit the metadata
	// field when it's reached.
	if len(meta.Before) != shape.Arity-1 {
		return nil, fmt.Errorf("incorrect row arity: expected %d but got %d", shape.Arity, len(meta.Before))
	}

	var err error
	var subsequentField bool
	buf = append(buf, '{')
	for idx, vidx := range shape.Swizzle {
		if vidx >= len(meta.Before) {
			// Skip the nonexistent "final" value index, which represents the metadata
			// that we are already in the process of serializing.
			continue
		}
		var val = meta.Before[vidx]
		if val == rowValueOmitted {
			// The `rowValueOmitted` value is a special sentinel used to indicate that a
			// particular field should be omitted from the output document entirely.
			continue
		}
		if subsequentField {
			buf = append(buf, ',')
		}
		subsequentField = true
		buf = append(buf, shape.Prefixes[idx]...)
		buf, err = meta.Info.Transcoders[vidx].TranscodeJSON(buf, val)
		if err != nil {
			return nil, fmt.Errorf("error encoding field %q: %w", shape.Names[idx], err)
		}
	}
	return append(buf, '}'), nil
}

func (source *mysqlSourceInfo) AppendJSON(buf []byte) ([]byte, error) {
	buf = append(buf, '{')
	if source.Millis != 0 {
		buf = append(buf, `"ts_ms":`...)
		buf = strconv.AppendInt(buf, int64(source.Millis), 10)
		buf = append(buf, ',')
	}
	buf = append(buf, `"schema":`...)
	buf = json.AppendEscape(buf, source.Schema, 0)
	if source.Snapshot {
		buf = append(buf, `,"snapshot":true,"table":`...)
	} else {
		buf = append(buf, `,"table":`...)
	}
	buf = json.AppendEscape(buf, source.Table, 0)
	buf = append(buf, `,"cursor":`...)
	buf = json.AppendEscape(buf, source.Cursor.BinlogFile, 0)
	buf[len(buf)-1] = ':'               // Turn the closing quote into a colon separator
	if source.Cursor.BinlogOffset > 0 { // All valid offsets are >= 4, offset == 0 only occurs in backfills
		buf = strconv.AppendInt(buf, int64(source.Cursor.BinlogOffset), 10)
		buf = append(buf, ':')
	}
	buf = strconv.AppendInt(buf, int64(source.Cursor.RowIndex), 10)
	buf = append(buf, '"')
	if source.TxID != "" {
		buf = append(buf, `,"txid":`...)
		buf = json.AppendEscape(buf, source.TxID, 0)
	}
	return append(buf, '}'), nil
}
