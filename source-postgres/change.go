package main

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"unsafe"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/segmentio/encoding/json"
)

type postgresCommitEvent struct {
	CommitLSN pglogrepl.LSN
}

func (postgresCommitEvent) IsDatabaseEvent() {}
func (postgresCommitEvent) IsCommitEvent()   {}

func (evt *postgresCommitEvent) String() string {
	return fmt.Sprintf("Commit(%s)", evt.CommitLSN.String())
}

func (evt *postgresCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	return json.AppendEscape(buf, evt.CommitLSN.String(), 0), nil
}

// A jsonTranscoder is something which can transcode a PostgreSQL wire protocol value into JSON.
type jsonTranscoder interface {
	TranscodeJSON(buf []byte, v []byte) ([]byte, error)
}

// An fdbTranscoder is something which can transcode a PostgreSQL wire protocol value into an FDB tuple value.
type fdbTranscoder interface {
	TranscodeFDB(buf []byte, v []byte) ([]byte, error)
}

type jsonTranscoderFunc func(buf []byte, v []byte) ([]byte, error)

func (f jsonTranscoderFunc) TranscodeJSON(buf []byte, v []byte) ([]byte, error) {
	if v == nil {
		// A nil byte slice should always be serialized as JSON null.
		return append(buf, []byte("null")...), nil
	}
	return f(buf, v)
}

type fdbTranscoderFunc func(buf []byte, v []byte) ([]byte, error)

func (f fdbTranscoderFunc) TranscodeFDB(buf []byte, v []byte) ([]byte, error) {
	if v == nil {
		// In general row keys don't contain nil values, but it is possible in
		// certain corner cases (mainly involving overriding the row key selection
		// to something other than the primary key). In that case, we should always
		// translate a nil byte slice to the nil tuple element 0x00.
		return append(buf, 0x00), nil
	}
	return f(buf, v)
}

// postgresChangeSharedInfo holds information which can be shared across many change events from a given source.
type postgresChangeSharedInfo struct {
	StreamID    sqlcapture.StreamID // StreamID of the table this change event came from.
	Shape       *encrow.Shape       // Shape of the row values, used to serialize them to JSON.
	Transcoders []jsonTranscoder    // Transcoders for column values, in DB column order.
}

// postgresChangeEvent is a specialized Postgres implementation of sqlcapture.ChangeEvent for PostgreSQL.
type postgresChangeEvent struct {
	Info *postgresChangeSharedInfo // Information about the source table structure which is shared across events.

	Meta postgresChangeMetadata // Document metadata object, which contains non-column values which vary by event.

	RowKey []byte   // Serialized row key, if applicable.
	Values [][]byte // Raw byte values of the row, in DB column order.
}

type postgresChangeMetadata struct {
	Info *postgresChangeSharedInfo `json:"-"` // Information about the source table structure which is shared across events. Used to serialize the before values.

	Operation sqlcapture.ChangeOp `json:"op"`
	Source    postgresSource      `json:"source"`
	Before    [][]byte            `json:"before,omitempty"` // Only present for UPDATE events.
}

// postgresSource is source metadata for data capture events.
//
// For more nuance on LSN vs Last LSN vs Final LSN, see:
//
//	https://github.com/postgres/postgres/blob/a8fd13cab0ba815e9925dc9676e6309f699b5f72/src/include/replication/reorderbuffer.h#L260-L280
//
// See also how Materialize deserializes Postgres Debezium envelopes:
//
//	https://github.com/MaterializeInc/materialize/blob/4fca6f51338b0da9a44dd3a75a5a4a5da37a9733/src/interchange/src/avro/envelope_debezium.rs#L275
type postgresSource struct {
	sqlcapture.SourceCommon

	// This is a compact array to reduce noise in generated JSON outputs,
	// and because a lexicographic ordering is also a correct event ordering.
	Location [3]int `json:"loc" jsonschema:"description=Location of this WAL event as [last Commit.EndLSN; event LSN; current Begin.FinalLSN]. See https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html"`

	// Fields which are part of the Debezium Postgres representation but are not included here:
	// * `lsn` is the log sequence number of this event. It's equal to loc[1].
	// * `sequence` is a string-serialized JSON array which embeds a lexicographic
	//    ordering of all events. It's equal to [loc[0], loc[1]].

	TxID uint32 `json:"txid,omitempty" jsonschema:"description=The 32-bit transaction ID assigned by Postgres to the commit which produced this change."`

	Tag string `json:"tag,omitempty" jsonschema:"description=Optional 'Source Tag' property as defined in the endpoint configuration."`
}

// Named constants for the LSN locations within a postgresSource.Location.
const (
	PGLocLastCommitEndLSN = 0 // Index of last Commit.EndLSN in postgresSource.Location.
	PGLocEventLSN         = 1 // Index of this event LSN in postgresSource.Location.
	PGLocBeginFinalLSN    = 2 // Index of current Begin.FinalLSN in postgresSource.Location.
)

func (s *postgresSource) Common() sqlcapture.SourceCommon {
	return s.SourceCommon
}

func (postgresChangeEvent) IsDatabaseEvent() {}
func (postgresChangeEvent) IsChangeEvent()   {}

func (e *postgresChangeEvent) String() string {
	return fmt.Sprintf("Change(%s)", e.Info.StreamID)
}

func (e *postgresChangeEvent) StreamID() sqlcapture.StreamID {
	return e.Info.StreamID
}

func (e *postgresChangeEvent) GetRowKey() []byte {
	return e.RowKey
}

// AppendJSON appends the JSON representation of the change event to the provided buffer.
//
// In theory it should be equivalent to `json.Append(buf, <doc>, 0)` for an appropriately
// constructed capture document value <doc>, but we're using a modified implementation of
// the `encrow.Shape` encoding method for maximum efficiency and to implement omitted
// values correctly.
func (e *postgresChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
	var shape = e.Info.Shape

	// The number of byte values should be one less than the shape's arity, because the last field of
	// the shape is the metadata. Note that this also means there can never be an empty shape here.
	if len(e.Values) != shape.Arity-1 {
		return nil, fmt.Errorf("incorrect row arity: expected %d but got %d", shape.Arity, len(e.Values))
	}

	var err error
	var subsequentField bool
	buf = append(buf, '{')
	for idx, vidx := range shape.Swizzle {
		var val []byte
		if vidx < len(e.Values) {
			val = e.Values[vidx]
			if unsafe.SliceData(val) == unsafe.SliceData(tupleValueOmitted) {
				// The `tupleValueOmitted` value is a special sentinel used to indicate that a
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
// hand-crafted sequence of appends to avoid any reflection overheads.
func (meta *postgresChangeMetadata) AppendJSON(buf []byte) ([]byte, error) {
	var err error
	buf = append(buf, []byte(`{"op":"`)...)
	buf = append(buf, []byte(meta.Operation)...)
	buf = append(buf, []byte(`","source":`)...)
	buf, err = meta.Source.AppendJSON(buf)
	if err != nil {
		return nil, fmt.Errorf("error encoding source metadata: %w", err)
	}
	if meta.Before != nil {
		buf = append(buf, []byte(`,"before":`)...)
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
func (meta *postgresChangeMetadata) AppendBeforeJSON(buf []byte) ([]byte, error) {
	var shape = meta.Info.Shape

	// The number of byte values should be one less than the shape's arity, because the last field of
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
		if unsafe.SliceData(val) == unsafe.SliceData(tupleValueOmitted) {
			// The `tupleValueOmitted` value is a special sentinel used to indicate that a
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

// AppendJSON appends the JSON representation of the source metadata to the provided buffer.
//
// In theory it should be equivalent to `json.Append(buf, source, 0)`, but we're using a
// hand-crafted sequence of appends to avoid any reflection overheads.
func (source *postgresSource) AppendJSON(buf []byte) ([]byte, error) {
	buf = append(buf, '{')
	if source.Millis != 0 {
		buf = append(buf, []byte(`"ts_ms":`)...)
		buf = strconv.AppendInt(buf, int64(source.Millis), 10)
		buf = append(buf, ',')
	}
	buf = append(buf, []byte(`"schema":`)...)
	buf = json.AppendEscape(buf, source.Schema, 0)
	if source.Snapshot {
		buf = append(buf, []byte(`,"snapshot":true,"table":`)...)
	} else {
		buf = append(buf, []byte(`,"table":`)...)
	}
	buf = json.AppendEscape(buf, source.Table, 0)
	buf = append(buf, []byte(`,"loc":[`)...)
	buf = strconv.AppendInt(buf, int64(source.Location[0]), 10)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(source.Location[1]), 10)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(source.Location[2]), 10)
	buf = append(buf, ']')
	if source.TxID != 0 {
		buf = append(buf, []byte(`,"txid":`)...)
		buf = strconv.AppendInt(buf, int64(source.TxID), 10)
	}
	if source.Tag != "" {
		buf = append(buf, `,"tag":`...)
		buf = json.AppendEscape(buf, source.Tag, 0)
	}
	return append(buf, '}'), nil
}

// constructJSONTranscoder returns a jsonTranscoder for a specific column value.
func (db *postgresDatabase) constructJSONTranscoder(discoveredColumnType any, isPrimaryKey bool, typeMap *pgtype.Map, fieldDescription *pgconn.FieldDescription) jsonTranscoder {
	var codec pgtype.Codec
	if pgType, ok := typeMap.TypeForOID(fieldDescription.DataTypeOID); ok {
		codec = pgType.Codec
	} else {
		codec = &pgtype.TextCodec{} // Default to text codec for unknown OIDs
	}

	switch codec.(type) {
	case pgtype.TextCodec:
		// Special case for the text codec (used for TEXT and VARCHAR and a few other column
		// types), which just casts its input bytes to a Go string. That cast can be relatively
		// expensive, and in our particular case we know that we are never going to modify the
		// bytes and only need to marshal the string as a JSON string with escaping, so we can
		// go significantly faster by using an unsafe cast.
		return jsonTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
			if len(bs) > truncateColumnThreshold {
				// TODO(wgd): Consider moving all truncation earlier in the message receive/decode process
				bs = bs[:truncateColumnThreshold] // Truncate large text values
			}
			var str = *(*string)(unsafe.Pointer(&bs))
			return json.AppendEscape(buf, str, 0), nil
		})
	case pgtype.Int2Codec:
		switch fieldDescription.Format {
		case pgtype.TextFormatCode:
			return jsonTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				return append(buf, bs...), nil // Text-format integers are just decimal numbers, which are valid JSON
			})
		case pgtype.BinaryFormatCode:
			return jsonTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				if len(bs) != 2 {
					return nil, fmt.Errorf("expected 2 bytes for INT2, got %d bytes", len(bs))
				}
				return strconv.AppendInt(buf, int64(int16(binary.BigEndian.Uint16(bs))), 10), nil
			})
		}
	case pgtype.Int4Codec:
		switch fieldDescription.Format {
		case pgtype.TextFormatCode:
			return jsonTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				return append(buf, bs...), nil // Text-format integers are just decimal numbers, which are valid JSON
			})
		case pgtype.BinaryFormatCode:
			return jsonTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				if len(bs) != 4 {
					return nil, fmt.Errorf("expected 4 bytes for INT4, got %d bytes", len(bs))
				}
				return strconv.AppendInt(buf, int64(int32(binary.BigEndian.Uint32(bs))), 10), nil
			})
		}
	case pgtype.Int8Codec:
		switch fieldDescription.Format {
		case pgtype.TextFormatCode:
			return jsonTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				return append(buf, bs...), nil // Text-format integers are just decimal numbers, which are valid JSON
			})
		case pgtype.BinaryFormatCode:
			return jsonTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				if len(bs) != 8 {
					return nil, fmt.Errorf("expected 8 bytes for INT8, got %d bytes", len(bs))
				}
				return strconv.AppendInt(buf, int64(binary.BigEndian.Uint64(bs)), 10), nil
			})
		}
	}

	// This is the main value encoding logic used for most data types outside of special cases.
	// We go the long way, first using PGX to decode the value and then translating it to another
	// Go value and then finally serializing that as JSON.
	var columnType postgresTypeDescription = unknownColumnType
	if t, ok := discoveredColumnType.(postgresTypeDescription); ok {
		columnType = t
	}
	return jsonTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
		var val any
		var err error
		if bs != nil {
			val, err = codec.DecodeValue(typeMap, fieldDescription.DataTypeOID, fieldDescription.Format, bs)
		}
		if err != nil {
			return nil, fmt.Errorf("error decoding value: %w", err)
		} else if translated, err := db.translateRecordField(columnType, isPrimaryKey, val); err != nil {
			return nil, fmt.Errorf("error translating value %v for JSON serialization: %w", val, err)
		} else {
			return json.Append(buf, translated, json.EscapeHTML|json.SortMapKeys) // Consider removing json.EscapeHTML, though it will change some outputs like the `circle` column type
		}
	})
}

// unknownColumnType is the "discovered type" of a column which doesn't currently show up in discovery results.
//
// The fact that this is a situation which can occur (typically when a column is renamed or dropped) is why we
// really ought to try and drive our value translation purely from type OIDs, but currently there are a few cases
// where we still depend on discovery information.
var unknownColumnType = postgresBasicType{Name: "unknown-column-type"}

// constructFDBTranscoder returns an fdbTranscoder for a specific column value.
func (db *postgresDatabase) constructFDBTranscoder(typeMap *pgtype.Map, fieldDescription *pgconn.FieldDescription) fdbTranscoder {
	var codec pgtype.Codec
	if pgType, ok := typeMap.TypeForOID(fieldDescription.DataTypeOID); ok {
		codec = pgType.Codec
	} else {
		codec = &pgtype.TextCodec{} // Default to text codec for unknown OIDs
	}

	switch codec.(type) {
	case pgtype.TextCodec:
		return fdbTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
			return sqlcapture.AppendBytesFDB(buf, sqlcapture.FDBStringCode, bs)
		})
	case pgtype.Int2Codec:
		switch fieldDescription.Format {
		case pgtype.TextFormatCode:
			return fdbTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				var val, err = strconv.ParseInt(string(bs), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("error parsing INT2 value %q for column %q: %w", string(bs), string(fieldDescription.Name), err)
				}
				return sqlcapture.AppendIntFDB(buf, val)
			})
		case pgtype.BinaryFormatCode:
			return fdbTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				if len(bs) != 2 {
					return nil, fmt.Errorf("expected 2 bytes for INT2, got %d bytes", len(bs))
				}
				return sqlcapture.AppendIntFDB(buf, int64(int16(binary.BigEndian.Uint16(bs))))
			})
		}
	case pgtype.Int4Codec:
		switch fieldDescription.Format {
		case pgtype.TextFormatCode:
			return fdbTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				var val, err = strconv.ParseInt(string(bs), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("error parsing INT4 value %q for column %q: %w", string(bs), string(fieldDescription.Name), err)
				}
				return sqlcapture.AppendIntFDB(buf, val)
			})
		case pgtype.BinaryFormatCode:
			return fdbTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				if len(bs) != 4 {
					return nil, fmt.Errorf("expected 4 bytes for INT4, got %d bytes", len(bs))
				}
				return sqlcapture.AppendIntFDB(buf, int64(int32(binary.BigEndian.Uint32(bs))))
			})
		}
	case pgtype.Int8Codec:
		switch fieldDescription.Format {
		case pgtype.TextFormatCode:
			return fdbTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				var val, err = strconv.ParseInt(string(bs), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("error parsing INT8 value %q for column %q: %w", string(bs), string(fieldDescription.Name), err)
				}
				return sqlcapture.AppendIntFDB(buf, val)
			})
		case pgtype.BinaryFormatCode:
			return fdbTranscoderFunc(func(buf []byte, bs []byte) ([]byte, error) {
				if len(bs) != 8 {
					return nil, fmt.Errorf("expected 8 bytes for INT8, got %d bytes", len(bs))
				}
				return sqlcapture.AppendIntFDB(buf, int64(binary.BigEndian.Uint64(bs)))
			})
		}
	}

	return fdbTranscoderFunc(func(buf, v []byte) ([]byte, error) {
		var val any
		var err error
		if v != nil {
			val, err = codec.DecodeValue(typeMap, fieldDescription.DataTypeOID, fieldDescription.Format, v)
		}
		if err != nil {
			return nil, fmt.Errorf("error decoding value: %w", err)
		} else if translated, err := encodeKeyFDB(val); err != nil {
			return nil, fmt.Errorf("error translating value %v for FDB serialization: %w", val, err)
		} else {
			return sqlcapture.AppendFDB(buf, translated)
		}
	})
}
