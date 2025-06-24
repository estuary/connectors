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

// A jsonTranscoder is a function which transcodes a PostgreSQL wire protocol value
// into JSON and appends it to the provided buffer.
type jsonTranscoder func(buf []byte, v []byte) ([]byte, error)

// An fdbTranscoder is a function which transcodes a PostgreSQL wire protocol value
// into the FDB tuple encoding and appends it to the provided buffer.
type fdbTranscoder func(buf []byte, v []byte) ([]byte, error)

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
	Operation sqlcapture.ChangeOp `json:"op"`
	Source    postgresSource      `json:"source"`
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

func (e *postgresChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
	var shape = e.Info.Shape

	// The number of byte values should be one less than the shape's arity, because the last value
	// is the metadata. Note that this also means there can never be an empty shape here.
	if len(e.Values) != shape.Arity-1 {
		return nil, fmt.Errorf("incorrect row arity: expected %d but got %d", shape.Arity, len(e.Values))
	}

	var err error
	buf = append(buf, '{')
	for idx, vidx := range shape.Swizzle {
		if idx > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, shape.Prefixes[idx]...)
		if vidx < len(e.Values) {
			buf, err = e.Info.Transcoders[vidx](buf, e.Values[vidx])
		} else {
			buf, err = e.Meta.AppendJSON(buf) // The last value index is the metadata
		}
		if err != nil {
			return nil, fmt.Errorf("error encoding field %q: %w", shape.Names[idx], err)
		}
	}
	return append(buf, '}'), nil
}

func (meta *postgresChangeMetadata) AppendJSON(buf []byte) ([]byte, error) {
	var err error
	buf = append(buf, []byte(`{"op":"`)...)
	buf = append(buf, []byte(meta.Operation)...)
	buf = append(buf, []byte(`","source":`)...)
	buf, err = meta.Source.AppendJSON(buf)
	if err != nil {
		return nil, fmt.Errorf("error encoding source metadata: %w", err)
	}
	// TODO(wgd): Add /_meta/before in the future when this is extended to support replication
	return append(buf, '}'), nil
}

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
	return append(buf, '}'), nil
}

// backfillJSONTranscoder returns a jsonTranscoder for a specific column in a Postgres backfill.
func (db *postgresDatabase) backfillJSONTranscoder(typeMap *pgtype.Map, fieldDescription *pgconn.FieldDescription, columnInfo *sqlcapture.ColumnInfo, isPrimaryKey bool) jsonTranscoder {
	// Look up the PGX type/codec for the column OID. If not found, we use a generic transcoder
	// which treats the value as text or bytes depending on the format.
	//
	// TODO(wgd): Test if we could just swap this out for TextCodec without losing anything.
	var pgType, ok = typeMap.TypeForOID(fieldDescription.DataTypeOID)
	if !ok {
		return func(buf []byte, bs []byte) ([]byte, error) {
			var val any
			switch fieldDescription.Format {
			case pgtype.TextFormatCode:
				val = string(bs)
			case pgtype.BinaryFormatCode:
				newBuf := make([]byte, len(bs))
				copy(newBuf, bs)
				val = newBuf
			default:
				return nil, fmt.Errorf("unknown format code %d", fieldDescription.Format)
			}
			if translated, err := db.translateRecordField(columnInfo, isPrimaryKey, val); err != nil {
				return nil, fmt.Errorf("error translating value %v: %w", val, err)
			} else {
				return json.Append(buf, translated, json.EscapeHTML) // Consider removing json.EscapeHTML, though it may change some outputs
			}
		}
	}
	var codec = pgType.Codec

	// Special case for the text codec (used for TEXT and VARCHAR and a few other column
	// types), which just casts its input bytes to a Go string. That cast can be relatively
	// expensive, and in our particular case we know that we are never going to modify the
	// bytes and only need to marshal the string as a JSON string with escaping, so we can
	// go significantly faster by using an unsafe cast.
	if _, ok := codec.(pgtype.TextCodec); ok {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if bs == nil {
				return append(buf, []byte("null")...), nil // Literally append "null" when it's nil
			}
			var str = *(*string)(unsafe.Pointer(&bs))
			return json.AppendEscape(buf, str, 0), nil
		}
	}

	if _, ok := codec.(pgtype.Int2Codec); ok && fieldDescription.Format == pgtype.BinaryFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if bs == nil {
				return append(buf, []byte("null")...), nil // Literally append "null" when it's nil
			} else if len(bs) != 2 {
				return nil, fmt.Errorf("expected 2 bytes for INT2, got %d bytes", len(bs))
			}
			return strconv.AppendInt(buf, int64(int16(binary.BigEndian.Uint16(bs))), 10), nil
		}
	}

	if _, ok := codec.(pgtype.Int4Codec); ok && fieldDescription.Format == pgtype.BinaryFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if bs == nil {
				return append(buf, []byte("null")...), nil // Literally append "null" when it's nil
			} else if len(bs) != 4 {
				return nil, fmt.Errorf("expected 4 bytes for INT4, got %d bytes", len(bs))
			}
			return strconv.AppendInt(buf, int64(int32(binary.BigEndian.Uint32(bs))), 10), nil
		}
	}

	if _, ok := codec.(pgtype.Int8Codec); ok && fieldDescription.Format == pgtype.BinaryFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if bs == nil {
				return append(buf, []byte("null")...), nil // Literally append "null" when it's nil
			} else if len(bs) != 8 {
				return nil, fmt.Errorf("expected 8 bytes for INT8, got %d bytes", len(bs))
			}
			return strconv.AppendInt(buf, int64(binary.BigEndian.Uint64(bs)), 10), nil
		}
	}

	if _, ok := codec.(pgtype.Int2Codec); ok && fieldDescription.Format == pgtype.TextFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if bs == nil {
				return append(buf, []byte("null")...), nil // Literally append "null" when it's nil
			}
			return append(buf, bs...), nil // Text-format integers are just decimal numbers, which are valid JSON
		}
	}

	if _, ok := codec.(pgtype.Int4Codec); ok && fieldDescription.Format == pgtype.TextFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if bs == nil {
				return append(buf, []byte("null")...), nil // Literally append "null" when it's nil
			}
			return append(buf, bs...), nil // Text-format integers are just decimal numbers, which are valid JSON
		}
	}

	if _, ok := codec.(pgtype.Int8Codec); ok && fieldDescription.Format == pgtype.TextFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if bs == nil {
				return append(buf, []byte("null")...), nil // Literally append "null" when it's nil
			}
			return append(buf, bs...), nil // Text-format integers are just decimal numbers, which are valid JSON
		}
	}

	// This is the main value encoding logic used for most data types outside of special cases.
	// We go the long way, first using PGX to decode the value and then translating it to another
	// Go value and then finally serializing that as JSON.
	return func(buf []byte, bs []byte) ([]byte, error) {
		var val any
		var err error
		if bs == nil {
			val = nil
		} else {
			val, err = codec.DecodeValue(typeMap, fieldDescription.DataTypeOID, fieldDescription.Format, bs)
		}
		if err != nil {
			return nil, fmt.Errorf("error decoding value for column %q: %w", string(fieldDescription.Name), err)
		} else if translated, err := db.translateRecordField(columnInfo, isPrimaryKey, val); err != nil {
			return nil, fmt.Errorf("error translating value %v: %w", val, err)
		} else {
			return json.Append(buf, translated, json.EscapeHTML) // Consider removing json.EscapeHTML, though it will change some outputs like the `circle` column type
		}
	}
}

// backfillFDBTranscoder returns an fdbTranscoder for a specific column in a Postgres backfill.
func (db *postgresDatabase) backfillFDBTranscoder(typeMap *pgtype.Map, fieldDescription *pgconn.FieldDescription, columnInfo *sqlcapture.ColumnInfo) fdbTranscoder {
	var columnType = columnInfo.DataType
	var codec pgtype.Codec
	if pgType, ok := typeMap.TypeForOID(fieldDescription.DataTypeOID); ok {
		codec = pgType.Codec
	} else {
		codec = &pgtype.TextCodec{} // Default to text codec for unknown OIDs
	}

	if _, ok := codec.(pgtype.TextCodec); ok {
		return func(buf []byte, bs []byte) ([]byte, error) {
			return sqlcapture.AppendBytesFDB(buf, sqlcapture.FDBStringCode, bs)
		}
	}

	if _, ok := codec.(pgtype.Int2Codec); ok && fieldDescription.Format == pgtype.BinaryFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if len(bs) != 2 {
				return nil, fmt.Errorf("expected 2 bytes for INT2, got %d bytes", len(bs))
			}
			return sqlcapture.AppendIntFDB(buf, int64(int16(binary.BigEndian.Uint16(bs))))
		}
	}

	if _, ok := codec.(pgtype.Int4Codec); ok && fieldDescription.Format == pgtype.BinaryFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if len(bs) != 4 {
				return nil, fmt.Errorf("expected 4 bytes for INT4, got %d bytes", len(bs))
			}
			return sqlcapture.AppendIntFDB(buf, int64(int32(binary.BigEndian.Uint32(bs))))
		}
	}

	if _, ok := codec.(pgtype.Int8Codec); ok && fieldDescription.Format == pgtype.BinaryFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			if len(bs) != 8 {
				return nil, fmt.Errorf("expected 8 bytes for INT8, got %d bytes", len(bs))
			}
			return sqlcapture.AppendIntFDB(buf, int64(binary.BigEndian.Uint64(bs)))
		}
	}

	if _, ok := codec.(pgtype.Int2Codec); ok && fieldDescription.Format == pgtype.TextFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			var val, err = strconv.ParseInt(string(bs), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing INT2 value %q for column %q: %w", string(bs), string(fieldDescription.Name), err)
			}
			return sqlcapture.AppendIntFDB(buf, val)
		}
	}

	if _, ok := codec.(pgtype.Int4Codec); ok && fieldDescription.Format == pgtype.TextFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			var val, err = strconv.ParseInt(string(bs), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing INT4 value %q for column %q: %w", string(bs), string(fieldDescription.Name), err)
			}
			return sqlcapture.AppendIntFDB(buf, val)
		}
	}

	if _, ok := codec.(pgtype.Int8Codec); ok && fieldDescription.Format == pgtype.TextFormatCode {
		return func(buf []byte, bs []byte) ([]byte, error) {
			var val, err = strconv.ParseInt(string(bs), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing INT8 value %q for column %q: %w", string(bs), string(fieldDescription.Name), err)
			}
			return sqlcapture.AppendIntFDB(buf, val)
		}
	}

	return func(buf, v []byte) ([]byte, error) {
		var val any
		var err error
		if v == nil {
			val = nil
		} else {
			val, err = codec.DecodeValue(typeMap, fieldDescription.DataTypeOID, fieldDescription.Format, v)
		}
		if err != nil {
			return nil, fmt.Errorf("error decoding column %q: %w", string(fieldDescription.Name), err)
		} else if translated, err := encodeKeyFDB(val, columnType); err != nil {
			return nil, fmt.Errorf("error translating column %q: %w", string(fieldDescription.Name), err)
		} else {
			return sqlcapture.AppendFDB(buf, translated)
		}
	}
}

// replicationJSONTranscoder returns a jsonTranscoder for a specific column of a specific table in a Postgres replication stream.
func (db *postgresDatabase) replicationJSONTranscoder(typeMap *pgtype.Map, typeOID uint32, columnInfo *sqlcapture.ColumnInfo, isPrimaryKey bool) jsonTranscoder {
	var fieldDescription = &pgconn.FieldDescription{
		Name:        columnInfo.Name,
		DataTypeOID: typeOID,
		Format:      pgtype.TextFormatCode, // Replication streams use text format
	}
	return db.backfillJSONTranscoder(typeMap, fieldDescription, columnInfo, isPrimaryKey)
}

// replicationFDBTranscoder returns an fdbTranscoder for a specific column of a specific table in a Postgres replication stream.
func (db *postgresDatabase) replicationFDBTranscoder(typeMap *pgtype.Map, typeOID uint32, columnInfo *sqlcapture.ColumnInfo) fdbTranscoder {
	var fieldDescription = &pgconn.FieldDescription{
		Name:        columnInfo.Name,
		DataTypeOID: typeOID,
		Format:      pgtype.TextFormatCode, // Replication streams use text format
	}
	return db.backfillFDBTranscoder(typeMap, fieldDescription, columnInfo)
}

// TODO(wgd): Reinstate this timestamptz special case
// if _, ok := err.(*time.ParseError); ok && fd.DataTypeOID == pgtype.TimestamptzOID {
// 	// PostgreSQL supports dates/timestamps with years greater than 9999 or less than
// 	// 0 AD, but the PGX client library uses a naive `time.Parse()` call which doesn't
// 	// support 5-digit or negative years. We can't represent those as RFC3339 timestamps
// 	// anyway, so if a timestamp fails to parse just map it to a sentinel value.
// 	value = negativeInfinityTimestamp
// } else if err != nil {
// 	return nil, fmt.Errorf("error decoding value for column %q: %w", string(fd.Name), err)
// }
