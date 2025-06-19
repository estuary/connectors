package main

import (
	"fmt"
	"sync"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/segmentio/encoding/json"
)

// postgresBackfillMetadata holds metadata which can be shared across many backfill events.
type postgresBackfillMetadata struct {
	StreamID sqlcapture.StreamID // StreamID of the table this backfill event came from.
	Shape    *encrow.Shape       // Shape of the row values, used to serialize them to JSON.
}

// postgresBackfillEvent is a specialized Postgres implementation of sqlcapture.ChangeEvent for backfill rows.
type postgresBackfillEvent struct {
	Meta *postgresBackfillMetadata

	RowKey      []byte                   // Serialized row key, if applicable.
	Values      [][]byte                 // Raw byte values of the row, in DB column order.
	DocMetadata postgresDocumentMetadata // Document metadata object, which will be appended to the end of the row values.
}

type postgresDocumentMetadata struct {
	Operation sqlcapture.ChangeOp `json:"op"`
	Source    postgresSource      `json:"source"`
}

func (postgresBackfillEvent) IsDatabaseEvent() {}

func (e *postgresBackfillEvent) String() string {
	return fmt.Sprintf("Backfill(%s)", e.Meta.StreamID)
}

func (e *postgresBackfillEvent) StreamID() sqlcapture.StreamID {
	return e.Meta.StreamID
}

func (e *postgresBackfillEvent) GetRowKey() []byte {
	return e.RowKey
}

// eventValuesPool is a sync.Pool used to reduce allocations when encoding backfill events.
var eventValuesPool = sync.Pool{New: func() any { return make([]any, 16) }}

func (e *postgresBackfillEvent) MarshalToJSON(buf []byte) ([]byte, error) {
	var vals = eventValuesPool.Get().([]any)[:0]
	defer eventValuesPool.Put(vals)
	for _, v := range e.Values {
		vals = append(vals, v)
	}
	vals = append(vals, &e.DocMetadata)
	return e.Meta.Shape.Encode(buf, vals)
}

// valueEncoderFunc is a generic encrow.ValueEncoder which wraps a function, typically a closure.
type valueEncoderFunc struct {
	MarshalFunc func(buf []byte, v any) ([]byte, error)
}

// MarshalTo encodes a value as JSON and appends it to the provided buffer.
func (e *valueEncoderFunc) MarshalTo(buf []byte, v any) ([]byte, error) {
	return e.MarshalFunc(buf, v)
}

// backfillValueEncoder returns an encrow.ValueEncoder for encoding values of a specific column in a Postgres backfill.
func (db *postgresDatabase) backfillValueEncoder(typeMap *pgtype.Map, fieldDescription *pgconn.FieldDescription, columnInfo *sqlcapture.ColumnInfo, isPrimaryKey bool) encrow.ValueEncoder {
	var pgType, ok = typeMap.TypeForOID(fieldDescription.DataTypeOID)
	if ok {
		return &valueEncoderFunc{func(buf []byte, v any) ([]byte, error) {
			if bs, ok := v.([]byte); !ok {
				return nil, fmt.Errorf("expected []byte, got %T", v)
			} else if val, err := pgType.Codec.DecodeValue(typeMap, fieldDescription.DataTypeOID, fieldDescription.Format, bs); err != nil {
				return nil, fmt.Errorf("error decoding value for column %q: %w", string(fieldDescription.Name), err)
			} else if translated, err := db.translateRecordField(columnInfo, isPrimaryKey, val); err != nil {
				return nil, fmt.Errorf("error translating value %v: %w", v, err)
			} else {
				return json.Append(buf, translated, 0)
			}
		}}
	} else {
		return &valueEncoderFunc{func(buf []byte, v any) ([]byte, error) {
			var bs, ok = v.([]byte)
			if !ok {
				return nil, fmt.Errorf("expected []byte, got %T", v)
			}
			switch fieldDescription.Format {
			case pgtype.TextFormatCode:
				return json.AppendEscape(buf, string(bs), 0), nil
			case pgtype.BinaryFormatCode:
				newBuf := make([]byte, len(bs))
				copy(newBuf, bs)
				return json.Append(buf, newBuf, 0)
			default:
				return nil, fmt.Errorf("unknown format code %d", fieldDescription.Format)
			}
		}}
	}
}
