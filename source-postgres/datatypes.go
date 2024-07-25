package main

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	infinityTimestamp         = "9999-12-31T23:59:59Z"
	negativeInfinityTimestamp = "0000-01-01T00:00:00Z"
	rfc3339TimeFormat         = "15:04:05.999999999Z07:00"
	truncateColumnThreshold   = 8 * 1024 * 1024 // Arbitrarily selected value
)

func registerDatatypeTweaks(m *pgtype.Map) error {
	// Prefer text format for 'timestamptz' column results. This is important because the
	// text format is reported in the configured time zone of the server (since we haven't
	// set it on our session) while the binary format is always a Unix microsecond timestamp
	// in UTC and then Go's time.Unix() turns that into a local-time value. Thus without this
	// customization backfills and replication will report the same instant in time in different
	// time zones. The easiest way to make them consistent is to just ask for text here.
	var tstz = &pgtype.Type{Name: "timestamptz", OID: pgtype.TimestamptzOID, Codec: &preferTextCodec{inner: &pgtype.TimestamptzCodec{}}}
	m.RegisterType(tstz)

	// Also prefer text format for the endpoints of 'tstzrange' values for the same reason.
	m.RegisterType(&pgtype.Type{Name: "tstzrange", OID: pgtype.TstzrangeOID, Codec: &preferTextCodec{&pgtype.RangeCodec{ElementType: tstz}}})

	// Decode array column types into the dimensioned `pgtype.Array[any]` type rather than
	// the default behavior of a flattened `[]any` list.
	//
	// List of array type OIDs taken from initDefaultMap() at pgtype_default.go:110
	//
	// TODO(wgd): How does this apply to arrays of user-defined data types like enums?
	var arrayTypeOIDs = []uint32{
		pgtype.ACLItemArrayOID, pgtype.BitArrayOID, pgtype.BoolArrayOID, pgtype.BoxArrayOID, pgtype.BPCharArrayOID, pgtype.ByteaArrayOID, pgtype.QCharArrayOID, pgtype.CIDArrayOID,
		pgtype.CIDRArrayOID, pgtype.CircleArrayOID, pgtype.DateArrayOID, pgtype.DaterangeArrayOID, pgtype.Float4ArrayOID, pgtype.Float8ArrayOID, pgtype.InetArrayOID, pgtype.Int2ArrayOID,
		pgtype.Int4ArrayOID, pgtype.Int4rangeArrayOID, pgtype.Int8ArrayOID, pgtype.Int8rangeArrayOID, pgtype.IntervalArrayOID, pgtype.JSONArrayOID, pgtype.JSONBArrayOID, pgtype.JSONPathArrayOID,
		pgtype.LineArrayOID, pgtype.LsegArrayOID, pgtype.MacaddrArrayOID, pgtype.NameArrayOID, pgtype.NumericArrayOID, pgtype.NumrangeArrayOID, pgtype.OIDArrayOID, pgtype.PathArrayOID,
		pgtype.PointArrayOID, pgtype.PolygonArrayOID, pgtype.RecordArrayOID, pgtype.TextArrayOID, pgtype.TIDArrayOID, pgtype.TimeArrayOID, pgtype.TimestampArrayOID, pgtype.TimestamptzArrayOID,
		pgtype.TsrangeArrayOID, pgtype.TstzrangeArrayOID, pgtype.UUIDArrayOID, pgtype.VarbitArrayOID, pgtype.VarcharArrayOID, pgtype.XIDArrayOID,
	}
	for _, oid := range arrayTypeOIDs {
		var typ, ok = m.TypeForOID(oid)
		if !ok {
			return fmt.Errorf("error adjusting array codec: OID %d not found", oid)
		}
		codec, ok := typ.Codec.(*pgtype.ArrayCodec)
		if !ok {
			return fmt.Errorf("error adjusting array codec: OID %d does not have array codec", oid)
		}
		m.RegisterType(&pgtype.Type{
			Name:  typ.Name,
			OID:   typ.OID,
			Codec: &customDecodingCodec{decodeDimensionedArray, &pgtype.ArrayCodec{ElementType: codec.ElementType}},
		})
	}

	// 'Decode' JSON and JSONB column values by directly turning the bytes into a json.RawMessage
	// rather than unmarshalling them as the normal DecodeValue implementation for these types would.
	m.RegisterType(&pgtype.Type{
		Name:  "json",
		OID:   pgtype.JSONOID,
		Codec: &customDecodingCodec{decodeRawJSON, &pgtype.JSONCodec{Marshal: json.Marshal, Unmarshal: json.Unmarshal}},
	})
	m.RegisterType(&pgtype.Type{
		Name:  "jsonb",
		OID:   pgtype.JSONBOID,
		Codec: &customDecodingCodec{decodeRawJSONB, &pgtype.JSONBCodec{Marshal: json.Marshal, Unmarshal: json.Unmarshal}},
	})
	return nil
}

// preferTextCodec wraps a PGX datatype codec to make it always report text as its preferred format if supported.
type preferTextCodec struct{ inner pgtype.Codec }

func (c *preferTextCodec) FormatSupported(f int16) bool { return c.inner.FormatSupported(f) }
func (c *preferTextCodec) PreferredFormat() int16 {
	if c.FormatSupported(pgtype.TextFormatCode) {
		return pgtype.TextFormatCode
	}
	return c.inner.PreferredFormat()
}
func (c *preferTextCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	return c.inner.PlanEncode(m, oid, format, value)
}
func (c *preferTextCodec) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	return c.inner.PlanScan(m, oid, format, target)
}
func (c *preferTextCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	return c.inner.DecodeDatabaseSQLValue(m, oid, format, src)
}
func (c *preferTextCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	return c.inner.DecodeValue(m, oid, format, src)
}

// customDecodingCodec wraps an inner codec but overrides the DecodeValue function with a custom version.
type customDecodingCodec struct {
	decode func(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error)
	inner  pgtype.Codec
}

func (c *customDecodingCodec) FormatSupported(f int16) bool { return c.inner.FormatSupported(f) }
func (c *customDecodingCodec) PreferredFormat() int16       { return c.inner.PreferredFormat() }
func (c *customDecodingCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	return c.inner.PlanEncode(m, oid, format, value)
}
func (c *customDecodingCodec) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	return c.inner.PlanScan(m, oid, format, target)
}
func (c *customDecodingCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	return c.inner.DecodeDatabaseSQLValue(m, oid, format, src)
}
func (c *customDecodingCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	return c.decode(m, oid, format, src)
}

// decodeDimensionedArray is a custom DecodeValue function which decodes into a `pgtype.Array[any]`
func decodeDimensionedArray(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	var dst pgtype.Array[any]
	err := m.PlanScan(oid, format, &dst).Scan(src, &dst)
	return dst, err
}

// decodeRawJSON is a custom DecodeValue function which turns the input bytes directly into a json.RawMessage.
func decodeRawJSON(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	return json.RawMessage(src), nil
}

// decodeRawJSONB is a custom DecodeValue function which turns the input bytes directly into a json.RawMessage,
// but if the input format is binary it strips off a leading '0x01' prefix.
func decodeRawJSONB(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	if format == pgtype.BinaryFormatCode {
		if len(src) == 0 || src[0] != 1 {
			return nil, fmt.Errorf("invalid binary-format jsonb value: %#v", src)
		}
		src = src[1:]
	}
	return json.RawMessage(src), nil
}

func translateRecordFields(table *sqlcapture.DiscoveryInfo, f map[string]interface{}) error {
	if f == nil {
		return nil
	}
	for id, val := range f {
		var columnInfo *sqlcapture.ColumnInfo
		if table != nil {
			if info, ok := table.Columns[id]; ok {
				columnInfo = &info
			}
		}

		var translated, err = translateRecordField(columnInfo, val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

func oversizePlaceholderJSON(orig []byte) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"flow_truncated":true,"original_size":%d}`, len(orig)))
}

// translateRecordField "translates" a value from the PostgreSQL driver into
// an appropriate JSON-encodeable output format. As a concrete example, the
// PostgreSQL `cidr` type becomes a `*net.IPNet`, but the default JSON
// marshalling of a `net.IPNet` isn't a great fit and we'd prefer to use
// the `String()` method to get the usual "192.168.100.0/24" notation.
func translateRecordField(column *sqlcapture.ColumnInfo, val interface{}) (interface{}, error) {
	var dataType any
	if column != nil {
		dataType = column.DataType
	}

	switch dataType {
	case "timetz":
		if x, ok := val.(string); ok {
			var formats = []string{
				"15:04:05.999999999Z07:00",
				"15:04:05.999999999Z07",
				"15:04:05Z07:00",
				"15:04:05Z07",
			}
			for _, format := range formats {
				if t, err := time.Parse(format, x); err == nil {
					return t.Format(rfc3339TimeFormat), nil
				}
			}
		}
	}
	switch x := val.(type) {
	case net.HardwareAddr: // column types 'macaddr' and 'macaddr8'
		return x.String(), nil
	case [16]uint8: // column type 'uuid'
		var s = new(strings.Builder)
		for i := range x {
			if i == 4 || i == 6 || i == 8 || i == 10 {
				s.WriteString("-")
			}
			fmt.Fprintf(s, "%02x", x[i])
		}
		return s.String(), nil
	case string:
		if len(x) > truncateColumnThreshold {
			return x[:truncateColumnThreshold], nil
		}
		return x, nil
	case []byte:
		if len(x) > truncateColumnThreshold {
			return x[:truncateColumnThreshold], nil
		}
		return x, nil
	case json.RawMessage:
		if len(x) > truncateColumnThreshold {
			return oversizePlaceholderJSON(x), nil
		}
		return x, nil
	case pgtype.Array[any]:
		return translateArray(column, x)
	case pgtype.Range[any]:
		return stringifyRange(x)
	case pgtype.Text:
		if len(x.String) > truncateColumnThreshold {
			return x.String[:truncateColumnThreshold], nil
		}
		return x.String, nil
	case pgtype.InfinityModifier:
		// Postgres has special timestamp values for "infinity" and "-infinity" which it handles
		// internally for performing comparisions. We do our best to represent these as an RFC3339
		// timestamp string here, as the smallest possible timestamp in the case of negative
		// infinity and the largest possible for infinity. There is also a pgtype.None infinity
		// modifier which is being left unhandled currently as I don't know that it has any meaning
		// for a captured value.
		if x == pgtype.Infinity {
			return infinityTimestamp, nil
		} else if x == pgtype.NegativeInfinity {
			return negativeInfinityTimestamp, nil
		}
	case time.Time:
		return formatRFC3339(x)
	case pgtype.Time:
		return x.Microseconds, nil // For historical reasons, times (note: not timestamps) without time zone are serialized as Unix microseconds
	case pgtype.Numeric:
		// By default a Numeric value is marshalled to a JSON number, but most JSON parsers and
		// encoders suffer from precision issues so we really want them to be turned into a JSON
		// string containing the formatted number instead.
		var bs, err = json.Marshal(x)
		if err != nil {
			return nil, err
		}
		return string(bs), nil
	case pgtype.Bits:
		return x.Value() // The Value() method encodes to the desired "0101" text string
	case pgtype.Interval:
		return x.Value()
	case pgtype.Line:
		return x.Value()
	case pgtype.Lseg:
		return x.Value()
	case pgtype.Box:
		return x.Value()
	case pgtype.Path:
		return x.Value()
	case pgtype.Polygon:
		return x.Value()
	case pgtype.Circle:
		return x.Value()
	}

	return val, nil
}

func stringifyRange(r pgtype.Range[any]) (string, error) {
	if r.LowerType == pgtype.Empty || r.UpperType == pgtype.Empty {
		return "empty", nil
	}

	var buf = new(strings.Builder)
	switch r.LowerType {
	case pgtype.Inclusive:
		buf.WriteString("[")
	case pgtype.Exclusive, pgtype.Unbounded:
		buf.WriteString("(")
	}
	if r.LowerType == pgtype.Inclusive || r.LowerType == pgtype.Exclusive {
		if translated, err := translateRecordField(nil, r.Lower); err != nil {
			fmt.Fprintf(buf, "%v", r.Lower)
		} else {
			fmt.Fprintf(buf, "%v", translated)
		}
	}
	buf.WriteString(",")
	if r.UpperType == pgtype.Inclusive || r.UpperType == pgtype.Exclusive {
		if translated, err := translateRecordField(nil, r.Upper); err != nil {
			fmt.Fprintf(buf, "%v", r.Upper)
		} else {
			fmt.Fprintf(buf, "%v", translated)
		}
	}
	switch r.UpperType {
	case pgtype.Inclusive:
		buf.WriteString("]")
	case pgtype.Exclusive, pgtype.Unbounded:
		buf.WriteString(")")
	}
	return buf.String(), nil
}

func formatRFC3339(t time.Time) (any, error) {
	if t.Year() < 0 || t.Year() > 9999 {
		// We could in theory clamp excessively large years to positive infinity, but this
		// is of limited usefulness since these are never real dates, they're mostly just
		// dumb typos like `20221` and so we might as well normalize errors consistently.
		return negativeInfinityTimestamp, nil
	}
	return t.Format(time.RFC3339Nano), nil
}

func translateArray(_ *sqlcapture.ColumnInfo, x pgtype.Array[any]) (any, error) {
	var dims []int
	for _, dim := range x.Dims {
		dims = append(dims, int(dim.Length))
	}
	for idx := range x.Elements {
		var translated, err = translateRecordField(nil, x.Elements[idx])
		if err != nil {
			return nil, err
		}
		x.Elements[idx] = translated
	}
	return map[string]any{
		"dimensions": dims,
		"elements":   x.Elements,
	}, nil
}
