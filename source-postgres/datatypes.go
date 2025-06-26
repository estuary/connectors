package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	infinityTimestamp         = "9999-12-31T23:59:59Z"
	negativeInfinityTimestamp = "0000-01-01T00:00:00Z"
	infinityDate              = "9999-12-31"
	negativeInfinityDate      = "0000-01-01"
	rfc3339TimeFormat         = "15:04:05.999999999Z07:00"
	truncateColumnThreshold   = 8 * 1024 * 1024 // Arbitrarily selected value
)

func registerDatatypeTweaks(ctx context.Context, conn *pgx.Conn, m *pgtype.Map) error {
	// Prefer text format for 'timestamptz' column results. This is important because the
	// text format is reported in the configured time zone of the server (since we haven't
	// set it on our session) while the binary format is always a Unix microsecond timestamp
	// in UTC and then Go's time.Unix() turns that into a local-time value. Thus without this
	// customization backfills and replication will report the same instant in time in different
	// time zones. The easiest way to make them consistent is to just ask for text here.
	// Register custom codecs for 'timestamp' and 'timestamptz' column types. There are two
	// reasons for this:
	//   1. The timestamptz text format communicates a time zone while the binary format doesn't,
	//      so for consistency with replication (which is always text format) we need to prefer
	//      text format for backfills as well.
	//   2. Both 'timestamp' and 'timestamptz' need to handle dates with years greater than 9999 or less than 0 AD,
	var tsType = &pgtype.Type{Name: "timestamp", OID: pgtype.TimestampOID, Codec: &customTimestampCodec{}}
	var tstzType = &pgtype.Type{Name: "timestamptz", OID: pgtype.TimestamptzOID, Codec: &customTimestamptzCodec{}}
	m.RegisterType(tsType)
	m.RegisterType(tstzType)

	// Also prefer text format for the endpoints of 'tsrange' and 'tstzrange' values for the same reason.
	m.RegisterType(&pgtype.Type{Name: "tsrange", OID: pgtype.TstzrangeOID, Codec: &preferTextCodec{&pgtype.RangeCodec{ElementType: tsType}}})
	m.RegisterType(&pgtype.Type{Name: "tstzrange", OID: pgtype.TstzrangeOID, Codec: &preferTextCodec{&pgtype.RangeCodec{ElementType: tstzType}}})

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

	// Query pg_catalog.pg_type for specific extension types we support, and install the
	// appropriate scalar and array codecs for them.
	var queryExtensionTypes = `
		SELECT t.oid, n.nspname, t.typname, t.typarray
		  FROM pg_catalog.pg_type t
		  JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		  WHERE n.nspname = 'public' AND t.typname IN ('citext');
	`
	var extensionTypes, err = conn.Query(ctx, queryExtensionTypes)
	if err != nil {
		return fmt.Errorf("error querying extension types: %w", err)
	}
	defer extensionTypes.Close()
	for extensionTypes.Next() {
		var typeOID, arrayOID uint32
		var schemaName, typeName string
		if err := extensionTypes.Scan(&typeOID, &schemaName, &typeName, &arrayOID); err != nil {
			return fmt.Errorf("error querying extension types: %w", err)
		}

		// Map the type name to the appropriate codec. This right here is the bit we'll need to tweak
		// if/when we support an extension type that isn't just a glorified string.
		//
		// But for now 'citext' is the only one, and it's a string, so there's no need making this
		// more complicated than it needs to be.
		var typeCodec = pgtype.TextCodec{}

		// Register mappings for the type and also arrays of that type
		var baseType = &pgtype.Type{OID: typeOID, Name: typeName, Codec: typeCodec}
		m.RegisterType(baseType)
		if arrayOID != 0 {
			var arrayCodec = &customDecodingCodec{decodeDimensionedArray, &pgtype.ArrayCodec{ElementType: baseType}}
			m.RegisterType(&pgtype.Type{OID: arrayOID, Name: "_" + typeName, Codec: arrayCodec})
		}
	}
	if extensionTypes.Err() != nil {
		return fmt.Errorf("error querying extension types: %w", err)
	}
	return nil
}

// The custom codec we register for timestamp columns.
type customTimestampCodec struct{ inner pgtype.TimestampCodec }

func (c *customTimestampCodec) FormatSupported(f int16) bool { return c.inner.FormatSupported(f) }
func (c *customTimestampCodec) PreferredFormat() int16       { return c.inner.PreferredFormat() }
func (c *customTimestampCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	return c.inner.PlanEncode(m, oid, format, value)
}
func (c *customTimestampCodec) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	return c.inner.PlanScan(m, oid, format, target)
}
func (c *customTimestampCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	return c.inner.DecodeDatabaseSQLValue(m, oid, format, src)
}
func (c *customTimestampCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	var val, err = c.inner.DecodeValue(m, oid, format, src)
	if _, ok := err.(*time.ParseError); ok {
		// PostgreSQL supports dates/timestamps with years greater than 9999 or less than 0 AD,
		// but Go time.Parse() doesn't and neither can they be represented as RFC3339 timestamps,
		// so we detect a parse error and replace it with a sentinel value that can.
		return negativeInfinityTimestamp, nil
	} else if err != nil {
		return nil, err
	}
	return val, nil
}

// The custom codec we register for timestamptz columns.
type customTimestamptzCodec struct{ inner pgtype.TimestamptzCodec }

func (c *customTimestamptzCodec) FormatSupported(f int16) bool { return c.inner.FormatSupported(f) }
func (c *customTimestamptzCodec) PreferredFormat() int16       { return pgtype.TextFormatCode }
func (c *customTimestamptzCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	return c.inner.PlanEncode(m, oid, format, value)
}
func (c *customTimestamptzCodec) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	return c.inner.PlanScan(m, oid, format, target)
}
func (c *customTimestamptzCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	return c.inner.DecodeDatabaseSQLValue(m, oid, format, src)
}
func (c *customTimestamptzCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	var val, err = c.inner.DecodeValue(m, oid, format, src)
	if _, ok := err.(*time.ParseError); ok {
		// PostgreSQL supports dates/timestamps with years greater than 9999 or less than 0 AD,
		// but Go time.Parse() doesn't and neither can they be represented as RFC3339 timestamps,
		// so we detect a parse error and replace it with a sentinel value that can.
		return negativeInfinityTimestamp, nil
	} else if err != nil {
		return nil, err
	}
	return val, nil
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

func (db *postgresDatabase) translateRecordFields(table *sqlcapture.DiscoveryInfo, f map[string]interface{}) error {
	if f == nil {
		return nil
	}
	for id, val := range f {
		var columnInfo *sqlcapture.ColumnInfo
		var isPrimaryKey bool
		if table != nil {
			if info, ok := table.Columns[id]; ok {
				columnInfo = &info
			}
			isPrimaryKey = slices.Contains(table.PrimaryKey, id)
		}

		var translated, err = db.translateRecordField(columnInfo, isPrimaryKey, val)
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
func (db *postgresDatabase) translateRecordField(column *sqlcapture.ColumnInfo, isPrimaryKey bool, val any) (any, error) {
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
	case "time":
		if t, ok := val.(pgtype.Time); ok {
			if db.featureFlags["time_as_time"] {
				return time.UnixMicro(t.Microseconds).UTC().Format(rfc3339TimeFormat), nil
			} else {
				return t.Microseconds, nil // Historical behavior
			}
		} else if val == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unexpected value for column of type %q: %#v", dataType, val)
	case "date":
		if t, ok := val.(time.Time); ok {
			if db.featureFlags["date_as_date"] {
				if t.Year() < 0 || t.Year() > 9999 {
					// A valid `format: date` satisfies the RFC3339 full-date production rule, which only
					// permits a positive 4-digit year. Replace out-of-range dates with the zero date.
					t = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
				}
				return t.Format("2006-01-02"), nil
			} else {
				return formatRFC3339(t) // Historical behavior
			}
		} else if x, ok := val.(pgtype.InfinityModifier); ok {
			if !db.featureFlags["date_as_date"] {
				if x == pgtype.Infinity {
					return infinityTimestamp, nil
				}
				return negativeInfinityTimestamp, nil
			}
			if x == pgtype.Infinity {
				return infinityDate, nil
			}
			return negativeInfinityDate, nil
		} else if val == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unexpected value for column of type %q: %#v", dataType, val)
	}
	switch x := val.(type) {
	case net.HardwareAddr: // column types 'macaddr' and 'macaddr8'
		return x.String(), nil
	case [16]uint8: // column type 'uuid'
		return uuid.UUID(x).String(), nil
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
	case float32:
		if str, ok := stringifySpecialFloats(float64(x)); ok {
			return str, nil
		}
		if isPrimaryKey {
			return strconv.FormatFloat(float64(x), 'f', -1, 32), nil
		}
	case float64:
		if str, ok := stringifySpecialFloats(x); ok {
			return str, nil
		}
		if isPrimaryKey {
			return strconv.FormatFloat(x, 'f', -1, 64), nil
		}
	case json.RawMessage:
		if len(x) > truncateColumnThreshold {
			return oversizePlaceholderJSON(x), nil
		}
		return x, nil
	case pgtype.Array[any]:
		return db.translateArray(column, isPrimaryKey, x)
	case pgtype.Range[any]:
		return db.stringifyRange(x, isPrimaryKey)
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
	case pgtype.Numeric:
		return x.Value() // Happily the stringified representations of "NaN", "Infinity", and "-Infinity" exactly match what we need
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

// stringifySpecialFloats replaces the legal IEEE-754 values NaN/Infinity/-Infinity (which
// don't have a defined JSON representation) with equivalent strings.
func stringifySpecialFloats(x float64) (string, bool) {
	if math.IsNaN(x) {
		return "NaN", true
	} else if math.IsInf(x, +1) {
		return "Infinity", true
	} else if math.IsInf(x, -1) {
		return "-Infinity", true
	}
	return "", false
}

func (db *postgresDatabase) stringifyRange(r pgtype.Range[any], isPrimaryKey bool) (string, error) {
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
		if translated, err := db.translateRecordField(nil, isPrimaryKey, r.Lower); err != nil {
			fmt.Fprintf(buf, "%v", r.Lower)
		} else {
			fmt.Fprintf(buf, "%v", translated)
		}
	}
	buf.WriteString(",")
	if r.UpperType == pgtype.Inclusive || r.UpperType == pgtype.Exclusive {
		if translated, err := db.translateRecordField(nil, isPrimaryKey, r.Upper); err != nil {
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

func (db *postgresDatabase) translateArray(column *sqlcapture.ColumnInfo, isPrimaryKey bool, x pgtype.Array[any]) (any, error) {
	// Construct a ColumnInfo representing a theoretical scalar version of the array column
	var scalarColumn *sqlcapture.ColumnInfo
	if column != nil {
		var copyColumn = *column
		scalarColumn = &copyColumn
		if str, ok := scalarColumn.DataType.(string); ok {
			scalarColumn.DataType = strings.TrimLeft(str, "_")
		}
	}

	// Translate the values of x.Elements in place (since we're discarding the original
	// pgtype.Array value after this).
	for idx := range x.Elements {
		var translated, err = db.translateRecordField(scalarColumn, isPrimaryKey, x.Elements[idx])
		if err != nil {
			return nil, err
		}
		x.Elements[idx] = translated
	}

	// If we're supposed to produce flat element arrays, just return the elements array now.
	if db.featureFlags["flatten_arrays"] && !db.featureFlags["multidimensional_arrays"] {
		return x.Elements, nil
	}

	var dims = make([]int, 0)
	for _, dim := range x.Dims {
		dims = append(dims, int(dim.Length))
	}

	// If we're supposed to output the old object representation, return that object now.
	if !db.featureFlags["flatten_arrays"] && !db.featureFlags["multidimensional_arrays"] {
		return map[string]any{
			"dimensions": dims,
			"elements":   x.Elements,
		}, nil
	}

	// Otherwise we need to un-flatten the elements array into the appropriate dimensionality.
	return unflattenArray(x.Elements, dims)
}

func unflattenArray(elements []any, dims []int) (any, error) {
	// Special cases
	if len(dims) == 0 {
		return []any{}, nil
	} else if len(dims) == 1 {
		return elements, nil
	}

	// Validate correct element count (should always be the case since it's
	// coming from PostgreSQL, but just in case we ought to pad it out with
	// nulls or something)
	var cardinality = dims[0]
	for _, d := range dims[1:] {
		cardinality *= d
	}
	if len(elements) != cardinality {
		elements = append(elements, make([]any, cardinality-len(elements))...)
	}

	// Iterate from last dimension to first, chunking up the elements into slices
	// of the appropriate length and then replacing the original elements array
	// with the new chunked version. We won't need to actually do anything with
	// the first dimension since the elements array will have the appropriate
	// length once we reach it.
	for dimensionIndex := len(dims) - 1; dimensionIndex > 0; dimensionIndex-- {
		var dimensionSize = dims[dimensionIndex]
		var chunks = make([]any, len(elements)/dimensionSize)
		for idx := range chunks {
			chunks[idx] = elements[idx*dimensionSize : (idx+1)*dimensionSize]
		}
		elements = chunks
	}
	return elements, nil
}

func encodeKeyFDB(key, ktype interface{}) (tuple.TupleElement, error) {
	switch key := key.(type) {
	case [16]uint8:
		var id, err = uuid.FromBytes(key[:])
		if err != nil {
			return nil, fmt.Errorf("error parsing uuid: %w", err)
		}
		return id.String(), nil
	case net.HardwareAddr: // column types 'macaddr' and 'macaddr8'
		return key.String(), nil
	case time.Time:
		return key.Format(sortableRFC3339Nano), nil
	case pgtype.Numeric:
		return encodePgNumericKeyFDB(key)
	default:
		return key, nil
	}
}

func decodeKeyFDB(t tuple.TupleElement) (interface{}, error) {
	switch t := t.(type) {
	case tuple.Tuple:
		if d := maybeDecodePgNumericTuple(t); d != nil {
			return d, nil
		}

		return nil, errors.New("failed in decoding the fdb tuple")
	default:
		return t, nil
	}
}
