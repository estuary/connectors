package main

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	"github.com/sirupsen/logrus"
)

const mysqlTimestampLayout = "2006-01-02 15:04:05"

var errDatabaseTimezoneUnknown = errors.New("system variable 'time_zone' or timezone from capture configuration must contain a valid IANA time zone name or +HH:MM offset (go.estuary.dev/80J6rX)")

func (db *mysqlDatabase) constructJSONTranscoder(isBackfill bool, columnType any) (jsonTranscoder, error) {
	if columnType == nil {
		return nil, fmt.Errorf("unknown column type")
	}
	if columnType, ok := columnType.(*mysqlColumnType); ok {
		return columnType.constructJSONTranscoder(isBackfill)
	}

	switch columnType {
	case "float":
		return jsonTranscoderFunc(transcodeFloatValue), nil
	case "double":
		return jsonTranscoderFunc(transcodeDoubleValue), nil
	case "decimal":
		return jsonTranscoderFunc(transcodeDecimalValue), nil
	case "bit":
		return jsonTranscoderFunc(transcodeBitValue), nil
	case "binary", "varbinary":
		// transcodeVarbinaryValue is theoretically used for BINARY(n) in very old captures
		// whose replication metadata predates the max-length handling, but most BINARY(n)
		// values will go through the mysqlBinaryTranscoder logic instead.
		return jsonTranscoderFunc(transcodeVarbinaryValue), nil
	case "blob", "tinyblob", "mediumblob", "longblob":
		return jsonTranscoderFunc(transcodeBlobValue), nil
	case "time":
		return jsonTranscoderFunc(transcodeTimeValue), nil
	case "json":
		return jsonTranscoderFunc(transcodeJSONValue), nil
	case "timestamp":
		return jsonTranscoderFunc(transcodeTimestampValue), nil
	case "datetime":
		return &mysqlDatetimeTranscoder{Location: db.datetimeLocation}, nil
	case "date":
		return jsonTranscoderFunc(transcodeDateValue), nil
	case "year":
		return jsonTranscoderFunc(transcodeYearValue), nil
	case "uuid": // The 'UUID' column type is only in MariaDB
		return jsonTranscoderFunc(transcodeUUIDValue), nil
	default:
		return nil, fmt.Errorf("unhandled column type %v", columnType)
	}
}

func transcodeFloatValue(buf []byte, val any) ([]byte, error) {
	if n, ok := val.(float64); ok {
		// Converting floats to strings requires accurate knowledge of the float
		// precision to not be like `123.45600128173828` so a 'float' column must
		// be truncated back down to float32 here. Note that MySQL translates a
		// column type like FLOAT(53) where N>23 into a 'double' column type, so
		// we can trust that the type name here reflects the desired precision.
		return json.Append(buf, float32(n), 0)
	} else {
		return json.Append(buf, val, 0)
	}
}

func transcodeDoubleValue(buf []byte, val any) ([]byte, error) {
	return json.Append(buf, val, 0)
}

func transcodeDecimalValue(buf []byte, val any) ([]byte, error) {
	if bs, ok := val.([]byte); ok {
		return json.Append(buf, string(bs), 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeBitValue(buf []byte, val any) ([]byte, error) {
	if bs, ok := val.([]byte); ok {
		var acc uint64
		for _, x := range bs {
			acc = (acc << 8) | uint64(x)
		}
		return json.Append(buf, acc, 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeVarbinaryValue(buf []byte, val any) ([]byte, error) {
	if str, ok := val.(string); ok {
		val = []byte(str)
	}
	if bs, ok := val.([]byte); ok {
		if len(bs) == 0 {
			return json.Append(buf, []byte{}, 0)
		}
		if len(bs) > truncateColumnThreshold {
			return json.Append(buf, bs[:truncateColumnThreshold], 0)
		}
		return json.Append(buf, bs, 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeBlobValue(buf []byte, val any) ([]byte, error) {
	if bs, ok := val.([]byte); ok {
		if len(bs) > truncateColumnThreshold {
			return json.Append(buf, bs[:truncateColumnThreshold], 0)
		}
		return json.Append(buf, bs, 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeTimeValue(buf []byte, val any) ([]byte, error) {
	if bs, ok := val.([]byte); ok {
		// The MySQL client library parsing logic for TIME columns is
		// kind of dumb and inserts either '-' or '\x00' as the first
		// byte of a time value. We want to strip off a leading null
		// if present.
		if len(bs) > 0 && bs[0] == 0 {
			bs = bs[1:]
		}
		return json.Append(buf, string(bs), 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeJSONValue(buf []byte, val any) ([]byte, error) {
	if str, ok := val.(string); ok {
		if len(str) == 0 {
			return json.Append(buf, nil, 0) // Translate empty string as null
		}
		val = json.RawMessage(str)
	} else if bs, ok := val.([]byte); ok {
		if len(bs) == 0 {
			return json.Append(buf, nil, 0) // Translate empty string as null
		}
		val = json.RawMessage(bs)
	}
	if bs, ok := val.(json.RawMessage); ok {
		if len(bs) > truncateColumnThreshold {
			val = json.RawMessage(fmt.Sprintf(`{"flow_truncated":true,"original_size":%d}`, len(bs)))
		} else if !json.Valid(bs) {
			// If the contents of a JSON column are malformed and non-empty we
			// don't really have any option other than stringifying it. But we
			// can wrap it in an object with an 'invalidJSON' property so that
			// there's at least some hope of identifying such values later on.
			val = map[string]any{"invalidJSON": string(bs)}
		}
	}
	return json.Append(buf, val, json.EscapeHTML|json.SortMapKeys)
}

func transcodeTimestampValue(buf []byte, val any) ([]byte, error) {
	if bs, ok := val.([]byte); ok {
		val = string(bs)
	}
	if str, ok := val.(string); ok {
		// Per the MySQL docs:
		//
		//  > Invalid DATE, DATETIME, or TIMESTAMP values are converted to the "zero" value
		//  > of the appropriate type ('0000-00-00' or '0000-00-00 00:00:00')"
		//
		// But month 0 and day 0 don't exist so this can't be parsed and even if
		// it could it wouldn't be a valid RFC3339 timestamp. Since this is the
		// "your data is junk" sentinel value we replace it with a similar one
		// that actually is a valid RFC3339 timestamp.
		//
		// MySQL doesn't allow timestamp values with a zero YYYY-MM-DD to have
		// nonzero fractional seconds, so a simple prefix match can be used.
		if strings.HasPrefix(str, "0000-00-00 00:00:00") {
			return json.Append(buf, "0001-01-01T00:00:00Z", 0)
		}
		var inputTimestamp = normalizeMySQLTimestamp(str)
		t, err := time.Parse(mysqlTimestampLayout, inputTimestamp)
		if err != nil {
			return nil, fmt.Errorf("error parsing timestamp %q: %w", inputTimestamp, err)
		}
		return json.Append(buf, t.Format(time.RFC3339Nano), 0)
	}
	return json.Append(buf, val, 0)
}

type mysqlDatetimeTranscoder struct {
	Location *time.Location
}

func (t *mysqlDatetimeTranscoder) TranscodeJSON(buf []byte, val any) ([]byte, error) {
	if bs, ok := val.([]byte); ok {
		val = string(bs)
	}
	if str, ok := val.(string); ok {
		// See note above in the "timestamp" case about replacing this default sentinel
		// value with a valid RFC3339 timestamp sentinel value. The same reasoning applies
		// here for "datetime".
		if strings.HasPrefix(str, "0000-00-00 00:00:00") {
			return json.Append(buf, "0001-01-01T00:00:00Z", 0)
		}
		if t.Location == nil {
			return nil, fmt.Errorf("unable to translate DATETIME values: %w", errDatabaseTimezoneUnknown)
		}
		var inputTimestamp = normalizeMySQLTimestamp(str)
		t, err := time.ParseInLocation(mysqlTimestampLayout, inputTimestamp, t.Location)
		if err != nil {
			return nil, fmt.Errorf("error parsing datetime %q: %w", inputTimestamp, err)
		}
		return json.Append(buf, t.UTC().Format(time.RFC3339Nano), 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeDateValue(buf []byte, val any) ([]byte, error) {
	if str, ok := val.(string); ok {
		return json.Append(buf, normalizeMySQLDate(str), 0)
	} else if bs, ok := val.([]byte); ok {
		return json.Append(buf, normalizeMySQLDate(string(bs)), 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeYearValue(buf []byte, val any) ([]byte, error) {
	return json.Append(buf, val, 0)
}

func transcodeUUIDValue(buf []byte, val any) ([]byte, error) {
	if bs, ok := val.([]byte); ok {
		if parsed, err := uuid.Parse(string(bs)); err == nil {
			return json.Append(buf, parsed.String(), 0)
		} else if parsed, err := uuid.FromBytes(bs); err == nil {
			return json.Append(buf, parsed.String(), 0)
		} else {
			return nil, fmt.Errorf("error parsing UUID: %w", err)
		}
	}
	return json.Append(buf, val, 0)
}

func normalizeMySQLTimestamp(ts string) string {
	// Split timestamp into "YYYY-MM-DD" and "HH:MM:SS ..." portions
	var tsBits = strings.SplitN(ts, " ", 2)
	if len(tsBits) != 2 {
		return ts
	}
	tsBits[0] = normalizeMySQLDate(tsBits[0])    // Normalize the date
	var normalized = tsBits[0] + " " + tsBits[1] // Reassemble date + time
	if normalized != ts {
		logrus.WithFields(logrus.Fields{
			"input":  ts,
			"output": normalized,
		}).Debug("normalized illegal timestamp")
	}
	return normalized
}

func normalizeMySQLDate(str string) string {
	// Split "YYYY-MM-DD" into "YYYY" "MM" and "DD" portions
	var bits = strings.Split(str, "-")
	if len(bits) != 3 {
		return str
	}
	// Replace zero-valued year/month/day with ones instead
	if bits[0] == "0000" {
		bits[0] = "0001"
	}
	if bits[1] == "00" {
		bits[1] = "01"
	}
	if bits[2] == "00" {
		bits[2] = "01"
	}
	return bits[0] + "-" + bits[1] + "-" + bits[2]
}

func (t *mysqlColumnType) constructJSONTranscoder(isBackfill bool) (jsonTranscoder, error) {
	switch t.Type {
	case "enum":
		return &mysqlEnumTranscoder{Values: t.EnumValues}, nil
	case "set":
		return &mysqlSetTranscoder{Values: t.EnumValues}, nil
	case "boolean":
		return jsonTranscoderFunc(transcodeBooleanValue), nil
	case "tinyint":
		if t.Unsigned {
			return jsonTranscoderFunc(transcodeTinyintUnsigned), nil
		}
		return jsonTranscoderFunc(transcodeTinyint), nil
	case "smallint":
		if t.Unsigned {
			return jsonTranscoderFunc(transcodeSmallintUnsigned), nil
		}
		return jsonTranscoderFunc(transcodeSmallint), nil
	case "mediumint":
		if t.Unsigned {
			return jsonTranscoderFunc(transcodeMediumintUnsigned), nil
		}
		return jsonTranscoderFunc(transcodeMediumint), nil
	case "int":
		if t.Unsigned {
			return jsonTranscoderFunc(transcodeIntUnsigned), nil
		}
		return jsonTranscoderFunc(transcodeInt), nil
	case "bigint":
		if t.Unsigned {
			return jsonTranscoderFunc(transcodeBigintUnsigned), nil
		}
		return jsonTranscoderFunc(transcodeBigint), nil
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		if isBackfill {
			return &mysqlTextTranscoderBackfill{}, nil
		}
		return &mysqlTextTranscoderReplication{Charset: t.Charset}, nil
	case "binary":
		return &mysqlBinaryTranscoder{MaxLength: t.MaxLength}, nil
	default:
		return nil, fmt.Errorf("unhandled complex column type %v", t.Type)
	}
}

type mysqlEnumTranscoder struct {
	Values []string
}

func (t *mysqlEnumTranscoder) TranscodeJSON(buf []byte, val any) ([]byte, error) {
	if index, ok := val.(int64); ok {
		if 0 <= index && int(index) < len(t.Values) {
			return json.Append(buf, t.Values[index], 0)
		}
		return nil, fmt.Errorf("enum value out of range: index %d does not match known options %q, backfill the table to reinitialize the inconsistent table metadata", index, t.Values)
	} else if bs, ok := val.([]byte); ok {
		return json.Append(buf, string(bs), 0)
	}
	return json.Append(buf, val, 0)
}

type mysqlSetTranscoder struct {
	Values []string
}

func (t *mysqlSetTranscoder) TranscodeJSON(buf []byte, val any) ([]byte, error) {
	if bitfield, ok := val.(int64); ok {
		var acc strings.Builder
		for idx := 0; idx < len(t.Values); idx++ {
			if bitfield&(1<<idx) != 0 {
				if acc.Len() > 0 {
					acc.WriteByte(',')
				}
				acc.WriteString(t.Values[idx])
			}
		}
		return json.Append(buf, acc.String(), 0)
	} else if bs, ok := val.([]byte); ok {
		return json.Append(buf, string(bs), 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeBooleanValue(buf []byte, val any) ([]byte, error) {
	if val == nil {
		return json.Append(buf, nil, 0)
	}
	// This is an ugly hack, but it works without requiring a bunch of annoying type
	// assertions to make sure we handle the possible value types coming from backfills
	// and from replication, and it's significantly less likely to break in the future.
	return json.Append(buf, fmt.Sprintf("%d", val) != "0", 0)
}

func (db *mysqlDatabase) constructFDBTranscoder(isBackfill bool, columnType any) (fdbTranscoder, error) {
	_ = isBackfill // Currently unused
	return fdbTranscoderFunc(func(buf []byte, v any) ([]byte, error) {
		if translated, err := encodeKeyFDB(v, columnType); err != nil {
			return nil, fmt.Errorf("error translating value %v for FDB serialization: %w", v, err)
		} else {
			return sqlcapture.AppendFDB(buf, translated)
		}
	}), nil
}

func encodeKeyFDB(key, ktype any) (tuple.TupleElement, error) {
	if columnType, ok := ktype.(*mysqlColumnType); ok {
		return columnType.encodeKeyFDB(key)
	} else if typeName, ok := ktype.(string); ok {
		switch typeName {
		case "decimal":
			if val, ok := key.([]byte); ok {
				// TODO(wgd): This should probably be done in a more principled way, but
				// this is a viable placeholder solution.
				return strconv.ParseFloat(string(val), 64)
			}
		}
	}
	return key, nil
}

func (t *mysqlColumnType) encodeKeyFDB(val any) (tuple.TupleElement, error) {
	switch t.Type {
	case "enum":
		if bs, ok := val.([]byte); ok {
			if idx := slices.Index(t.EnumValues, string(bs)); idx >= 0 {
				return idx, nil
			}
			return val, fmt.Errorf("internal error: failed to translate enum value %q to integer index", string(bs))
		}
		return val, nil
	case "boolean", "tinyint", "smallint", "mediumint", "int", "bigint":
		return val, nil
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		// Backfill text keys are serialized as the raw bytes or string we receive, which is generally
		// fine because we always receive backfill results in UTF-8.
		return val, nil
	case "binary", "varbinary":
		// Binary keys are serialized as the raw bytes, which provides correct
		// lexicographic ordering in FDB tuples
		return val, nil
	}
	return val, fmt.Errorf("internal error: failed to encode column of type %q as backfill key", t.Type)
}

func decodeKeyFDB(t tuple.TupleElement) (any, error) {
	switch v := t.(type) {
	case []byte:
		return string(v), nil
	}
	return t, nil
}

func transcodeTinyintUnsigned(buf []byte, val any) ([]byte, error) {
	if sval, ok := val.(int8); ok {
		return json.Append(buf, uint8(sval), 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeTinyint(buf []byte, val any) ([]byte, error) {
	return json.Append(buf, val, 0)
}

func transcodeSmallintUnsigned(buf []byte, val any) ([]byte, error) {
	if sval, ok := val.(int16); ok {
		return json.Append(buf, uint16(sval), 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeSmallint(buf []byte, val any) ([]byte, error) {
	return json.Append(buf, val, 0)
}

func transcodeMediumintUnsigned(buf []byte, val any) ([]byte, error) {
	if sval, ok := val.(int32); ok {
		// A MySQL 'MEDIUMINT' is a 24-bit integer value which is stored into an int32 by the client library,
		// so we convert to a uint32 and mask off any sign-extended upper bits.
		return json.Append(buf, uint32(sval)&0x00FFFFFF, 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeMediumint(buf []byte, val any) ([]byte, error) {
	return json.Append(buf, val, 0)
}

func transcodeIntUnsigned(buf []byte, val any) ([]byte, error) {
	if sval, ok := val.(int32); ok {
		return json.Append(buf, uint32(sval), 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeInt(buf []byte, val any) ([]byte, error) {
	return json.Append(buf, val, 0)
}

func transcodeBigintUnsigned(buf []byte, val any) ([]byte, error) {
	if sval, ok := val.(int64); ok {
		return json.Append(buf, uint64(sval), 0)
	}
	return json.Append(buf, val, 0)
}

func transcodeBigint(buf []byte, val any) ([]byte, error) {
	return json.Append(buf, val, 0)
}

type mysqlTextTranscoderBackfill struct{}

func (t *mysqlTextTranscoderBackfill) TranscodeJSON(buf []byte, val any) ([]byte, error) {
	if str, ok := val.(string); ok {
		return json.Append(buf, str, 0)
	} else if bs, ok := val.([]byte); ok {
		// Backfills always return string results as UTF-8
		return json.Append(buf, string(bs), 0)
	} else if val == nil {
		return json.Append(buf, nil, 0)
	}
	return nil, fmt.Errorf("internal error: text column value must be bytes or nil: got %v", val)
}

type mysqlTextTranscoderReplication struct {
	Charset string
}

func (t *mysqlTextTranscoderReplication) TranscodeJSON(buf []byte, val any) ([]byte, error) {
	if str, ok := val.(string); ok {
		return json.Append(buf, str, 0)
	} else if bs, ok := val.([]byte); ok {
		var str, err = decodeBytesToString(t.Charset, bs)
		if err != nil {
			return nil, err
		}
		return json.Append(buf, str, 0)
	} else if val == nil {
		return json.Append(buf, nil, 0)
	}
	return nil, fmt.Errorf("internal error: text column value must be bytes or nil: got %v", val)
}

type mysqlBinaryTranscoder struct {
	MaxLength int
}

func (t *mysqlBinaryTranscoder) TranscodeJSON(buf []byte, val any) ([]byte, error) {
	if str, ok := val.(string); ok {
		// Binary column values arriving via replication are decoded into a string (which
		// is not valid UTF-8, it's just an immutable sequence of bytes), so we cast that
		// back to []byte so we can handle backfill / replication values consistently.
		val = []byte(str)
	}
	if bs, ok := val.([]byte); ok {
		if len(bs) < t.MaxLength {
			// Binary column values are stored in the binlog with trailing nulls removed,
			// and returned from backfill queries with the trailing nulls. We have to either
			// strip or pad the values to make these match, and since BINARY(N) is a fixed-
			// length column type the obvious most-correct answer is probably to zero-pad
			// the replicated values back up to the column size.
			bs = append(bs, make([]byte, t.MaxLength-len(bs))...)
		}
		if len(bs) > truncateColumnThreshold {
			bs = bs[:truncateColumnThreshold]
		}
		return json.Append(buf, bs, 0)
	}
	return json.Append(buf, val, 0)
}
