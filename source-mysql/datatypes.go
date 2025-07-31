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
	return jsonTranscoderFunc(func(buf []byte, v any) ([]byte, error) {
		if translated, err := db.translateRecordField(isBackfill, columnType, v); err != nil {
			return nil, fmt.Errorf("error translating value %v for JSON serialization: %w", v, err)
		} else {
			return json.Append(buf, translated, json.EscapeHTML|json.SortMapKeys)
		}
	}), nil
}

func (db *mysqlDatabase) translateRecordField(isBackfill bool, columnType any, val any) (any, error) {
	if columnType == nil {
		return nil, fmt.Errorf("unknown column type")
	}
	if columnType, ok := columnType.(*mysqlColumnType); ok {
		return columnType.translateRecordField(isBackfill, val)
	}
	if str, ok := val.(string); ok {
		val = []byte(str)
	}
	switch val := val.(type) {
	case float64:
		switch columnType {
		case "float":
			// Converting floats to strings requires accurate knowledge of the float
			// precision to not be like `123.45600128173828` so a 'float' column must
			// be truncated back down to float32 here. Note that MySQL translates a
			// column type like FLOAT(53) where N>23 into a 'double' column type, so
			// we can trust that the type name here reflects the desired precision.
			return float32(val), nil
		}
	case []byte:
		// Make a solely-owned copy of any byte data. The MySQL client library does
		// some dirty memory-reuse hackery which is only safe so long as byte data
		// returned from a query is fully consumed before `results.Close()` is called.
		//
		// We don't currently guarantee that, so this copy is necessary to avoid any
		// chance of memory corruption.
		//
		// This can be removed after the backfill buffering changes of August 2023
		// are complete, since once that's done results should be fully processed
		// as soon as they're received.
		val = append(make([]byte, 0, len(val)), val...)
		if typeName, ok := columnType.(string); ok {
			switch typeName {
			case "bit":
				var acc uint64
				for _, x := range val {
					acc = (acc << 8) | uint64(x)
				}
				return acc, nil
			case "binary", "varbinary":
				// Unlike BINARY(N) columns, VARBINARY(N) doesn't play around with adding or removing
				// trailing null bytes so we don't have to do anything to correct for that here. The
				// "binary" case here is a fallback for captures whose metadata predates the change
				// to discover binary columns as a complex type with length property.
				if len(val) > truncateColumnThreshold {
					val = val[:truncateColumnThreshold]
				}
				return val, nil
			case "blob", "tinyblob", "mediumblob", "longblob":
				if len(val) > truncateColumnThreshold {
					val = val[:truncateColumnThreshold]
				}
				return val, nil
			case "time":
				// The MySQL client library parsing logic for TIME columns is
				// kind of dumb and inserts either '-' or '\x00' as the first
				// byte of a time value. We want to strip off a leading null
				// if present.
				if len(val) > 0 && val[0] == 0 {
					val = val[1:]
				}
				return string(val), nil
			case "json":
				if len(val) == 0 {
					// The empty string is technically invalid JSON but null should be
					// a reasonable translation.
					return nil, nil
				}
				if len(val) > truncateColumnThreshold {
					val = oversizePlaceholderJSON(val)
				}
				if !json.Valid(val) {
					// If the contents of a JSON column are malformed and non-empty we
					// don't really have any option other than stringifying it. But we
					// can wrap it in an object with an 'invalidJSON' property so that
					// there's at least some hope of identifying such values later on.
					return map[string]any{"invalidJSON": string(val)}, nil
				}
				return json.RawMessage(val), nil
			case "timestamp":
				// Per the MySQL docs:
				//
				//  > Invalid DATE, DATETIME, or TIMESTAMP values are converted to the “zero” value
				//  > of the appropriate type ('0000-00-00' or '0000-00-00 00:00:00')"
				//
				// But month 0 and day 0 don't exist so this can't be parsed and even if
				// it could it wouldn't be a valid RFC3339 timestamp. Since this is the
				// "your data is junk" sentinel value we replace it with a similar one
				// that actually is a valid RFC3339 timestamp.
				//
				// MySQL doesn't allow timestamp values with a zero YYYY-MM-DD to have
				// nonzero fractional seconds, so a simple prefix match can be used.
				if strings.HasPrefix(string(val), "0000-00-00 00:00:00") {
					return "0001-01-01T00:00:00Z", nil
				}
				var inputTimestamp = normalizeMySQLTimestamp(string(val))
				t, err := time.Parse(mysqlTimestampLayout, inputTimestamp)
				if err != nil {
					return nil, fmt.Errorf("error parsing timestamp %q: %w", inputTimestamp, err)
				}
				return t.Format(time.RFC3339Nano), nil
			case "datetime":
				// See note above in the "timestamp" case about replacing this default sentinel
				// value with a valid RFC3339 timestamp sentinel value. The same reasoning applies
				// here for "datetime".
				if strings.HasPrefix(string(val), "0000-00-00 00:00:00") {
					return "0001-01-01T00:00:00Z", nil
				}
				if db.datetimeLocation == nil {
					return nil, fmt.Errorf("unable to translate DATETIME values: %w", errDatabaseTimezoneUnknown)
				}
				var inputTimestamp = normalizeMySQLTimestamp(string(val))
				t, err := time.ParseInLocation(mysqlTimestampLayout, inputTimestamp, db.datetimeLocation)
				if err != nil {
					return nil, fmt.Errorf("error parsing datetime %q: %w", inputTimestamp, err)
				}
				return t.UTC().Format(time.RFC3339Nano), nil
			case "date":
				return normalizeMySQLDate(string(val)), nil
			case "uuid": // The 'UUID' column type is only in MariaDB
				if parsed, err := uuid.Parse(string(val)); err == nil {
					return parsed.String(), nil
				} else if parsed, err := uuid.FromBytes(val); err == nil {
					return parsed.String(), nil
				} else {
					return nil, fmt.Errorf("error parsing UUID: %w", err)
				}
			}
		}
		if len(val) > truncateColumnThreshold {
			val = val[:truncateColumnThreshold]
		}
		return string(val), nil
	}
	return val, nil
}

func oversizePlaceholderJSON(orig []byte) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"flow_truncated":true,"original_size":%d}`, len(orig)))
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

func (t *mysqlColumnType) translateRecordField(isBackfill bool, val any) (any, error) {
	switch t.Type {
	case "enum":
		if index, ok := val.(int64); ok {
			if 0 <= index && int(index) < len(t.EnumValues) {
				return t.EnumValues[index], nil
			}
			return "", fmt.Errorf("enum value out of range: index %d does not match known options %q, backfill the table to reinitialize the inconsistent table metadata", index, t.EnumValues)
		} else if bs, ok := val.([]byte); ok {
			return string(bs), nil
		}
		return val, nil
	case "set":
		if bitfield, ok := val.(int64); ok {
			var acc strings.Builder
			for idx := 0; idx < len(t.EnumValues); idx++ {
				if bitfield&(1<<idx) != 0 {
					if acc.Len() > 0 {
						acc.WriteByte(',')
					}
					acc.WriteString(t.EnumValues[idx])
				}
			}
			return acc.String(), nil
		} else if bs, ok := val.([]byte); ok {
			return string(bs), nil
		}
		return val, nil
	case "boolean":
		if val == nil {
			return nil, nil
		}
		// This is an ugly hack, but it works without requiring a bunch of annoying type
		// assertions to make sure we handle the possible value types coming from backfills
		// and from replication, and it's significantly less likely to break in the future.
		return fmt.Sprintf("%d", val) != "0", nil
	case "tinyint":
		if sval, ok := val.(int8); ok && t.Unsigned {
			return uint8(sval), nil
		}
		return val, nil
	case "smallint":
		if sval, ok := val.(int16); ok && t.Unsigned {
			return uint16(sval), nil
		}
		return val, nil
	case "mediumint":
		if sval, ok := val.(int32); ok && t.Unsigned {
			// A MySQL 'MEDIUMINT' is a 24-bit integer value which is stored into an int32 by the client library,
			// so we convert to a uint32 and mask off any sign-extended upper bits.
			return uint32(sval) & 0x00FFFFFF, nil
		}
		return val, nil
	case "int":
		if sval, ok := val.(int32); ok && t.Unsigned {
			return uint32(sval), nil
		}
		return val, nil
	case "bigint":
		if sval, ok := val.(int64); ok && t.Unsigned {
			return uint64(sval), nil
		}
		return val, nil
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		if str, ok := val.(string); ok {
			return str, nil
		} else if bs, ok := val.([]byte); ok {
			if isBackfill {
				// Backfills always return string results as UTF-8
				return string(bs), nil
			}
			return decodeBytesToString(t.Charset, bs)
		} else if val == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("internal error: text column value must be bytes or nil: got %v", val)
	case "binary":
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
			return bs, nil
		}
		return val, nil
	}
	return val, fmt.Errorf("error translating value of complex column type %q", t.Type)
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
