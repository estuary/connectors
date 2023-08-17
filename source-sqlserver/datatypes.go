package main

import (
	"fmt"
	"time"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	// The standard library `time.RFC3339Nano` is wrong for historical reasons, this
	// format string is better because it always uses 9-digit fractional seconds, and
	// thus it can be sorted lexicographically as bytes.
	sortableRFC3339Nano = "2006-01-02T15:04:05.000000000Z07:00"

	// SQL Server sort ordering for `uniqueidentifier` columns is absolutely insane,
	// so we have to reshuffle the underlying bytes pretty hard to produce a row key
	// whose bytewise lexicographic ordering matches SQL Server sorting rules.
	uniqueIdentifierTag = "uuid"

	// It is important that we be able to match the sort ordering that SQL Server
	// applies to text/character primary keys. To do this, we translate the values
	// of these columns into a tuple ("ctext", <sorting key>, "original string").
	//
	// The sorting key is a sequence of bytes which exists solely so that the
	// bytewise lexicographic ordering of these values will match the SQL Server
	// collation ordering of the same values. When decoding a key the sort-key
	// field is ignored and we just use the original input string, since that
	// way the collation-sortable encoding doesn't have to be reversible.
	collatedTextTag = "ctext"
)

func encodeKeyFDB(key, ktype interface{}) (tuple.TupleElement, error) {
	switch key := key.(type) {
	case time.Time:
		return key.Format(sortableRFC3339Nano), nil
	case []byte:
		switch ktype {
		case "uniqueidentifier":
			// SQL Server sorts `uniqueidentifier` columns in a bizarre order, but
			// by shuffling the bytes here and reversing it when decoding we can get
			// the serialized form to sort lexicographically. See TestUUIDCaptureOrder
			// for the test that ensures the encoding works correctly.
			var enc = make([]byte, 16)
			copy(enc[0:6], key[10:16])
			copy(enc[6:8], key[8:10])
			copy(enc[8:10], key[6:8])
			copy(enc[10:12], key[4:6])
			copy(enc[12:16], key[0:4])
			return tuple.Tuple{uniqueIdentifierTag, enc}, nil
		}
	case string:
		if textInfo, ok := ktype.(*sqlserverTextColumnType); ok && predictableCollation(textInfo) {
			var textSortingKey, err = encodeCollationSortKey(textInfo, key)
			if err != nil {
				return nil, fmt.Errorf("error serializing collated text in key: %w", err)
			}
			return tuple.Tuple{collatedTextTag, textSortingKey, key}, nil
		}
	}
	return key, nil
}

func decodeKeyFDB(t tuple.TupleElement) (interface{}, error) {
	switch t := t.(type) {
	case tuple.Tuple:
		if len(t) == 0 {
			return nil, fmt.Errorf("internal error: malformed row key contains empty tuple")
		}
		switch t[0] {
		case uniqueIdentifierTag:
			// Reverse the shuffling done by encodeKeyFDB
			var enc, key = t[1].([]byte), make([]byte, 16)
			copy(key[10:16], enc[0:6])
			copy(key[8:10], enc[6:8])
			copy(key[6:8], enc[8:10])
			copy(key[4:6], enc[10:12])
			copy(key[0:4], enc[12:16])
			return key, nil
		case collatedTextTag:
			return t[2], nil
		default:
			return nil, fmt.Errorf("internal error: unknown tuple tag %q", t[0])
		}
	default:
		return t, nil
	}
}

func (db *sqlserverDatabase) translateRecordFields(columnTypes map[string]interface{}, f map[string]interface{}) error {
	if columnTypes == nil {
		return fmt.Errorf("unknown column types")
	}
	if f == nil {
		return nil
	}
	for id, val := range f {
		var translated, err = db.translateRecordField(columnTypes[id], val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

func (db *sqlserverDatabase) translateRecordField(columnType interface{}, val interface{}) (interface{}, error) {
	log.WithFields(log.Fields{
		"type":  columnType,
		"value": val,
	}).Trace("translate record field")
	switch val := val.(type) {
	case []byte:
		switch columnType {
		case "numeric", "decimal", "money", "smallmoney":
			return string(val), nil
		case "uniqueidentifier":
			// Words cannot describe how much this infuriates me. Byte-swap
			// the first eight bytes of the UUID so that values will actually
			// round-trip correctly from the '00112233-4455-6677-8899-AABBCCDDEEFF'
			// textual input format to UUIDs serialized as JSON.
			val[0], val[1], val[2], val[3] = val[3], val[2], val[1], val[0]
			val[4], val[5] = val[5], val[4]
			val[6], val[7] = val[7], val[6]
			u, err := uuid.FromBytes(val)
			if err != nil {
				return nil, err
			}
			return u.String(), nil
		}
	case time.Time:
		switch columnType {
		case "date":
			// Date columns aren't timezone aware and shouldn't pretend to be valid
			// timestamps, so we format them back to a simple YYYY-MM-DD string here.
			return val.Format("2006-01-02"), nil
		case "time":
			return val.Format("15:04:05.9999999"), nil
		case "datetime", "datetime2", "smalldatetime":
			// The SQL Server client library translates DATETIME columns into Go time.Time values
			// in the UTC location. We need to reinterpret the same YYYY-MM-DD HH:MM:SS.NNN values
			// in the actual user-specified location instead.
			return time.Date(val.Year(), val.Month(), val.Day(), val.Hour(), val.Minute(), val.Second(), val.Nanosecond(), db.datetimeLocation), nil
		}
	}
	return val, nil
}
