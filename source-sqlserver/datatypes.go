package main

import (
	"fmt"
	"time"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// The standard library `time.RFC3339Nano` is wrong for historical reasons, this
// format string is better because it always uses 9-digit fractional seconds, and
// thus it can be sorted lexicographically as bytes.
const sortableRFC3339Nano = "2006-01-02T15:04:05.000000000Z07:00"

func encodeKeyFDB(key, ktype interface{}) (tuple.TupleElement, error) {
	switch key := key.(type) {
	case time.Time:
		return key.Format(sortableRFC3339Nano), nil
	default:
		return key, nil
	}
}

func decodeKeyFDB(t tuple.TupleElement) (interface{}, error) {
	switch t := t.(type) {
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
			// round-trip correctly.
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
