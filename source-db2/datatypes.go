package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/invopop/jsonschema"
)

type db2ColumnType struct {
	Name     string
	Length   int
	Scale    int
	Codepage int
}

// translateDB2Value uses the source column type, which can differ from the CD column type.
func (db *db2Database) translateDB2Value(value any, columnType db2ColumnType) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch columnType.Name {
	case "CHARACTER", "CHAR", "VARCHAR", "LONG VARCHAR", "CLOB", "GRAPHIC", "VARGRAPHIC", "LONG VARGRAPHIC", "DBCLOB", "XML", "DECIMAL", "DECFLOAT":
		if columnType.isBitData() {
			return value, nil
		}
		if bs, ok := value.([]byte); ok {
			value = string(bs)
		}
		if columnType.Name == "DECFLOAT" && value == "sNaN" {
			return "NaN", nil
		}
	case "TIMESTAMP":
		var timestamp, err = db2TimestampText(value)
		if err != nil {
			return nil, err
		}
		var location = db.datetimeLocation
		if location == nil {
			location = time.UTC
		}
		var t, parseErr = time.ParseInLocation("2006-01-02 15:04:05", timestamp, location)
		if parseErr != nil {
			return nil, parseErr
		}
		// Keep the original fraction: time.Time only retains nine fractional digits.
		return t.Format("2006-01-02T15:04:05") + timestamp[19:] + t.Format("Z07:00"), nil
	case "DATE":
		if t, ok := value.(time.Time); ok {
			return t.Format("2006-01-02"), nil
		}
	case "TIME":
		switch value := value.(type) {
		case time.Time:
			return value.Format("15:04:05"), nil
		case string:
			return strings.ReplaceAll(value, ".", ":"), nil
		case []byte:
			return strings.ReplaceAll(string(value), ".", ":"), nil
		}
	default:
		if bs, ok := value.([]byte); ok && columnType.Codepage != 0 {
			return string(bs), nil
		}
	}
	return value, nil
}

func db2TimestampText(value any) (string, error) {
	var timestamp string
	switch value := value.(type) {
	case time.Time:
		return value.Format("2006-01-02 15:04:05.999999999"), nil
	case string:
		timestamp = value
	case []byte:
		timestamp = string(value)
	default:
		return "", fmt.Errorf("unexpected Db2 timestamp type %T", value)
	}
	if len(timestamp) < 19 {
		return "", fmt.Errorf("invalid Db2 timestamp %q", timestamp)
	}
	if timestamp[10] == '-' {
		timestamp = timestamp[:10] + " " + strings.ReplaceAll(timestamp[11:19], ".", ":") + timestamp[19:]
	}
	return timestamp, nil
}

func (ct db2ColumnType) projection(name string) string {
	var column = quoteIdentifier(name)
	if ct.Name == "TIME" {
		// Preserve 24:00:00, which the driver's time.Time conversion normalizes to midnight.
		return "CAST(" + column + " AS VARCHAR(8)) AS " + column
	}
	if ct.Name == "TIMESTAMP" && ct.Scale > 9 {
		return "CAST(" + column + " AS VARCHAR(32)) AS " + column
	}
	if ct.Name == "GRAPHIC" || ct.Name == "VARGRAPHIC" || ct.Name == "LONG VARGRAPHIC" || ct.Name == "DBCLOB" {
		// Use SQL_C_CHAR to preserve empty strings, embedded NULs, and Unicode.
		return "CLOB(" + column + ") AS " + column
	}
	return column
}

func (ct db2ColumnType) predictableKeyOrdering() bool {
	// Text collations and formatted numeric/temporal values need unfiltered backfills.
	switch ct.Name {
	case "SMALLINT", "INTEGER", "BIGINT", "BOOLEAN", "BINARY", "VARBINARY":
		return true
	default:
		return false
	}
}

func (db *db2Database) translateRecord(fields map[string]any, columns map[string]sqlcapture.ColumnInfo) error {
	for name, value := range fields {
		var column, ok = columns[name]
		if !ok {
			return fmt.Errorf("source type for column %q is unknown", name)
		}
		var err error
		fields[name], err = db.translateDB2Value(value, column.DataType.(db2ColumnType))
		if err != nil {
			return fmt.Errorf("error translating column %q: %w", name, err)
		}
	}
	return nil
}

func (db *db2Database) encodeRowKey(key []string, fields map[string]any, columns map[string]sqlcapture.ColumnInfo) ([]byte, error) {
	for _, name := range key {
		if _, ok := fields[name]; !ok {
			return nil, fmt.Errorf("key column %q missing from row", name)
		}
		if _, ok := columns[name]; !ok {
			return nil, fmt.Errorf("key column %q missing from source table", name)
		}
	}
	return sqlcapture.EncodeRowKey(key, fields, columns, func(value any, column sqlcapture.ColumnInfo) (tuple.TupleElement, error) {
		var columnType = column.DataType.(db2ColumnType)
		// Keep timezone-naive timestamps in Db2 syntax so they can be bound back to a query.
		if value != nil && columnType.Name == "TIMESTAMP" {
			return db2TimestampText(value)
		}
		return db.translateDB2Value(value, columnType)
	})
}

func (ct db2ColumnType) String() string {
	if ct.isBitData() {
		return ct.Name + " FOR BIT DATA"
	}
	return ct.Name
}

func (ct db2ColumnType) isBitData() bool {
	return ct.Codepage == 0 && (ct.Name == "CHARACTER" || ct.Name == "CHAR" || ct.Name == "VARCHAR" || ct.Name == "LONG VARCHAR")
}

func (db *db2Database) TranslateDBToJSONType(column sqlcapture.ColumnInfo, isPrimaryKey bool) (*jsonschema.Schema, error) {
	var ct, ok = column.DataType.(db2ColumnType)
	if !ok {
		return nil, fmt.Errorf("unhandled column type %T", column.DataType)
	}
	var schema = &jsonschema.Schema{Extras: make(map[string]any)}
	switch strings.ToUpper(ct.Name) {
	case "BOOLEAN":
		schema.Type = "boolean"
	case "SMALLINT", "INTEGER", "BIGINT":
		schema.Type = "integer"
	case "REAL", "DOUBLE":
		schema.Type = "number"
	case "DECIMAL", "DECFLOAT":
		schema.Type, schema.Format = "string", "number"
	case "CHARACTER", "CHAR", "VARCHAR", "LONG VARCHAR":
		schema.Type = "string"
		if ct.isBitData() {
			schema.Extras["contentEncoding"] = "base64"
		} else if ct.Length > 0 {
			var length = uint64(ct.Length)
			schema.MaxLength = &length
		}
	case "CLOB", "GRAPHIC", "VARGRAPHIC", "LONG VARGRAPHIC", "DBCLOB", "XML":
		schema.Type = "string"
	case "BINARY", "VARBINARY", "BLOB":
		schema.Type = "string"
		schema.Extras["contentEncoding"] = "base64"
	case "DATE":
		schema.Type, schema.Format = "string", "date"
	case "TIME":
		schema.Type = "string" // Db2 TIME has no timezone offset.
	case "TIMESTAMP":
		schema.Type, schema.Format = "string", "date-time"
	default:
		return nil, fmt.Errorf("unhandled Db2 type %q", ct.Name)
	}
	if column.IsNullable {
		schema.Extras["type"] = []string{schema.Type, "null"}
		schema.Type = ""
	}
	return schema, nil
}
