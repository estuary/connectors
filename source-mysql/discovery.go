package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/invopop/jsonschema"
	"github.com/sirupsen/logrus"
)

func (db *mysqlDatabase) DiscoverTables(ctx context.Context) (map[string]sqlcapture.TableInfo, error) {
	// Enumerate every column of every table, and then aggregate into a
	// map from StreamID to TableInfo structs.
	var columns, err = getColumns(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("error discovering columns: %w", err)
	}
	var tableMap = make(map[string]sqlcapture.TableInfo)
	for _, column := range columns {
		// Create or look up the appropriate TableInfo struct for a given schema+name
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = tableMap[streamID]
		if !ok {
			info = sqlcapture.TableInfo{Schema: column.TableSchema, Name: column.TableName}
		}

		// The 'Stream IDs' used for table info lookup are case insensitive, so we
		// need to double-check that there isn't a collision between two case variants
		// of the same name.
		if info.Schema != column.TableSchema || info.Name != column.TableName {
			var nameA = fmt.Sprintf("%s.%s", info.Schema, info.Name)
			var nameB = fmt.Sprintf("%s.%s", column.TableSchema, column.TableName)
			return nil, fmt.Errorf("table name collision between %q and %q", nameA, nameB)
		}

		// Finally we can add to the column info map and column-name-ordering list
		if info.Columns == nil {
			info.Columns = make(map[string]sqlcapture.ColumnInfo)
		}
		info.Columns[column.Name] = column
		info.ColumnNames = append(info.ColumnNames, column.Name)
		tableMap[streamID] = info
	}

	// Enumerate the primary key of every table and add that information
	// into the table map.
	primaryKeys, err := getPrimaryKeys(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database primary keys: %w", err)
	}
	for id, key := range primaryKeys {
		// The `getColumns()` query implements the "exclude system schemas" logic,
		// so here we ignore primary key information for tables we don't care about.
		var info, ok = tableMap[id]
		if !ok {
			continue
		}
		info.PrimaryKey = key
		tableMap[id] = info
	}

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		for id, info := range tableMap {
			logrus.WithFields(logrus.Fields{
				"stream":     id,
				"keyColumns": info.PrimaryKey,
			}).Debug("discovered table")
		}
	}

	// If there are zero tables, or there's one table but it's the
	// watermarks table, log a warning.
	var _, watermarksPresent = tableMap[db.WatermarksTable()]
	if len(tableMap) == 0 || len(tableMap) == 1 && watermarksPresent {
		logrus.Warn("no tables discovered")
		logrus.Warn("note that source-mysql will not discover tables in the system schemas 'information_schema', 'mysql', 'performance_schema', or 'sys'")
	}

	return tableMap, nil
}

func (db *mysqlDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo) (*jsonschema.Schema, error) {
	var schema columnSchema
	if typeName, ok := column.DataType.(string); ok {
		schema, ok = mysqlTypeToJSON[typeName]
		if !ok {
			return nil, fmt.Errorf("unhandled MySQL type %q", typeName)
		}
		schema.nullable = column.IsNullable
	} else if columnType, ok := column.DataType.(*mysqlColumnType); ok {
		schema, ok = mysqlTypeToJSON[columnType.Type]
		if !ok {
			return nil, fmt.Errorf("unhandled MySQL type %q", typeName)
		}
		schema.nullable = column.IsNullable
		schema.extras = make(map[string]interface{})
		if columnType.Type == "enum" {
			var options []interface{}
			for _, val := range columnType.EnumValues {
				options = append(options, val)
			}
			if column.IsNullable {
				options = append(options, nil)
			}
			schema.extras["enum"] = options
		}
		// TODO(wgd): Is there a good way to describe possible SET values
		// as a JSON schema? Currently discovery just says 'string'.
	} else {
		return nil, fmt.Errorf("unhandled MySQL type %#v", column.DataType)
	}

	// Pass-through the column description.
	if column.Description != nil {
		schema.description = *column.Description
	}
	return schema.toType(), nil
}

func translateRecordFields(columnTypes map[string]interface{}, f map[string]interface{}) error {
	if columnTypes == nil {
		return fmt.Errorf("unknown column types")
	}
	if f == nil {
		return nil
	}
	for id, val := range f {
		var translated, err = translateRecordField(columnTypes[id], val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

func translateRecordField(columnType interface{}, val interface{}) (interface{}, error) {
	if columnType == nil {
		return nil, fmt.Errorf("unknown column type")
	}
	if str, ok := val.(string); ok {
		val = []byte(str)
	}
	if columnType, ok := columnType.(*mysqlColumnType); ok {
		return columnType.translateRecordField(val)
	}
	switch val := val.(type) {
	case []byte:
		if typeName, ok := columnType.(string); ok {
			switch typeName {
			case "bit":
				var acc uint64
				for _, x := range val {
					acc = (acc << 8) | uint64(x)
				}
				return acc, nil
			case "binary", "varbinary":
				return val, nil
			case "blob", "tinyblob", "mediumblob", "longblob":
				return val, nil
			case "json":
				return json.RawMessage(val), nil
			}
		}
		return string(val), nil
	}
	return val, nil
}

const queryDiscoverColumns = `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable, data_type, column_type
  FROM information_schema.columns
  WHERE table_schema != 'information_schema' AND table_schema != 'performance_schema'
    AND table_schema != 'mysql' AND table_schema != 'sys'
  ORDER BY table_schema, table_name, ordinal_position;`

func getColumns(ctx context.Context, conn *client.Conn) ([]sqlcapture.ColumnInfo, error) {
	var results, err = conn.Execute(queryDiscoverColumns)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %w", err)
	}
	defer results.Close()

	var columns []sqlcapture.ColumnInfo
	for _, row := range results.Values {
		var typeName = string(row[5].AsString())
		var dataType interface{}
		if typeName == "enum" {
			dataType = &mysqlColumnType{Type: "enum", EnumValues: parseEnumValues(string(row[6].AsString()))}
		} else if typeName == "set" {
			dataType = &mysqlColumnType{Type: "set", EnumValues: parseEnumValues(string(row[6].AsString()))}
		} else {
			dataType = typeName
		}
		columns = append(columns, sqlcapture.ColumnInfo{
			TableSchema: string(row[0].AsString()),
			TableName:   string(row[1].AsString()),
			Index:       int(row[2].AsInt64()),
			Name:        string(row[3].AsString()),
			IsNullable:  string(row[4].AsString()) != "NO",
			DataType:    dataType,
		})
	}
	return columns, err
}

type mysqlColumnType struct {
	Type       string   `json:"type" mapstructure:"type"`           // The basic name of the column type.
	EnumValues []string `json:"enum,omitempty" mapstructure:"enum"` // The list of values which an enum (or set) column can contain.
}

func (t *mysqlColumnType) translateRecordField(val interface{}) (interface{}, error) {
	logrus.WithFields(logrus.Fields{
		"type":  fmt.Sprintf("%#v", t),
		"value": fmt.Sprintf("%#v", val),
	}).Info("translating record field")

	switch t.Type {
	case "enum":
		if index, ok := val.(int64); ok {
			if 1 <= index && index <= int64(len(t.EnumValues)) {
				return t.EnumValues[index-1], nil
			}
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
	}
	return val, fmt.Errorf("error translating value of complex column type %q", t.Type)
}

// enumValuesRegexp matches a MySQL-format single-quoted string followed by
// a comma or EOL. It uses non-capturing groups for the alternations on string
// body characters and terminator so that submatch #1 is the full string body.
// The options for string body characters are, in order, two successive quotes,
// anything backslash-escaped, and anything that isn't a single-quote.
var enumValuesRegexp = regexp.MustCompile(`'((?:''|\\.|[^'])+)'(?:,|$)`)

// enumValueReplacements contains the complete list of MySQL string escapes from
// https://dev.mysql.com/doc/refman/8.0/en/string-literals.html#character-escape-sequences
// plus the `''` repeated-single-quote mechanism.
var enumValueReplacements = map[string]string{
	`''`: "'",
	`\0`: "\x00",
	`\'`: "'",
	`\"`: `"`,
	`\b`: "\b",
	`\n`: "\n",
	`\r`: "\r",
	`\t`: "\t",
	`\Z`: "\x1A",
	`\\`: "\\",
	`\%`: "%",
	`\_`: "_",
}

func parseEnumValues(details string) []string {
	// The detailed type description looks something like `enum('foo', 'bar,baz', 'asdf')`
	// so we start by extracting the parenthesized portion.
	if i := strings.Index(details, "("); i >= 0 {
		if j := strings.Index(details, ")"); j > i {
			details = details[i+1 : j]
		}
	}

	// Apply a regex which matches each `'foo',` clause in the enum/set description,
	// and take submatch #1 which is the body of each string.
	var opts []string
	for _, match := range enumValuesRegexp.FindAllStringSubmatch(details, -1) {
		var opt = match[1]
		for old, new := range enumValueReplacements {
			opt = strings.ReplaceAll(opt, old, new)
		}
		opts = append(opts, opt)
	}
	return opts
}

const queryDiscoverPrimaryKeys = `
SELECT table_schema, table_name, column_name, seq_in_index
  FROM information_schema.statistics
  WHERE index_name = 'primary'
  ORDER BY table_schema, table_name, seq_in_index;
`

// getPrimaryKeys queries the database to produce a map from table names to
// primary keys. Table names are fully qualified as "<schema>.<name>", and
// primary keys are represented as a list of column names, in the order that
// they form the table's primary key.
func getPrimaryKeys(ctx context.Context, conn *client.Conn) (map[string][]string, error) {
	var results, err = conn.Execute(queryDiscoverPrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("error querying primary keys: %w", err)
	}
	defer results.Close()

	var keys = make(map[string][]string)
	for _, row := range results.Values {
		var streamID = sqlcapture.JoinStreamID(string(row[0].AsString()), string(row[1].AsString()))
		var columnName, index = string(row[2].AsString()), int(row[3].AsInt64())
		logrus.WithFields(logrus.Fields{
			"stream": streamID,
			"column": columnName,
			"index":  index,
		}).Trace("discovered primary-key column")
		keys[streamID] = append(keys[streamID], columnName)
		if index != len(keys[streamID]) {
			return nil, fmt.Errorf("primary key column %q (of table %q) appears out of order", columnName, streamID)
		}
	}
	return keys, nil
}

type columnSchema struct {
	contentEncoding string
	description     string
	format          string
	nullable        bool
	extras          map[string]interface{}
	type_           string
}

func (s columnSchema) toType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format:      s.format,
		Description: s.description,
		Extras:      make(map[string]interface{}),
	}
	for k, v := range s.extras {
		out.Extras[k] = v
	}

	if s.contentEncoding != "" {
		out.Extras["contentEncoding"] = s.contentEncoding // New in 2019-09.
	}

	if s.type_ == "" {
		// No type constraint.
	} else if s.nullable {
		out.Extras["type"] = []string{s.type_, "null"} // Use variadic form.
	} else {
		out.Type = s.type_
	}
	return out
}

var mysqlTypeToJSON = map[string]columnSchema{
	"tinyint":   {type_: "integer"},
	"smallint":  {type_: "integer"},
	"mediumint": {type_: "integer"},
	"int":       {type_: "integer"},
	"bigint":    {type_: "integer"},
	"bit":       {type_: "integer"},

	"float":   {type_: "number"},
	"double":  {type_: "number"},
	"decimal": {type_: "string"},

	"char":    {type_: "string"},
	"varchar": {type_: "string"},

	"tinytext":   {type_: "string"},
	"text":       {type_: "string"},
	"mediumtext": {type_: "string"},
	"longtext":   {type_: "string"},

	"binary":     {type_: "string", contentEncoding: "base64"},
	"varbinary":  {type_: "string", contentEncoding: "base64"},
	"tinyblob":   {type_: "string", contentEncoding: "base64"},
	"blob":       {type_: "string", contentEncoding: "base64"},
	"mediumblob": {type_: "string", contentEncoding: "base64"},
	"longblob":   {type_: "string", contentEncoding: "base64"},

	"enum": {type_: "string"},
	"set":  {type_: "string"},

	"date":     {type_: "string"},
	"datetime": {type_: "string"},
	// "timestamp": {type_: "string"}, // TODO(wgd): Enable after fixing timezone conversion inconsistencies
	"time": {type_: "string"},
	"year": {type_: "integer"},

	"json": {},
}
