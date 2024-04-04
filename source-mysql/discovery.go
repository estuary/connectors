package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/invopop/jsonschema"
	"github.com/sirupsen/logrus"
)

func (db *mysqlDatabase) DiscoverTables(ctx context.Context) (map[string]*sqlcapture.DiscoveryInfo, error) {
	var tableMap = make(map[string]*sqlcapture.DiscoveryInfo)
	var tables, err = getTables(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %w", err)
	}
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)
		tableMap[streamID] = table
	}

	// Enumerate every column of every table, and then aggregate into a
	// map from StreamID to TableInfo structs.
	columns, err := getColumns(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("error discovering columns: %w", err)
	}
	for _, column := range columns {
		// Create or look up the appropriate TableInfo struct for a given schema+name
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = tableMap[streamID]
		if !ok {
			continue // Ignore information about excluded tables
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
		var info, ok = tableMap[id]
		if !ok {
			continue // Ignore information about excluded tables
		}
		info.PrimaryKey = key
		tableMap[id] = info
	}

	// Enumerate secondary indexes and use those as a fallback where present
	// for tables whose primary key is unset.
	secondaryIndexes, err := getSecondaryIndexes(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database secondary indexes: %w", err)
	}
	for streamID, indexColumns := range secondaryIndexes {
		var info, ok = tableMap[streamID]
		if !ok || info.PrimaryKey != nil {
			continue
		}
		for _, columns := range indexColumns {
			// Test that for each column the value is non-nullable
			if columnsNonNullable(info.Columns, columns) {
				logrus.WithFields(logrus.Fields{
					"table": streamID,
					"index": columns,
				}).Trace("using unique secondary index as primary key")
				info.PrimaryKey = columns
				break
			} else {
				logrus.WithFields(logrus.Fields{
					"table": streamID,
					"index": columns,
				}).Trace("cannot use secondary index because some of its columns are nullable")
			}
		}
	}

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		for id, info := range tableMap {
			logrus.WithFields(logrus.Fields{
				"stream":     id,
				"keyColumns": info.PrimaryKey,
			}).Debug("discovered table")
		}
	}

	return tableMap, nil
}

func columnsNonNullable(columnsInfo map[string]sqlcapture.ColumnInfo, columnNames []string) bool {
	for _, columnName := range columnNames {
		if columnsInfo[columnName].IsNullable {
			return false
		}
	}
	return true
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

func (db *mysqlDatabase) translateRecordFields(columnTypes map[string]interface{}, f map[string]interface{}) error {
	if columnTypes == nil {
		return fmt.Errorf("unknown column types")
	}
	if f == nil {
		return nil
	}
	for id, val := range f {
		// MariaDB versions 10.4 and up include a synthetic `DB_ROW_HASH_1` column in the
		// binlog row change events for certain tables with unique hash indices (see issue
		// https://github.com/estuary/connectors/issues/1344). In such cases we won't have
		// any type information for the column, but we also don't want it anyway, so as a
		// special case we just delete the 'DB_ROW_HASH_1' property from the document.
		if id == "DB_ROW_HASH_1" && columnTypes[id] == nil {
			delete(f, id)
			continue
		}
		var translated, err = db.translateRecordField(columnTypes[id], val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

const mysqlTimestampLayout = "2006-01-02 15:04:05"

var errDatabaseTimezoneUnknown = errors.New("system variable 'time_zone' or timezone from capture configuration must contain a valid IANA time zone name or +HH:MM offset (go.estuary.dev/80J6rX)")

func (db *mysqlDatabase) translateRecordField(columnType interface{}, val interface{}) (interface{}, error) {
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
		val = append([]byte(nil), val...)
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
			}
		}
		return string(val), nil
	}
	return val, nil
}

func normalizeMySQLTimestamp(ts string) string {
	// Split timestamp into "YYYY-MM-DD" and "HH:MM:SS ..." portions
	var tsBits = strings.SplitN(ts, " ", 2)
	if len(tsBits) != 2 {
		return ts
	}
	// Split "YYYY-MM-DD" into "YYYY" "MM" and "DD" portions
	var ymdBits = strings.Split(tsBits[0], "-")
	if len(ymdBits) != 3 {
		return ts
	}
	// Replace zero-valued year/month/day with ones instead
	if ymdBits[0] == "0000" {
		ymdBits[0] = "0001"
	}
	if ymdBits[1] == "00" {
		ymdBits[1] = "01"
	}
	if ymdBits[2] == "00" {
		ymdBits[2] = "01"
	}
	// Reassemble the Year/Month/Day and tack on the rest of the original timestamp
	var normalized = fmt.Sprintf("%s-%s-%s %s", ymdBits[0], ymdBits[1], ymdBits[2], tsBits[1])
	if normalized != ts {
		logrus.WithFields(logrus.Fields{
			"input":  ts,
			"output": normalized,
		}).Debug("normalized illegal timestamp")
	}
	return normalized
}

const queryDiscoverTables = `
  SELECT table_schema, table_name, table_type
  FROM information_schema.tables
  WHERE table_schema != 'information_schema' AND table_schema != 'performance_schema'
    AND table_schema != 'mysql' AND table_schema != 'sys';`

func getTables(ctx context.Context, conn *client.Conn) ([]*sqlcapture.DiscoveryInfo, error) {
	var results, err = conn.Execute(queryDiscoverTables)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	defer results.Close()

	var tables []*sqlcapture.DiscoveryInfo
	for _, row := range results.Values {
		tables = append(tables, &sqlcapture.DiscoveryInfo{
			Schema:    string(row[0].AsString()),
			Name:      string(row[1].AsString()),
			BaseTable: strings.EqualFold(string(row[2].AsString()), "BASE TABLE"),
		})
	}
	return tables, nil
}

const queryDiscoverColumns = `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable, data_type, column_type
  FROM information_schema.columns
  ORDER BY table_schema, table_name, ordinal_position;`

func getColumns(ctx context.Context, conn *client.Conn) ([]sqlcapture.ColumnInfo, error) {
	var results, err = conn.Execute(queryDiscoverColumns)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %w", err)
	}
	defer results.Close()

	var columns []sqlcapture.ColumnInfo
	for _, row := range results.Values {
		columns = append(columns, sqlcapture.ColumnInfo{
			TableSchema: string(row[0].AsString()),
			TableName:   string(row[1].AsString()),
			Index:       int(row[2].AsInt64()),
			Name:        string(row[3].AsString()),
			IsNullable:  string(row[4].AsString()) != "NO",
			DataType:    parseDataType(string(row[5].AsString()), string(row[6].AsString())),
		})
	}
	return columns, err
}

func parseDataType(typeName, fullColumnType string) any {
	if typeName == "enum" {
		// Illegal values are represented internally by MySQL as the integer 0. Adding
		// this to the list as the zero-th element allows everything else to flow naturally.
		return &mysqlColumnType{Type: "enum", EnumValues: append([]string{""}, parseEnumValues(fullColumnType)...)}
	} else if typeName == "set" {
		return &mysqlColumnType{Type: "set", EnumValues: parseEnumValues(fullColumnType)}
	}
	return typeName
}

type mysqlColumnType struct {
	Type       string   `json:"type" mapstructure:"type"`           // The basic name of the column type.
	EnumValues []string `json:"enum,omitempty" mapstructure:"enum"` // The list of values which an enum (or set) column can contain.
}

func (t *mysqlColumnType) translateRecordField(val interface{}) (interface{}, error) {
	logrus.WithFields(logrus.Fields{
		"type":  fmt.Sprintf("%#v", t),
		"value": fmt.Sprintf("%#v", val),
	}).Trace("translating record field")

	switch t.Type {
	case "enum":
		if index, ok := val.(int64); ok {
			if 0 <= index && int(index) < len(t.EnumValues) {
				return t.EnumValues[index], nil
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
	}
	return val, fmt.Errorf("internal error: failed to encode column of type %q as backfill key", t.Type)
}

// enumValuesRegexp matches a MySQL-format single-quoted string followed by
// a comma or EOL. It uses non-capturing groups for the alternations on string
// body characters and terminator so that submatch #1 is the full string body.
// The options for string body characters are, in order, two successive quotes,
// anything backslash-escaped, and anything that isn't a single-quote.
var enumValuesRegexp = regexp.MustCompile(`'((?:''|\\.|[^'])+)'(?:,|$)`)

// enumValueReplacements contains the complete list of MySQL string escapes from
// https://dev.mysql.com/doc/refman/8.0/en/string-literals.html#character-escape-sequences
// plus the `'​'` repeated-single-quote mechanism.
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
		opts = append(opts, decodeMySQLString(match[1]))
	}
	return opts
}

// decodeStringMySQL decodes a MySQL-format single-quoted string (including
// possible backslash escapes) and returns it in unquoted, unescaped form.
func decodeMySQLString(qstr string) string {
	if strings.HasPrefix(qstr, "'") && strings.HasSuffix(qstr, "'") {
		qstr = strings.TrimPrefix(qstr, "'")
		qstr = strings.TrimSuffix(qstr, "'")
		for old, new := range enumValueReplacements {
			qstr = strings.ReplaceAll(qstr, old, new)
		}
	}
	return qstr
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

const queryDiscoverSecondaryIndices = `
SELECT stat.table_schema,
       stat.table_name,
       stat.index_name,
	   stat.column_name,
	   stat.seq_in_index
FROM information_schema.statistics stat
     JOIN information_schema.table_constraints tco
          ON stat.table_schema = tco.table_schema
          AND stat.table_name = tco.table_name
          AND stat.index_name = tco.constraint_name
WHERE stat.non_unique = 0
      AND stat.table_schema NOT IN ('information_schema', 'sys', 'performance_schema', 'mysql')
      AND tco.constraint_type != 'PRIMARY KEY'
ORDER BY stat.table_schema, stat.table_name, stat.index_name, stat.seq_in_index
`

func getSecondaryIndexes(ctx context.Context, conn *client.Conn) (map[string]map[string][]string, error) {
	var results, err = conn.Execute(queryDiscoverSecondaryIndices)
	if err != nil {
		return nil, fmt.Errorf("error querying secondary indexes: %w", err)
	}
	defer results.Close()

	// Run the 'list secondary indexes' query and aggregate results into
	// a `map[StreamID]map[IndexName][]ColumnName`
	var streamIndexColumns = make(map[string]map[string][]string)
	for _, row := range results.Values {
		var streamID = sqlcapture.JoinStreamID(string(row[0].AsString()), string(row[1].AsString()))
		var indexName, columnName = string(row[2].AsString()), string(row[3].AsString())
		var keySequence = int(row[4].AsInt64())
		var indexColumns = streamIndexColumns[streamID]
		if indexColumns == nil {
			indexColumns = make(map[string][]string)
			streamIndexColumns[streamID] = indexColumns
		}
		indexColumns[indexName] = append(indexColumns[indexName], columnName)
		if len(indexColumns[indexName]) != keySequence {
			return nil, fmt.Errorf("internal error: secondary index key ordering failure: index %q on stream %q: column %q appears out of order", indexName, streamID, columnName)
		}
	}
	return streamIndexColumns, err
}

type columnSchema struct {
	contentEncoding string
	description     string
	format          string
	nullable        bool
	extras          map[string]interface{}
	jsonType        string
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

	if s.jsonType == "" {
		// No type constraint.
	} else if s.nullable {
		out.Extras["type"] = []string{s.jsonType, "null"} // Use variadic form.
	} else {
		out.Type = s.jsonType
	}
	return out
}

var mysqlTypeToJSON = map[string]columnSchema{
	"tinyint":   {jsonType: "integer"},
	"smallint":  {jsonType: "integer"},
	"mediumint": {jsonType: "integer"},
	"int":       {jsonType: "integer"},
	"bigint":    {jsonType: "integer"},
	"bit":       {jsonType: "integer"},

	"float":   {jsonType: "number"},
	"double":  {jsonType: "number"},
	"decimal": {jsonType: "string", format: "number"},

	"char":    {jsonType: "string"},
	"varchar": {jsonType: "string"},

	"tinytext":   {jsonType: "string"},
	"text":       {jsonType: "string"},
	"mediumtext": {jsonType: "string"},
	"longtext":   {jsonType: "string"},

	"binary":     {jsonType: "string", contentEncoding: "base64"},
	"varbinary":  {jsonType: "string", contentEncoding: "base64"},
	"tinyblob":   {jsonType: "string", contentEncoding: "base64"},
	"blob":       {jsonType: "string", contentEncoding: "base64"},
	"mediumblob": {jsonType: "string", contentEncoding: "base64"},
	"longblob":   {jsonType: "string", contentEncoding: "base64"},

	"enum": {jsonType: "string"},
	"set":  {jsonType: "string"},

	"date":      {jsonType: "string"},
	"datetime":  {jsonType: "string", format: "date-time"},
	"timestamp": {jsonType: "string", format: "date-time"},
	"time":      {jsonType: "string"},
	"year":      {jsonType: "integer"},

	"json": {},
}
