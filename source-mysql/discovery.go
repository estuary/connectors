package main

import (
	"context"
	"encoding/json"
	"fmt"

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
	var colSchema, ok = mysqlTypeToJSON[column.DataType]
	if !ok {
		return nil, fmt.Errorf("unhandled MySQL type %q", column.DataType)
	}
	colSchema.nullable = column.IsNullable

	// Pass-through the column description.
	if column.Description != nil {
		colSchema.description = *column.Description
	}
	return colSchema.toType(), nil
}

func translateRecordFields(columnTypes map[string]string, f map[string]interface{}) error {
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

func translateRecordField(columnType string, val interface{}) (interface{}, error) {
	if columnType == "" {
		return nil, fmt.Errorf("unknown column type")
	}
	if str, ok := val.(string); ok {
		val = []byte(str)
	}
	switch val := val.(type) {
	case []byte:
		switch columnType {
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
		default:
			return string(val), nil
		}
	}
	return val, nil
}

const queryDiscoverColumns = `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable, data_type
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
		columns = append(columns, sqlcapture.ColumnInfo{
			TableSchema: string(row[0].AsString()),
			TableName:   string(row[1].AsString()),
			Index:       int(row[2].AsInt64()),
			Name:        string(row[3].AsString()),
			IsNullable:  string(row[4].AsString()) != "NO",
			DataType:    string(row[5].AsString()),
		})
	}
	return columns, err
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
	type_           string
}

func (s columnSchema) toType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format:      s.format,
		Description: s.description,
		Extras:      make(map[string]interface{}),
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

	// "enum": {type_: "string"}, // TODO(wgd): Enable after fixing translation for enum columns
	// "set": {type_: "string"}, // TODO(wgd): Enable after fixing translation for set columns

	"date":     {type_: "string"},
	"datetime": {type_: "string"},
	// "timestamp": {type_: "string"}, // TODO(wgd): Enable after fixing timezone conversion inconsistencies
	"time": {type_: "string"},
	"year": {type_: "integer"},

	"json": {},
}
