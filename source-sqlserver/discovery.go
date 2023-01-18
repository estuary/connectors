package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

// DiscoverTables queries the database for information about tables available for capture.
func (db *sqlserverDatabase) DiscoverTables(ctx context.Context) (map[string]*sqlcapture.DiscoveryInfo, error) {
	// Get lists of all columns and primary keys in the database
	var columns, err = getColumns(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database columns: %w", err)
	}
	primaryKeys, err := getPrimaryKeys(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database primary keys: %w", err)
	}

	// Aggregate column information into DiscoveryInfo structs using a map
	// from fully-qualified table names to the corresponding info.
	var tableMap = make(map[string]*sqlcapture.DiscoveryInfo)
	for _, column := range columns {
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = tableMap[streamID]
		if !ok {
			info = &sqlcapture.DiscoveryInfo{Schema: column.TableSchema, Name: column.TableName}
		}

		// The 'Stream IDs' used for table info lookup are case insensitive, so we
		// need to double-check that there isn't a collision between two case variants
		// of the same name.
		if info.Schema != column.TableSchema || info.Name != column.TableName {
			var nameA = fmt.Sprintf("%s.%s", info.Schema, info.Name)
			var nameB = fmt.Sprintf("%s.%s", column.TableSchema, column.TableName)
			return nil, fmt.Errorf("table name collision between %q and %q", nameA, nameB)
		}

		if info.Columns == nil {
			info.Columns = make(map[string]sqlcapture.ColumnInfo)
		}
		info.Columns[column.Name] = column
		info.ColumnNames = append(info.ColumnNames, column.Name)
		tableMap[streamID] = info
	}

	// Add primary-key information to the tables map
	for id, key := range primaryKeys {
		var info, ok = tableMap[id]
		if !ok {
			continue
		}
		info.PrimaryKey = key
		tableMap[id] = info
	}

	if log.IsLevelEnabled(log.DebugLevel) {
		for id, info := range tableMap {
			log.WithFields(log.Fields{
				"stream":     id,
				"keyColumns": info.PrimaryKey,
			}).Debug("discovered table")
		}
	}

	return tableMap, nil
}

const queryDiscoverColumns = `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable, data_type
  FROM information_schema.columns
  WHERE table_schema != 'information_schema' AND table_schema != 'performance_schema'
  ORDER BY table_schema, table_name, ordinal_position;`

func getColumns(ctx context.Context, conn *sql.DB) ([]sqlcapture.ColumnInfo, error) {
	var rows, err = conn.QueryContext(ctx, queryDiscoverColumns)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %w", err)
	}
	defer rows.Close()

	var columns []sqlcapture.ColumnInfo
	for rows.Next() {
		var ci sqlcapture.ColumnInfo
		var isNullable, dataType string
		if err := rows.Scan(&ci.TableSchema, &ci.TableName, &ci.Index, &ci.Name, &isNullable, &dataType); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		ci.IsNullable = isNullable != "NO"
		ci.DataType = dataType
		columns = append(columns, ci)
	}
	return columns, nil
}

const queryDiscoverPrimaryKeys = `
SELECT table_schema, table_name, column_name, ordinal_position
  FROM information_schema.key_column_usage
  ORDER BY table_schema, table_name, ordinal_position;
`

func getPrimaryKeys(ctx context.Context, conn *sql.DB) (map[string][]string, error) {
	var rows, err = conn.QueryContext(ctx, queryDiscoverPrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("error querying primary keys: %w", err)
	}
	defer rows.Close()

	var keys = make(map[string][]string)
	for rows.Next() {
		var tableSchema, tableName, columnName string
		var index int
		if err := rows.Scan(&tableSchema, &tableName, &columnName, &index); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
		keys[streamID] = append(keys[streamID], columnName)
		if index != len(keys[streamID]) {
			return nil, fmt.Errorf("primary key column %q (of table %q) appears out of order", columnName, streamID)
		}
	}
	return keys, nil
}

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *sqlserverDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo) (*jsonschema.Schema, error) {
	var schema columnSchema
	if typeName, ok := column.DataType.(string); ok {
		schema, ok = sqlserverTypeToJSON[typeName]
		if !ok {
			return nil, fmt.Errorf("unhandled SQL Server type %q", typeName)
		}
		schema.nullable = column.IsNullable
	} else {
		return nil, fmt.Errorf("unhandled SQL Server type %#v", column.DataType)
	}

	// Pass-through the column description.
	if column.Description != nil {
		schema.description = *column.Description
	}
	return schema.toType(), nil
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

var sqlserverTypeToJSON = map[string]columnSchema{
	"nvarchar": {jsonType: "string"},
	"varchar":  {jsonType: "string"},
	"nchar":    {jsonType: "string"},
	"char":     {jsonType: "string"},
	"ntext":    {jsonType: "string"},
	"text":     {jsonType: "string"},

	"datetime": {jsonType: "string", format: "date-time"},

	"bit":      {jsonType: "integer"},
	"int":      {jsonType: "integer"},
	"smallint": {jsonType: "integer"},

	"float": {jsonType: "number"},
	"real":  {jsonType: "number"},
}
