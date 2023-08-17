package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/estuary/connectors/go/pkg/slices"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

// DiscoverTables queries the database for information about tables available for capture.
func (db *sqlserverDatabase) DiscoverTables(ctx context.Context) (map[string]*sqlcapture.DiscoveryInfo, error) {
	// Get lists of all tables, columns and primary keys in the database
	var tables, err = getTables(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database tables: %w", err)
	}
	columns, err := getColumns(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database columns: %w", err)
	}
	primaryKeys, err := getPrimaryKeys(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database primary keys: %w", err)
	}
	secondaryIndexes, err := getSecondaryIndexes(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database secondary indexes: %w", err)
	}

	// Aggregate column information into DiscoveryInfo structs using a map
	// from fully-qualified table names to the corresponding info.
	var tableMap = make(map[string]*sqlcapture.DiscoveryInfo)
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)
		tableMap[streamID] = table
	}
	for _, column := range columns {
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = tableMap[streamID]
		if !ok {
			continue
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

	// For tables which have no primary key but have a valid secondary index,
	// fill in that secondary index as the 'primary key' for our purposes.
	for streamID, indexColumns := range secondaryIndexes {
		var info, ok = tableMap[streamID]
		if !ok || info.PrimaryKey != nil {
			continue
		}
		for _, columns := range indexColumns {
			// Test that for each column the value is non-nullable
			if columnsNonNullable(info.Columns, columns) {
				log.WithFields(log.Fields{
					"table": streamID,
					"index": columns,
				}).Trace("using unique secondary index as primary key")
				info.PrimaryKey = columns
				break
			} else {
				log.WithFields(log.Fields{
					"table": streamID,
					"index": columns,
				}).Trace("cannot use secondary index because some of its columns are nullable")
			}
		}
	}

	// Determine whether the database sorts the keys of a each table in a
	// predictable order or not. The term "predictable" here specifically
	// means "able to be reproduced using bytewise lexicographic ordering of
	// the serialized row keys generated by this connector".
	for _, info := range tableMap {
		for _, colName := range info.PrimaryKey {
			if textType, ok := info.Columns[colName].DataType.(*sqlserverTextColumnType); ok {
				if !predictableCollation(textType) {
					info.UnpredictableKeyOrdering = true
				}
			}
		}
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

func columnsNonNullable(columnsInfo map[string]sqlcapture.ColumnInfo, columnNames []string) bool {
	for _, columnName := range columnNames {
		if columnsInfo[columnName].IsNullable {
			return false
		}
	}
	return true
}

const queryDiscoverTables = `
  SELECT table_schema, table_name, table_type
  FROM information_schema.tables
  WHERE table_schema != 'information_schema' AND table_schema != 'performance_schema'
    AND table_schema != 'sys' AND table_schema != 'cdc'
	AND table_name != 'systranschemas';`

func getTables(ctx context.Context, conn *sql.DB) ([]*sqlcapture.DiscoveryInfo, error) {
	rows, err := conn.QueryContext(ctx, queryDiscoverTables)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	defer rows.Close()

	var tables []*sqlcapture.DiscoveryInfo
	for rows.Next() {
		var tableSchema, tableName, tableType string
		if err := rows.Scan(&tableSchema, &tableName, &tableType); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		tables = append(tables, &sqlcapture.DiscoveryInfo{
			Schema:    tableSchema,
			Name:      tableName,
			BaseTable: strings.EqualFold(tableType, "BASE TABLE"),
		})
	}
	return tables, nil
}

const queryDiscoverColumns = `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable, data_type, collation_name, character_maximum_length
  FROM information_schema.columns
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
		var isNullable, typeName string
		var collationName *string
		var maxCharLength *int
		if err := rows.Scan(&ci.TableSchema, &ci.TableName, &ci.Index, &ci.Name, &isNullable, &typeName, &collationName, &maxCharLength); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		ci.IsNullable = isNullable != "NO"
		if slices.Contains([]string{"char", "varchar", "nchar", "nvarchar", "text", "ntext"}, typeName) {
			// The collation name should never be null for a text column type, but if it
			// is we'll just default to the string 'NULL' so it's clear what the error is.
			var collation = "NULL"
			if collationName != nil {
				collation = *collationName
			}
			var fullType = typeName
			if slices.Contains([]string{"char", "varchar", "nchar", "nvarchar"}, typeName) {
				// We arbitrarily use max(64, <database maximum>) here just to be extra conservative
				// and provide a sane default in the event of NULL or zero values. The size here only
				// matters for the 'declared parameter' hack used in backfill queries, so overestimating
				// is very benign.
				var columnSize int = 64
				if maxCharLength != nil && *maxCharLength >= columnSize {
					columnSize = *maxCharLength
				}
				fullType = fmt.Sprintf("%s(%d)", typeName, columnSize)
			}
			ci.DataType = &sqlserverTextColumnType{
				Type:      typeName,
				Collation: collation,
				FullType:  fullType,
			}
		} else {
			ci.DataType = typeName
		}
		columns = append(columns, ci)
	}
	return columns, nil
}

type sqlserverTextColumnType struct {
	Type      string // The basic type of the column (char / varchar / nchar / nvarchar / text / ntext)
	Collation string // The collation used by the column
	FullType  string // The full type of the column, such as `varchar(32)` for example
}

// Joining on the 6-tuple {CONSTRAINT,TABLE}_{CATALOG,SCHEMA,NAME} is probably
// overkill but shouldn't hurt, and helps to make absolutely sure that we're
// matching up the constraint type with the column names/positions correctly.
const queryDiscoverPrimaryKeys = `
SELECT kcu.table_schema, kcu.table_name, kcu.column_name, kcu.ordinal_position
  FROM information_schema.key_column_usage kcu
  JOIN information_schema.table_constraints tcs
    ON  tcs.constraint_catalog = kcu.constraint_catalog
    AND tcs.constraint_schema = kcu.constraint_schema
    AND tcs.constraint_name = kcu.constraint_name
    AND tcs.table_catalog = kcu.table_catalog
    AND tcs.table_schema = kcu.table_schema
    AND tcs.table_name = kcu.table_name
  WHERE tcs.constraint_type = 'PRIMARY KEY'
  ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position;
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

const queryDiscoverSecondaryIndices = `
SELECT sch.name, tbl.name, COALESCE(idx.name, 'null'), COL_NAME(ic.object_id,ic.column_id), ic.key_ordinal
FROM sys.indexes idx
     JOIN sys.tables tbl ON tbl.object_id = idx.object_id
	 JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
	 JOIN sys.index_columns ic ON ic.index_id = idx.index_id AND ic.object_id = tbl.object_id
WHERE ic.key_ordinal != 0 AND idx.is_unique = 1 AND sch.name NOT IN ('cdc')
ORDER BY sch.name, tbl.name, idx.name, ic.key_ordinal
`

func getSecondaryIndexes(ctx context.Context, conn *sql.DB) (map[string]map[string][]string, error) {
	var rows, err = conn.QueryContext(ctx, queryDiscoverSecondaryIndices)
	if err != nil {
		return nil, fmt.Errorf("error querying secondary indexes: %w", err)
	}
	defer rows.Close()

	// Run the 'list secondary indexes' query and aggregate results into
	// a `map[StreamID]map[IndexName][]ColumnName`
	var streamIndexColumns = make(map[string]map[string][]string)
	for rows.Next() {
		var tableSchema, tableName, indexName, columnName string
		var keySequence int
		if err := rows.Scan(&tableSchema, &tableName, &indexName, &columnName, &keySequence); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
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

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *sqlserverDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo) (*jsonschema.Schema, error) {
	var schema columnSchema
	if typeInfo, ok := column.DataType.(*sqlserverTextColumnType); ok {
		if schema, ok = sqlserverTypeToJSON[typeInfo.Type]; !ok {
			return nil, fmt.Errorf("unhandled SQL Server type %q (found on column %q of table %q)", typeInfo.Type, column.Name, column.TableName)
		}
	} else if typeName, ok := column.DataType.(string); ok {
		if schema, ok = sqlserverTypeToJSON[typeName]; !ok {
			return nil, fmt.Errorf("unhandled SQL Server type %q (found on column %q of table %q)", typeName, column.Name, column.TableName)
		}
	} else {
		return nil, fmt.Errorf("unhandled SQL Server type %#v (found on column %q of table %q)", column.DataType, column.Name, column.TableName)
	}

	// Pass-through the column nullability and description.
	schema.nullable = column.IsNullable
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
	"bigint":   {jsonType: "integer"},
	"int":      {jsonType: "integer"},
	"smallint": {jsonType: "integer"},
	"tinyint":  {jsonType: "integer"},

	"numeric":    {jsonType: "string", format: "number"},
	"decimal":    {jsonType: "string", format: "number"},
	"money":      {jsonType: "string", format: "number"},
	"smallmoney": {jsonType: "string", format: "number"},

	"bit": {jsonType: "boolean"},

	"float": {jsonType: "number"},
	"real":  {jsonType: "number"},

	"char":     {jsonType: "string"},
	"varchar":  {jsonType: "string"},
	"text":     {jsonType: "string"},
	"nchar":    {jsonType: "string"},
	"nvarchar": {jsonType: "string"},
	"ntext":    {jsonType: "string"},

	"binary":    {jsonType: "string", contentEncoding: "base64"},
	"varbinary": {jsonType: "string", contentEncoding: "base64"},
	"image":     {jsonType: "string", contentEncoding: "base64"},

	"date":           {jsonType: "string", format: "date"},
	"time":           {jsonType: "string", format: "time"},
	"datetimeoffset": {jsonType: "string", format: "date-time"},

	"uniqueidentifier": {jsonType: "string", format: "uuid"},

	"xml": {jsonType: "string"},

	"datetime":      {jsonType: "string", format: "date-time"},
	"datetime2":     {jsonType: "string", format: "date-time"},
	"smalldatetime": {jsonType: "string", format: "date-time"},

	"hierarchyid": {jsonType: "string", contentEncoding: "base64"},
}
