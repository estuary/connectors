package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

// Discover enumerates tables and views from `information_schema.tables` and generates
// placeholder capture queries for those tables.
func (drv *BatchSQLDriver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	var db, err = drv.Connect(ctx, &cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tableInfo, err := drv.discoverTables(ctx, db, &cfg)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %w", err)
	}

	var bindings []*pc.Response_Discovered_Binding
	for tableID, table := range tableInfo {
		// Exclude tables in "system schemas" such as information_schema or pg_catalog.
		if slices.Contains(drv.ExcludedSystemSchemas, table.Schema) {
			continue
		}

		var recommendedName = recommendedCatalogName(table.Schema, table.Name)
		var res, err = drv.GenerateResource(&cfg, recommendedName, table.Schema, table.Name, table.Type)
		if err != nil {
			log.WithFields(log.Fields{
				"reason": err,
				"table":  tableID,
				"type":   table.Type,
			}).Warn("unable to generate resource spec")
			continue
		}
		resourceConfigJSON, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("error serializing resource spec: %w", err)
		}

		collectionSchema, collectionKey, err := generateCollectionSchema(&cfg, table, true)
		if err != nil {
			return nil, fmt.Errorf("error generating %q collection schema: %w", tableID, err)
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    recommendedName,
			ResourceConfigJson: resourceConfigJSON,
			DocumentSchemaJson: collectionSchema,
			Key:                collectionKey,
			ResourcePath:       []string{res.Name},
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

func (drv *BatchSQLDriver) discoverTables(ctx context.Context, db *client.Conn, cfg *Config) (map[string]*discoveredTable, error) {
	tables, err := discoverTables(ctx, db, cfg.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	columns, err := discoverColumns(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("error listing columns: %w", err)
	}
	keys, err := discoverPrimaryKeys(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("error listing primary keys: %w", err)
	}

	var tableInfo = make(map[string]*discoveredTable)
	for _, table := range tables {
		var tableID = table.Schema + "." + table.Name
		tableInfo[tableID] = table
	}
	for _, column := range columns {
		var tableID = column.Schema + "." + column.Table
		if table, ok := tableInfo[tableID]; ok {
			table.columns = append(table.columns, column)
		}
	}

	for _, key := range keys {
		var tableID = key.Schema + "." + key.Table
		if table, ok := tableInfo[tableID]; ok {
			table.key = key
		}
	}
	return tableInfo, nil
}

var (
	// The fallback key of discovered collections when the source table has no primary key.
	fallbackKey = []string{"/_meta/row_id"}

	// Old captures used a different fallback key which included a value identifying
	// the specific polling iteration which produced the document. This proved less
	// than ideal for full-refresh bindings on keyless tables.
	fallbackKeyOld = []string{"/_meta/polled", "/_meta/index"}
)

func generateCollectionSchema(cfg *Config, table *discoveredTable, fullWriteSchema bool) (json.RawMessage, []string, error) {
	// Extract useful key and column type information
	var keyColumns []string
	if table.key != nil {
		keyColumns = table.key.Columns
	}
	var columnTypes = make(map[string]columnType)
	for _, column := range table.columns {
		columnTypes[column.Name] = column.DataType
	}

	// Generate schema for the metadata via reflection
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var metadataSchema = reflector.ReflectFromType(reflect.TypeOf(documentMetadata{}))
	if !cfg.Advanced.parsedFeatureFlags["keyless_row_id"] { // Don't include row_id as required on old captures with keyless_row_id off
		metadataSchema.Required = slices.DeleteFunc(metadataSchema.Required, func(s string) bool { return s == "row_id" })
	}
	metadataSchema.Definitions = nil
	if metadataSchema.Extras == nil {
		metadataSchema.Extras = make(map[string]any)
	}
	if fullWriteSchema {
		metadataSchema.AdditionalProperties = nil
	} else {
		metadataSchema.Extras["additionalProperties"] = false
	}

	var required = append([]string{"_meta"}, keyColumns...)
	var properties = map[string]*jsonschema.Schema{
		"_meta": metadataSchema,
	}
	for colName, colType := range columnTypes {
		var colSchema = colType.JSONSchema()
		if types, ok := colSchema.Extras["type"].([]string); ok && len(types) > 1 && !fullWriteSchema {
			// Remove null as an option when there are multiple type options and we don't want nullability
			colSchema.Extras["type"] = slices.DeleteFunc(types, func(t string) bool { return t == "null" })
		}
		properties[colName] = colSchema
	}

	var schema = &jsonschema.Schema{
		Type:     "object",
		Required: required,
		Extras: map[string]interface{}{
			"properties":     properties,
			"x-infer-schema": true,
		},
	}
	if !fullWriteSchema {
		schema.Extras["additionalProperties"] = false
	}

	// Marshal schema to JSON
	bs, err := json.Marshal(schema)
	if err != nil {
		return nil, nil, fmt.Errorf("error serializing schema: %w", err)
	}

	// If the table has a primary key then convert it to a collection key, otherwise
	// recommend an appropriate fallback collection key.
	var collectionKey []string
	if keyColumns != nil {
		for _, colName := range keyColumns {
			collectionKey = append(collectionKey, primaryKeyToCollectionKey(colName))
		}
	} else if !cfg.Advanced.parsedFeatureFlags["keyless_row_id"] {
		collectionKey = fallbackKeyOld
	} else {
		collectionKey = fallbackKey
	}
	return json.RawMessage(bs), collectionKey, nil
}

// primaryKeyToCollectionKey converts a database primary key column name into a Flow collection key
// JSON pointer with escaping for '~' and '/' applied per RFC6901.
func primaryKeyToCollectionKey(key string) string {
	// Any encoded '~' must be escaped first to prevent a second escape on escaped '/' values as
	// '~1'.
	key = strings.ReplaceAll(key, "~", "~0")
	key = strings.ReplaceAll(key, "/", "~1")
	return "/" + key
}

const queryDiscoverTables = `SELECT table_schema, table_name, table_type FROM information_schema.tables;`

type discoveredTable struct {
	Schema string
	Name   string
	Type   string // Usually 'BASE TABLE' or 'VIEW'

	columns []*discoveredColumn
	key     *discoveredPrimaryKey
}

func discoverTables(ctx context.Context, db *client.Conn, discoverSchemas []string) ([]*discoveredTable, error) {
	var results, err = db.Execute(queryDiscoverTables)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %w", err)
	}
	defer results.Close()

	var tables []*discoveredTable
	for _, row := range results.Values {
		var tableSchema = string(row[0].AsString())
		var tableName = string(row[1].AsString())
		var tableType = string(row[2].AsString())

		if len(discoverSchemas) > 0 && !slices.Contains(discoverSchemas, tableSchema) {
			log.WithFields(log.Fields{"schema": tableSchema, "table": tableName}).Debug("ignoring table")
			continue
		}
		tables = append(tables, &discoveredTable{
			Schema: tableSchema,
			Name:   tableName,
			Type:   tableType,
		})
	}
	return tables, nil
}

type discoveredColumn struct {
	Schema      string     // The schema in which the table resides
	Table       string     // The name of the table with this column
	Name        string     // The name of the column
	Index       int        // The ordinal position of the column within a row
	IsNullable  bool       // Whether the column can be null
	DataType    columnType // The datatype of the column
	Description *string    // The description of the column, if present and known
}

type columnType interface {
	JSONSchema() *jsonschema.Schema
}

type basicColumnType struct {
	jsonTypes       []string
	contentEncoding string
	format          string
	nullable        bool
	description     string
}

func (ct *basicColumnType) JSONSchema() *jsonschema.Schema {
	var sch = &jsonschema.Schema{
		Format:      ct.format,
		Extras:      make(map[string]interface{}),
		Description: ct.description,
	}

	if ct.contentEncoding != "" {
		sch.Extras["contentEncoding"] = ct.contentEncoding // New in 2019-09.
	}

	if ct.jsonTypes != nil {
		var types = append([]string(nil), ct.jsonTypes...)
		if ct.nullable {
			types = append(types, "null")
		}
		if len(types) == 1 {
			sch.Type = types[0]
		} else {
			sch.Extras["type"] = types
		}
	}
	return sch
}

const queryDiscoverColumns = `
SELECT table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, column_type
  FROM information_schema.columns
  ORDER BY table_schema, table_name, ordinal_position;`

func discoverColumns(ctx context.Context, db *client.Conn) ([]*discoveredColumn, error) {
	var results, err = db.Execute(queryDiscoverColumns)
	if err != nil {
		return nil, fmt.Errorf("error discovering column types: %w", err)
	}
	defer results.Close()

	var columns []*discoveredColumn
	for _, row := range results.Values {
		var tableSchema = string(row[0].AsString())
		var tableName = string(row[1].AsString())
		var columnName = string(row[2].AsString())
		var ordinalPosition = int(row[3].AsInt64())
		var isNullable = string(row[4].AsString()) != "NO"
		var typeName = string(row[5].AsString())
		// var fullColumnType = string(row[6].AsString())

		var dataType, ok = databaseTypeToJSON[typeName]
		if !ok {
			dataType = basicColumnType{description: "using catch-all schema"}
		}
		dataType.nullable = isNullable

		// Append source type information to the description
		if dataType.description != "" {
			dataType.description += " "
		}
		var nullabilityDescription = ""
		if !isNullable {
			nullabilityDescription = "non-nullable "
		}
		dataType.description += fmt.Sprintf("(source type: %s%s)", nullabilityDescription, typeName)

		var column = &discoveredColumn{
			Schema:     tableSchema,
			Table:      tableName,
			Name:       columnName,
			Index:      ordinalPosition,
			IsNullable: isNullable,
			DataType:   &dataType,
		}
		columns = append(columns, column)
	}
	return columns, nil
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
    AND tcs.table_schema = kcu.table_schema
    AND tcs.table_name = kcu.table_name
  WHERE tcs.constraint_type = 'PRIMARY KEY'
  ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position;
`

type discoveredPrimaryKey struct {
	Schema  string
	Table   string
	Columns []string
}

func discoverPrimaryKeys(ctx context.Context, db *client.Conn) ([]*discoveredPrimaryKey, error) {
	var results, err = db.Execute(queryDiscoverPrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("error discovering primary keys: %w", err)
	}
	defer results.Close()

	var keysByTable = make(map[string]*discoveredPrimaryKey)
	for _, row := range results.Values {
		var tableSchema = string(row[0].AsString())
		var tableName = string(row[1].AsString())
		var columnName = string(row[2].AsString())
		var ordinalPosition = int(row[3].AsInt64())

		var tableID = tableSchema + "." + tableName
		var keyInfo = keysByTable[tableID]
		if keyInfo == nil {
			keyInfo = &discoveredPrimaryKey{
				Schema: tableSchema,
				Table:  tableName,
			}
			keysByTable[tableID] = keyInfo
		}
		keyInfo.Columns = append(keyInfo.Columns, columnName)
		if ordinalPosition != len(keyInfo.Columns) {
			return nil, fmt.Errorf("primary key column %q (of table %q) appears out of order", columnName, tableID)
		}
	}

	var keys []*discoveredPrimaryKey
	for _, key := range keysByTable {
		keys = append(keys, key)
	}
	return keys, nil
}

var databaseTypeToJSON = map[string]basicColumnType{
	"tinyint":   {jsonTypes: []string{"integer"}},
	"smallint":  {jsonTypes: []string{"integer"}},
	"mediumint": {jsonTypes: []string{"integer"}},
	"int":       {jsonTypes: []string{"integer"}},
	"bigint":    {jsonTypes: []string{"integer"}},
	"bit":       {jsonTypes: []string{"integer"}},

	"float":   {jsonTypes: []string{"number"}},
	"double":  {jsonTypes: []string{"number"}},
	"decimal": {jsonTypes: []string{"string"}, format: "number"},

	"char":       {jsonTypes: []string{"string"}},
	"varchar":    {jsonTypes: []string{"string"}},
	"tinytext":   {jsonTypes: []string{"string"}},
	"text":       {jsonTypes: []string{"string"}},
	"mediumtext": {jsonTypes: []string{"string"}},
	"longtext":   {jsonTypes: []string{"string"}},

	"binary":     {jsonTypes: []string{"string"}},
	"varbinary":  {jsonTypes: []string{"string"}},
	"tinyblob":   {jsonTypes: []string{"string"}},
	"blob":       {jsonTypes: []string{"string"}},
	"mediumblob": {jsonTypes: []string{"string"}},
	"longblob":   {jsonTypes: []string{"string"}},

	"enum": {jsonTypes: []string{"string"}},
	"set":  {jsonTypes: []string{"string"}},

	"date": {jsonTypes: []string{"string"}}, // Not valid for format: date until we sanitize values like '0000-00-00' to valid RFC3339 dates

	"datetime":  {jsonTypes: []string{"string"}},
	"timestamp": {jsonTypes: []string{"string"}},
	"time":      {jsonTypes: []string{"string"}},
	"year":      {jsonTypes: []string{"integer"}},

	"json": {},
}

var catalogNameSanitizerRe = regexp.MustCompile(`(?i)[^a-z0-9\-_.]`)

func recommendedCatalogName(schema, table string) string {
	var catalogName string
	// Omit 'default schema' names for Postgres and SQL Server. There is
	// no default schema for MySQL databases.
	if schema == "public" || schema == "dbo" {
		catalogName = table
	} else {
		catalogName = schema + "_" + table
	}
	return catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(catalogName), "_")
}
