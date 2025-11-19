package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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

	// Generate discovery resource and collection schema for this table
	var bindings []*pc.Response_Discovered_Binding
	for tableID, table := range tableInfo {
		var resourceName = recommendedResourceName(table.Schema, table.Name)
		var res, err = drv.GenerateResource(&cfg, resourceName, table.Schema, table.Name, table.Type)
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

		// Generate a collection schema from the column types and key column names of this table.
		generatedSchema, collectionKey, err := generateCollectionSchema(&cfg, table, true)
		if err != nil {
			log.WithFields(log.Fields{"table": tableID, "err": err}).Warn("unable to generate collection schema")
			continue
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    recommendedCatalogName(table.Schema, table.Name),
			ResourceConfigJson: resourceConfigJSON,
			DocumentSchemaJson: generatedSchema,
			Key:                collectionKey,
			ResourcePath:       []string{res.Name},
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

func (drv *BatchSQLDriver) discoverTables(ctx context.Context, db *sql.DB, cfg *Config) (map[string]*discoveredTable, error) {
	// Run discovery queries in parallel for lower discovery latency on large databases.
	// TODO(wgd): See if there's a nice context-and-errors-aware promise library we could use
	// here instead of just doing the same copy-paste channels-and-errgroup pattern thrice.
	var tablesCh = make(chan []*discoveredTable, 1)
	var columnsCh = make(chan []*discoveredColumn, 1)
	var keysCh = make(chan []*discoveredPrimaryKey, 1)
	var workerGroup, workerCtx = errgroup.WithContext(ctx)
	workerGroup.Go(func() error {
		tables, err := discoverTables(workerCtx, db, cfg.Advanced.DiscoverSchemas)
		if err != nil {
			return fmt.Errorf("error listing tables: %w", err)
		}
		tablesCh <- tables
		return nil
	})
	workerGroup.Go(func() error {
		columns, err := discoverColumns(workerCtx, db, cfg.Advanced.DiscoverSchemas)
		if err != nil {
			return fmt.Errorf("error listing columns: %w", err)
		}
		columnsCh <- columns
		return nil
	})
	workerGroup.Go(func() error {
		keys, err := discoverPrimaryKeys(workerCtx, db, cfg.Advanced.DiscoverSchemas)
		if err != nil {
			return fmt.Errorf("error listing primary keys: %w", err)
		}
		keysCh <- keys
		return nil
	})
	if err := workerGroup.Wait(); err != nil {
		return nil, err
	}
	var tables = <-tablesCh
	var columns = <-columnsCh
	var keys = <-keysCh

	// Aggregate information by table
	var tableInfo = make(map[string]*discoveredTable)
	for _, table := range tables {
		var tableID = table.Schema + "." + table.Name
		tableInfo[tableID] = table
	}
	for _, column := range columns {
		var tableID = column.Schema + "." + column.Table
		if table, ok := tableInfo[tableID]; ok {
			table.columns = append(table.columns, column)
			if column.Index != len(table.columns) {
				return nil, fmt.Errorf("internal error: column %q of table %q appears out of order", column.Name, tableID)
			}
		}
	}
	for _, key := range keys {
		var tableID = key.Schema + "." + key.Table
		if table, ok := tableInfo[tableID]; ok {
			table.keys = append(table.keys, key)
			if key.Index != len(table.keys) {
				return nil, fmt.Errorf("internal error: primary key column %q of table %q appears out of order", key.Column, tableID)
			}
		}
	}
	return tableInfo, nil
}

var (
	// The fallback key of discovered collections when the source table has no primary key.
	fallbackKey = []string{"/_meta/row_id"}

	// Before the /_meta/row_id property was added, captures left the collection key empty
	// for tables without a source PK, forcing users to pick one themselves.
	fallbackKeyOld = []string{}
)

func generateCollectionSchema(cfg *Config, table *discoveredTable, fullWriteSchema bool) (json.RawMessage, []string, error) {
	// Extract useful key and column type information
	var keyColumns []string
	for _, key := range table.keys {
		keyColumns = append(keyColumns, key.Column)
	}
	var columnTypes = make(map[string]columnType)
	for _, column := range table.columns {
		columnTypes[column.Name] = column.DataType
	}

	// Generate schema for the metadata via reflection
	var reflector = jsonschema.Reflector{
		ExpandedStruct:            true,
		DoNotReference:            true,
		AllowAdditionalProperties: fullWriteSchema,
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
		if sourceSchema, ok := metadataSchema.Properties.Get("source"); ok {
			sourceSchema.AdditionalProperties = nil
		}
	} else {
		metadataSchema.Extras["additionalProperties"] = false
	}

	var properties = map[string]*jsonschema.Schema{
		"_meta": metadataSchema,
	}
	var required = []string{"_meta"}

	for colName, colType := range columnTypes {
		var colSchema = colType.JSONSchema()
		var isPrimaryKey = slices.Contains(keyColumns, colName)
		if types, ok := colSchema.Extras["type"].([]string); ok && len(types) > 1 && !fullWriteSchema {
			// Remove null as an option when there are multiple type options and we don't want nullability
			colSchema.Extras["type"] = slices.DeleteFunc(types, func(t string) bool { return t == "null" })
		}
		properties[colName] = colSchema

		// When generating a write schema, only primary key columns are required.
		// Otherwise, when generating a sourced schema, all columns are required
		// (and are turned off by inference if they're omitted in captured documents).
		if !fullWriteSchema || isPrimaryKey {
			required = append(required, colName)
		}
	}
	slices.Sort(required) // Stable ordering.

	var extras = map[string]any{
		"properties": properties,
	}
	if cfg.Advanced.parsedFeatureFlags["use_schema_inference"] {
		extras["x-infer-schema"] = true
	}
	var schema = &jsonschema.Schema{
		Type:     "object",
		Required: required,
		Extras:   extras,
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
	} else {
		collectionKey = fallbackKey
		if !cfg.Advanced.parsedFeatureFlags["keyless_row_id"] {
			collectionKey = fallbackKeyOld
		}
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

type discoveredTable struct {
	Schema string
	Name   string
	Type   string // Usually 'BASE TABLE' or 'VIEW'

	columns []*discoveredColumn
	keys    []*discoveredPrimaryKey
}

func discoverTables(ctx context.Context, db *sql.DB, discoverSchemas []string) ([]*discoveredTable, error) {
	var query = new(strings.Builder)
	var args []any

	fmt.Fprintf(query, "SELECT nc.nspname AS table_schema,")
	fmt.Fprintf(query, "       c.relname AS table_name,")
	fmt.Fprintf(query, "       CASE")
	fmt.Fprintf(query, "         WHEN c.relkind = ANY (ARRAY['r'::\"char\", 'p'::\"char\"]) THEN 'BASE TABLE'::text")
	fmt.Fprintf(query, "         WHEN c.relkind = 'v'::\"char\" THEN 'VIEW'::text")
	fmt.Fprintf(query, "         WHEN c.relkind = 'f'::\"char\" THEN 'FOREIGN'::text")
	fmt.Fprintf(query, "         ELSE ''::text")
	fmt.Fprintf(query, "       END::information_schema.character_data AS table_type")
	fmt.Fprintf(query, " FROM pg_catalog.pg_class c")
	fmt.Fprintf(query, " JOIN pg_catalog.pg_namespace nc ON (nc.oid = c.relnamespace)")
	fmt.Fprintf(query, " WHERE c.relkind IN ('r', 'p', 'v', 'f')")
	if len(discoverSchemas) > 0 {
		fmt.Fprintf(query, "  AND nc.nspname = ANY ($1)")
		args = append(args, discoverSchemas)
	} else {
		fmt.Fprintf(query, "  AND nc.nspname NOT IN ('pg_catalog', 'pg_internal', 'information_schema', 'catalog_history', 'cron')")
	}
	fmt.Fprintf(query, ";")

	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query %q: %w", query.String(), err)
	}
	defer rows.Close()

	var tables []*discoveredTable
	for rows.Next() {
		var tableSchema, tableName, tableType string
		if err := rows.Scan(&tableSchema, &tableName, &tableType); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
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
	minLength       *uint64
	maxLength       *uint64
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

	// Copy the min/max lengths if present
	sch.MinLength = ct.minLength
	sch.MaxLength = ct.maxLength

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

func discoverColumns(ctx context.Context, db *sql.DB, discoverSchemas []string) ([]*discoveredColumn, error) {
	var query = new(strings.Builder)
	var args []any
	fmt.Fprintf(query, "SELECT nc.nspname as table_schema,")
	fmt.Fprintf(query, "       c.relname as table_name,")
	fmt.Fprintf(query, "       a.attname as column_name,")
	fmt.Fprintf(query, "       a.attnum as column_index,")
	fmt.Fprintf(query, "       NOT (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS is_nullable,")
	fmt.Fprintf(query, "       COALESCE(bt.typname, t.typname) AS udt_name,")
	fmt.Fprintf(query, "       t.typtype::text AS typtype,")
	fmt.Fprintf(query, "       a.atttypmod-4 AS char_max_length")
	fmt.Fprintf(query, "  FROM pg_catalog.pg_attribute a")
	fmt.Fprintf(query, "  JOIN pg_catalog.pg_type t ON a.atttypid = t.oid")
	fmt.Fprintf(query, "  JOIN pg_catalog.pg_class c ON a.attrelid = c.oid")
	fmt.Fprintf(query, "  JOIN pg_catalog.pg_namespace nc ON c.relnamespace = nc.oid")
	fmt.Fprintf(query, "  LEFT JOIN (pg_catalog.pg_type bt JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid)")
	fmt.Fprintf(query, "    ON t.typtype = 'd'::\"char\" AND t.typbasetype = bt.oid")
	fmt.Fprintf(query, "  WHERE a.attnum > 0")
	fmt.Fprintf(query, "    AND NOT a.attisdropped")
	fmt.Fprintf(query, "    AND c.relkind IN ('r', 'p', 'v', 'f')")
	if len(discoverSchemas) > 0 {
		fmt.Fprintf(query, "    AND nc.nspname = ANY ($1)")
		args = append(args, discoverSchemas)
	} else {
		fmt.Fprintf(query, "    AND nc.nspname NOT IN ('pg_catalog', 'pg_internal', 'information_schema', 'catalog_history', 'cron')")
	}
	fmt.Fprintf(query, "  ORDER BY nc.nspname, c.relname, a.attnum;")

	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query %q: %w", query.String(), err)
	}
	defer rows.Close()

	var columns []*discoveredColumn
	for rows.Next() {
		var tableSchema, tableName, columnName string
		var columnIndex int
		var isNullable bool
		var typeName, typeType string
		var charMaxLength sql.NullInt64
		if err := rows.Scan(&tableSchema, &tableName, &columnName, &columnIndex, &isNullable, &typeName, &typeType, &charMaxLength); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		// Decode column type information into a usable form
		var dataType basicColumnType
		switch typeType {
		case "e": // enum values are captured as strings
			dataType = basicColumnType{jsonTypes: []string{"string"}}
		case "r", "m": // ranges and multiranges are captured as strings
			dataType = basicColumnType{jsonTypes: []string{"string"}}
		default:
			var ok bool
			dataType, ok = databaseTypeToJSON[typeName]
			if !ok {
				dataType = basicColumnType{description: fmt.Sprintf("using catch-all schema (unknown type %q)", typeName)}
			}
		}
		dataType.nullable = isNullable

		// Add length constraints for char/varchar columns with defined lengths
		if charMaxLength.Valid && charMaxLength.Int64 > 0 {
			switch typeName {
			case "bpchar": // CHAR(n) - fixed-length, blank-padded
				// For fixed-length character types, set both minLength and maxLength
				var length = uint64(charMaxLength.Int64)
				dataType.minLength = &length
				dataType.maxLength = &length
			case "varchar": // VARCHAR(n) - variable-length with limit
				// For variable-length character types, only set maxLength
				var length = uint64(charMaxLength.Int64)
				dataType.maxLength = &length
			case "varbyte": // VARBYTE(n) - variable-length binary (Redshift specific)
				// For variable-length binary types, calculate max base64 encoded length
				// Binary data is base64 encoded: every 3 bytes becomes 4 characters
				var base64Length = uint64((charMaxLength.Int64 + 2) / 3 * 4)
				dataType.maxLength = &base64Length
			case "bytea":
				// PostgreSQL BYTEA doesn't take a fixed length constraint so we don't
				// have to worry about binary types with explicit lengths, this case
				// is just for documentation.
			}
		}

		// Append source type information to the description
		if dataType.description != "" {
			dataType.description += " "
		}
		var nullabilityDescription = ""
		if !isNullable {
			nullabilityDescription = "non-nullable "
		}
		dataType.description += fmt.Sprintf("(source type: %s%s)", nullabilityDescription, typeName)

		columns = append(columns, &discoveredColumn{
			Schema:     tableSchema,
			Table:      tableName,
			Name:       columnName,
			Index:      columnIndex,
			IsNullable: isNullable,
			DataType:   &dataType,
		})
	}
	return columns, nil
}

type discoveredPrimaryKey struct {
	Schema string
	Table  string
	Column string
	Index  int
}

func discoverPrimaryKeys(ctx context.Context, db *sql.DB, discoverSchemas []string) ([]*discoveredPrimaryKey, error) {
	var query = new(strings.Builder)
	var args []any

	fmt.Fprintf(query, "SELECT nr.nspname::information_schema.sql_identifier AS table_schema,")
	fmt.Fprintf(query, "       r.relname::information_schema.sql_identifier AS table_name,")
	fmt.Fprintf(query, "       a.attname::information_schema.sql_identifier AS column_name,")
	fmt.Fprintf(query, "       pos.n::information_schema.cardinal_number AS ordinal_position")
	fmt.Fprintf(query, "  FROM pg_namespace nr,")
	fmt.Fprintf(query, "       pg_class r,")
	fmt.Fprintf(query, "       pg_attribute a,")
	fmt.Fprintf(query, "       pg_constraint c,")
	fmt.Fprintf(query, "       generate_series(1,100,1) pos(n)")
	fmt.Fprintf(query, "  WHERE nr.oid = r.relnamespace")
	fmt.Fprintf(query, "    AND r.oid = a.attrelid")
	fmt.Fprintf(query, "    AND r.oid = c.conrelid")
	fmt.Fprintf(query, "    AND c.conkey[pos.n] = a.attnum")
	fmt.Fprintf(query, "    AND NOT a.attisdropped")
	fmt.Fprintf(query, "    AND c.contype = 'p'::\"char\"")
	fmt.Fprintf(query, "    AND r.relkind = 'r'::\"char\"")
	if len(discoverSchemas) > 0 {
		fmt.Fprintf(query, "    AND nr.nspname = ANY ($1)")
		args = append(args, discoverSchemas)
	} else {
		fmt.Fprintf(query, "    AND nr.nspname NOT IN ('pg_catalog', 'pg_internal', 'information_schema', 'catalog_history', 'cron')")
	}
	fmt.Fprintf(query, "  ORDER BY r.relname, pos.n;")

	rows, err := db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query %q: %w", query.String(), err)
	}
	defer rows.Close()

	var keys []*discoveredPrimaryKey
	for rows.Next() {
		var tableSchema, tableName, columnName string
		var ordinalPosition int
		if err := rows.Scan(&tableSchema, &tableName, &columnName, &ordinalPosition); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		keys = append(keys, &discoveredPrimaryKey{
			Schema: tableSchema,
			Table:  tableName,
			Column: columnName,
			Index:  ordinalPosition,
		})
	}
	return keys, nil
}

var databaseTypeToJSON = map[string]basicColumnType{
	"bool": {jsonTypes: []string{"boolean"}},

	"int2": {jsonTypes: []string{"integer"}},
	"int4": {jsonTypes: []string{"integer"}},
	"int8": {jsonTypes: []string{"integer"}},

	"numeric": {jsonTypes: []string{"string"}, format: "number"},
	"float4":  {jsonTypes: []string{"number", "string"}, format: "number"},
	"float8":  {jsonTypes: []string{"number", "string"}, format: "number"},

	"varchar": {jsonTypes: []string{"string"}},
	"bpchar":  {jsonTypes: []string{"string"}},
	"text":    {jsonTypes: []string{"string"}},
	"bytea":   {jsonTypes: []string{"string"}, contentEncoding: "base64"},
	"varbyte": {jsonTypes: []string{"string"}, contentEncoding: "base64"},
	"xml":     {jsonTypes: []string{"string"}},
	"bit":     {jsonTypes: []string{"string"}},
	"varbit":  {jsonTypes: []string{"string"}},

	"json":     {},
	"jsonb":    {},
	"jsonpath": {jsonTypes: []string{"string"}},

	// Domain-Specific Types
	"date":        {jsonTypes: []string{"string"}, format: "date-time"},
	"timestamp":   {jsonTypes: []string{"string"}, format: "date-time"},
	"timestamptz": {jsonTypes: []string{"string"}, format: "date-time"},
	"time":        {jsonTypes: []string{"string"}}, // Not 'format: time' because it has no time zone.
	"timetz":      {jsonTypes: []string{"string"}, format: "time"},
	"interval":    {jsonTypes: []string{"string"}},
	"money":       {jsonTypes: []string{"string"}},
	"point":       {jsonTypes: []string{"string"}},
	"line":        {jsonTypes: []string{"string"}},
	"lseg":        {jsonTypes: []string{"string"}},
	"box":         {jsonTypes: []string{"string"}},
	"path":        {jsonTypes: []string{"string"}},
	"polygon":     {jsonTypes: []string{"string"}},
	"circle":      {jsonTypes: []string{"string"}},
	"inet":        {jsonTypes: []string{"string"}},
	"cidr":        {jsonTypes: []string{"string"}},
	"macaddr":     {jsonTypes: []string{"string"}},
	"macaddr8":    {jsonTypes: []string{"string"}},
	"tsvector":    {jsonTypes: []string{"string"}},
	"tsquery":     {jsonTypes: []string{"string"}},
	"uuid":        {jsonTypes: []string{"string"}, format: "uuid"},
}

var catalogNameSanitizerRe = regexp.MustCompile(`(?i)[^a-z0-9\-_.]`)

func recommendedCatalogName(schema, table string) string {
	schema = catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(schema), "_")
	table = catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(table), "_")
	return schema + "/" + table
}

// recommendedResourceName implements the old name-recommendation logic so that the name
// field of discovered bindings remains unchanged even though the catalog name is different.
func recommendedResourceName(schema, table string) string {
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
