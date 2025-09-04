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
)

// Discover enumerates tables and views from `information_schema.tables` and generates
// placeholder capture queries for thos tables.
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
		var resourceName = recommendedResourceName(table.Owner, table.Name)
		var res, err = drv.GenerateResource(resourceName, table.Owner, table.Name, "TABLE")
		if err != nil {
			log.WithFields(log.Fields{
				"reason": err,
				"table":  tableID,
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
			RecommendedName:    recommendedCatalogName(table.Owner, table.Name),
			ResourceConfigJson: resourceConfigJSON,
			DocumentSchemaJson: collectionSchema,
			Key:                collectionKey,
			ResourcePath:       []string{res.Name},
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

func (drv *BatchSQLDriver) discoverTables(ctx context.Context, db *sql.DB, cfg *Config) (map[string]*discoveredTable, error) {
	tables, err := discoverTables(ctx, db, cfg.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	columns, err := discoverColumns(ctx, db, cfg.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("error listing columns: %w", err)
	}
	keys, err := discoverPrimaryKeys(ctx, db, cfg.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("error listing primary keys: %w", err)
	}

	var tableInfo = make(map[string]*discoveredTable)
	for _, table := range tables {
		var tableID = table.Owner + "." + table.Name
		tableInfo[tableID] = table
	}
	for _, column := range columns {
		var tableID = column.Owner + "." + column.Table
		if table, ok := tableInfo[tableID]; ok {
			table.columns = append(table.columns, column)
		}
	}
	for _, key := range keys {
		var tableID = key.Owner + "." + key.Table
		if table, ok := tableInfo[tableID]; ok {
			table.key = key
		}
	}
	return tableInfo, nil
}

// The fallback collection key just refers to the polling iteration and result index of each document.
var fallbackKey = []string{"/_meta/polled", "/_meta/index"}

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
	metadataSchema.Definitions = nil
	if metadataSchema.Extras == nil {
		metadataSchema.Extras = make(map[string]any)
	}
	if fullWriteSchema {
		metadataSchema.AdditionalProperties = nil
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

type discoveredTable struct {
	Owner string
	Name  string

	columns []*discoveredColumn
	key     *discoveredPrimaryKey
}

func discoverTables(ctx context.Context, db *sql.DB, discoverSchemas []string) ([]*discoveredTable, error) {
	var query = new(strings.Builder)

	fmt.Fprintf(query, "SELECT DISTINCT(NVL(IOT_NAME, TABLE_NAME)) AS table_name,")
	fmt.Fprintf(query, "owner FROM all_tables")
	fmt.Fprintf(query, " WHERE tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'SAMPLESCHEMA')")
	fmt.Fprintf(query, " AND owner NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'RDSADMIN', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS', 'GGS_ADMIN', 'GSMADMIN_INTERNAL')")
	fmt.Fprintf(query, " AND table_name NOT IN ('DBTOOLS$EXECUTION_HISTORY')")

	if len(discoverSchemas) > 0 {
		var schemasComma = strings.Join(discoverSchemas, "','")
		fmt.Fprintf(query, " AND owner IN ('%s')", schemasComma)
	}

	rows, err := db.QueryContext(ctx, query.String())
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query %q: %w", query.String(), err)
	}
	defer rows.Close()

	var tables []*discoveredTable
	for rows.Next() {
		var tableOwner, tableName string
		if err := rows.Scan(&tableName, &tableOwner); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		tables = append(tables, &discoveredTable{
			Owner: tableOwner,
			Name:  tableName,
		})
	}
	return tables, nil
}

type discoveredColumn struct {
	Owner       string
	Table       string     // The name of the table with this column
	Name        string     // The name of the column
	Index       int        // The ordinal position of the column within a row
	DataType    columnType // The datatype of the column
	Description *string    // The description of the column, if present and known
}

type columnType interface {
	IsNullable() bool
	JSONSchema() *jsonschema.Schema
}

type basicColumnType struct {
	jsonTypes       []string
	contentEncoding string
	format          string
	nullable        bool
	description     string
}

func (ct *basicColumnType) IsNullable() bool {
	return ct.nullable
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

func discoverColumns(ctx context.Context, db *sql.DB, discoverSchemas []string) ([]*discoveredColumn, error) {
	var query = new(strings.Builder)

	fmt.Fprintf(query, "SELECT t.owner, t.table_name, t.column_name, t.column_id,")
	fmt.Fprintf(query, " t.nullable, t.data_type, t.data_precision, t.data_scale, t.data_length")
	fmt.Fprintf(query, " FROM all_tab_columns t")
	fmt.Fprintf(query, " WHERE t.owner NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'RDSADMIN', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS', 'GGS_ADMIN', 'GSMADMIN_INTERNAL')")
	fmt.Fprintf(query, " AND t.table_name NOT IN ('DBTOOLS$EXECUTION_HISTORY')")
	fmt.Fprintf(query, " ORDER BY t.table_name, t.column_id")

	rows, err := db.QueryContext(ctx, query.String())
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query %q: %w", query.String(), err)
	}
	defer rows.Close()

	var columns []*discoveredColumn
	for rows.Next() {
		var tableOwner, tableName, columnName string
		var columnID int
		var isNullable string
		var typeName string
		var dataPrecision sql.NullInt16
		var dataScale sql.NullInt16
		var dataLength int

		if err := rows.Scan(&tableOwner, &tableName, &columnName, &columnID, &isNullable, &typeName, &dataPrecision, &dataScale, &dataLength); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		var dataType, err = databaseTypeToJSON(typeName, dataPrecision, dataScale, isNullable != "N")
		if err != nil {
			return nil, fmt.Errorf("error converting column type %q to JSON: %w", typeName, err)
		}

		var column = &discoveredColumn{
			Owner:    tableOwner,
			Table:    tableName,
			Name:     columnName,
			Index:    columnID,
			DataType: dataType,
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func databaseTypeToJSON(typeName string, dataPrecision, dataScale sql.NullInt16, isNullable bool) (columnType, error) {
	var dataType basicColumnType

	var isInteger = dataScale.Int16 == 0
	if typeName == "NUMBER" && !dataScale.Valid && !dataPrecision.Valid {
		// when scale and precision are both null, both have the maximum value possible
		// equivalent to NUMBER(38, 127)
		dataType.format = "number"
		dataType.jsonTypes = []string{"string"}
	} else if typeName == "NUMBER" && isInteger {
		// data_precision null defaults to precision 38
		dataType.format = "integer"
		dataType.jsonTypes = []string{"string"}
	} else if slices.Contains([]string{"FLOAT", "NUMBER"}, typeName) {
		dataType.format = "number"
		dataType.jsonTypes = []string{"string"}
	} else if slices.Contains([]string{"CHAR", "VARCHAR", "VARCHAR2", "NCHAR", "NVARCHAR2"}, typeName) {
		dataType.jsonTypes = []string{"string"}
	} else if strings.Contains(typeName, "WITH TIME ZONE") {
		dataType.jsonTypes = []string{"string"}
		dataType.format = "date-time"
	} else if typeName == "DATE" || strings.Contains(typeName, "TIMESTAMP") {
		dataType.jsonTypes = []string{"string"}
	} else if strings.Contains(typeName, "INTERVAL") {
		dataType.jsonTypes = []string{"string"}
	} else if slices.Contains([]string{"CLOB", "RAW"}, typeName) {
		dataType.jsonTypes = []string{"string"}
	} else {
		dataType.description = "using catch-all schema"
	}
	dataType.nullable = isNullable

	// Append source type information to the description
	if dataType.description != "" {
		dataType.description += " "
	}
	var nullabilityDescription = ""
	if isNullable {
		nullabilityDescription = "non-nullable "
	}
	dataType.description += fmt.Sprintf("(source type: %s%s)", nullabilityDescription, typeName)
	return &dataType, nil
}

type discoveredPrimaryKey struct {
	Owner   string
	Table   string
	Columns []string
}

func discoverPrimaryKeys(ctx context.Context, db *sql.DB, discoverSchemas []string) ([]*discoveredPrimaryKey, error) {
	var query = new(strings.Builder)

	fmt.Fprintf(query, "SELECT t.owner, t.table_name, t.column_name, c.position")
	fmt.Fprintf(query, " FROM all_tab_columns t")
	fmt.Fprintf(query, " INNER JOIN (")
	fmt.Fprintf(query, "   SELECT c.owner, c.table_name, c.constraint_type, ac.column_name, ac.position FROM all_constraints c")
	fmt.Fprintf(query, "     INNER JOIN all_cons_columns ac ON (")
	fmt.Fprintf(query, "         c.constraint_name = ac.constraint_name")
	fmt.Fprintf(query, "         AND c.table_name = ac.table_name")
	fmt.Fprintf(query, "         AND c.owner = ac.owner")
	fmt.Fprintf(query, "         AND c.constraint_type = 'P'")
	fmt.Fprintf(query, "       )")
	fmt.Fprintf(query, "     ) c")
	fmt.Fprintf(query, " ON (t.owner = c.owner AND t.table_name = c.table_name AND t.column_name = c.column_name)")
	fmt.Fprintf(query, " WHERE t.owner NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'RDSADMIN', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS', 'GGS_ADMIN', 'GSMADMIN_INTERNAL')")
	fmt.Fprintf(query, " AND t.table_name NOT IN ('DBTOOLS$EXECUTION_HISTORY')")
	fmt.Fprintf(query, " ORDER BY t.table_name, c.position")

	rows, err := db.QueryContext(ctx, query.String())
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query %q: %w", query.String(), err)
	}
	defer rows.Close()

	var keysByTable = make(map[string]*discoveredPrimaryKey)
	for rows.Next() {
		var tableOwner, tableName, columnName string
		var ordinalPosition int

		if err := rows.Scan(&tableOwner, &tableName, &columnName, &ordinalPosition); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		var tableID = tableOwner + "." + tableName
		var keyInfo = keysByTable[tableID]
		if keyInfo == nil {
			keyInfo = &discoveredPrimaryKey{Owner: tableOwner, Table: tableName}
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

var catalogNameSanitizerRe = regexp.MustCompile(`(?i)[^a-z0-9\-_.]`)

func recommendedCatalogName(schema, table string) string {
	schema = catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(schema), "_")
	table = catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(table), "_")
	return schema + "/" + table
}

// recommendedResourceName implements the old name-recommendation logic so that the name
// field of discovered bindings remains unchanged even though the catalog name is different.
func recommendedResourceName(schema, table string) string {
	var catalogName = schema + "_" + table
	return catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(catalogName), "_")
}
