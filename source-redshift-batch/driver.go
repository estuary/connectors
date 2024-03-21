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
	"text/template"
	"time"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	// When processing large tables we need to emit checkpoints in between query result
	// rows, since we don't want a single Flow transaction to have contain millions of
	// documents. But emitting a checkpoint after every row could be a significant drag
	// on overall throughput, and isn't really needed anyway. So instead we emit one for
	// every N rows, plus one when the query results are fully processed.
	documentsPerCheckpoint = 1000
)

// BatchSQLDriver represents a generic "batch SQL" capture behavior, parameterized
// by a config schema, connect function, and value translation logic.
type BatchSQLDriver struct {
	DocumentationURL string
	ConfigSchema     json.RawMessage

	Connect          func(ctx context.Context, cfg *Config) (*sql.DB, error)
	TranslateValue   func(val any, databaseTypeName string) (any, error)
	GenerateResource func(resourceName, schemaName, tableName, tableType string) (*Resource, error)
}

// Resource represents the capture configuration of a single resource binding.
type Resource struct {
	Name         string   `json:"name" jsonschema:"title=Name,description=The unique name of this resource." jsonschema_extras:"order=0"`
	Template     string   `json:"template" jsonschema:"title=Query Template,description=The query template (pkg.go.dev/text/template) which will be rendered and then executed." jsonschema_extras:"multiline=true,order=3"`
	Cursor       []string `json:"cursor" jsonschema:"title=Cursor Columns,description=The names of columns which should be persisted between query executions as a cursor." jsonschema_extras:"order=2"`
	PollSchedule string   `json:"poll,omitempty" jsonschema:"title=Polling Schedule,description=When and how often to execute the fetch query (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day." jsonschema_extras:"order=1,pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
}

var templateFuncs = template.FuncMap{
	"add": func(a, b int) int { return a + b },
}

// Validate checks that the resource spec possesses all required properties.
func (r Resource) Validate() error {
	var requiredProperties = [][]string{
		{"name", r.Name},
		{"template", r.Template},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	if _, err := template.New("query").Funcs(templateFuncs).Parse(r.Template); err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}
	if slices.Contains(r.Cursor, "") {
		return fmt.Errorf("cursor column names can't be empty (got %q)", r.Cursor)
	}
	if r.PollSchedule != "" {
		if err := schedule.Validate(r.PollSchedule); err != nil {
			return fmt.Errorf("invalid polling schedule %q: %w", r.PollSchedule, err)
		}
	}
	return nil
}

// documentMetadata contains the source metadata located at /_meta
type documentMetadata struct {
	Polled time.Time `json:"polled" jsonschema:"title=Polled Timestamp,description=The time at which the update query which produced this document as executed."`
	Index  int       `json:"index" jsonschema:"title=Result Index,description=The index of this document within the query execution which produced it."`
}

func generateCollectionSchema(keyColumns []string, columnTypes map[string]columnType) (json.RawMessage, error) {
	// Generate schema for the metadata via reflection
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var metadataSchema = reflector.ReflectFromType(reflect.TypeOf(documentMetadata{}))
	metadataSchema.Definitions = nil
	metadataSchema.AdditionalProperties = nil

	var required = append([]string{"_meta"}, keyColumns...)
	var properties = map[string]*jsonschema.Schema{
		"_meta": metadataSchema,
	}
	for colName, colType := range columnTypes {
		properties[colName] = colType.JSONSchema()
	}

	var schema = &jsonschema.Schema{
		Type:                 "object",
		Required:             required,
		AdditionalProperties: nil,
		Extras: map[string]interface{}{
			"properties": properties,
		},
	}

	// Marshal schema to JSON
	bs, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("error serializing schema: %w", err)
	}
	return json.RawMessage(bs), nil
}

// Spec returns metadata about the capture connector.
func (drv *BatchSQLDriver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	resourceSchema, err := schemagen.GenerateSchema("Batch SQL Resource Spec", &Resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         drv.ConfigSchema,
		ResourceConfigSchemaJson: resourceSchema,
		DocumentationUrl:         drv.DocumentationURL,
		ResourcePathPointers:     []string{"/name"},
	}, nil
}

// Apply does nothing for batch SQL captures.
func (BatchSQLDriver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

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

	// Run discovery queries
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

	// Aggregate column information by table
	var columnsByTable = make(map[string][]*discoveredColumn)
	for _, column := range columns {
		var tableID = column.Schema + "." + column.Table
		columnsByTable[tableID] = append(columnsByTable[tableID], column)
		if column.Index != len(columnsByTable[tableID]) {
			return nil, fmt.Errorf("internal error: column %q of table %q appears out of order", column.Name, tableID)
		}
	}

	// Aggregate primary-key information by table
	var keysByTable = make(map[string][]*discoveredPrimaryKey)
	for _, key := range keys {
		var tableID = key.Schema + "." + key.Table
		keysByTable[tableID] = append(keysByTable[tableID], key)
		if key.Index != len(keysByTable[tableID]) {
			return nil, fmt.Errorf("internal error: primary key column %q of table %q appears out of order", key.Column, tableID)
		}
	}

	// Generate discovery resource and collection schema for this table
	var bindings []*pc.Response_Discovered_Binding
	for _, table := range tables {
		var tableID = table.Schema + "." + table.Name

		var recommendedName = recommendedCatalogName(table.Schema, table.Name)
		var res, err = drv.GenerateResource(recommendedName, table.Schema, table.Name, table.Type)
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
		var keyColumns []string
		for _, key := range keysByTable[tableID] {
			keyColumns = append(keyColumns, key.Column)
		}

		var columnTypes = make(map[string]columnType)
		for _, column := range columnsByTable[tableID] {
			columnTypes[column.Name] = column.DataType
		}

		generatedSchema, err := generateCollectionSchema(keyColumns, columnTypes)
		if err != nil {
			log.WithFields(log.Fields{"table": tableID, "err": err}).Warn("unable to generate collection schema")
			continue
		}

		var collectionKey []string
		for _, colName := range keyColumns {
			collectionKey = append(collectionKey, primaryKeyToCollectionKey(colName))
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    recommendedName,
			ResourceConfigJson: resourceConfigJSON,
			DocumentSchemaJson: generatedSchema,
			Key:                collectionKey,
			ResourcePath:       []string{res.Name},
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
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
	jsonType        string
	contentEncoding string
	format          string
	nullable        bool
	description     string
}

func (ct *basicColumnType) JSONSchema() *jsonschema.Schema {
	var sch = &jsonschema.Schema{
		Format: ct.format,
		Extras: make(map[string]interface{}),
	}

	if ct.contentEncoding != "" {
		sch.Extras["contentEncoding"] = ct.contentEncoding // New in 2019-09.
	}

	if ct.jsonType == "" {
		// No type constraint.
	} else if !ct.nullable {
		sch.Type = ct.jsonType
	} else {
		sch.Extras["type"] = []string{ct.jsonType, "null"}
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
	fmt.Fprintf(query, "       t.typtype::text AS typtype")
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
		if err := rows.Scan(&tableSchema, &tableName, &columnName, &columnIndex, &isNullable, &typeName, &typeType); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		// Decode column type information into a usable form
		var dataType columnType
		switch typeType {
		case "e": // enum values are captured as strings
			dataType = &basicColumnType{jsonType: "string"}
		case "r", "m": // ranges and multiranges are captured as strings
			dataType = &basicColumnType{jsonType: "string"}
		default:
			var ok bool
			dataType, ok = databaseTypeToJSON[typeName]
			if !ok {
				dataType = &basicColumnType{description: fmt.Sprintf("using catch-all schema for unknown type %q", typeName)}
			}
		}

		columns = append(columns, &discoveredColumn{
			Schema:     tableSchema,
			Table:      tableName,
			Name:       columnName,
			Index:      columnIndex,
			IsNullable: isNullable,
			DataType:   dataType,
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

var databaseTypeToJSON = map[string]columnType{
	"bool": &basicColumnType{jsonType: "boolean"},

	"int2": &basicColumnType{jsonType: "integer"},
	"int4": &basicColumnType{jsonType: "integer"},
	"int8": &basicColumnType{jsonType: "integer"},

	"numeric": &basicColumnType{jsonType: "string", format: "number"},
	"float4":  &basicColumnType{jsonType: "number"},
	"float8":  &basicColumnType{jsonType: "number"},

	"varchar": &basicColumnType{jsonType: "string"},
	"bpchar":  &basicColumnType{jsonType: "string"},
	"text":    &basicColumnType{jsonType: "string"},
	"bytea":   &basicColumnType{jsonType: "string", contentEncoding: "base64"},
	"xml":     &basicColumnType{jsonType: "string"},
	"bit":     &basicColumnType{jsonType: "string"},
	"varbit":  &basicColumnType{jsonType: "string"},

	"json":     &basicColumnType{},
	"jsonb":    &basicColumnType{},
	"jsonpath": &basicColumnType{jsonType: "string"},

	// Domain-Specific Types
	"date":        &basicColumnType{jsonType: "string", format: "date-time"},
	"timestamp":   &basicColumnType{jsonType: "string", format: "date-time"},
	"timestamptz": &basicColumnType{jsonType: "string", format: "date-time"},
	"time":        &basicColumnType{jsonType: "integer"},
	"timetz":      &basicColumnType{jsonType: "string", format: "time"},
	"interval":    &basicColumnType{jsonType: "string"},
	"money":       &basicColumnType{jsonType: "string"},
	"point":       &basicColumnType{jsonType: "string"},
	"line":        &basicColumnType{jsonType: "string"},
	"lseg":        &basicColumnType{jsonType: "string"},
	"box":         &basicColumnType{jsonType: "string"},
	"path":        &basicColumnType{jsonType: "string"},
	"polygon":     &basicColumnType{jsonType: "string"},
	"circle":      &basicColumnType{jsonType: "string"},
	"inet":        &basicColumnType{jsonType: "string"},
	"cidr":        &basicColumnType{jsonType: "string"},
	"macaddr":     &basicColumnType{jsonType: "string"},
	"macaddr8":    &basicColumnType{jsonType: "string"},
	"tsvector":    &basicColumnType{jsonType: "string"},
	"tsquery":     &basicColumnType{jsonType: "string"},
	"uuid":        &basicColumnType{jsonType: "string", format: "uuid"},
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

// Validate checks that the configuration appears correct and that we can connect
// to the database and execute queries.
func (drv *BatchSQLDriver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	// Unmarshal the configuration and verify that we can connect to the database
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

	// Unmarshal and validate resource bindings to make sure they're well-formed too.
	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Name},
		})
	}
	return &pc.Response_Validated{Bindings: out}, nil
}

// Pull is the heart of a capture connector and outputs a neverending stream of documents.
func (drv *BatchSQLDriver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg Config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	var db, err = drv.Connect(stream.Context(), &cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	var bindings []bindingInfo
	for idx, binding := range open.Capture.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		bindings = append(bindings, bindingInfo{
			resource: res,
			index:    idx,
			stateKey: boilerplate.StateKey(binding.StateKey),
		})
	}

	var state captureState
	if open.StateJson != nil {
		if err := pf.UnmarshalStrict(open.StateJson, &state); err != nil {
			return fmt.Errorf("parsing state checkpoint: %w", err)
		}
	}

	migrated, err := migrateState(&state, open.Capture.Bindings)
	if err != nil {
		return fmt.Errorf("migrating binding states: %w", err)
	}

	state, err = updateResourceStates(state, bindings)
	if err != nil {
		return fmt.Errorf("error initializing resource states: %w", err)
	}

	if err := stream.Ready(false); err != nil {
		return err
	}

	if migrated {
		if cp, err := json.Marshal(state); err != nil {
			return fmt.Errorf("error serializing checkpoint: %w", err)
		} else if err := stream.Checkpoint(cp, false); err != nil {
			return fmt.Errorf("updating migrated checkpoint: %w", err)
		}
	}

	var capture = &capture{
		Config:         &cfg,
		State:          &state,
		DB:             db,
		Bindings:       bindings,
		Output:         stream,
		TranslateValue: drv.TranslateValue,
	}
	return capture.Run(stream.Context())
}

func updateResourceStates(prevState captureState, bindings []bindingInfo) (captureState, error) {
	var newState = captureState{
		Streams: make(map[boilerplate.StateKey]*streamState),
	}
	for _, binding := range bindings {
		var sk = binding.stateKey
		var res = binding.resource
		var stream = prevState.Streams[sk]
		if stream != nil && !slices.Equal(stream.CursorNames, res.Cursor) {
			log.WithFields(log.Fields{
				"name": res.Name,
				"prev": stream.CursorNames,
				"next": res.Cursor,
			}).Warn("cursor columns changed, resetting stream state")
			stream = nil
		}
		if stream == nil {
			stream = &streamState{CursorNames: res.Cursor}
		}
		newState.Streams[sk] = stream
	}
	return newState, nil
}

type capture struct {
	Config         *Config
	State          *captureState
	DB             *sql.DB
	Bindings       []bindingInfo
	Output         *boilerplate.PullOutput
	TranslateValue func(val any, databaseTypeName string) (any, error)
}

type bindingInfo struct {
	resource Resource
	index    int
	stateKey boilerplate.StateKey
}

type captureState struct {
	Streams    map[boilerplate.StateKey]*streamState `json:"bindingStateV1,omitempty"`
	OldStreams map[string]*streamState               `json:"Streams,omitempty"` // TODO(whb): Remove once all captures have migrated.
}

func migrateState(state *captureState, bindings []*pf.CaptureSpec_Binding) (bool, error) {
	if state.Streams != nil && state.OldStreams != nil {
		return false, fmt.Errorf("application error: both Streams and OldStreams were non-nil")
	} else if state.Streams != nil {
		log.Info("skipping state migration since it's already done")
		return false, nil
	}

	state.Streams = make(map[boilerplate.StateKey]*streamState)

	for _, b := range bindings {
		if b.StateKey == "" {
			return false, fmt.Errorf("state key was empty for binding %s", b.ResourcePath)
		}

		var res Resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return false, fmt.Errorf("parsing resource config: %w", err)
		}

		ll := log.WithFields(log.Fields{
			"stateKey": b.StateKey,
			"name":     res.Name,
		})

		stateFromOldStreams, ok := state.OldStreams[res.Name]
		if !ok {
			// This may happen if the connector has never emitted any checkpoints.
			ll.Warn("no state found for binding while migrating state")
			continue
		}

		state.Streams[boilerplate.StateKey(b.StateKey)] = stateFromOldStreams
		ll.Info("migrated binding state")
	}

	state.OldStreams = nil

	return true, nil
}

type streamState struct {
	CursorNames  []string
	CursorValues []any
	LastPolled   time.Time
}

func (s *captureState) Validate() error {
	return nil
}

func (c *capture) Run(ctx context.Context) error {
	var eg, workerCtx = errgroup.WithContext(ctx)
	for idx, binding := range c.Bindings {
		if idx > 0 {
			// Slightly stagger worker thread startup. Five seconds should be long
			// enough for most fast queries to complete their first execution, and
			// the hope is this reduces peak load on both the database and us.
			time.Sleep(5 * time.Second)
		}
		var binding = binding // Copy for goroutine closure
		eg.Go(func() error { return c.worker(workerCtx, &binding) })
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("capture terminated with error: %w", err)
	}
	return nil
}

func (c *capture) worker(ctx context.Context, binding *bindingInfo) error {
	var res = binding.resource
	log.WithFields(log.Fields{
		"name":   res.Name,
		"tmpl":   res.Template,
		"cursor": res.Cursor,
		"poll":   res.PollSchedule,
	}).Info("starting worker")

	var queryTemplate, err = template.New("query").Funcs(templateFuncs).Parse(res.Template)
	if err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}

	for ctx.Err() == nil {
		if err := c.poll(ctx, binding, queryTemplate); err != nil {
			return fmt.Errorf("error polling binding %q: %w", res.Name, err)
		}
	}
	return ctx.Err()
}

func (c *capture) poll(ctx context.Context, binding *bindingInfo, tmpl *template.Template) error {
	var res = binding.resource
	var stateKey = binding.stateKey
	var state, ok = c.State.Streams[stateKey]
	if !ok {
		return fmt.Errorf("internal error: no state for stream %q", res.Name)
	}
	var cursorNames = state.CursorNames
	var cursorValues = state.CursorValues

	var quotedCursorNames []string
	for _, cursorName := range cursorNames {
		quotedCursorNames = append(quotedCursorNames, quoteColumnName(cursorName))
	}

	var templateArg = map[string]any{
		"IsFirstQuery": len(cursorValues) == 0,
		"CursorFields": quotedCursorNames,
	}

	// Polling schedule can be configured per binding. If unset, falls back to the
	// connector global polling schedule.
	var pollScheduleStr = c.Config.Advanced.PollSchedule
	if res.PollSchedule != "" {
		pollScheduleStr = res.PollSchedule
	}
	var pollSchedule, err = schedule.Parse(pollScheduleStr)
	if err != nil {
		return fmt.Errorf("failed to parse polling schedule %q: %w", pollScheduleStr, err)
	}
	log.WithFields(log.Fields{
		"name": res.Name,
		"poll": pollScheduleStr,
	}).Info("waiting for next scheduled poll")
	if err := schedule.WaitForNext(ctx, pollSchedule, state.LastPolled); err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"name": res.Name,
		"poll": pollScheduleStr,
		"prev": state.LastPolled.Format(time.RFC3339Nano),
	}).Info("ready to poll")

	var queryBuf = new(strings.Builder)
	if err := tmpl.Execute(queryBuf, templateArg); err != nil {
		return fmt.Errorf("error generating query: %w", err)
	}
	var query = queryBuf.String()

	log.WithFields(log.Fields{
		"query": query,
		"args":  cursorValues,
	}).Info("executing query")
	var pollTime = time.Now().UTC()
	state.LastPolled = pollTime

	rows, err := c.DB.QueryContext(ctx, query, cursorValues...)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error processing query result: %w", err)
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("error processing query result: %w", err)
	}
	for i, columnType := range columnTypes {
		log.WithFields(log.Fields{
			"idx":          i,
			"name":         columnType.DatabaseTypeName(),
			"scanTypeName": columnType.ScanType().Name(),
		}).Debug("column type")
	}

	var columnIndices = make(map[string]int)
	for idx, name := range columnNames {
		columnIndices[name] = idx
	}
	var cursorIndices []int
	for _, cursorName := range cursorNames {
		cursorIndices = append(cursorIndices, columnIndices[cursorName])
	}

	var columnValues = make([]any, len(columnNames))
	var columnPointers = make([]any, len(columnValues))
	for i := range columnPointers {
		columnPointers[i] = &columnValues[i]
	}

	var shape = encrow.NewShape(append(columnNames, "_meta"))
	var rowValues = make([]any, len(columnNames)+1)
	var serializedDocument []byte

	var count int
	for rows.Next() {
		if err := rows.Scan(columnPointers...); err != nil {
			return fmt.Errorf("error scanning result row: %w", err)
		}

		for idx, val := range columnValues {
			var translatedVal, err = c.TranslateValue(val, columnTypes[idx].DatabaseTypeName())
			if err != nil {
				return fmt.Errorf("error translating column %q value: %w", columnNames[idx], err)
			}
			rowValues[idx] = translatedVal
		}
		rowValues[len(rowValues)-1] = &documentMetadata{
			Polled: pollTime,
			Index:  count,
		}

		serializedDocument, err = shape.Encode(serializedDocument, rowValues)
		if err != nil {
			return fmt.Errorf("error serializing document: %w", err)
		} else if err := c.Output.Documents(binding.index, serializedDocument); err != nil {
			return fmt.Errorf("error emitting document: %w", err)
		}

		if len(cursorValues) != len(cursorNames) {
			// Allocate a new values list if needed. This is done inside of the loop, so
			// an empty result set will be a no-op even when the previous state is nil.
			cursorValues = make([]any, len(cursorNames))
		}
		for i, j := range cursorIndices {
			cursorValues[i] = rowValues[j]
		}
		state.CursorValues = cursorValues

		count++
		if count%documentsPerCheckpoint == 0 {
			if err := c.streamStateCheckpoint(stateKey, state); err != nil {
				return err
			}
		}
	}

	if count%documentsPerCheckpoint != 0 {
		if err := c.streamStateCheckpoint(stateKey, state); err != nil {
			return err
		}
	}

	log.WithFields(log.Fields{
		"query": query,
		"count": count,
	}).Info("query complete")
	return nil
}

func (c *capture) streamStateCheckpoint(sk boilerplate.StateKey, state *streamState) error {
	var checkpointPatch = captureState{Streams: make(map[boilerplate.StateKey]*streamState)}
	checkpointPatch.Streams[sk] = state

	if checkpointJSON, err := json.Marshal(checkpointPatch); err != nil {
		return fmt.Errorf("error serializing state checkpoint: %w", err)
	} else if err := c.Output.Checkpoint(checkpointJSON, true); err != nil {
		return fmt.Errorf("error emitting checkpoint: %w", err)
	}
	return nil
}

func quoteColumnName(name string) string {
	// From https://www.postgresql.org/docs/14/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS:
	//
	//     Quoted identifiers can contain any character, except the character with code zero.
	//     (To include a double quote, write two double quotes.)
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
