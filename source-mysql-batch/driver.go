package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// BatchSQLDriver represents a generic "batch SQL" capture behavior, parameterized
// by a config schema, connect function, and value translation logic.
type BatchSQLDriver struct {
	DocumentationURL string
	ConfigSchema     json.RawMessage

	Connect               func(ctx context.Context, configJSON json.RawMessage) (*sql.DB, error)
	TranslateValue        func(val any, databaseTypeName string) (any, error)
	GenerateResource      func(resourceName, schemaName, tableName, tableType string) (*Resource, error)
	ExcludedSystemSchemas []string
}

// Resource represents the capture configuration of a single resource binding.
type Resource struct {
	Name         string   `json:"name" jsonschema:"title=Name,description=The unique name of this resource."`
	Template     string   `json:"template" jsonschema:"title=Query Template,description=The query template (pkg.go.dev/text/template) which will be rendered and then executed." jsonschema_extras:"multiline=true"`
	Cursor       []string `json:"cursor" jsonschema:"title=Cursor Columns,description=The names of columns which should be persisted between query executions as a cursor."`
	PollInterval string   `json:"poll,omitempty" jsonschema:"title=Poll Interval,description=How often to execute the fetch query. Defaults to 5 minutes if unset."`
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
	if _, err := template.New("query").Parse(r.Template); err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}
	if r.PollInterval != "" {
		if _, err := time.ParseDuration(r.PollInterval); err != nil {
			return fmt.Errorf("invalid poll interval %q: %w", r.PollInterval, err)
		}
	}
	return nil
}

// documentMetadata contains the source metadata located at /_meta
type documentMetadata struct {
	Polled time.Time `json:"polled" jsonschema:"title=Polled Timestamp,description=The time at which the update query which produced this document as executed."`
	Index  int       `json:"index" jsonschema:"title=Result Index,description=The index of this document within the query execution which produced it."`
}

// The fallback collection key just refers to the polling iteration and result index of each document.
var fallbackKey = []string{"/_meta/polled", "/_meta/index"}

func generateCollectionSchema(keyColumns []string, columnTypes map[string]*jsonschema.Schema) (json.RawMessage, error) {
	// Generate schema for the metadata via reflection
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var metadataSchema = reflector.ReflectFromType(reflect.TypeOf(documentMetadata{}))
	metadataSchema.Definitions = nil
	metadataSchema.AdditionalProperties = nil

	var required = []string{"_meta"}
	var properties = map[string]*jsonschema.Schema{
		"_meta": metadataSchema,
	}

	for _, colName := range keyColumns {
		var columnType = columnTypes[colName]
		if columnType == nil {
			return nil, fmt.Errorf("unable to add key column %q to schema: type unknown", colName)
		}
		properties[colName] = columnType
		required = append(required, colName)
	}

	var schema = &jsonschema.Schema{
		Type:                 "object",
		Required:             required,
		AdditionalProperties: nil,
		Extras: map[string]interface{}{
			"properties":     properties,
			"x-infer-schema": true,
		},
	}

	// Marshal schema to JSON
	bs, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("error serializing schema: %w", err)
	}
	return json.RawMessage(bs), nil
}

var minimalSchema = func() json.RawMessage {
	var schema, err = generateCollectionSchema(nil, nil)
	if err != nil {
		panic(err)
	}
	return schema
}()

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
	}, nil
}

// Apply does nothing for batch SQL captures.
func (BatchSQLDriver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

// Discover enumerates tables and views from `information_schema.tables` and generates
// placeholder capture queries for thos tables.
func (drv *BatchSQLDriver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var db, err = drv.Connect(ctx, req.ConfigJson)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tables, err := discoverTables(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	keys, err := discoverPrimaryKeys(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("error listing primary keys: %w", err)
	}

	// We rebuild this map even though `discoverPrimaryKeys` already built and
	// discarded it, in order to keep the `discoverTables` / `discoverPrimaryKeys`
	// interface as stupidly simple as possible, which ought to make it just that
	// little bit easier to do this generically for multiple databases.
	var keysByTable = make(map[string]*discoveredPrimaryKey)
	for _, key := range keys {
		keysByTable[key.Schema+"."+key.Table] = key
	}

	var bindings []*pc.Response_Discovered_Binding
	for _, table := range tables {
		// Exclude tables in "system schemas" such as information_schema or pg_catalog.
		if slices.Contains(drv.ExcludedSystemSchemas, table.Schema) {
			continue
		}

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

		// Try to generate a useful collection schema, but on error fall back to the
		// minimal schema with the default key [/_meta/polled, /_meta/index].
		var collectionSchema = minimalSchema
		var collectionKey = fallbackKey
		if tableKey, ok := keysByTable[tableID]; ok {
			if generatedSchema, err := generateCollectionSchema(tableKey.Columns, tableKey.ColumnTypes); err == nil {
				collectionSchema = generatedSchema
				collectionKey = nil
				for _, colName := range tableKey.Columns {
					collectionKey = append(collectionKey, primaryKeyToCollectionKey(colName))
				}
			} else {
				log.WithFields(log.Fields{"table": tableID, "err": err}).Warn("unable to generate collection schema")
			}
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    recommendedName,
			ResourceConfigJson: resourceConfigJSON,
			DocumentSchemaJson: collectionSchema,
			Key:                collectionKey,
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

const queryDiscoverTables = `SELECT table_schema, table_name, table_type FROM information_schema.tables;`

type discoveredTable struct {
	Schema string
	Name   string
	Type   string // Usually 'BASE TABLE' or 'VIEW'
}

func discoverTables(ctx context.Context, db *sql.DB) ([]*discoveredTable, error) {
	rows, err := db.QueryContext(ctx, queryDiscoverTables)
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query: %w", err)
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

// Joining on the 6-tuple {CONSTRAINT,TABLE}_{CATALOG,SCHEMA,NAME} is probably
// overkill but shouldn't hurt, and helps to make absolutely sure that we're
// matching up the constraint type with the column names/positions correctly.
const queryDiscoverPrimaryKeys = `
SELECT kcu.table_schema, kcu.table_name, kcu.column_name, kcu.ordinal_position, col.data_type
  FROM information_schema.key_column_usage kcu
  JOIN information_schema.table_constraints tcs
    ON  tcs.constraint_catalog = kcu.constraint_catalog
    AND tcs.constraint_schema = kcu.constraint_schema
    AND tcs.constraint_name = kcu.constraint_name
    AND tcs.table_schema = kcu.table_schema
    AND tcs.table_name = kcu.table_name
  JOIN information_schema.columns col
    ON  col.table_schema = kcu.table_schema
	AND col.table_name = kcu.table_name
	AND col.column_name = kcu.column_name
  WHERE tcs.constraint_type = 'PRIMARY KEY'
  ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position;
`

type discoveredPrimaryKey struct {
	Schema      string
	Table       string
	Columns     []string
	ColumnTypes map[string]*jsonschema.Schema
}

func discoverPrimaryKeys(ctx context.Context, db *sql.DB) ([]*discoveredPrimaryKey, error) {
	rows, err := db.QueryContext(ctx, queryDiscoverPrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query: %w", err)
	}
	defer rows.Close()

	var keysByTable = make(map[string]*discoveredPrimaryKey)
	for rows.Next() {
		var tableSchema, tableName, columnName, dataType string
		var ordinalPosition int
		if err := rows.Scan(&tableSchema, &tableName, &columnName, &ordinalPosition, &dataType); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		var tableID = tableSchema + "." + tableName
		var keyInfo = keysByTable[tableID]
		if keyInfo == nil {
			keyInfo = &discoveredPrimaryKey{
				Schema:      tableSchema,
				Table:       tableName,
				ColumnTypes: make(map[string]*jsonschema.Schema),
			}
			keysByTable[tableID] = keyInfo
		}
		keyInfo.Columns = append(keyInfo.Columns, columnName)
		if jsonSchema, ok := databaseTypeToJSON[dataType]; ok {
			keyInfo.ColumnTypes[columnName] = jsonSchema
		} else {
			// Assume unknown types are strings, because it's almost always a string.
			// (Or it's an object, but those can't be Flow collection keys anyway)
			keyInfo.ColumnTypes[columnName] = &jsonschema.Schema{Type: "string"}
		}
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

var databaseTypeToJSON = map[string]*jsonschema.Schema{
	"int":      {Type: "integer"},
	"bigint":   {Type: "integer"},
	"smallint": {Type: "integer"},
	"tinyint":  {Type: "integer"},
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
	// Perform discovery, which inherently validates that the config is well-formed
	// and that we can connect to the database and execute (some) queries.
	if _, err := drv.Discover(ctx, &pc.Request_Discover{
		ConnectorType: req.ConnectorType,
		ConfigJson:    req.ConfigJson,
	}); err != nil {
		return nil, err
	}

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
	var db, err = drv.Connect(stream.Context(), open.Capture.ConfigJson)
	if err != nil {
		return err
	}
	defer db.Close()

	var resources []Resource
	for _, binding := range open.Capture.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		resources = append(resources, res)
	}

	if open.StateJson != nil {
		// TODO: Consider implementing persistent state, keyed on the resource name property.
	}

	var capture = &capture{
		DB:             db,
		Resources:      resources,
		Output:         stream,
		TranslateValue: drv.TranslateValue,
	}
	return capture.Run(stream.Context())
}

type capture struct {
	DB             *sql.DB
	Resources      []Resource
	Output         *boilerplate.PullOutput
	TranslateValue func(val any, databaseTypeName string) (any, error)
}

func (c *capture) Run(ctx context.Context) error {
	// Notify Flow that we're starting.
	if err := c.Output.Ready(false); err != nil {
		return err
	}

	var eg, workerCtx = errgroup.WithContext(ctx)
	for idx, res := range c.Resources {
		if idx > 0 {
			// Slightly stagger worker thread startup. Five seconds should be long
			// enough for most fast queries to complete their first execution, and
			// the hope is this reduces peak load on both the database and us.
			time.Sleep(5 * time.Second)
		}
		var idx, res = idx, res // Copy for goroutine closure
		eg.Go(func() error { return c.worker(workerCtx, idx, &res) })
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("capture terminated with error: %w", err)
	}
	return nil
}

func (c *capture) worker(ctx context.Context, bindingIndex int, res *Resource) error {
	log.WithFields(log.Fields{
		"name":   res.Name,
		"tmpl":   res.Template,
		"cursor": res.Cursor,
		"poll":   res.PollInterval,
	}).Info("starting worker")

	var queryTemplate, err = template.New("query").Parse(res.Template)
	if err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}

	var pollInterval = 5 * time.Minute
	if res.PollInterval != "" {
		var dt, err = time.ParseDuration(res.PollInterval)
		if err != nil {
			return fmt.Errorf("invalid poll interval %q: %w", res.PollInterval, err)
		}
		pollInterval = dt
	}

	var cursorValues []any
	for ctx.Err() == nil {
		log.WithField("name", res.Name).Info("polling for updates")
		var nextCursor, err = c.poll(ctx, bindingIndex, queryTemplate, res.Cursor, cursorValues)
		if err != nil {
			return fmt.Errorf("error polling table: %w", err)
		}
		cursorValues = nextCursor
		time.Sleep(pollInterval)
	}
	return ctx.Err()
}

var queryPlaceholderRegexp = regexp.MustCompile(`([?]|:[0-9]+)`)

// expandQueryPlaceholders simulates numbered query placeholders in MySQL by replacing
// each numbered placeholder `:N` with a question mark while replacing the input list
// of argument values into an output list corresponding to the usage sequence.
func expandQueryPlaceholders(query string, argvals []any) (string, []any) {
	var argseq []any
	var prevIndex int
	query = queryPlaceholderRegexp.ReplaceAllStringFunc(query, func(param string) string {
		if param == "?" {
			prevIndex++
			argseq = append(argseq, argvals[prevIndex])
			return "?"
		}

		var paramIndex, err = strconv.ParseInt(strings.TrimPrefix(param, ":"), 10, 64)
		if err != nil {
			panic(fmt.Errorf("internal error: failed to parse parameter name %q", param))
		}
		argseq = append(argseq, argvals[paramIndex])
		prevIndex = int(paramIndex)
		return "?"
	})
	return query, argseq
}

func quoteColumnName(name string) string {
	// Per https://dev.mysql.com/doc/refman/8.0/en/identifiers.html, the identifier quote character
	// is the backtick (`). If the identifier itself contains a backtick, it must be doubled.
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (c *capture) poll(ctx context.Context, bindingIndex int, tmpl *template.Template, cursorNames []string, cursorValues []any) ([]any, error) {
	var quotedCursorNames []string
	for _, cursorName := range cursorNames {
		quotedCursorNames = append(quotedCursorNames, quoteColumnName(cursorName))
	}
	var templateArg = map[string]any{
		"IsFirstQuery": len(cursorValues) == 0,
		"CursorFields": quotedCursorNames,
	}

	var queryBuf = new(strings.Builder)
	if err := tmpl.Execute(queryBuf, templateArg); err != nil {
		return nil, fmt.Errorf("error generating query: %w", err)
	}
	var query, args = expandQueryPlaceholders(queryBuf.String(), cursorValues)

	var pollTime = time.Now().UTC()

	log.WithFields(log.Fields{"query": query, "args": args}).Info("executing query")
	var rows, err = c.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error processing query result: %w", err)
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("error processing query result: %w", err)
	}
	for i, columnType := range columnTypes {
		log.WithFields(log.Fields{
			"idx":          i,
			"name":         columnType.DatabaseTypeName(),
			"scanTypeName": columnType.ScanType().Name(),
		}).Debug("column type")
	}

	var resultRow = make(map[string]any)
	var translatedRow = make(map[string]any)
	var columnValues = make([]any, len(columnNames))
	var columnPointers = make([]any, len(columnValues))
	for i := range columnPointers {
		columnPointers[i] = &columnValues[i]
	}

	var count int
	for rows.Next() {
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		for i, name := range columnNames {
			var translatedVal, err = c.TranslateValue(columnValues[i], columnTypes[i].DatabaseTypeName())
			if err != nil {
				return nil, fmt.Errorf("error translating column value: %w", err)
			}
			translatedRow[name] = translatedVal
		}

		translatedRow["_meta"] = &documentMetadata{
			Polled: pollTime,
			Index:  count,
		}

		var bs, err = json.Marshal(translatedRow)
		if err != nil {
			return nil, fmt.Errorf("error serializing document: %w", err)
		} else if err := c.Output.Documents(bindingIndex, bs); err != nil {
			return nil, fmt.Errorf("error emitting document: %w", err)
		} else if err := c.Output.Checkpoint(json.RawMessage(`{}`), true); err != nil {
			return nil, fmt.Errorf("error emitting checkpoint: %w", err)
		}

		if len(cursorValues) != len(cursorNames) {
			// Allocate a new values list if needed. This is done inside of the loop, so
			// an empty result set will be a no-op even when the previous state is nil.
			cursorValues = make([]any, len(cursorNames))
		}
		for i, name := range columnNames {
			resultRow[name] = columnValues[i]
		}
		for i, name := range cursorNames {
			cursorValues[i] = resultRow[name]
		}

		count++
	}
	log.WithFields(log.Fields{
		"query": query,
		"count": count,
	}).Info("query complete")
	return cursorValues, nil
}
