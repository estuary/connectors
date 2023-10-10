package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/estuary/connectors/go/encrow"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// BatchSQLDriver represents a generic "batch SQL" capture behavior, parameterized
// by a config schema, connect function, and value translation logic.
type BatchSQLDriver struct {
	DocumentationURL string
	ConfigSchema     json.RawMessage

	Connect               func(ctx context.Context, cfg *Config) (*client.Conn, error)
	GenerateResource      func(resourceName, schemaName, tableName, tableType string) (*Resource, error)
	ExcludedSystemSchemas []string
}

// Resource represents the capture configuration of a single resource binding.
type Resource struct {
	Name         string   `json:"name" jsonschema:"title=Name,description=The unique name of this resource." jsonschema_extras:"order=0"`
	Template     string   `json:"template" jsonschema:"title=Query Template,description=The query template (pkg.go.dev/text/template) which will be rendered and then executed." jsonschema_extras:"multiline=true,order=3"`
	Cursor       []string `json:"cursor,omitempty" jsonschema:"title=Cursor Columns,description=The names of columns which should be persisted between query executions as a cursor." jsonschema_extras:"order=2"`
	PollInterval string   `json:"poll,omitempty" jsonschema:"title=Poll Interval,description=How often to execute the fetch query (overrides the connector default setting)" jsonschema_extras:"order=1"`
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

func discoverTables(ctx context.Context, db *client.Conn) ([]*discoveredTable, error) {
	var results, err = db.Execute(queryDiscoverTables)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %w", err)
	}
	defer results.Close()

	var tables []*discoveredTable
	for _, row := range results.Values {
		tables = append(tables, &discoveredTable{
			Schema: string(row[0].AsString()),
			Name:   string(row[1].AsString()),
			Type:   string(row[2].AsString()),
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
		var dataType = string(row[4].AsString())

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
		Config:    &cfg,
		DB:        db,
		Resources: resources,
		Output:    stream,
	}
	return capture.Run(stream.Context())
}

type capture struct {
	Config    *Config
	DB        *client.Conn
	Resources []Resource
	Output    *boilerplate.PullOutput
	Mutex     sync.Mutex
}

func (c *capture) Run(ctx context.Context) error {
	// Notify Flow that we're starting.
	if err := c.Output.Ready(false); err != nil {
		return err
	}

	var eg, workerCtx = errgroup.WithContext(ctx)
	for idx, res := range c.Resources {
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

	// Polling interval can be configured per binding. If unset, falls back to the
	// connector global polling interval. If that's also unset, falls back to 24h.
	var pollStr = "24h"
	if c.Config.Advanced.PollInterval != "" {
		pollStr = c.Config.Advanced.PollInterval
	}
	if res.PollInterval != "" {
		pollStr = res.PollInterval
	}
	pollInterval, err := time.ParseDuration(pollStr)
	if err != nil {
		return fmt.Errorf("invalid poll interval %q: %w", res.PollInterval, err)
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
	// Acquire mutex so that only one worker at a time can be actively executing a query.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

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

	log.WithFields(log.Fields{"query": query, "args": args}).Info("executing query")

	// There is no helper function for a streaming select query _with arguments_,
	// so we have to drop down a level and prepare the statement ourselves here.
	var stmt, err = c.DB.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("error preparing query %q: %w", query, err)
	}
	defer stmt.Close()

	var result mysql.Result
	defer result.Close() // Ensure the resultset allocated during ExecuteSelectStreaming is returned to the pool when done

	var pollTime = time.Now().UTC()
	var count int

	var shape *encrow.Shape
	var cursorIndices []int
	var rowValues []any

	if err := stmt.ExecuteSelectStreaming(&result, func(row []mysql.FieldValue) error {
		if shape == nil {
			// Construct a Shape corresponding to the columns of these result rows
			var fieldNames = []string{"_meta"}
			for _, field := range result.Fields {
				fieldNames = append(fieldNames, string(field.Name))
			}
			shape = encrow.NewShape(fieldNames)

			// Also build a list of column indices which should be used as the cursor
			var fieldIndices = make(map[string]int)
			for idx, name := range fieldNames {
				fieldIndices[name] = idx
			}
			for _, cursorName := range cursorNames {
				cursorIndices = append(cursorIndices, fieldIndices[cursorName])
			}
		}

		if len(rowValues) != len(row)+1 {
			// Allocate an array of N+1 elements to hold the translated row values
			// plus the `_meta` property we add.
			rowValues = make([]any, len(row)+1)
		}
		rowValues[0] = &documentMetadata{
			Polled: pollTime,
			Index:  count,
		}
		for idx, val := range row {
			var translatedValue, err = translateMySQLValue(val.Value())
			if err != nil {
				return fmt.Errorf("error translating column %q value: %w", string(result.Fields[idx].Name), err)
			}
			rowValues[idx+1] = translatedValue
		}

		var bs, err = shape.Encode(rowValues)
		if err != nil {
			return fmt.Errorf("error serializing document: %w", err)
		} else if err := c.Output.Documents(bindingIndex, bs); err != nil {
			return fmt.Errorf("error emitting document: %w", err)
		} else if err := c.Output.Checkpoint(json.RawMessage(`{}`), true); err != nil {
			return fmt.Errorf("error emitting checkpoint: %w", err)
		}

		if len(cursorValues) != len(cursorNames) {
			// Allocate a new values list if needed. This is done inside of the loop, so
			// an empty result set will be a no-op even when the previous state is nil.
			cursorValues = make([]any, len(cursorNames))
		}
		for i, j := range cursorIndices {
			cursorValues[i] = rowValues[j]
		}
		count++

		return nil
	}, nil, args...); err != nil {
		return nil, fmt.Errorf("error executing backfill query: %w", err)
	}

	log.WithFields(log.Fields{
		"query": query,
		"count": count,
	}).Info("query complete")
	return cursorValues, nil
}
