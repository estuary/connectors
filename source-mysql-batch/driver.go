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
	"github.com/estuary/connectors/go/schedule"
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

	Connect               func(ctx context.Context, cfg *Config) (*client.Conn, error)
	GenerateResource      func(resourceName, schemaName, tableName, tableType string) (*Resource, error)
	ExcludedSystemSchemas []string
}

// Resource represents the capture configuration of a single resource binding.
type Resource struct {
	Name         string   `json:"name" jsonschema:"title=Name,description=The unique name of this resource." jsonschema_extras:"order=0"`
	Template     string   `json:"template" jsonschema:"title=Query Template,description=The query template (pkg.go.dev/text/template) which will be rendered and then executed." jsonschema_extras:"multiline=true,order=3"`
	Cursor       []string `json:"cursor,omitempty" jsonschema:"title=Cursor Columns,description=The names of columns which should be persisted between query executions as a cursor." jsonschema_extras:"order=2"`
	PollSchedule string   `json:"poll,omitempty" jsonschema:"title=Polling Schedule,description=When and how often to execute the fetch query (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day." jsonschema_extras:"order=1,pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
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
		ResourcePathPointers:     []string{"/name"},
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

	tables, err := discoverTables(ctx, db, cfg.Advanced.DiscoverSchemas)
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

const queryDiscoverTables = `SELECT table_schema, table_name, table_type FROM information_schema.tables;`

type discoveredTable struct {
	Schema string
	Name   string
	Type   string // Usually 'BASE TABLE' or 'VIEW'
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
		Config:   &cfg,
		State:    &state,
		DB:       db,
		Bindings: bindings,
		Output:   stream,
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
	Config   *Config
	State    *captureState
	DB       *client.Conn
	Bindings []bindingInfo
	Output   *boilerplate.PullOutput
	Mutex    sync.Mutex
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
	for _, binding := range c.Bindings {
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

	var queryTemplate, err = template.New("query").Parse(res.Template)
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

var queryPlaceholderRegexp = regexp.MustCompile(`([?]|:[0-9]+|@flow_cursor_value\[[0-9]+\])`)

// expandQueryPlaceholders simulates numbered query placeholders in MySQL by replacing
// each numbered placeholder `@flow_cursor_value[N]` with a question mark while replacing
// the input list of argument values into an output list corresponding to the usage sequence.
func expandQueryPlaceholders(query string, argvals []any) (string, []any, error) {
	var errReturn error
	var argseq []any
	var prevIndex int
	query = queryPlaceholderRegexp.ReplaceAllStringFunc(query, func(param string) string {
		if param == "?" {
			prevIndex++
			argseq = append(argseq, argvals[prevIndex])
			return "?"
		}

		// Deprecated and soon-to-be-removed numbered placeholder syntax.
		if strings.HasPrefix(param, ":") {
			var paramIndex, err = strconv.ParseInt(strings.TrimPrefix(param, ":"), 10, 64)
			if err != nil {
				errReturn = fmt.Errorf("internal error: failed to parse parameter name %q", param)
			} else if paramIndex < 0 || int(paramIndex) >= len(argvals) {
				errReturn = fmt.Errorf("parameter index %d outside of valid range [0,%d)", paramIndex, len(argvals))
			} else {
				argseq = append(argseq, argvals[paramIndex])
				prevIndex = int(paramIndex)
			}
			return "?"
		}

		if strings.HasPrefix(param, "@flow_cursor_value[") {
			param = strings.TrimPrefix(param, "@flow_cursor_value[")
			param = strings.TrimSuffix(param, "]")
			var paramIndex, err = strconv.ParseInt(strings.TrimPrefix(param, ":"), 10, 64)
			if err != nil {
				errReturn = fmt.Errorf("internal error: failed to parse parameter name %q", param)
			} else if paramIndex < 0 || int(paramIndex) >= len(argvals) {
				errReturn = fmt.Errorf("parameter index %d outside of valid range [0,%d)", paramIndex, len(argvals))
			} else {
				argseq = append(argseq, argvals[paramIndex])
				prevIndex = int(paramIndex)
			}
			return "?"
		}

		return param
	})
	return query, argseq, errReturn
}

func quoteColumnName(name string) string {
	// Per https://dev.mysql.com/doc/refman/8.0/en/identifiers.html, the identifier quote character
	// is the backtick (`). If the identifier itself contains a backtick, it must be doubled.
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
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

	// Acquire mutex so that only one worker at a time can be actively executing a query.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	var queryBuf = new(strings.Builder)
	if err := tmpl.Execute(queryBuf, templateArg); err != nil {
		return fmt.Errorf("error generating query: %w", err)
	}
	log.WithFields(log.Fields{
		"query":  queryBuf.String(),
		"values": cursorValues,
	}).Debug("expanding query")
	query, args, err := expandQueryPlaceholders(queryBuf.String(), cursorValues)
	if err != nil {
		return fmt.Errorf("error expanding query placeholders: %w", err)
	}

	log.WithFields(log.Fields{
		"query": query,
		"args":  args,
	}).Info("executing query")
	var pollTime = time.Now().UTC()
	state.LastPolled = pollTime

	// There is no helper function for a streaming select query _with arguments_,
	// so we have to drop down a level and prepare the statement ourselves here.
	stmt, err := c.DB.Prepare(query)
	if err != nil {
		return fmt.Errorf("error preparing query %q: %w", query, err)
	}
	defer stmt.Close()

	var result mysql.Result
	defer result.Close() // Ensure the resultset allocated during ExecuteSelectStreaming is returned to the pool when done

	var count int
	var shape *encrow.Shape
	var cursorIndices []int
	var rowValues []any
	var serializedDocument []byte

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
		return nil
	}, nil, args...); err != nil {
		return fmt.Errorf("error executing backfill query: %w", err)
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
