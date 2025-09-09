package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
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

	// We have a watchdog timeout which fires if we're sitting there waiting for query
	// results but haven't received anything for a while. This constant specifies the
	// duration of that timeout while waiting for for the first result row.
	//
	// It is useful to have two different timeouts for the first result row versus any
	// subsequent rows, because sometimes when using a cursor the queries can have some
	// nontrivial startup cost and we don't want to make things any more failure-prone
	// than we have to.
	pollingWatchdogFirstRowTimeout = 30 * time.Minute

	// The duration of the no-data watchdog for subsequent rows after the first one.
	// This timeout can be less generous, because after the first row is received we
	// ought to be getting a consistent stream of results until the query completes.
	pollingWatchdogTimeout = 5 * time.Minute

	// The minimum time between schema discovery operations. This avoids situations
	// where we have a ton of bindings all coming due for polling around the same
	// time and just keep running discovery over and over.
	minDiscoveryInterval = 5 * time.Minute
)

var (
	// TestShutdownAfterQuery is a test behavior flag which causes the capture to
	// shut down after issuing one query to each binding. It is always false in
	// normal operation.
	TestShutdownAfterQuery = false
)

// BatchSQLDriver represents a generic "batch SQL" capture behavior, parameterized
// by a config schema, connect function, and value translation logic.
type BatchSQLDriver struct {
	DocumentationURL string
	ConfigSchema     json.RawMessage

	Connect             func(ctx context.Context, cfg *Config) (*sql.DB, error)
	TranslateValue      func(val any, databaseTypeName string) (any, error)
	GenerateResource    func(cfg *Config, resourceName, schemaName, tableName, tableType string) (*Resource, error)
	SelectQueryTemplate func(res *Resource) (string, error)
}

// Resource represents the capture configuration of a single resource binding.
type Resource struct {
	Name         string   `json:"name" jsonschema:"title=Name,description=The unique name of this resource." jsonschema_extras:"order=0"`
	SchemaName   string   `json:"schema,omitempty" jsonschema:"title=Schema Name,description=The name of the schema in which the captured table lives. The query template must be overridden if this is unset."  jsonschema_extras:"order=1"`
	TableName    string   `json:"table,omitempty" jsonschema:"title=Table Name,description=The name of the table to be captured. The query template must be overridden if this is unset."  jsonschema_extras:"order=2"`
	Cursor       []string `json:"cursor,omitempty" jsonschema:"title=Cursor Columns,description=The names of columns which should be persisted between query executions as a cursor." jsonschema_extras:"order=3"`
	PollSchedule string   `json:"poll,omitempty" jsonschema:"title=Polling Schedule,description=When and how often to execute the fetch query (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day." jsonschema_extras:"order=4,pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	Template     string   `json:"template,omitempty" jsonschema:"title=Query Template Override,description=Optionally overrides the query template which will be rendered and then executed. Consult documentation for examples." jsonschema_extras:"multiline=true,order=5"`
}

// Validate checks that the resource spec possesses all required properties.
func (r Resource) Validate() error {
	var requiredProperties = [][]string{
		{"name", r.Name},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	if r.Template == "" && (r.SchemaName == "" || r.TableName == "") {
		return fmt.Errorf("must specify schema+table name or else a template override")
	}
	if r.Template != "" {
		if _, err := template.New("query").Funcs(templateFuncs).Parse(r.Template); err != nil {
			return fmt.Errorf("error parsing template: %w", err)
		}
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

// documentMetadata contains the metadata located at /_meta
type documentMetadata struct {
	Polled time.Time `json:"polled" jsonschema:"title=Polled Timestamp,description=The time at which the update query which produced this document as executed."`
	Index  int       `json:"index" jsonschema:"title=Result Index,description=The index of this document within the query execution which produced it."`
	RowID  int64     `json:"row_id" jsonschema:"title=Row ID,description=Row ID of the Document, counting up from zero."`
	Op     string    `json:"op,omitempty" jsonschema:"title=Change Operation,description=Operation type (c: Create / u: Update / d: Delete),enum=c,enum=u,enum=d,default=u"`

	Source documentSourceMetadata `json:"source"`
}

// documentSourceMetadata contains the source metadata located at /_meta/source
type documentSourceMetadata struct {
	Resource string `json:"resource" jsonschema:"description=Resource name of the binding from which this document was captured."`
	Schema   string `json:"schema,omitempty" jsonschema:"description=Database schema from which the document was read."`
	Table    string `json:"table,omitempty" jsonschema:"description=Database table from which the document was read."`
	Tag      string `json:"tag,omitempty" jsonschema:"description=Optional 'Source Tag' property as defined in the endpoint configuration."`
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
			resource:      &res,
			index:         idx,
			stateKey:      boilerplate.StateKey(binding.StateKey),
			collectionKey: binding.Collection.Key,
		})
	}

	var state captureState
	if open.StateJson != nil {
		if err := pf.UnmarshalStrict(open.StateJson, &state); err != nil {
			return fmt.Errorf("parsing state checkpoint: %w", err)
		}
	}

	state, err = updateResourceStates(state, bindings)
	if err != nil {
		return fmt.Errorf("error initializing resource states: %w", err)
	}

	if err := stream.Ready(false); err != nil {
		return err
	}

	var capture = &capture{
		Driver:         drv,
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
	Driver         *BatchSQLDriver
	Config         *Config
	State          *captureState
	DB             *sql.DB
	Bindings       []bindingInfo
	Output         *boilerplate.PullOutput
	TranslateValue func(val any, databaseTypeName string) (any, error)

	shared struct {
		sync.RWMutex
		lastDiscoveryTime time.Time // The last time we ran schema discovery.
	}
}

type bindingInfo struct {
	resource      *Resource
	index         int
	stateKey      boilerplate.StateKey
	collectionKey []string // The key of the output collection, as an array of JSON pointers.
}

type captureState struct {
	Streams map[boilerplate.StateKey]*streamState `json:"bindingStateV1,omitempty"`
}

type streamState struct {
	CursorNames   []string
	CursorValues  []any
	LastPolled    time.Time
	DocumentCount int64 // A count of the number of documents emitted since the last full refresh started.
}

func (s *captureState) Validate() error {
	return nil
}

func (c *capture) Run(ctx context.Context) error {
	// Always discover and output SourcedSchemas once at startup.
	if err := c.emitSourcedSchemas(ctx); err != nil {
		return err
	}

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

func (c *capture) emitSourcedSchemas(ctx context.Context) error {
	if !c.Config.Advanced.parsedFeatureFlags["emit_sourced_schemas"] {
		return nil
	}

	c.shared.Lock()
	defer c.shared.Unlock()

	// Skip re-running discovery and emitting schemas if we just did it.
	if time.Since(c.shared.lastDiscoveryTime) < minDiscoveryInterval {
		return nil
	}
	c.shared.lastDiscoveryTime = time.Now()

	// Discover tables and emit SourcedSchema updates
	var tableInfo, err = c.Driver.discoverTables(ctx, c.DB, c.Config)
	if err != nil {
		return fmt.Errorf("error discovering tables: %w", err)
	}
	for _, binding := range c.Bindings {
		if binding.resource.SchemaName == "" || binding.resource.TableName == "" || binding.resource.Template != "" {
			continue // Skip bindings with no schema/table or a custom query because we can't know what their schema will look like just from discovery info
		}
		var tableID = binding.resource.SchemaName + "." + binding.resource.TableName
		var table, ok = tableInfo[tableID]
		if !ok {
			continue // Skip bindings for which we don't have any corresponding discovery information
		}
		if schema, _, err := generateCollectionSchema(c.Config, table, false); err != nil {
			return fmt.Errorf("error generating schema for table %q: %w", tableID, err)
		} else if err := c.Output.SourcedSchema(binding.index, schema); err != nil {
			return fmt.Errorf("error emitting schema for table %q: %w", tableID, err)
		}
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

	templateString, err := c.Driver.SelectQueryTemplate(res)
	if err != nil {
		return fmt.Errorf("error selecting query template: %w", err)
	}
	queryTemplate, err := template.New("query").Funcs(templateFuncs).Parse(templateString)
	if err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}

	for ctx.Err() == nil {
		if err := c.poll(ctx, binding, queryTemplate); err != nil {
			return fmt.Errorf("error polling binding %q: %w", res.Name, err)
		}
		if TestShutdownAfterQuery {
			return nil // In tests, we want each worker to shut down after one poll
		}
	}
	return ctx.Err()
}

func (c *capture) poll(ctx context.Context, binding *bindingInfo, tmpl *template.Template) error {
	// Trigger rediscovery and output new SourcedSchemas before each polling operation.
	if err := c.emitSourcedSchemas(ctx); err != nil {
		return err
	}

	var res = binding.resource
	var stateKey = binding.stateKey
	var state, ok = c.State.Streams[stateKey]
	if !ok {
		return fmt.Errorf("internal error: no state for stream %q", res.Name)
	}
	var cursorNames = state.CursorNames
	var cursorValues = state.CursorValues

	// If the key of the output collection for this binding is the Row ID then we
	// can automatically provide useful `/_meta/op` values and inferred deletions.
	var isRowIDKey = len(binding.collectionKey) == 1 && binding.collectionKey[0] == "/_meta/row_id"

	// Two distinct concepts:
	// - If we have no resume cursor _columns_ then every query is a full refresh.
	// - If we have no resume cursor _values_ then this is a backfill query, which
	//   could either be a full refresh or the initial query of an incremental binding.
	var isFullRefresh = len(cursorNames) == 0
	var isInitialBackfill = len(cursorValues) == 0

	var quotedCursorNames []string
	for _, cursorName := range cursorNames {
		quotedCursorNames = append(quotedCursorNames, quoteIdentifier(cursorName))
	}

	var templateArg = map[string]any{
		"IsFirstQuery": len(cursorValues) == 0,
		"CursorFields": quotedCursorNames,
		"SchemaName":   res.SchemaName,
		"TableName":    res.TableName,
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
		"prev": state.LastPolled.Format(time.RFC3339Nano),
	}).Info("waiting for next scheduled poll")
	if err := schedule.WaitForNext(ctx, pollSchedule, state.LastPolled); err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"name": res.Name,
		"poll": pollScheduleStr,
	}).Info("ready to poll")

	var queryBuf = new(strings.Builder)
	if err := tmpl.Execute(queryBuf, templateArg); err != nil {
		return fmt.Errorf("error generating query: %w", err)
	}
	var query = queryBuf.String()

	// For incremental updates of a binding with a cursor, continue counting from where
	// we left off. For initial backfills (which includes the first query of an incremental
	// binding as well as every query of a full-refresh binding) restart from zero.
	var nextRowID = state.DocumentCount
	if isInitialBackfill {
		nextRowID = 0
	}

	log.WithFields(log.Fields{
		"query": query,
		"args":  cursorValues,
		"rowID": nextRowID,
	}).Info("executing query")
	var pollTime = time.Now().UTC()

	// Set up a watchdog timeout which will terminate the capture task if no data is
	// received after a long period of time. The deferred stop ensures that the timeout
	// will always be cancelled for good when the polling operation finishes.
	var watchdog = time.AfterFunc(pollingWatchdogFirstRowTimeout, func() {
		log.WithField("name", res.Name).Fatal("polling timed out")
	})
	defer watchdog.Stop()

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
		if columnIndex, ok := columnIndices[cursorName]; ok {
			cursorIndices = append(cursorIndices, columnIndex)
		} else {
			return fmt.Errorf("cursor column %q not found in query result", cursorName)
		}
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
		watchdog.Reset(pollingWatchdogTimeout) // Reset the no-data watchdog timeout after each row received

		for idx, val := range columnValues {
			var translatedVal, err = c.TranslateValue(val, columnTypes[idx].DatabaseTypeName())
			if err != nil {
				return fmt.Errorf("error translating column %q value: %w", columnNames[idx], err)
			}
			rowValues[idx] = translatedVal
		}
		var metadata = &documentMetadata{
			RowID:  nextRowID,
			Polled: pollTime,
			Index:  count,
			Source: documentSourceMetadata{
				Resource: res.Name,
				Schema:   res.SchemaName,
				Table:    res.TableName,
				Tag:      c.Config.Advanced.SourceTag,
			},
		}
		if isRowIDKey {
			// When the output key of a binding is the row ID, we can provide useful
			// create/update change operation values based on that row ID. This logic
			// will always set the operation to "c" for a cursor-incremental binding
			// with row ID key since the row ID is always increasing.
			if nextRowID < state.DocumentCount {
				metadata.Op = "u"
			} else {
				metadata.Op = "c"
			}
		}
		rowValues[len(rowValues)-1] = metadata

		serializedDocument, err = shape.Encode(serializedDocument[:0], rowValues)
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
		nextRowID++
		if count%documentsPerCheckpoint == 0 {
			// When a full-refresh binding outputs into a collection with key `/_meta/row_id`
			// we rely on the persisted DocumentCount not being updated until after the whole
			// update query completes successfully, so that we can infer deletions of any rows
			// between the last rowID of the latest query and the persisted DocumentCount.
			//
			// But when emitting partial-progress updates on a _non_ full-refresh binding, we
			// need to update the persisted DocumentCount on each partial progress checkpoint
			// so that the rowID the next poll resumes from will match the persisted cursor.
			if !isFullRefresh {
				state.DocumentCount = nextRowID
			}
			if err := c.streamStateCheckpoint(stateKey, state); err != nil {
				return err
			}
		}
		if count%100000 == 1 {
			log.WithFields(log.Fields{
				"name":  res.Name,
				"count": count,
				"rowID": nextRowID,
			}).Info("processing query results")
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error processing results iterator: %w", err)
	}
	log.WithFields(log.Fields{
		"name":  res.Name,
		"query": query,
		"count": count,
		"rowID": nextRowID,
		"docs":  state.DocumentCount,
	}).Info("polling complete")

	// A full-refresh binding whose output collection uses the key /_meta/row_id can
	// infer deletions whenever a refresh yields fewer rows than last time.
	if isRowIDKey && isFullRefresh {
		var pollTimestamp = pollTime.Format(time.RFC3339Nano)
		for i := int64(0); i < state.DocumentCount-nextRowID; i++ {
			// These inferred-deletion documents are simple enough that we can generate
			// them with a simple Sprintf rather than going through a whole JSON encoder.
			var doc = fmt.Sprintf(
				`{"_meta":{"polled":%q,"index":%d,"row_id":%d,"op":"d"}}`,
				pollTimestamp, count+int(i), nextRowID+i)
			if err := c.Output.Documents(binding.index, json.RawMessage(doc)); err != nil {
				return fmt.Errorf("error emitting document: %w", err)
			}
		}
	}

	state.LastPolled = pollTime
	state.DocumentCount = nextRowID // Always update persisted count on successful completion
	if err := c.streamStateCheckpoint(stateKey, state); err != nil {
		return err
	}
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

func quoteIdentifier(name string) string {
	// From https://www.postgresql.org/docs/14/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS:
	//
	//     Quoted identifiers can contain any character, except the character with code zero.
	//     (To include a double quote, write two double quotes.)
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
