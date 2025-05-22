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
	"golang.org/x/sync/semaphore"
)

var (
	// When processing large tables we like to emit checkpoints every so often. But
	// emitting a checkpoint after every row could be a significant drag on overall
	// throughput, and isn't really needed anyway. So instead we emit one for every
	// N rows, plus another when the query results are fully processed.
	documentsPerCheckpoint = 1000

	// A flag used during tests to shut down the connector gracefully after one `poll` run
	TestShutdownAfterQuery = false

	// The minimum time between schema discovery operations. This avoids situations
	// where we have a ton of bindings all coming due for polling around the same
	// time and just keep running discovery over and over.
	minDiscoveryInterval = 5 * time.Minute
)

const (
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

	// Maximum number of concurrent polling operations to run at once. Might be
	// worth making this configurable in the advanced endpoint settings.
	maxConcurrentQueries = 1
)

// BatchSQLDriver represents a generic "batch SQL" capture behavior, parameterized
// by a config schema, connect function, and value translation logic.
type BatchSQLDriver struct {
	DocumentationURL string
	ConfigSchema     json.RawMessage

	Connect             func(ctx context.Context, cfg *Config) (*sql.DB, error)
	TranslateValue      func(val any, databaseTypeName string) (any, error)
	GenerateResource    func(resourceName, schemaName, tableName, tableType string) (*Resource, error)
	SelectQueryTemplate func(res *Resource) (string, error)
}

// Resource represents the capture configuration of a single resource binding.
type Resource struct {
	Name string `json:"name" jsonschema:"title=Resource Name,description=The unique name of this resource." jsonschema_extras:"order=0"`

	Owner     string   `json:"owner,omitempty" jsonschema:"title=Owner,description=The name of the owner to which the captured table belongs. The query template must be overridden if this is unset."  jsonschema_extras:"order=1"`
	TableName string   `json:"table,omitempty" jsonschema:"title=Table Name,description=The name of the table to be captured. The query template must be overridden if this is unset."  jsonschema_extras:"order=2"`
	Cursor    []string `json:"cursor,omitempty" jsonschema:"title=Cursor Columns,description=The names of columns which should be persisted between query executions as a cursor." jsonschema_extras:"order=3"`

	PollSchedule string `json:"poll,omitempty" jsonschema:"title=Polling Schedule,description=When and how often to execute the fetch query (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day." jsonschema_extras:"order=4,pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	Template     string `json:"template,omitempty" jsonschema:"title=Query Template Override,description=Optionally overrides the query template which will be rendered and then executed. Consult documentation for examples." jsonschema_extras:"multiline=true,order=5"`
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
	if r.Template == "" && (r.Owner == "" || r.TableName == "") {
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

// documentMetadata contains the source metadata located at /_meta
type documentMetadata struct {
	Polled time.Time `json:"polled" jsonschema:"title=Polled Timestamp,description=The time at which the update query which produced this document as executed."`
	Index  int       `json:"index" jsonschema:"title=Result Index,description=The index of this document within the query execution which produced it."`
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
			resource: &res,
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
		Sempahore:      semaphore.NewWeighted(maxConcurrentQueries),
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
	Driver    *BatchSQLDriver
	Config    *Config
	State     *captureState
	DB        *sql.DB
	Bindings  []bindingInfo
	Output    *boilerplate.PullOutput
	Sempahore *semaphore.Weighted

	shared struct {
		sync.RWMutex
		lastDiscoveryTime time.Time // The last time we ran schema discovery.
	}

	TranslateValue func(val any, databaseTypeName string) (any, error)
}

type bindingInfo struct {
	resource *Resource
	index    int
	stateKey boilerplate.StateKey
}

type captureState struct {
	Streams map[boilerplate.StateKey]*streamState `json:"bindingStateV1,omitempty"`
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
	// Always discover and output SourcedSchemas once at startup.
	if err := c.emitSourcedSchemas(ctx); err != nil {
		return err
	}

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
		if binding.resource.Owner == "" || binding.resource.TableName == "" || binding.resource.Template != "" {
			continue // Skip bindings with no schema/table or a custom query because we can't know what their schema will look like just from discovery info
		}
		var tableID = binding.resource.Owner + "." + binding.resource.TableName
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
	var stateKey = binding.stateKey
	var state, ok = c.State.Streams[stateKey]
	if !ok {
		return fmt.Errorf("internal error: no state for stream %q", res.Name)
	}

	// Polling schedule can be configured per binding. If unset, falls back to the connector default.
	var pollScheduleStr = c.Config.Advanced.PollSchedule
	if res.PollSchedule != "" {
		pollScheduleStr = res.PollSchedule
	}
	var pollSchedule, err = schedule.Parse(pollScheduleStr)
	if err != nil {
		return fmt.Errorf("failed to parse polling schedule %q: %w", pollScheduleStr, err)
	}

	log.WithFields(log.Fields{
		"name":   res.Name,
		"tmpl":   res.Template,
		"cursor": res.Cursor,
		"poll":   pollSchedule,
		"last":   state.LastPolled.Format(time.RFC3339Nano),
	}).Info("starting worker")

	for ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond) // Short delay to make startup logs tidier

		// Wait for next scheduled polling cycle.
		if err := schedule.WaitForNext(ctx, pollSchedule, state.LastPolled); err != nil {
			return err
		}
		var pollTime = time.Now().UTC()
		log.WithFields(log.Fields{
			"name": res.Name,
			"poll": pollScheduleStr,
			"prev": state.LastPolled.Format(time.RFC3339Nano),
		}).Info("ready to poll")

		time.Sleep(100 * time.Millisecond) // Short delay to make startup logs tidier

		// Acquire semaphore and execute polling operation.
		if err := c.Sempahore.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("error acquiring semaphore: %w", err)
		}
		if err := func() error {
			// Ensure that the semaphore is released when we finish polling.
			defer c.Sempahore.Release(1)
			log.WithFields(log.Fields{"name": res.Name}).Info("polling binding")
			if err := c.poll(ctx, binding); err != nil {
				return fmt.Errorf("error polling binding %q: %w", res.Name, err)
			}
			return nil
		}(); err != nil {
			return err
		}

		state.LastPolled = pollTime
		if err := c.streamStateCheckpoint(stateKey, state); err != nil {
			return err
		}

		if TestShutdownAfterQuery {
			return nil // In tests, we want each worker to shut down after one poll
		}
	}
	return ctx.Err()
}

func (c *capture) poll(ctx context.Context, binding *bindingInfo) error {
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

	templateString, err := c.Driver.SelectQueryTemplate(res)
	if err != nil {
		return fmt.Errorf("error selecting query template: %w", err)
	}
	queryTemplate, err := template.New("query").Funcs(templateFuncs).Parse(templateString)
	if err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}

	var quotedCursorNames []string
	for _, cursorName := range cursorNames {
		quotedCursorNames = append(quotedCursorNames, quoteIdentifier(cursorName))
	}

	var templateArg = map[string]any{
		"IsFirstQuery": len(cursorValues) == 0,
		"CursorFields": quotedCursorNames,
		"Owner":        res.Owner,
		"TableName":    res.TableName,
	}

	var queryBuf = new(strings.Builder)
	if err := queryTemplate.Execute(queryBuf, templateArg); err != nil {
		return fmt.Errorf("error generating query: %w", err)
	}
	var query = queryBuf.String()

	log.WithFields(log.Fields{
		"query": query,
		"args":  cursorValues,
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

	// When capturing with ORA_ROWSCN (txid), since this cursor is not unique for rows,
	// we need to ensure we only emit a checkpoint after we have captured all of the rows
	// with the same SCN
	var scnCursor = len(cursorNames) == 1 && cursorNames[0] == "TXID"
	var lastSCN any
	if scnCursor && state.CursorValues != nil {
		lastSCN = state.CursorValues[0]
	}

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

		// If ORA_ROWSCN is our cursor, only emit a checkpoint when we have captured all of the rows with the same SCN
		if scnCursor {
			if lastSCN == nil {
				lastSCN = cursorValues[0]
			} else if lastSCN != cursorValues[0] {
				state.CursorValues = []any{lastSCN}
				lastSCN = cursorValues[0]
			}
		} else {
			state.CursorValues = cursorValues
		}

		count++
		if count%documentsPerCheckpoint == 0 {
			if err := c.streamStateCheckpoint(stateKey, state); err != nil {
				return err
			}
		}
		if count%100000 == 1 {
			log.WithFields(log.Fields{
				"name":  res.Name,
				"count": count,
			}).Info("processing query results")
		}
	}

	// After having completed a query, emit a final checkpoint with the last seen SCN
	// Since the query is not paginated (i.e. we query all available results), the SCN
	// should be the latest available SCN in that table. Any new updates to the table which may
	// update the data will have a higher SCN
	state.CursorValues = []any{lastSCN}

	log.WithFields(log.Fields{
		"name":  res.Name,
		"query": query,
		"count": count,
	}).Info("polling complete")
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error processing results iterator: %w", err)
	}
	state.LastPolled = pollTime
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
	// From https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Database-Object-Names-and-Qualifiers.html#GUID-3C59E44A-5140-4BCA-B9E1-3039C8050C49:
	//
	//     A quoted identifier begins and ends with double quotation marks (").
	//     Quoted identifiers can contain any characters and punctuations marks as well as spaces.
	//     However, neither quoted nor nonquoted identifiers can contain double quotation marks or the null character (\0)
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
