package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	bqclient "github.com/estuary/connectors/go/capture/bigquery/client"
	"github.com/estuary/connectors/go/capture/bigquery/datatypes"
	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

const (
	// Emit a checkpoint every N rows during a long-running query so the cursor
	// position is persisted incrementally and a mid-query crash recovers with
	// bounded re-work.
	documentsPerCheckpoint = 1000

	// Watchdog timeout while waiting for the first result row of a query.
	// BigQuery queries against multi-billion-row tables can have nontrivial
	// startup cost, so this is generous.
	pollingWatchdogFirstRowTimeout = 30 * time.Minute

	// Watchdog timeout between subsequent rows. Once the result stream is
	// flowing we expect rows at a steady rate.
	pollingWatchdogTimeout = 5 * time.Minute
)

// documentMetadata contains the metadata located at /_meta of each emitted
// document.
type documentMetadata struct {
	Polled time.Time              `json:"polled" jsonschema:"title=Polled Timestamp,description=The time at which the readout that produced this document began."`
	Index  int                    `json:"index" jsonschema:"title=Result Index,description=The index of this document within the table query that produced it."`
	Source documentSourceMetadata `json:"source"`
}

// documentSourceMetadata contains the source metadata located at
// /_meta/source.
type documentSourceMetadata struct {
	Dataset    string `json:"dataset" jsonschema:"description=BigQuery dataset from which the document was read."`
	StreamType string `json:"stream_type" jsonschema:"description=GA4 logical stream that produced the document."`
	TableDate  string `json:"table_date" jsonschema:"description=YYYYMMDD suffix of the source table from which the document was read."`
	Tag        string `json:"tag,omitempty" jsonschema:"description=Optional 'Source Tag' property as defined in the endpoint configuration."`
}

// Driver is the connector's protocol implementation.
type Driver struct {
	DocumentationURL string
	ConfigSchema     json.RawMessage

	Connect func(ctx context.Context, cfg *Config) (*bigquery.Client, error)
}

// Spec returns metadata about the capture connector.
func (d *Driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	resourceSchema, err := schemagen.GenerateSchema("GA4 BigQuery Resource Spec", &Resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}
	return &pc.Response_Spec{
		ConfigSchemaJson:         d.ConfigSchema,
		ResourceConfigSchemaJson: resourceSchema,
		DocumentationUrl:         d.DocumentationURL,
		ResourcePathPointers:     []string{"/dataset", "/stream_type"},
	}, nil
}

// Apply does nothing.
func (Driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

// Validate checks that the configuration parses and that bindings are
// well-formed.
func (d *Driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	bq, err := d.Connect(ctx, &cfg)
	if err != nil {
		return nil, err
	}
	defer bq.Close()

	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		if err := res.Validate(); err != nil {
			return nil, fmt.Errorf("invalid resource for binding %q: %w", res.Dataset+"/"+string(res.StreamType), err)
		}
		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Dataset, string(res.StreamType)},
		})
	}
	return &pc.Response_Validated{Bindings: out}, nil
}

type captureState struct {
	Streams map[boilerplate.StateKey]*streamState `json:"bindingStateV1"`
}

// streamState is the per-binding scheduling and progress state.
//
// By default and in the steady state, each table goes through two captures: once
// when it's the newest, and a final capture when it reaches the other end of the
// live window to ensure that any late-arriving events are ingested eventually.
//
// In cases where there exists a backlog of uncaptured tables larger than the live
// window (initial backfills and paused-capture catchup), this backlog will only
// be captured once as these tables should not be updated further.
//
// There is an optional "capture intermediate tables" mode. In this mode, each
// table in the live window is recaptured once per polling cycle instead of just
// capturing the endpoints. This trades increased capture cost for fresher data
// in the 1-day and 2-day-old tables.
//
// PrimaryThrough and FinalThrough are dataset-level scheduling cursors which
// advance at enqueue-time, never backwards. They decide which tables future
// polling cycles consider.
//
// The within-table resume cursor on Current is independent and exists only to
// recover an interrupted readout.
type streamState struct {
	// Current is the in-progress table readout, or nil between readouts.
	Current *currentTable `json:"current"`
	// Pending is the queue of suffixes awaiting readout, sorted descending
	// (Pending[0] is the newest queued suffix).
	Pending []string `json:"pending"`
	// PrimaryThrough is the largest suffix already queued for primary
	// capture. Advances at enqueue-time, never moves backwards.
	PrimaryThrough string `json:"primary_through"`
	// FinalThrough is the largest suffix already queued for final
	// capture. Advances at enqueue-time, never moves backwards.
	FinalThrough string `json:"final_through"`
	// LastPollStarted is the wall-clock time of the most recent polling
	// cycle. Used to compute when the next poll fires.
	LastPollStarted time.Time `json:"last_poll_started"`
}

// currentTable tracks an active or interrupted readout. The cursor is a
// within-table resume position; on crash, the readout is resumed from this
// point. Scheduling cursors live on streamState, not here.
type currentTable struct {
	Suffix    string    `json:"suffix"`
	Cursor    any       `json:"cursor"`
	StartedAt time.Time `json:"started_at"` // Start timestamp of this readout
	Index     int       `json:"row_id"`     // Current row index into the table
}

func (s *captureState) Validate() error { return nil }

// Pull is the main capture loop.
func (d *Driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg Config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	bq, err := d.Connect(stream.Context(), &cfg)
	if err != nil {
		return err
	}
	defer bq.Close()

	var bindings []bindingInfo
	for idx, binding := range open.Capture.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		if err := res.Validate(); err != nil {
			return fmt.Errorf("invalid resource: %w", err)
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
	state = initStreamStates(state, bindings)

	if err := stream.Ready(false); err != nil {
		return err
	}

	pollSchedule, err := schedule.Parse(cfg.Advanced.PollSchedule)
	if err != nil {
		return fmt.Errorf("parsing polling schedule %q: %w", cfg.Advanced.PollSchedule, err)
	}

	c := &capture{
		Driver:       d,
		Config:       &cfg,
		DB:           bq,
		Bindings:     bindings,
		State:        &state,
		Output:       stream,
		PollSchedule: pollSchedule,
	}
	return c.Run(stream.Context())
}

func initStreamStates(prev captureState, bindings []bindingInfo) captureState {
	var next = captureState{Streams: make(map[boilerplate.StateKey]*streamState)}
	for _, b := range bindings {
		var s = prev.Streams[b.stateKey]
		if s == nil {
			s = &streamState{}
		}
		next.Streams[b.stateKey] = s
	}
	return next
}

type bindingInfo struct {
	resource *Resource
	index    int
	stateKey boilerplate.StateKey
}

type capture struct {
	Driver       *Driver
	Config       *Config
	DB           *bigquery.Client
	Bindings     []bindingInfo
	State        *captureState
	Output       *boilerplate.PullOutput
	PollSchedule schedule.Schedule
}

// Run is the connector-wide scheduler. At every iteration it picks the
// highest-priority unit of work across all bindings:
//
//  1. Run a polling cycle on any binding whose schedule has fired.
//  2. Resume an interrupted readout (Current != nil), to drain crashed-mid
//     state.
//  3. Start a new readout, picking the binding with the newest queued suffix
//     (ties broken by binding order).
//  4. Sleep until the earliest pending poll-due time.
//
// No preemption: a poll cycle and a single-table readout each run to
// completion before the loop reconsiders priorities.
func (c *capture) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if b := c.findPollDue(); b != nil {
			if err := c.runPollCycle(ctx, b); err != nil {
				return err
			}
			continue
		}

		if b := c.findResumable(); b != nil {
			if err := c.readout(ctx, b); err != nil {
				return err
			}
			continue
		}

		if b, suffix := c.pickHighestPending(); b != nil {
			if err := c.startReadout(ctx, b, suffix); err != nil {
				return err
			}
			continue
		}

		if err := c.sleepUntilNextDue(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// findPollDue returns the first binding whose poll schedule has fired since
// LastPollStarted, or nil.
func (c *capture) findPollDue() *bindingInfo {
	var now = time.Now()
	for i := range c.Bindings {
		var state = c.State.Streams[c.Bindings[i].stateKey]
		if !now.Before(c.PollSchedule.Next(state.LastPollStarted)) {
			return &c.Bindings[i]
		}
	}
	return nil
}

// findResumable returns the first binding with a Current readout to drain,
// or nil.
func (c *capture) findResumable() *bindingInfo {
	for i := range c.Bindings {
		if c.State.Streams[c.Bindings[i].stateKey].Current != nil {
			return &c.Bindings[i]
		}
	}
	return nil
}

// pickHighestPending picks the binding whose Pending[0] is newest across all
// bindings; ties on suffix break by binding order (the first binding seen
// with that suffix wins). Returns (nil, "") if no bindings have anything
// pending.
func (c *capture) pickHighestPending() (*bindingInfo, string) {
	var best *bindingInfo
	var bestSuffix string
	for i := range c.Bindings {
		var b = &c.Bindings[i]
		var s = c.State.Streams[b.stateKey]
		if len(s.Pending) == 0 {
			continue
		}
		var head = s.Pending[0]
		if best == nil || head > bestSuffix {
			best = b
			bestSuffix = head
		}
	}
	return best, bestSuffix
}

// sleepUntilNextDue blocks until the earliest pollDue time across all
// bindings (or context cancellation).
func (c *capture) sleepUntilNextDue(ctx context.Context) error {
	var nextWake time.Time
	for i := range c.Bindings {
		var t = c.PollSchedule.Next(c.State.Streams[c.Bindings[i].stateKey].LastPollStarted)
		if nextWake.IsZero() || t.Before(nextWake) {
			nextWake = t
		}
	}
	return sleepUntil(ctx, nextWake)
}

func sleepUntil(ctx context.Context, t time.Time) error {
	var d = time.Until(t)
	if d <= 0 {
		return nil
	}
	var timer = time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// runPollCycle anchors the live window on the dataset's existing tables,
// merges new enqueueings into Pending, and advances PrimaryThrough /
// FinalThrough. Atomic checkpoint at the end.
func (c *capture) runPollCycle(ctx context.Context, binding *bindingInfo) error {
	var state = c.State.Streams[binding.stateKey]
	var stream = streams[binding.resource.StreamType]

	state.LastPollStarted = time.Now().UTC()

	suffixes, err := c.enumerateStreamTables(ctx, binding.resource.Dataset, stream)
	if err != nil {
		return fmt.Errorf("enumerating tables for %s/%s: %w", binding.resource.Dataset, binding.resource.StreamType, err)
	}

	if len(suffixes) == 0 {
		log.WithFields(log.Fields{
			"dataset":    binding.resource.Dataset,
			"streamType": binding.resource.StreamType,
		}).Info("polling cycle: dataset has no matching tables")
		return c.checkpoint()
	}

	var added = evaluatePollCycle(state, suffixes, c.Config.Advanced.WindowDays, c.Config.Advanced.CaptureIntermediate, c.Config.Advanced.parsedMinDate)

	log.WithFields(log.Fields{
		"dataset":        binding.resource.Dataset,
		"streamType":     binding.resource.StreamType,
		"datasetTables":  len(suffixes),
		"enqueued":       added,
		"pendingTotal":   len(state.Pending),
		"primaryThrough": state.PrimaryThrough,
		"finalThrough":   state.FinalThrough,
	}).Info("polling cycle complete")

	return c.checkpoint()
}

// evaluatePollCycle is the pure-logic core of a polling cycle: given the
// dataset's current suffixes and the binding's existing state, it decides
// what to enqueue, advances scheduling cursors, and re-sorts Pending. The
// state struct is mutated in place. Returns the number of new entries
// added to Pending. Caller is responsible for IO (table enumeration,
// checkpoint emission). Suffixes must be sorted ascending.
func evaluatePollCycle(state *streamState, suffixes []string, windowDays int, captureIntermediate bool, minDate string) int {
	if len(suffixes) == 0 {
		return 0
	}

	var windowLatest = suffixes[len(suffixes)-1]
	var oldestIdx = len(suffixes) - windowDays
	if oldestIdx < 0 {
		oldestIdx = 0
	}
	var windowOldest = suffixes[oldestIdx]

	var pendingSet = make(map[string]bool, len(state.Pending))
	for _, s := range state.Pending {
		pendingSet[s] = true
	}

	var added int
	for _, T := range suffixes {
		if minDate != "" && T < minDate {
			continue
		}
		if T <= state.FinalThrough {
			continue
		}
		if pendingSet[T] {
			continue
		}

		var shouldEnqueue bool
		switch {
		case T <= windowOldest:
			shouldEnqueue = true
		case T > state.PrimaryThrough:
			shouldEnqueue = true
		default:
			shouldEnqueue = captureIntermediate
		}
		if !shouldEnqueue {
			continue
		}

		if state.Current != nil && state.Current.Suffix == T {
			// New Pending entry will fully re-read this table; drop the
			// in-progress readout.
			state.Current = nil
		}
		state.Pending = append(state.Pending, T)
		pendingSet[T] = true
		added++
	}

	if windowLatest > state.PrimaryThrough {
		state.PrimaryThrough = windowLatest
	}
	if windowOldest > state.FinalThrough {
		state.FinalThrough = windowOldest
	}

	sort.Sort(sort.Reverse(sort.StringSlice(state.Pending)))
	return added
}

// startReadout pops the head of Pending into Current and runs the readout.
// The Pending → Current move and the readout's row emissions are persisted
// in successive checkpoints; on crash, the readout resumes from Current.
func (c *capture) startReadout(ctx context.Context, binding *bindingInfo, suffix string) error {
	var state = c.State.Streams[binding.stateKey]
	state.Current = &currentTable{
		Suffix:    suffix,
		StartedAt: time.Now().UTC(),
	}
	state.Pending = state.Pending[1:]
	if err := c.checkpoint(); err != nil {
		return err
	}
	return c.readout(ctx, binding)
}

// readout drains the Current table for the given binding. On clean
// completion (or a missing-table warning), Current is cleared and a final
// checkpoint is emitted.
func (c *capture) readout(ctx context.Context, binding *bindingInfo) error {
	var state = c.State.Streams[binding.stateKey]
	var stream = streams[binding.resource.StreamType]

	if err := c.queryTable(ctx, binding, stream); err != nil {
		if isTableNotFound(err) {
			log.WithFields(log.Fields{
				"dataset":     binding.resource.Dataset,
				"streamType":  binding.resource.StreamType,
				"tableSuffix": state.Current.Suffix,
			}).Warn("source table missing; skipping (already advanced past it at enqueue-time)")
		} else {
			return fmt.Errorf("reading %s.%s%s: %w", binding.resource.Dataset, stream.TablePrefix, state.Current.Suffix, err)
		}
	}

	state.Current = nil
	return c.checkpoint()
}

// isTableNotFound reports whether err is a BigQuery 404 (typically because
// the table was dropped between enumeration and readout).
func isTableNotFound(err error) bool {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code == 404
	}
	// Some BigQuery error paths surface the 404 as a plain error string.
	return strings.Contains(err.Error(), "Not found: Table")
}

// enumerateStreamTables returns sorted YYYYMMDD suffixes of all tables in
// the dataset matching the stream's prefix. The reference is fully-qualified
// with the configured ProjectID so the query resolves correctly when the
// BigQuery client is connected to a different billing project.
func (c *capture) enumerateStreamTables(ctx context.Context, dataset string, stream *streamDef) ([]string, error) {
	var query = fmt.Sprintf(
		"SELECT table_name FROM %s.%s.INFORMATION_SCHEMA.TABLES WHERE table_name LIKE '%s%%'",
		bqclient.QuoteIdentifier(c.Config.ProjectID),
		bqclient.QuoteIdentifier(dataset),
		stream.TablePrefix,
	)
	rows, err := c.DB.Query(query).Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying INFORMATION_SCHEMA.TABLES: %w", err)
	}
	var suffixes []string
	for {
		var row []bigquery.Value
		if err := rows.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		var name, _ = row[0].(string)
		var suffix = name[len(stream.TablePrefix):]
		if isDateSuffix(suffix) {
			suffixes = append(suffixes, suffix)
		}
	}
	sort.Strings(suffixes)
	return suffixes, nil
}

// queryTable executes a query against state.Current.Suffix, emitting result
// rows as documents and advancing the within-table cursor. Resumes from
// Current.Cursor if set.
func (c *capture) queryTable(ctx context.Context, binding *bindingInfo, stream *streamDef) error {
	var state = c.State.Streams[binding.stateKey]
	var current = state.Current
	var query = buildTableQuery(c.Config.ProjectID, binding.resource.Dataset, stream, current.Suffix, current.Cursor)
	var params = buildQueryParams(stream, current.Cursor)

	log.WithFields(log.Fields{
		"dataset":     binding.resource.Dataset,
		"streamType":  binding.resource.StreamType,
		"tableSuffix": current.Suffix,
		"cursor":      fmt.Sprintf("%v", current.Cursor),
	}).Info("querying table")

	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var watchdog = time.AfterFunc(pollingWatchdogFirstRowTimeout, func() {
		log.WithFields(log.Fields{
			"dataset":     binding.resource.Dataset,
			"streamType":  binding.resource.StreamType,
			"tableSuffix": current.Suffix,
		}).Warn("polling watchdog fired, cancelling query")
		cancel()
	})
	defer watchdog.Stop()

	var q = c.DB.Query(query)
	q.Parameters = params
	rows, err := q.Read(queryCtx)
	if err != nil {
		return wrapWatchdogErr(ctx, queryCtx, err)
	}

	var shape *encrow.Shape
	var cursorColumnIdx = -1
	var rowValues []any
	var serializedDoc []byte
	var resultRowCount = 0
	var documentsSinceLastCheckpoint = 0
	var lastEmittedCursor any
	var haveLastEmittedCursor bool

	for {
		var row []bigquery.Value
		if err := rows.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return wrapWatchdogErr(ctx, queryCtx, err)
		}
		watchdog.Reset(pollingWatchdogTimeout)

		if shape == nil {
			var fieldNames = []string{"_meta"}
			for _, field := range rows.Schema {
				fieldNames = append(fieldNames, field.Name)
			}
			shape = encrow.NewShape(fieldNames)
			for i, name := range fieldNames {
				if name == stream.CursorColumn {
					cursorColumnIdx = i
					break
				}
			}
			if cursorColumnIdx < 0 {
				return fmt.Errorf("cursor column %q not present in result schema", stream.CursorColumn)
			}
		}
		if len(rowValues) != len(row)+1 {
			rowValues = make([]any, len(row)+1)
		}

		rowValues[0] = &documentMetadata{
			Polled: current.StartedAt,
			Index:  current.Index,
			Source: documentSourceMetadata{
				Dataset:    binding.resource.Dataset,
				StreamType: string(binding.resource.StreamType),
				TableDate:  current.Suffix,
				Tag:        c.Config.Advanced.SourceTag,
			},
		}
		for i, val := range row {
			translated, err := datatypes.TranslateValue(val, rows.Schema[i])
			if err != nil {
				return fmt.Errorf("translating column %q: %w", rows.Schema[i].Name, err)
			}
			rowValues[i+1] = translated
		}

		// Extract resume cursor and emit checkpoint only when it's been a bit since
		// the last checkpoint _and_ the current row's cursor value differs from the
		// previous one (meaning that this is a safe `> cursor` resume point).
		var rowCursor = rowValues[cursorColumnIdx]
		if documentsSinceLastCheckpoint >= documentsPerCheckpoint {
			if haveLastEmittedCursor && !cursorValuesEqual(lastEmittedCursor, rowCursor) {
				current.Cursor = lastEmittedCursor
				if err := c.incrementalCheckpoint(binding.stateKey, current); err != nil {
					return err
				}
				documentsSinceLastCheckpoint = 0
			}
		}

		serializedDoc, err = shape.Encode(serializedDoc[:0], rowValues)
		if err != nil {
			return fmt.Errorf("serializing document: %w", err)
		}
		if err := c.Output.Documents(binding.index, serializedDoc); err != nil {
			return fmt.Errorf("emitting document: %w", err)
		}

		lastEmittedCursor = rowCursor
		haveLastEmittedCursor = true
		current.Index++
		resultRowCount++
		documentsSinceLastCheckpoint++
	}

	log.WithFields(log.Fields{
		"dataset":     binding.resource.Dataset,
		"streamType":  binding.resource.StreamType,
		"tableSuffix": current.Suffix,
		"results":     resultRowCount,
	}).Info("table query complete")
	return nil
}

func cursorValuesEqual(a, b any) bool {
	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case nil:
		return b == nil
	default:
		return reflect.DeepEqual(a, b)
	}
}

// wrapWatchdogErr converts a context-cancelled error into a more descriptive
// timeout error if cancellation came from the watchdog (i.e., parent ctx is
// still healthy).
func wrapWatchdogErr(parent, query context.Context, err error) error {
	if err == nil {
		return nil
	}
	if parent.Err() == nil && query.Err() != nil {
		return fmt.Errorf("polling watchdog timeout: %w", err)
	}
	return fmt.Errorf("error reading query results: %w", err)
}

func buildTableQuery(projectID, dataset string, stream *streamDef, suffix string, cursor any) string {
	var tableRef = bqclient.QuoteIdentifier(projectID) + "." + bqclient.QuoteTableName(dataset, stream.TablePrefix+suffix)
	var cursorCol = bqclient.QuoteIdentifier(stream.CursorColumn)
	if cursor == nil {
		return fmt.Sprintf("SELECT * FROM %s ORDER BY %s", tableRef, cursorCol)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s > @cursor ORDER BY %s", tableRef, cursorCol, cursorCol)
}

func buildQueryParams(stream *streamDef, cursor any) []bigquery.QueryParameter {
	if cursor == nil {
		return nil
	}
	var bound = cursor
	switch stream.CursorType {
	case "INT64":
		// JSON unmarshals integers into float64 by default. Convert to int64
		// when the cursor was reloaded from persisted state.
		switch v := cursor.(type) {
		case float64:
			bound = int64(v)
		case int64:
			bound = v
		case int:
			bound = int64(v)
		}
	case "STRING":
		if s, ok := cursor.(string); ok {
			bound = s
		}
	}
	return []bigquery.QueryParameter{{Name: "cursor", Value: bound}}
}

func (c *capture) checkpoint() error {
	bs, err := json.Marshal(c.State)
	if err != nil {
		return fmt.Errorf("serializing state checkpoint: %w", err)
	}
	return c.Output.Checkpoint(bs, false)
}

func marshalIncrementalCheckpoint(sk boilerplate.StateKey, current *currentTable) ([]byte, error) {
	if current == nil {
		return nil, fmt.Errorf("missing current table state")
	}
	return json.Marshal(map[string]any{
		"bindingStateV1": map[boilerplate.StateKey]any{
			sk: map[string]any{"current": current},
		},
	})
}

// incrementalCheckpoint emits a narrow merge patch which assumes Current
// already exists in the persisted state. That is established by startReadout's
// full checkpoint, or by the state loaded when resuming an interrupted readout.
func (c *capture) incrementalCheckpoint(sk boilerplate.StateKey, current *currentTable) error {
	var bs, err = marshalIncrementalCheckpoint(sk, current)
	if err != nil {
		return fmt.Errorf("serializing readout progress checkpoint: %w", err)
	}
	return c.Output.Checkpoint(bs, true)
}

// Compile-time check that Driver satisfies the connector interface.
var _ boilerplate.Connector = &Driver{}
