package sqlcapture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/encoding/json"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/sirupsen/logrus"
)

var (
	// TestShutdownAfterBackfill is a test behavior flag which causes the capture
	// to shut down after backfilling one chunk from one table. It is always false
	// in normal operation.
	TestShutdownAfterBackfill = false

	// TestShutdownAfterCaughtUp is a test behavior flag which causes the capture
	// to shut down after replication is all caught up and all tables are fully
	// backfilled. It is always false in normal operation.
	TestShutdownAfterCaughtUp = false

	// StreamingFenceInterval is a constant controlling how frequently the capture
	// will establish a new fence during indefinite streaming. It's declared as a
	// variable so that it can be overridden during tests.
	StreamingFenceInterval = 60 * time.Second
)

// PersistentState represents the part of a connector's state which can be serialized
// and emitted in a state checkpoint, and resumed from after a restart.
type PersistentState struct {
	Cursor  string                               `json:"cursor"`                   // The replication cursor of the most recent 'Commit' event
	Streams map[boilerplate.StateKey]*TableState `json:"bindingStateV1,omitempty"` // A mapping from runtime-provided state keys to table-specific state.
}

// Validate performs basic sanity-checking after a state has been parsed from JSON. More
// detailed checks are performed later on.
func (ps *PersistentState) Validate() error {
	return nil
}

// BindingsInState returns all the bindings having a particular persisted state, in sorted order for
// reproducibility.
func (c *Capture) BindingsInState(modes ...BackfillMode) []*Binding {
	var bindings []*Binding
	for _, binding := range c.Bindings {
		if slices.Contains(modes, BackfillMode(c.State.Streams[binding.StateKey].Mode)) {
			bindings = append(bindings, binding)
		}
	}
	slices.SortFunc(bindings, func(a, b *Binding) int { return strings.Compare(a.StreamID, b.StreamID) })
	return bindings
}

// BindingsCurrentlyActive returns all the bindings currently being captured.
func (c *Capture) BindingsCurrentlyActive() []*Binding {
	return c.BindingsInState(TableStatePreciseBackfill, TableStateUnfilteredBackfill, TableStateKeylessBackfill, TableStateActive)
}

// BindingsCurrentlyBackfilling returns all the bindings undergoing some sort of backfill.
func (c *Capture) BindingsCurrentlyBackfilling() []*Binding {
	return c.BindingsInState(TableStatePreciseBackfill, TableStateUnfilteredBackfill, TableStateKeylessBackfill)
}

// TableState represents the serializable/resumable state of a particular table's capture.
// It is mostly concerned with the "backfill" scanning process and the transition from that
// to logical replication.
type TableState struct {
	// Mode is either "Backfill" during the backfill scanning process
	// or "Active" once the backfill is complete.
	Mode string `json:"mode"`
	// KeyColumns is the "primary key" used for ordering/chunking the backfill scan.
	KeyColumns []string `json:"key_columns"`
	// Scanned is a FoundationDB-serialized tuple representing the KeyColumns
	// values of the last row which has been backfilled. Replication events will
	// only be emitted for rows <= this value while backfilling is in progress.
	Scanned []byte `json:"scanned"`
	// Metadata is some arbitrary amount of database-specific metadata
	// which needs to be tracked persistently on a per-table basis. The
	// original purpose is/was for tracking table schema information.
	Metadata json.RawMessage `json:"metadata,omitempty"`
	// BackfilledCount is a counter of the number of rows backfilled.
	BackfilledCount int `json:"backfilled"`
	// dirty is set whenever the table state changes, and cleared whenever
	// a state update is emitted. It should never be serialized itself.
	dirty bool
}

const (
	// The table is not being captured, but it used to be. Since JSON-patch
	// deletion is tricky we represent this explicitly.
	TableStateIgnore = "Ignore"

	// The table is going to be backfilled and/or activated in the near future
	// when we reach a suitable point.
	TableStatePending = "Pending"

	// A backfill is currently ongoing with the keyed backfill strategy and a
	// replication event filter so that only rows in the backfilled region of
	// the table will be emitted. Uses a short name for historical reasons,
	// this used to be the only backfill mode.
	TableStatePreciseBackfill = "Backfill"

	// A backfill is currently ongoing with the keyed backfill strategy and no
	// replication event filter.
	TableStateUnfilteredBackfill = "UnfilteredBackfill"

	// A backfill is currently ongoing with the keyless backfill strategy and
	// no replication event filter.
	TableStateKeylessBackfill = "KeylessBackfill"

	// The table is fully backfilled and we're just streaming replication events.
	TableStateActive = "Active"

	// The table is supposed to be captured, but went missing. We're waiting for
	// it to come back at some point in order to mark it as pending again.
	TableStateMissing = "Missing"
)

// Capture encapsulates the generic process of capturing data from a SQL database
// via replication, backfilling preexisting table contents, and emitting records/state
// updates. It uses the `Database` interface to interact with a specific database.
type Capture struct {
	Bindings map[string]*Binding     // Map from fully-qualified stream IDs to the corresponding binding information
	State    *PersistentState        // State read from `state.json` and emitted as updates
	Output   *boilerplate.PullOutput // The encoder to which records and state updates are written
	Database Database                // The database-specific interface which is operated by the generic Capture logic

	// A mutex-guarded list of checkpoint cursor values. Values are appended by
	// emitState() whenever it outputs a checkpoint and removed whenever the
	// acknowledgement-relaying goroutine receives an Acknowledge message.
	pending struct {
		sync.Mutex
		cursors []string
	}
}

const (
	automatedDiagnosticsTimeout = 60 * time.Second // How long to wait *after the point where the fence was requested* before triggering automated diagnostics.
	streamIdleWarning           = 60 * time.Second // After `streamIdleWarning` has elapsed since the last replication event, we log a warning.
	streamProgressInterval      = 60 * time.Second // After `streamProgressInterval` the replication streaming code may log a progress report.
)

var (
	ErrFenceNotReached = fmt.Errorf("replication stream closed before reaching fence")
)

// Run is the top level entry point of the capture process.
func (c *Capture) Run(ctx context.Context) (err error) {
	// Perform discovery and log the full results for convenience. This info
	// will be needed when activating all currently-active bindings below.
	logrus.Info("discovering tables")
	discovery, err := c.Database.DiscoverTables(ctx)
	if err != nil {
		return fmt.Errorf("error discovering database tables: %w", err)
	}
	for streamID, discoveryInfo := range discovery {
		logrus.WithFields(logrus.Fields{
			"table":      streamID,
			"primaryKey": discoveryInfo.PrimaryKey,
			"columns":    discoveryInfo.Columns,
		}).Debug("discovered table")
	}

	if err := c.reconcileStateWithBindings(ctx); err != nil {
		return fmt.Errorf("error reconciling capture state with bindings: %w", err)
	}

	replStream, err := c.Database.ReplicationStream(ctx, c.State.Cursor)
	if err != nil {
		return fmt.Errorf("error creating replication stream: %w", err)
	}
	for _, binding := range c.BindingsCurrentlyActive() {
		var state = c.State.Streams[binding.StateKey]
		var streamID = binding.StreamID
		if err := replStream.ActivateTable(ctx, streamID, state.KeyColumns, discovery[streamID], state.Metadata); err != nil {
			return fmt.Errorf("error activating table %q: %w", streamID, err)
		}
	}
	if err := replStream.StartReplication(ctx); err != nil {
		return fmt.Errorf("error starting replication: %w", err)
	}
	defer func() {
		if streamErr := replStream.Close(ctx); streamErr != nil && errors.Is(err, ErrFenceNotReached) {
			err = streamErr
		}
	}()

	// Start a goroutine which will read acknowledgement messages from stdin
	// and relay them to the replication stream acknowledgement.
	go func() {
		if err := c.acknowledgeWorker(ctx, c.Output, replStream); err != nil {
			logrus.WithField("err", err).Fatal("error relaying acknowledgements from stdin")
		}
	}()

	// Perform an initial "catch-up" stream-to-fence before entering the main capture loop.
	if err := c.streamToFence(ctx, replStream, 0, true); err != nil {
		return fmt.Errorf("error streaming until fence: %w", err)
	}

	for ctx.Err() == nil {
		// If any streams are currently pending, initialize them so they can start backfilling.
		if err := c.activatePendingStreams(ctx, replStream); err != nil {
			return fmt.Errorf("error initializing pending streams: %w", err)
		}

		// If any tables are currently backfilling, go perform another backfill iteration.
		if c.BindingsCurrentlyBackfilling() != nil {
			if err := c.backfillStreams(ctx); err != nil {
				return fmt.Errorf("error performing backfill: %w", err)
			} else if err := c.streamToFence(ctx, replStream, 0, false); err != nil {
				return fmt.Errorf("error streaming until fence: %w", err)
			} else if err := c.emitState(); err != nil {
				return err
			}

			if TestShutdownAfterBackfill {
				return nil // In tests we sometimes want to shut down here
			}
			continue // Repeat the main loop from the top
		}

		// We often want to shut down at this point in tests. Before doing so, we emit
		// a state checkpoint to ensure that streams reliably transition into the Active
		// state during tests even if there is no backfill work to do.
		if TestShutdownAfterCaughtUp {
			return c.emitState()
		}

		// Finally, since there's no other work to do right now, we just stream changes for a period of time.
		if err := c.streamToFence(ctx, replStream, StreamingFenceInterval, true); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// reconcileStateWithBindings updates the capture's state to reflect any added or removed bindings.
func (c *Capture) reconcileStateWithBindings(_ context.Context) error {
	// Create the Streams map if nil
	if c.State.Streams == nil {
		c.State.Streams = make(map[boilerplate.StateKey]*TableState)
	}

	// Add a state entry for any new bindings, initially "Pending"
	allStreamsAreNew := true
	for _, binding := range c.Bindings {
		var stateKey = binding.StateKey

		// See if the stream is already initialized. If it's not, then create it.
		var streamState, ok = c.State.Streams[stateKey]
		if ok && streamState.Mode != TableStateIgnore {
			allStreamsAreNew = false
			continue
		}

		logrus.WithField("stateKey", stateKey).Info("binding added to capture")
		c.State.Streams[stateKey] = &TableState{Mode: TableStatePending, dirty: true}
	}

	// When a binding is removed we change the table state to "Ignore
	streamExistsInCatalog := func(sk boilerplate.StateKey) bool {
		for _, b := range c.Bindings {
			if b.StateKey == sk {
				return true
			}
		}
		return false
	}
	for stateKey, state := range c.State.Streams {
		if state.Mode != TableStateIgnore && !streamExistsInCatalog(stateKey) {
			logrus.WithField("stateKey", stateKey).Info("binding removed from capture")
			c.State.Streams[stateKey] = &TableState{
				Mode:     TableStateIgnore,
				Metadata: json.RawMessage("null"), // Explicit null to clear out old metadata
				dirty:    true,
			}
		}
	}

	// If all bindings are new (or there are no bindings), reset the replication cursor. This is
	// safe because logically if no streams are currently active then we can't miss any events of
	// interest when the replication stream jumps ahead, and doing this allows the user an easy
	// recovery path after WAL deletion, just hit the "Backfill Everything" button in the UI.
	if allStreamsAreNew {
		if len(c.Bindings) > 0 {
			logrus.Info("all bindings are new, resetting replication cursor")
		} else {
			logrus.Info("capture has no bindings, resetting replication cursor")
		}
		c.State.Cursor = ""
	}

	// Emit the new state to stdout. This isn't strictly necessary but it helps to make
	// the emitted sequence of state updates a lot more readable.
	return c.emitState()
}

// activatePendingStreams transitions streams from a "Pending" state to being captured once they're eligible.
func (c *Capture) activatePendingStreams(ctx context.Context, replStream ReplicationStream) error {
	// Fast path for the common case, so we can just run this function at the top of the main loop
	// and rely on it normally exiting quickly without doing any unnecessary work.
	if len(c.BindingsInState(TableStateMissing, TableStatePending)) == 0 {
		return nil
	}

	// Get the latest discovery information.
	var discovery, err = c.Database.DiscoverTables(ctx)
	if err != nil {
		return err
	}

	// See if any missing tables have since reappeared, and if so mark them as pending again.
	for _, binding := range c.BindingsInState(TableStateMissing) {
		if _, ok := discovery[binding.StreamID]; ok {
			c.State.Streams[binding.StateKey] = &TableState{Mode: TableStatePending, dirty: true}
		}
	}

	// Initialize all pending streams to an appropriate backfill or active state.
	for _, binding := range c.BindingsInState(TableStatePending) {
		var streamID = binding.StreamID
		var stateKey = binding.StateKey

		// Look up the stream state and mark it dirty since we intend to update it immediately.
		var state = c.State.Streams[stateKey]
		state.dirty = true

		var discoveryInfo = discovery[streamID]
		if discoveryInfo == nil {
			return fmt.Errorf("stream %q is a configured binding of this capture, but doesn't exist or isn't visible with current permissions", streamID)
		}

		// Only real tables with table_type = 'BASE TABLE' can be captured via replication.
		// Discovery should not suggest capturing from views, but there are an unknown
		// number of preexisting captures which may have views in their bindings, and we
		// would rather silently start ignoring these rather than making them immediately
		// be errors.
		//
		// This bit of logic is part of a bugfix in August 2023 and we would like to
		// remove it in the future once it will not break any otherwise-successful
		// captures.
		if !discoveryInfo.BaseTable {
			logrus.WithField("stream", streamID).Warn("automatically ignoring a binding whose type is not `BASE TABLE`")
			state.Mode = TableStateIgnore
			continue
		}

		// Select the appropriate state transition depending on the backfill mode in the resource config.
		logrus.WithFields(logrus.Fields{"stream": streamID, "mode": binding.Resource.Mode}).Info("activating replication for stream")
		switch binding.Resource.Mode {
		case BackfillModeAutomatic:
			if discoveryInfo.UnpredictableKeyOrdering {
				logrus.WithField("stream", streamID).Info("autoselected unfiltered (normal) backfill mode (database key ordering is unpredictable)")
				state.Mode = TableStateUnfilteredBackfill
			} else {
				logrus.WithField("stream", streamID).Info("autoselected precise backfill mode")
				state.Mode = TableStatePreciseBackfill
			}
		case BackfillModePrecise:
			logrus.WithField("stream", streamID).Info("user selected precise backfill mode")
			state.Mode = TableStatePreciseBackfill
		case BackfillModeNormal:
			logrus.WithField("stream", streamID).Info("user selected unfiltered (normal) backfill mode")
			state.Mode = TableStateUnfilteredBackfill
		case BackfillModeWithoutKey:
			logrus.WithField("stream", streamID).Info("user selected keyless backfill mode")
			state.Mode = TableStateKeylessBackfill
		case BackfillModeOnlyChanges:
			logrus.WithField("stream", streamID).Info("user selected only changes, skipping backfill")
			state.Mode = TableStateActive
		default:
			return fmt.Errorf("invalid backfill mode %q for stream %q", binding.Resource.Mode, streamID)
		}

		// When initializing a binding with the precise or unfiltered backfill modes, we'll need a primary key.
		if state.Mode == TableStatePreciseBackfill || state.Mode == TableStateUnfilteredBackfill {
			if len(binding.Resource.PrimaryKey) > 0 {
				logrus.WithFields(logrus.Fields{"stream": streamID, "key": binding.Resource.PrimaryKey}).Debug("backfill key overriden by resource config")
				state.KeyColumns = binding.Resource.PrimaryKey
			} else {
				logrus.WithFields(logrus.Fields{"stream": streamID, "key": binding.CollectionKey}).Debug("using collection primary key as backfill key")
				state.KeyColumns = nil
				for _, ptr := range binding.CollectionKey {
					state.KeyColumns = append(state.KeyColumns, collectionKeyToPrimaryKey(ptr))
				}
			}

			if !slices.Equal(state.KeyColumns, discoveryInfo.PrimaryKey) {
				logrus.WithFields(logrus.Fields{
					"stream":      streamID,
					"backfillKey": state.KeyColumns,
					"databaseKey": discoveryInfo.PrimaryKey,
				}).Warn("backfill key differs from database table primary key")
			}
		}

		if err := replStream.ActivateTable(ctx, streamID, state.KeyColumns, discoveryInfo, state.Metadata); err != nil {
			return fmt.Errorf("error activating replication for table %q: %w", streamID, err)
		}
	}

	// Transition streams from "Backfill" to "Active" if we're supposed to skip
	// backfilling that one. Combined with the previous Pending->Backfill logic
	// this may cause a newly-added stream to go Pending->Backfill->Active, but
	// it can also terminate a partially-completed backfill if the connector was
	// restarted with a changed configuration after the backfill began.
	//
	// This means that a backfill can still be terminated/skipped, even after the
	// user starts their capture and only then realizes that it's going to take days
	// to backfill their multiple-terabyte table.
	//
	// TODO(wgd): In theory this logic could have been merged with the new 'Backfill Mode'
	// configuration logic, but that would make that logic more complex and I would like
	// to deprecate the whole 'SkipBackfills' configuration property in the near future
	// so I have left this little blob of logic nicely self-contained here.
	for _, binding := range c.BindingsCurrentlyBackfilling() {
		if !c.Database.ShouldBackfill(binding.StreamID) {
			var state = c.State.Streams[binding.StateKey]
			logrus.WithFields(logrus.Fields{
				"stream":  binding.StreamID,
				"scanned": state.Scanned,
			}).Info("skipping backfill for stream")
			state.Mode = TableStateActive
			state.Scanned = nil
			state.dirty = true
			c.State.Streams[binding.StateKey] = state
		}
	}
	return nil
}

// streamToFence processes replication events until after a new fence is reached.
//
// If reportFlush is true then all database transaction commits will be reported as
// Flow transaction commits, and if it's false then they will not (this is used in
// backfills to ensure that a backfill chunk and all concurrent changes commit as
// a single Flow transaction).
//
// The fenceAfter argument is passed to the underlying replication stream, so that
// it can make sure to stream changes for at least that length of time.
func (c *Capture) streamToFence(ctx context.Context, replStream ReplicationStream, fenceAfter time.Duration, reportFlush bool) error {
	logrus.WithField("fenceAfter", fenceAfter.String()).Info("streaming to fence")

	// Log a warning and perform replication diagnostics if we don't observe the next fence within a few minutes
	var diagnosticsTimeout = time.AfterFunc(fenceAfter+automatedDiagnosticsTimeout, func() {
		logrus.Warn("replication streaming has been ongoing for an unexpectedly long amount of time, running replication diagnostics")
		if err := c.Database.ReplicationDiagnostics(ctx); err != nil {
			logrus.WithField("err", err).Error("replication diagnostics error")
		}
	})
	defer diagnosticsTimeout.Stop()

	// When streaming completes (because we've either reached the fence or encountered an error),
	// log the number of events which have been processed.
	var eventCount int
	defer func() { logrus.WithField("events", eventCount).Info("processed replication events") }()

	return replStream.StreamToFence(ctx, fenceAfter, func(event DatabaseEvent) error {
		eventCount++

		// Flush events update the checkpoint LSN and may trigger a state update.
		if event, ok := event.(*FlushEvent); ok {
			c.State.Cursor = event.Cursor
			if reportFlush {
				if err := c.emitState(); err != nil {
					return fmt.Errorf("error emitting state update: %w", err)
				}
			}
			return nil
		}

		return c.handleReplicationEvent(event)
	})
}

func (c *Capture) handleReplicationEvent(event DatabaseEvent) error {
	// Keepalive events do nothing.
	if _, ok := event.(*KeepaliveEvent); ok {
		return nil
	}

	// Table drop events indicate that the table has disappeared during replication.
	if event, ok := event.(*TableDropEvent); ok {
		var binding = c.Bindings[event.StreamID]
		if binding == nil {
			return nil // Should be impossible, but safe to ignore
		}
		logrus.WithFields(logrus.Fields{"stream": event.StreamID, "cause": event.Cause}).Info("marking table as missing")
		c.State.Streams[binding.StateKey] = &TableState{
			Mode:     TableStateMissing,
			Metadata: json.RawMessage("null"), // Explicit null to clear out old metadata
			dirty:    true,
		}
		return nil
	}

	// Metadata events update the per-table metadata and dirty flag.
	// They have no other effect, the new metadata will only be written
	// as part of a subsequent state checkpoint.
	if event, ok := event.(*MetadataEvent); ok {
		var binding = c.Bindings[event.StreamID]
		if binding == nil {
			// Metadata event for a table that we don't have a binding for, which can be ignored.
			return nil
		}

		var stateKey = binding.StateKey
		if state, ok := c.State.Streams[stateKey]; ok {
			logrus.WithField("stateKey", stateKey).Trace("stream metadata updated")
			state.Metadata = event.Metadata
			state.dirty = true
			c.State.Streams[stateKey] = state
		}
		return nil
	}

	// Any other events processed here must be ChangeEvents.
	if _, ok := event.(*ChangeEvent); !ok {
		return fmt.Errorf("unhandled replication event %q", event.String())
	}
	var change = event.(*ChangeEvent)
	var streamID = change.Source.Common().StreamID()
	var binding = c.Bindings[streamID]
	var tableState *TableState
	if binding != nil {
		tableState = c.State.Streams[binding.StateKey]
	}

	// Decide what to do with the change event based on the state of the table.
	if tableState == nil || tableState.Mode == "" || tableState.Mode == TableStateIgnore {
		logrus.WithFields(logrus.Fields{
			"stream": streamID,
			"op":     change.Operation,
		}).Debug("ignoring stream")
		return nil
	}
	if tableState.Mode == TableStateActive || tableState.Mode == TableStateKeylessBackfill || tableState.Mode == TableStateUnfilteredBackfill {
		if err := c.emitChange(change); err != nil {
			return fmt.Errorf("error handling replication event for %q: %w", streamID, err)
		}
		return nil
	}
	if tableState.Mode == TableStatePreciseBackfill {
		// When a table is being backfilled precisely, replication events lying within the
		// already-backfilled portion of the table should be emitted, while replication events
		// beyond that point should be ignored (since we'll reach that row later in the backfill).
		if compareTuples(change.RowKey, tableState.Scanned) <= 0 {
			if err := c.emitChange(change); err != nil {
				return fmt.Errorf("error handling replication event for %q: %w", streamID, err)
			}
		}
		return nil
	}

	return fmt.Errorf("table %q in invalid mode %q", streamID, tableState.Mode)
}

func (c *Capture) backfillStreams(ctx context.Context) error {
	var bindings = c.BindingsCurrentlyBackfilling()
	var streams = make([]string, 0, len(bindings))
	for _, b := range bindings {
		streams = append(streams, b.StreamID)
	}

	// Select one binding at random to backfill at a time. On average this works
	// as well as any other policy, and just doing one at a time means that we
	// can size the relevant constants without worrying about how many tables
	// might be concurrently backfilling.
	if len(streams) != 0 {
		var streamID = streams[rand.Intn(len(streams))]
		logrus.WithFields(logrus.Fields{
			"count":    len(streams),
			"selected": streamID,
		}).Info("backfilling streams")
		if err := c.backfillStream(ctx, streamID); err != nil {
			return err
		}
	}
	return nil
}

func (c *Capture) backfillStream(ctx context.Context, streamID string) error {
	var stateKey = c.Bindings[streamID].StateKey
	var streamState = c.State.Streams[stateKey]

	var discovery, err = c.Database.DiscoverTables(ctx)
	if err != nil {
		return err
	}
	discoveryInfo, ok := discovery[streamID]
	if !ok {
		return fmt.Errorf("unknown table %q", streamID)
	}

	// Process backfill query results as a callback-driven stream.
	var lastRowKey = streamState.Scanned
	var eventCount int
	backfillComplete, err := c.Database.ScanTableChunk(ctx, discoveryInfo, streamState, func(event *ChangeEvent) error {
		if streamState.Mode == TableStatePreciseBackfill && compareTuples(lastRowKey, event.RowKey) > 0 {
			// Sanity check that when performing a "precise" backfill the DB's ordering of
			// result rows must match our own bytewise lexicographic ordering of serialized
			// row keys.
			//
			// This ordering property must always be true for every possible key, because if it's
			// violated the filtering logic (applied based on row keys in handleReplicationEvent)
			// could miscategorize a replication event, which could result in incorrect data. But
			// we can't verify that for _every_ possible key, so instead we opportunistically check
			// ourselves here.
			//
			// If this property cannot be guaranteed for a particular table then the discovery logic
			// should have set `UnpredictableKeyOrdering` to true, which should have resulted in a
			// backfill using the "UnfilteredBackfill" mode instead, which would not perform that
			// filtering or this sanity check.
			return fmt.Errorf("scan key ordering failure: last=%q, next=%q", lastRowKey, event.RowKey)
		}
		lastRowKey = event.RowKey

		if err := c.emitChange(event); err != nil {
			return fmt.Errorf("error emitting %q backfill row: %w", streamID, err)
		}
		eventCount++
		return nil
	})
	if err != nil {
		return fmt.Errorf("error scanning table %q: %w", streamID, err)
	}

	// Update stream state to reflect backfill results
	logrus.WithFields(logrus.Fields{
		"stream": streamID,
		"rows":   eventCount,
	}).Info("processed backfill rows")
	var state = c.State.Streams[stateKey]
	state.BackfilledCount += eventCount
	if backfillComplete {
		logrus.WithField("stream", streamID).Info("backfill completed")
		state.Mode = TableStateActive
		state.Scanned = nil
	} else {
		state.Scanned = lastRowKey
	}
	state.dirty = true
	c.State.Streams[stateKey] = state
	return nil
}

func (c *Capture) emitChange(event *ChangeEvent) error {
	var record map[string]interface{}
	var meta = struct {
		Operation ChangeOp               `json:"op"`
		Source    SourceMetadata         `json:"source"`
		Before    map[string]interface{} `json:"before,omitempty"`
	}{
		Operation: event.Operation,
		Source:    event.Source,
		Before:    nil,
	}
	switch event.Operation {
	case InsertOp:
		record = event.After // Before is never used.
	case UpdateOp:
		meta.Before, record = event.Before, event.After
	case DeleteOp:
		record = event.Before // After is never used.
	}
	if record == nil {
		logrus.WithField("op", event.Operation).Warn("change event data map is nil")
		record = make(map[string]interface{})
	}
	record["_meta"] = &meta

	var sourceCommon = event.Source.Common()
	var streamID = JoinStreamID(sourceCommon.Schema, sourceCommon.Table)
	var binding, ok = c.Bindings[streamID]
	if !ok {
		return fmt.Errorf("capture output to invalid stream %q", streamID)
	}

	var bs, err = json.Marshal(record)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"document": fmt.Sprintf("%#v", record),
			"stream":   streamID,
			"err":      err,
		}).Error("document serialization error")
		return fmt.Errorf("error serializing document from stream %q: %w", streamID, err)
	}
	return c.Output.Documents(int(binding.Index), bs)
}

func (c *Capture) emitState() error {
	// Put together an update which includes only those streams which have changed
	// since the last state output. At the same time, clear the dirty flags on all
	// those tables.
	var streams = make(map[boilerplate.StateKey]*TableState)
	for stateKey, state := range c.State.Streams {
		if state.dirty {
			state.dirty = false
			streams[stateKey] = state
		}
	}
	var msg = &PersistentState{
		Cursor:  c.State.Cursor,
		Streams: streams,
	}

	c.pending.Lock()
	defer c.pending.Unlock()
	c.pending.cursors = append(c.pending.cursors, msg.Cursor)

	var bs, err = json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error serializing state checkpoint: %w", err)
	}
	logrus.WithField("state", string(bs)).Trace("emitting state update")
	return c.Output.Checkpoint(bs, true)
}

func (c *Capture) acknowledgeWorker(ctx context.Context, stream *boilerplate.PullOutput, replStream ReplicationStream) error {
	for {
		var request, err = stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if err = request.Validate_(); err != nil {
			return fmt.Errorf("validating request: %w", err)
		}

		switch {
		case request.Acknowledge != nil:
			if err := c.handleAcknowledgement(ctx, int(request.Acknowledge.Checkpoints), replStream); err != nil {
				return fmt.Errorf("error handling acknowledgement: %w", err)
			}
		default:
			return fmt.Errorf("unexpected message %#v", request)
		}
	}
}

func (c *Capture) handleAcknowledgement(ctx context.Context, count int, replStream ReplicationStream) error {
	c.pending.Lock()
	defer c.pending.Unlock()
	if count == 0 {
		// Originally the Acknowledge message implicitly meant acknowledging one checkpoint,
		// so in the absence of a count we will continue to interpret it that way.
		count = 1
	}
	if count > len(c.pending.cursors) {
		return fmt.Errorf("invalid acknowledgement count %d, only %d pending checkpoints", count, len(c.pending.cursors))
	}

	var cursor = c.pending.cursors[count-1]
	c.pending.cursors = c.pending.cursors[count:]
	logrus.WithFields(logrus.Fields{
		"count":  count,
		"cursor": cursor,
	}).Debug("acknowledged up to cursor")
	return replStream.Acknowledge(ctx, cursor)
}

type StreamID = string

// JoinStreamID combines a namespace and a stream name into a dotted name like "public.foo_table".
func JoinStreamID(namespace, stream string) StreamID {
	return strings.ToLower(namespace + "." + stream)
}
