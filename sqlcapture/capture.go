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
	pc "github.com/estuary/flow/go/protocols/capture"
	log "github.com/sirupsen/logrus"
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

	// When the TestShutdownAfterCaughtUp test behavior flag is hit, this variable
	// will be set to the final state checkpoint. It does nothing in real-world
	// operation. The purpose of this variable is solely so benchmarks can update
	// the final capture state checkpoint without having to process every message
	// the connector outputs, identify the checkpoints, and merge them together.
	FinalStateCheckpoint json.RawMessage

	// StreamingFenceInterval is a constant controlling how frequently the capture
	// will establish a new fence during indefinite streaming. It's declared as a
	// variable so that it can be overridden during tests.
	StreamingFenceInterval = 60 * time.Second
)

// PersistentState represents the part of a connector's state which can be serialized
// and emitted in a state checkpoint, and resumed from after a restart.
type PersistentState struct {
	Cursor  json.RawMessage                      `json:"cursor"`                   // The replication cursor of the most recent 'Commit' event
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
	slices.SortFunc(bindings, func(a, b *Binding) int { return strings.Compare(a.StreamID.String(), b.StreamID.String()) })
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
	// Scanned represents the current position of the backfill. For precise backfills the
	// byte serialization ordering must strictly match the database backfill ordering,
	// because we will only emit replication events for rows <= this value.
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
	Bindings map[StreamID]*Binding   // Map from fully-qualified stream IDs to the corresponding binding information
	State    *PersistentState        // State read from `state.json` and emitted as updates
	Output   *boilerplate.PullOutput // The encoder to which records and state updates are written
	Database Database                // The database-specific interface which is operated by the generic Capture logic

	emitChangeBuf []byte               // A reusable buffer used for serialized JSON documents in emitChange(). Note that this is not thread-safe, but since it's single-threaded we're okay for now.
	emitChangeMsg pc.Response          // A reusable object for emitting change events. This is not thread-safe, but since it's single-threaded we're okay for now.
	emitChangeDoc pc.Response_Captured // A reusable object for emitting change events. This is not thread-safe, but since it's single-threaded we're okay for now.

	// A mutex-guarded list of checkpoint cursor values. Values are appended by
	// emitState() whenever it outputs a checkpoint and removed whenever the
	// acknowledgement-relaying goroutine receives an Acknowledge message.
	pending struct {
		sync.Mutex
		cursors []json.RawMessage
	}
}

const (
	automatedDiagnosticsTimeout = 60 * time.Second // How long to wait *after the point where the fence was requested* before triggering automated diagnostics.
	streamIdleWarning           = 60 * time.Second // After `streamIdleWarning` has elapsed since the last replication event, we log a warning.
	streamProgressInterval      = 60 * time.Second // After `streamProgressInterval` the replication streaming code may log a progress report.
	rediscoverInterval          = 5 * time.Minute  // The capture will re-run discovery and reinitialize missing/pending tables this frequently.
)

var (
	ErrFenceNotReached = fmt.Errorf("replication stream closed before reaching fence")
)

// Run is the top level entry point of the capture process.
func (c *Capture) Run(ctx context.Context) (err error) {
	// Perform discovery and log the full results for convenience. This info
	// will be needed when activating all currently-active bindings below.
	log.WithField("eventType", "connectorStatus").Info("Discovering database tables")
	discovery, err := c.Database.DiscoverTables(ctx)
	if err != nil {
		return fmt.Errorf("error discovering database tables: %w", err)
	} else if err := c.emitSourcedSchemas(discovery); err != nil {
		return err
	}
	for streamID, discoveryInfo := range discovery {
		log.WithFields(log.Fields{
			"table":     streamID,
			"discovery": discoveryInfo,
		}).Debug("discovered table")
	}

	if err := c.reconcileStateWithBindings(ctx); err != nil {
		return fmt.Errorf("error reconciling capture state with bindings: %w", err)
	}

	log.WithField("cursor", string(c.State.Cursor)).WithField("eventType", "connectorStatus").Info("Initializing replication")
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
	if err := replStream.StartReplication(ctx, discovery); err != nil {
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
			log.WithField("err", err).Fatal("error relaying acknowledgements from stdin")
		}
	}()

	// Perform an initial "catch-up" stream-to-fence before entering the main capture loop.
	log.WithField("eventType", "connectorStatus").Info("Catching up on CDC history")
	if err := c.streamToFence(ctx, replStream, 0, true); err != nil {
		return fmt.Errorf("error streaming until fence: %w", err)
	}

	var rediscoverAfter time.Time
	for ctx.Err() == nil {
		if time.Now().After(rediscoverAfter) {
			log.WithField("eventType", "connectorStatus").Info("Rediscovering database tables")
			discovery, err = c.Database.DiscoverTables(ctx)
			if err != nil {
				return fmt.Errorf("error discovering database tables: %w", err)
			} else if err := c.emitSourcedSchemas(discovery); err != nil {
				return err
			} else if err := c.emitState(); err != nil { // Emit state so any SourcedSchema changes commit immediately.
				return err
			}
			// If any streams are currently pending, initialize them so they can start backfilling.
			if err := c.activatePendingStreams(ctx, discovery, replStream); err != nil {
				return fmt.Errorf("error initializing pending streams: %w", err)
			}
			rediscoverAfter = time.Now().Add(rediscoverInterval)
		}

		// If any tables are currently backfilling, go perform another backfill iteration.
		if c.BindingsCurrentlyBackfilling() != nil {
			log.WithField("eventType", "connectorStatus").Infof("Backfilling %d out of %d tables",
				len(c.BindingsCurrentlyBackfilling()),
				len(c.BindingsCurrentlyActive()),
			)
			if err := c.backfillStreams(ctx, discovery); err != nil {
				return fmt.Errorf("error performing backfill: %w", err)
			} else if err := c.streamToFence(ctx, replStream, 0, false); err != nil {
				return fmt.Errorf("error streaming until fence: %w", err)
			} else if err := c.emitState(); err != nil {
				return err
			}

			if TestShutdownAfterBackfill {
				log.Info("Shutting down after backfill due to TestShutdownAfterBackfill")
				return nil // In tests we sometimes want to shut down here
			}
			continue // Repeat the main loop from the top
		}

		// We often want to shut down at this point in tests. Before doing so, we emit
		// a state checkpoint to ensure that streams reliably transition into the Active
		// state during tests even if there is no backfill work to do.
		if TestShutdownAfterCaughtUp {
			log.Info("Shutting down after backfill due to TestShutdownAfterCaughtUp")
			if bs, err := json.Marshal(c.State); err == nil {
				FinalStateCheckpoint = bs // Set final state checkpoint
			}
			return c.emitState()
		}

		// Finally, since there's no other work to do right now, we just stream changes for a period of time.
		log.WithField("eventType", "connectorStatus").Infof("Streaming CDC events (all %d tables are backfilled)",
			len(c.BindingsCurrentlyActive()),
		)
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

		log.WithField("stateKey", stateKey).Info("binding added to capture")
		c.State.Streams[stateKey] = &TableState{Mode: TableStatePending, dirty: true}
	}

	// When a binding is removed we change the table state to "Ignore". The way our dirty-
	// flag update logic works makes a JSON-patch deletion of the whole thing tricky, but
	// we could theoretically change this someday if "excessive size of state checkpoints
	// due to deleted bindings" ever becomes an actual issue in practice.
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
			log.WithField("stateKey", stateKey).Info("binding removed from capture")
			c.State.Streams[stateKey] = &TableState{
				Mode:     TableStateIgnore,
				Metadata: json.RawMessage("null"), // Explicit null to clear out old metadata
				dirty:    true,
			}
		}
	}

	// This is part of a migration dated 2024-12-12 and can be removed within a few days. The
	// only purpose here is to make sure that preexisting only-changes bindings have an
	// initialized StateKey value, the same as activatePendingStreams has been modified to
	// produce going forward.
	for _, binding := range c.Bindings {
		if state, ok := c.State.Streams[binding.StateKey]; ok && binding.Resource.Mode == BackfillModeOnlyChanges && state.Mode == TableStateActive && state.KeyColumns == nil {
			if !slices.Equal(binding.CollectionKey, c.Database.FallbackCollectionKey()) {
				for _, ptr := range binding.CollectionKey {
					state.KeyColumns = append(state.KeyColumns, collectionKeyToPrimaryKey(ptr))
				}
			}
			if len(binding.Resource.PrimaryKey) > 0 {
				log.WithFields(log.Fields{"stateKey": binding.StateKey, "key": binding.Resource.PrimaryKey}).Debug("key overriden by resource config")
				state.KeyColumns = binding.Resource.PrimaryKey
			}
			log.WithField("stateKey", binding.StateKey).WithField("key", state.KeyColumns).Info("initialized missing KeyColumns state for only-changes binding")
			state.dirty = true
		}
	}

	// If all bindings are new (or there are no bindings), reset the replication cursor. This is
	// safe because logically if no streams are currently active then we can't miss any events of
	// interest when the replication stream jumps ahead, and doing this allows the user an easy
	// recovery path after WAL deletion, just hit the "Backfill Everything" button in the UI.
	if allStreamsAreNew {
		if len(c.Bindings) > 0 {
			log.Info("all bindings are new, resetting replication cursor")
		} else {
			log.Info("capture has no bindings, resetting replication cursor")
		}
		c.State.Cursor = nil
	}

	// Emit the new state to stdout. This isn't strictly necessary but it helps to make
	// the emitted sequence of state updates a lot more readable.
	return c.emitState()
}

// activatePendingStreams transitions streams from a "Pending" state to being captured once they're eligible.
func (c *Capture) activatePendingStreams(ctx context.Context, discovery map[StreamID]*DiscoveryInfo, replStream ReplicationStream) error {
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
			log.WithField("stream", streamID).Warn("automatically ignoring a binding whose type is not `BASE TABLE`")
			state.Mode = TableStateIgnore
			continue
		}

		// Translate the collection key pointers back into a list of column names which form the key.
		//
		// NOTE(2024-12-12): This can also be overridden by explicitly specifying a key in the resource
		// config, but that ability isn't surfaced to users and is such an old backwards-compatibility
		// feature that there may not actually be anyone using it. We should consider deprecation.
		state.KeyColumns = nil
		if !slices.Equal(binding.CollectionKey, c.Database.FallbackCollectionKey()) {
			for _, ptr := range binding.CollectionKey {
				state.KeyColumns = append(state.KeyColumns, collectionKeyToPrimaryKey(ptr))
			}
		}
		if len(binding.Resource.PrimaryKey) > 0 {
			log.WithFields(log.Fields{"stream": streamID, "key": binding.Resource.PrimaryKey}).Debug("key overriden by resource config")
			state.KeyColumns = binding.Resource.PrimaryKey
		}

		// Select the appropriate state transition depending on the backfill mode in the resource config.
		log.WithFields(log.Fields{"stream": streamID, "mode": binding.Resource.Mode}).Info("activating replication for stream")
		switch binding.Resource.Mode {
		case BackfillModeAutomatic:
			if discoveryInfo.UnpredictableKeyOrdering {
				log.WithField("stream", streamID).Info("autoselected unfiltered (normal) backfill mode (database key ordering is unpredictable)")
				state.Mode = TableStateUnfilteredBackfill
			} else {
				log.WithField("stream", streamID).Info("autoselected precise backfill mode")
				state.Mode = TableStatePreciseBackfill
			}
		case BackfillModePrecise:
			log.WithField("stream", streamID).Info("user selected precise backfill mode")
			state.Mode = TableStatePreciseBackfill
		case BackfillModeNormal:
			log.WithField("stream", streamID).Info("user selected unfiltered (normal) backfill mode")
			state.Mode = TableStateUnfilteredBackfill
		case BackfillModeWithoutKey:
			log.WithField("stream", streamID).Info("user selected keyless backfill mode")
			state.Mode = TableStateKeylessBackfill
			state.KeyColumns = nil
		case BackfillModeOnlyChanges:
			log.WithField("stream", streamID).Info("user selected only changes, skipping backfill")
			state.Mode = TableStateActive
		default:
			return fmt.Errorf("invalid backfill mode %q for stream %q", binding.Resource.Mode, streamID)
		}

		// Log an informational notice if the key we'll be using for a backfill differs from
		// the discovered primary key of a table.
		if state.Mode == TableStatePreciseBackfill || state.Mode == TableStateUnfilteredBackfill {
			log.WithFields(log.Fields{"stream": streamID, "key": state.KeyColumns}).Debug("using backfill key")
			if !slices.Equal(state.KeyColumns, discoveryInfo.PrimaryKey) {
				log.WithFields(log.Fields{
					"stream":      streamID,
					"backfillKey": state.KeyColumns,
					"databaseKey": discoveryInfo.PrimaryKey,
				}).Info("backfill key differs from database table primary key")
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
			log.WithFields(log.Fields{
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
	log.WithField("fenceAfter", fenceAfter.String()).Info("streaming to fence")

	// Log a warning and perform replication diagnostics if we don't observe the next fence within a few minutes
	var diagnosticsTimeout = time.AfterFunc(fenceAfter+automatedDiagnosticsTimeout, func() {
		log.Warn("replication streaming has been ongoing for an unexpectedly long amount of time, running replication diagnostics")
		if err := c.Database.ReplicationDiagnostics(ctx); err != nil {
			log.WithField("err", err).Error("replication diagnostics error")
		}
	})
	defer diagnosticsTimeout.Stop()

	// When streaming completes (because we've either reached the fence or encountered an error),
	// log the number of events which have been processed.
	var flushCount int  // Flush events
	var changeCount int // Change events
	var otherCount int  // All other events
	defer func() {
		log.WithFields(log.Fields{
			"change": changeCount,
			"flush":  flushCount,
			"other":  otherCount,
		}).Info("processed replication events")
	}()

	return replStream.StreamToFence(ctx, fenceAfter, func(event DatabaseEvent) error {
		// Flush events update the checkpoint LSN and may trigger a state update.
		if event, ok := event.(CommitEvent); ok {
			var cursorJSON, err = event.AppendJSON(nil)
			if err != nil {
				return fmt.Errorf("error serializing commit cursor: %w", err)
			}
			flushCount++
			c.State.Cursor = cursorJSON
			if reportFlush {
				if err := c.emitState(); err != nil {
					return fmt.Errorf("error emitting state update: %w", err)
				}
			}
			return nil
		}

		// The core event dispatch happens in handleReplicationEvent but it's useful to
		// count changes separately from other stuff here.
		if _, ok := event.(ChangeEvent); ok {
			changeCount++
		} else {
			otherCount++
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
		log.WithFields(log.Fields{"stream": event.StreamID, "cause": event.Cause}).Info("marking table as missing")
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
			log.WithField("stateKey", stateKey).Trace("stream metadata updated")
			state.Metadata = event.Metadata
			state.dirty = true
			c.State.Streams[stateKey] = state
		}
		return nil
	}

	// Any other events processed here must be ChangeEvents.
	if _, ok := event.(ChangeEvent); !ok {
		return fmt.Errorf("unhandled replication event %q", event.String())
	}
	var change = event.(ChangeEvent)
	var streamID = change.StreamID()
	var binding = c.Bindings[streamID]
	if binding == nil {
		log.WithField("event", event).Debug("no binding for stream, ignoring")
		return nil
	}
	var tableState = c.State.Streams[binding.StateKey]

	// Decide what to do with the change event based on the state of the table.
	if tableState == nil || tableState.Mode == "" || tableState.Mode == TableStateIgnore {
		log.WithField("event", event).Debug("inactive table, ignoring")
		return nil
	}
	if tableState.Mode == TableStateActive || tableState.Mode == TableStateKeylessBackfill || tableState.Mode == TableStateUnfilteredBackfill {
		if err := c.emitChange(binding, change); err != nil {
			return fmt.Errorf("error handling replication event for %q: %w", streamID, err)
		}
		return nil
	}
	if tableState.Mode == TableStatePreciseBackfill {
		// When a table is being backfilled precisely, replication events lying within the
		// already-backfilled portion of the table should be emitted, while replication events
		// beyond that point should be ignored (since we'll reach that row later in the backfill).
		if compareTuples(change.GetRowKey(), tableState.Scanned) <= 0 {
			if err := c.emitChange(binding, change); err != nil {
				return fmt.Errorf("error handling replication event for %q: %w", streamID, err)
			}
		}
		return nil
	}

	return fmt.Errorf("table %q in invalid mode %q", streamID, tableState.Mode)
}

func (c *Capture) backfillStreams(ctx context.Context, discovery map[StreamID]*DiscoveryInfo) error {
	var bindings = c.BindingsCurrentlyBackfilling()
	var streams = make([]StreamID, 0, len(bindings))
	for _, b := range bindings {
		streams = append(streams, b.StreamID)
	}

	// Select one binding at random to backfill at a time. On average this works
	// as well as any other policy, and just doing one at a time means that we
	// can size the relevant constants without worrying about how many tables
	// might be concurrently backfilling.
	if len(streams) != 0 {
		var streamID = streams[rand.Intn(len(streams))]
		log.WithFields(log.Fields{
			"count":    len(streams),
			"selected": streamID,
		}).Info("backfilling streams")
		if discoveryInfo, ok := discovery[streamID]; !ok {
			return fmt.Errorf("table %q missing from latest autodiscovery", streamID)
		} else if err := c.backfillStream(ctx, streamID, discoveryInfo); err != nil {
			return err
		}
	}
	return nil
}

func (c *Capture) backfillStream(ctx context.Context, streamID StreamID, discoveryInfo *DiscoveryInfo) error {
	var binding = c.Bindings[streamID]
	var stateKey = binding.StateKey
	var streamState = c.State.Streams[stateKey]

	// Process backfill query results as a callback-driven stream.
	var prevRowKey = streamState.Scanned // Only used for ordering sanity check in precise backfills
	var eventCount int
	backfillComplete, resumeCursor, err := c.Database.ScanTableChunk(ctx, discoveryInfo, streamState, func(event ChangeEvent) error {
		if streamState.Mode == TableStatePreciseBackfill {
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
			var rowKey = event.GetRowKey()
			if compareTuples(prevRowKey, rowKey) > 0 {
				return fmt.Errorf("scan key ordering failure: last=%q, next=%q", prevRowKey, rowKey)
			}
			prevRowKey = rowKey
		}

		if err := c.emitChange(binding, event); err != nil {
			return fmt.Errorf("error emitting %q backfill row: %w", streamID, err)
		}
		eventCount++
		return nil
	})
	if err != nil {
		return fmt.Errorf("error scanning table %q: %w", streamID, err)
	}

	// Update stream state to reflect backfill results
	log.WithFields(log.Fields{
		"stream": streamID,
		"rows":   eventCount,
	}).Info("processed backfill rows")
	var state = c.State.Streams[stateKey]
	state.BackfilledCount += eventCount
	if backfillComplete {
		log.WithField("stream", streamID).Info("backfill completed")
		state.Mode = TableStateActive
		state.Scanned = nil
	} else {
		state.Scanned = resumeCursor
	}
	state.dirty = true
	c.State.Streams[stateKey] = state
	return nil
}

func (c *Capture) emitChange(binding *Binding, event ChangeEvent) error {
	var err error
	var buf = c.emitChangeBuf[:0]
	buf, err = event.AppendJSON(buf)
	if err != nil {
		log.WithField("event", event).WithField("err", err).Error("error serializing change event")
		return fmt.Errorf("error serializing change event %q: %w", event.String(), err)
	}
	c.emitChangeBuf = buf

	c.emitChangeDoc.Binding = binding.Index
	c.emitChangeDoc.DocJson = buf
	c.emitChangeMsg.Captured = &c.emitChangeDoc
	return c.Output.Send(&c.emitChangeMsg)
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
	return c.Output.Checkpoint(bs, true)
}

// emitSourcedSchemas outputs a SourcedSchema update for every capture binding
// with corresponding discovery info in the provided map.
func (c *Capture) emitSourcedSchemas(discovery map[StreamID]*DiscoveryInfo) error {
	for _, binding := range c.Bindings {
		var info, ok = discovery[binding.StreamID]
		if !ok {
			continue // Only emit SourcedSchema updates for bindings present in discovery results
		}
		if !info.EmitSourcedSchemas {
			continue // Only emit SourcedSchema updates for tables with the feature enabled
		}
		var collectionSchema, _, err = generateCollectionSchema(c.Database, info, false)
		if err != nil {
			log.WithError(err).WithField("stream", binding.StreamID).Error("error generating schema")
			continue
		}
		if err := c.Output.SourcedSchema(int(binding.Index), collectionSchema); err != nil {
			return err
		}
	}
	return nil
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
	log.WithFields(log.Fields{
		"count":  count,
		"cursor": string(cursor),
	}).Debug("acknowledged up to cursor")
	return replStream.Acknowledge(ctx, cursor)
}

type StreamID struct {
	Schema string
	Table  string
}

func (s StreamID) String() string {
	return fmt.Sprintf("%s.%s", s.Schema, s.Table)
}

// This is a temporary hack to allow us to plumb through a feature flag setting for
// the gradual rollout of the "no longer lowercase stream IDs" behavior. This will
// eventually be the standard for all connectors if we can do it without breaking
// anything, but for now we want to be cautious and make it an opt-in.
//
// In an abstract sense we shouldn't be using a global variable for this, but as a
// practical matter there's only ever one capture running at a time so this is fine.
var LowercaseStreamIDs = true

// JoinStreamID combines a namespace and a stream name into a dotted name like "public.foo_table".
func JoinStreamID(namespace, stream string) StreamID {
	if LowercaseStreamIDs {
		namespace = strings.ToLower(namespace)
		stream = strings.ToLower(stream)
	}
	return StreamID{Schema: namespace, Table: stream}
}
