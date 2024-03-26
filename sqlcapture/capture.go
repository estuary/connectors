package sqlcapture

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/encoding/json"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// PersistentState represents the part of a connector's state which can be serialized
// and emitted in a state checkpoint, and resumed from after a restart.
type PersistentState struct {
	Cursor     string                               `json:"cursor"`                   // The replication cursor of the most recent 'Commit' event
	Streams    map[boilerplate.StateKey]*TableState `json:"bindingStateV1,omitempty"` // A mapping from runtime-provided state keys to table-specific state.
	OldStreams map[string]*TableState               `json:"streams,omitempty"`        // TODO(whb): Remove once all captures have migrated.
}

func migrateState(state *PersistentState, bindings []*pf.CaptureSpec_Binding) (bool, error) {
	if state.Streams != nil && state.OldStreams != nil {
		return false, fmt.Errorf("application error: both Streams and OldStreams were non-nil")
	} else if state.Streams != nil {
		logrus.Info("skipping state migration since it's already done")
		return false, nil
	} else if state.OldStreams == nil {
		logrus.Info("skipping state migration since there is no old state")
		return false, nil
	}

	state.Streams = make(map[boilerplate.StateKey]*TableState)

	for _, b := range bindings {
		if b.StateKey == "" {
			return false, fmt.Errorf("state key was empty for binding %s", b.ResourcePath)
		}

		var res Resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return false, fmt.Errorf("parsing resource config: %w", err)
		}

		var streamID = JoinStreamID(res.Namespace, res.Stream)

		ll := logrus.WithFields(logrus.Fields{
			"stateKey": b.StateKey,
			"resource": res,
			"streamID": streamID,
		})

		stateFromOld, ok := state.OldStreams[streamID]
		if !ok {
			// This should only happen if a new binding has been added at the same time as the
			// connector first restarted for the state migration.
			ll.Warn("no state found for binding while migrating state")
			continue
		}

		state.Streams[boilerplate.StateKey(b.StateKey)] = stateFromOld
		ll.Info("migrated binding state")
	}

	state.OldStreams = nil

	return true, nil
}

// Validate performs basic sanity-checking after a state has been parsed from JSON. More
// detailed checks are performed by UpdateState.
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
	return c.BindingsInState(TableModePreciseBackfill, TableModeUnfilteredBackfill, TableModeKeylessBackfill, TableModeActive)
}

// BindingsCurrentlyBackfilling returns all the bindings undergoing some sort of backfill.
func (c *Capture) BindingsCurrentlyBackfilling() []*Binding {
	return c.BindingsInState(TableModePreciseBackfill, TableModeUnfilteredBackfill, TableModeKeylessBackfill)
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

// The table's mode can be one of:
//
//	Ignore: The table is being deliberately ignored.
//	Pending: The table is new, and will start being backfilled soon.
//	PreciseBackfill: The table's rows are being backfilled and replication events will only be emitted for the already-backfilled portion.
//	UnfilteredBackfill: The table's rows are being backfilled as normal but all replication events will be emitted.
//	KeylessBackfill: The table's rows are being backfilled with a non-primary-key based strategy and all replication events will be emitted.
//	Active: The table finished backfilling and replication events are emitted for the entire table.
const (
	TableModeIgnore             = "Ignore"
	TableModePending            = "Pending"
	TableModePreciseBackfill    = "Backfill" // Short name for historical reasons, this used to be the only backfill mode
	TableModeUnfilteredBackfill = "UnfilteredBackfill"
	TableModeKeylessBackfill    = "KeylessBackfill"
	TableModeActive             = "Active"
)

// Capture encapsulates the generic process of capturing data from a SQL database
// via replication, backfilling preexisting table contents, and emitting records/state
// updates. It uses the `Database` interface to interact with a specific database.
type Capture struct {
	Bindings map[string]*Binding     // Map from fully-qualified stream IDs to the corresponding binding information
	State    *PersistentState        // State read from `state.json` and emitted as updates
	Output   *boilerplate.PullOutput // The encoder to which records and state updates are written
	Database Database                // The database-specific interface which is operated by the generic Capture logic

	discovery map[string]*DiscoveryInfo // Cached result of the most recent table discovery request

	// A mutex-guarded list of checkpoint cursor values. Values are appended by
	// emitState() whenever it outputs a checkpoint and removed whenever the
	// acknowledgement-relaying goroutine receives an Acknowledge message.
	pending struct {
		sync.Mutex
		cursors []string
	}
}

const (
	heartbeatWatermarkInterval = 60 * time.Second // When streaming indefinitely, write a heartbeat watermark this often
	streamIdleWarning          = 60 * time.Second // After `streamIdleWarning` has elapsed since the last replication event, we log a warning.
	streamProgressInterval     = 60 * time.Second // After `streamProgressInterval` the replication streaming code may log a progress report.
)

var (
	errWatermarkNotReached = fmt.Errorf("replication stream closed before reaching watermark")
)

// Run is the top level entry point of the capture process.
func (c *Capture) Run(ctx context.Context) (err error) {
	// Perform discovery and cache the result. This is used at startup when
	// updating the state to reflect catalog changes, and then later it is
	// plumbed through so that value translation can take column types into
	// account.
	logrus.Info("discovering tables")
	c.discovery, err = c.Database.DiscoverTables(ctx)
	if err != nil {
		return fmt.Errorf("error discovering database tables: %w", err)
	}
	for streamID, discoveryInfo := range c.discovery {
		logrus.WithFields(logrus.Fields{
			"table":      streamID,
			"primaryKey": discoveryInfo.PrimaryKey,
			"columns":    discoveryInfo.Columns,
		}).Debug("discovered table")
	}

	if err := c.updateState(ctx); err != nil {
		return fmt.Errorf("error updating capture state: %w", err)
	}

	replStream, err := c.Database.ReplicationStream(ctx, c.State.Cursor)
	if err != nil {
		return fmt.Errorf("error creating replication stream: %w", err)
	}
	for _, binding := range c.BindingsCurrentlyActive() {
		var state = c.State.Streams[binding.StateKey]
		var streamID = binding.StreamID
		if err := replStream.ActivateTable(ctx, streamID, state.KeyColumns, c.discovery[streamID], state.Metadata); err != nil {
			return fmt.Errorf("error activating table %q: %w", streamID, err)
		}
	}
	var watermarks = c.Database.WatermarksTable()
	if c.discovery[watermarks] == nil {
		return fmt.Errorf("watermarks table %q does not exist", watermarks)
	}
	if err := replStream.ActivateTable(ctx, watermarks, c.discovery[watermarks].PrimaryKey, c.discovery[watermarks], nil); err != nil {
		return fmt.Errorf("error activating table %q: %w", watermarks, err)
	}
	if err := replStream.StartReplication(ctx); err != nil {
		return fmt.Errorf("error starting replication: %w", err)
	}
	defer func() {
		if streamErr := replStream.Close(ctx); streamErr != nil {
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

	// Perform an initial "catch-up" stream-to-watermark before transitioning
	// any "Pending" streams into the "Backfill" state. This helps ensure that
	// a given stream only ever observes replication events which occur *after*
	// the connector was started.
	var watermark = uuid.New().String()
	if err := c.Database.WriteWatermark(ctx, watermark); err != nil {
		return fmt.Errorf("error writing next watermark: %w", err)
	}
	if err := c.streamCatchup(ctx, replStream, watermark); err != nil {
		return fmt.Errorf("error streaming until watermark: %w", err)
	}
	for _, binding := range c.BindingsInState(TableModePending) {
		var streamID = binding.StreamID
		var stateKey = binding.StateKey

		logrus.WithFields(logrus.Fields{"stream": streamID, "mode": binding.Resource.Mode}).Info("activating replication for stream")

		var state = c.State.Streams[stateKey]
		var discoveryInfo = c.discovery[streamID]
		switch binding.Resource.Mode {
		case BackfillModeAutomatic:
			if len(state.KeyColumns) == 0 {
				logrus.WithField("stream", streamID).Info("autoselected keyless backfill mode (table has no primary key)")
				state.Mode = TableModeKeylessBackfill
			} else if discoveryInfo != nil && discoveryInfo.UnpredictableKeyOrdering {
				logrus.WithField("stream", streamID).Info("autoselected unfiltered (normal) backfill mode (database key ordering is unpredictable)")
				state.Mode = TableModeUnfilteredBackfill
			} else {
				logrus.WithField("stream", streamID).Info("autoselected precise backfill mode")
				state.Mode = TableModePreciseBackfill
			}
		case BackfillModePrecise:
			logrus.WithField("stream", streamID).Info("user selected precise backfill mode")
			state.Mode = TableModePreciseBackfill
		case BackfillModeNormal:
			logrus.WithField("stream", streamID).Info("user selected unfiltered (normal) backfill mode")
			state.Mode = TableModeUnfilteredBackfill
		case BackfillModeWithoutKey:
			logrus.WithField("stream", streamID).Info("user selected keyless backfill mode")
			state.Mode = TableModeKeylessBackfill
		case BackfillModeOnlyChanges:
			logrus.WithField("stream", streamID).Info("user selected only changes, skipping backfill")
			state.Mode = TableModeActive
		default:
			return fmt.Errorf("invalid backfill mode %q for stream %q", binding.Resource.Mode, streamID)
		}
		state.dirty = true
		c.State.Streams[stateKey] = state

		if err := replStream.ActivateTable(ctx, streamID, state.KeyColumns, c.discovery[streamID], state.Metadata); err != nil {
			return fmt.Errorf("error activating %q for replication: %w", streamID, err)
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
		var streamID = binding.StreamID
		var stateKey = binding.StateKey
		if !c.Database.ShouldBackfill(streamID) {
			var state = c.State.Streams[stateKey]
			if state.Scanned == nil {
				logrus.WithField("stream", streamID).Info("skipping backfill for stream")
			} else {
				logrus.WithFields(logrus.Fields{
					"stream":  streamID,
					"scanned": state.Scanned,
				}).Info("terminating backfill early for stream")
			}
			state.Mode = TableModeActive
			state.Scanned = nil
			state.dirty = true
			c.State.Streams[stateKey] = state
		}
	}

	// Backfill any tables which require it
	for c.BindingsCurrentlyBackfilling() != nil {
		var watermark = uuid.New().String()
		if err := c.Database.WriteWatermark(ctx, watermark); err != nil {
			return fmt.Errorf("error writing next watermark: %w", err)
		} else if err := c.streamToWatermark(ctx, replStream, watermark); err != nil {
			return fmt.Errorf("error streaming until watermark: %w", err)
		} else if err := c.emitState(); err != nil {
			return err
		} else if err := c.backfillStreams(ctx); err != nil {
			return fmt.Errorf("error performing backfill: %w", err)
		}
	}

	// Once all backfills are complete, make sure an up-to-date state checkpoint
	// gets emitted. This ensures that streams reliably transition into the Active
	// state on the first connector run even if there are no replication events
	// occurring.
	logrus.Info("no tables currently require backfilling")
	if err := c.emitState(); err != nil {
		return err
	}

	// Once there is no more backfilling to do, just stream changes forever and emit
	// state updates on every transaction commit.
	if err := c.streamForever(ctx, replStream); err != nil {
		return err
	}
	return nil
}

func (c *Capture) updateState(ctx context.Context) error {
	// Create the Streams map if nil
	if c.State.Streams == nil {
		c.State.Streams = make(map[boilerplate.StateKey]*TableState)
	}

	// Streams may be added to the catalog at various times. We need to
	// initialize new state entries for these streams, and while we're at
	// it this is a good time to sanity-check the primary key configuration.
	allStreamsAreNew := true
	for streamID, binding := range c.Bindings {
		var stateKey = binding.StateKey
		var discoveryInfo = c.discovery[streamID]
		if discoveryInfo == nil {
			return fmt.Errorf("table %q is a configured binding of this capture, but doesn't exist or isn't visible with current permissions", streamID)
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
			if _, ok := c.State.Streams[stateKey]; ok {
				c.State.Streams[stateKey] = &TableState{Mode: TableModeIgnore, dirty: true}
			}
			continue
		}

		// Select the primary key from the first available source. In order of priority:
		//   - If the resource config specifies a primary key override then we'll use that.
		//   - Normally we'll use the primary key from the collection spec.
		//   - And as a fallback which *should* no longer be reachable, we'll use the
		//     primary key from the startup discovery run.
		logrus.WithFields(logrus.Fields{
			"stream":     streamID,
			"resource":   binding.Resource.PrimaryKey,
			"collection": binding.CollectionKey,
			"discovery":  discoveryInfo.PrimaryKey,
		}).Debug("selecting primary key")
		var primaryKey []string
		if binding.Resource.Mode == BackfillModeOnlyChanges || binding.Resource.Mode == BackfillModeWithoutKey {
			logrus.WithFields(logrus.Fields{"stream": streamID}).Debug("initializing stream state without primary key")
		} else if len(binding.Resource.PrimaryKey) > 0 {
			primaryKey = binding.Resource.PrimaryKey
			logrus.WithFields(logrus.Fields{"stream": streamID, "key": primaryKey}).Debug("using resource primary key")
		} else if len(binding.CollectionKey) > 0 {
			for _, ptr := range binding.CollectionKey {
				primaryKey = append(primaryKey, collectionKeyToPrimaryKey(ptr))
			}
			logrus.WithFields(logrus.Fields{"stream": streamID, "key": primaryKey}).Debug("using collection primary key")
		} else if len(discoveryInfo.PrimaryKey) > 0 {
			primaryKey = discoveryInfo.PrimaryKey
			logrus.WithFields(logrus.Fields{"stream": streamID, "key": primaryKey}).Warn("using discovery primary key -- this is DEPRECATED and also shouldn't be possible")
		} else {
			return fmt.Errorf("stream %q: primary key must be specified", streamID)
		}

		// See if the stream is already initialized. If it's not, then create it.
		var streamState, ok = c.State.Streams[stateKey]
		if !ok || streamState.Mode == TableModeIgnore {
			c.State.Streams[stateKey] = &TableState{Mode: TableModePending, KeyColumns: primaryKey, dirty: true}
			continue
		}
		allStreamsAreNew = false

		// The following safety checks only matter if we're actively backfilling the table or
		// intend to start backfilling it shortly. But checking for that would risk silently
		// failing if a new keyed backfill mode were added in the future, so instead we check
		// for the set of known-to-be-fine situations here.
		var notBackfilling = (streamState.Mode == TableModeIgnore) ||
			(streamState.Mode == TableModeActive) ||
			(streamState.Mode == TableModeKeylessBackfill) ||
			(streamState.Mode == TableModePending && binding.Resource.Mode == BackfillModeOnlyChanges) ||
			(streamState.Mode == TableModePending && binding.Resource.Mode == BackfillModeWithoutKey)
		if notBackfilling {
			continue
		}

		// Print a warning if the primary key we'll be using differs from the database's primary key.
		if strings.Join(primaryKey, ",") != strings.Join(discoveryInfo.PrimaryKey, ",") {
			logrus.WithFields(logrus.Fields{
				"stream":      streamID,
				"backfillKey": primaryKey,
				"databaseKey": discoveryInfo.PrimaryKey,
			}).Warn("primary key for backfill differs from database table primary key")
		}

		// Error out if the stream state was previously initialized with a different key
		if strings.Join(streamState.KeyColumns, ",") != strings.Join(primaryKey, ",") {
			return fmt.Errorf("stream %q: primary key %q doesn't match initialized scan key %q", streamID, primaryKey, streamState.KeyColumns)
		}
	}

	// Likewise streams may be removed from the catalog, and we need to forget
	// the corresponding state information.
	streamExistsInCatalog := func(sk boilerplate.StateKey) bool {
		for _, b := range c.Bindings {
			if b.StateKey == sk {
				return true
			}
		}
		return false
	}
	for stateKey, state := range c.State.Streams {
		if !streamExistsInCatalog(stateKey) && state.Mode != TableModeIgnore {
			logrus.WithField("stateKey", stateKey).Info("stream removed from catalog")
			c.State.Streams[stateKey] = &TableState{Mode: TableModeIgnore, dirty: true}
		}
	}

	// If all bindings have been removed or had their backfill counter incremented, forget the old
	// replication cursor. This is safe because logically if no streams are active or a re-backfill
	// has been requested for them then we can't miss any events of interest when the replication
	// stream jumps ahead, and doing this allows the user an easy recovery path after WAL deletion,
	// they just need to either increment the backfill counter for all bindings, or disable all
	// bindings and then re-enable them (which will cause them all to get backfilled anew, as they
	// should after such an event).
	if allStreamsAreNew {
		logrus.Info("no active bindings, resetting cursor")
		c.State.Cursor = ""
	}

	// Emit the new state to stdout. This isn't strictly necessary but it helps to make
	// the emitted sequence of state updates a lot more readable.
	return c.emitState()
}

func (c *Capture) streamCatchup(ctx context.Context, replStream ReplicationStream, watermark string) error {
	return c.streamToWatermarkWithOptions(ctx, replStream, watermark, true)
}

func (c *Capture) streamToWatermark(ctx context.Context, replStream ReplicationStream, watermark string) error {
	return c.streamToWatermarkWithOptions(ctx, replStream, watermark, false)
}

func (c *Capture) streamForever(ctx context.Context, replStream ReplicationStream) error {
	logrus.Info("streaming replication events indefinitely")
	for ctx.Err() == nil {
		// Spawn a worker goroutine which will write the watermark value after the appropriate interval.
		var watermark = uuid.New().String()
		var group, workerCtx = errgroup.WithContext(ctx)
		group.Go(func() error {
			select {
			case <-workerCtx.Done():
				logrus.WithField("watermark", watermark).Warn("not writing watermark due to context cancellation")
				return workerCtx.Err()
			case <-time.After(heartbeatWatermarkInterval):
				if err := c.Database.WriteWatermark(workerCtx, watermark); err != nil {
					logrus.WithField("watermark", watermark).Error("failed to write watermark")
					return fmt.Errorf("error writing next watermark: %w", err)
				}
				logrus.WithField("watermark", watermark).Debug("watermark written")
				return nil
			}
		})

		// Perform replication streaming until the watermark is reached (or the
		// watermark-writing thread returns an error which cancels the context).
		group.Go(func() error {
			if err := c.streamCatchup(workerCtx, replStream, watermark); err != nil {
				return fmt.Errorf("error streaming until watermark: %w", err)
			}
			return nil
		})

		if err := group.Wait(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// streamToWatermarkWithOptions implements two very similar operations:
//
//   - streamCatchup: Processes replication events until the watermark is reached
//     and then stops. Whenever a database transaction commit is reported, a Flow
//     state checkpoint update is emitted.
//   - streamToWatermark: Like streamCatchup except that no state checkpoints are
//     emitted during replication processing. This is done during backfills so that
//     a chunk of backfilled rows plus all relevant replication events will be
//     committed into the same Flow transaction.
//
// Both operations are implemented as a single function here with a parameter
// controlling checkpoint behavior because they're so similar.
func (c *Capture) streamToWatermarkWithOptions(ctx context.Context, replStream ReplicationStream, watermark string, reportFlush bool) error {
	logrus.WithField("watermark", watermark).Info("streaming to watermark")
	var watermarksTable = c.Database.WatermarksTable()
	var watermarkReached = false

	// Log a warning and perform replication diagnostics if we don't observe the watermark within a few minutes
	var diagnosticsTimeout = time.AfterFunc(2*heartbeatWatermarkInterval, func() {
		logrus.Warn("replication streaming has been ongoing for an unexpectedly long amount of time, running replication diagnostics")
		if err := c.Database.ReplicationDiagnostics(ctx); err != nil {
			logrus.WithField("err", err).Error("replication diagnostics error")
		}
	})
	defer diagnosticsTimeout.Stop()

	// When streaming completes (because we've either reached the watermark or encountered an error),
	// log the number of events which have been processed.
	var eventCount int
	defer func() { logrus.WithField("events", eventCount).Info("processed replication events") }()

	for event := range replStream.Events() {
		eventCount++

		// Flush events update the checkpointed LSN and trigger a state update.
		// If this is the commit after the target watermark, it also ends the loop.
		if event, ok := event.(*FlushEvent); ok {
			c.State.Cursor = event.Cursor
			if reportFlush {
				if err := c.emitState(); err != nil {
					return fmt.Errorf("error emitting state update: %w", err)
				}
			}
			if watermarkReached {
				return nil
			}
			continue
		}

		// Check for watermark change events and set a flag when the target watermark is reached.
		// The subsequent FlushEvent will exit the loop.
		if event, ok := event.(*ChangeEvent); ok {
			if event.Operation != DeleteOp && event.Source.Common().StreamID() == watermarksTable {
				var actual = event.After["watermark"]
				logrus.WithFields(logrus.Fields{"expected": watermark, "actual": actual}).Debug("watermark change")
				if actual == watermark {
					watermarkReached = true
				}
			}
		}

		if err := c.handleReplicationEvent(event); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errWatermarkNotReached
}

func (c *Capture) handleReplicationEvent(event DatabaseEvent) error {
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
	if tableState == nil || tableState.Mode == "" || tableState.Mode == TableModeIgnore {
		logrus.WithFields(logrus.Fields{
			"stream": streamID,
			"op":     change.Operation,
		}).Debug("ignoring stream")
		return nil
	}
	if tableState.Mode == TableModeActive || tableState.Mode == TableModeKeylessBackfill || tableState.Mode == TableModeUnfilteredBackfill {
		if err := c.emitChange(change); err != nil {
			return fmt.Errorf("error handling replication event for %q: %w", streamID, err)
		}
		return nil
	}
	if tableState.Mode == TableModePreciseBackfill {
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
			"streams":  streams,
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

	discoveryInfo, ok := c.discovery[streamID]
	if !ok {
		return fmt.Errorf("unknown table %q", streamID)
	}

	// Process backfill query results as a callback-driven stream.
	var lastRowKey = streamState.Scanned
	var eventCount int
	var err = c.Database.ScanTableChunk(ctx, discoveryInfo, streamState, func(event *ChangeEvent) error {
		if streamState.Mode == TableModePreciseBackfill && compareTuples(lastRowKey, event.RowKey) > 0 {
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
	var state = c.State.Streams[stateKey]
	if eventCount == 0 {
		state.Mode = TableModeActive
		state.Scanned = nil
	} else {
		state.Scanned = lastRowKey
		state.BackfilledCount += eventCount
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
	logrus.WithField("cursor", cursor).Trace("acknowledged up to cursor")
	replStream.Acknowledge(ctx, cursor)
	return nil
}

// JoinStreamID combines a namespace and a stream name into a dotted name like "public.foo_table".
func JoinStreamID(namespace, stream string) string {
	return strings.ToLower(namespace + "." + stream)
}
