package sqlcapture

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// PersistentState represents the part of a connector's state which can be serialized
// and emitted in a state checkpoint, and resumed from after a restart.
type PersistentState struct {
	Cursor  string                 `json:"cursor"`            // The replication cursor of the most recent 'Commit' event
	Streams map[string]*TableState `json:"streams,omitempty"` // A mapping from table IDs (<namespace>.<table>) to table-specific state.
}

// Validate performs basic sanity-checking after a state has been parsed from JSON. More
// detailed checks are performed by UpdateState.
func (ps *PersistentState) Validate() error {
	return nil
}

// StreamsInState returns the IDs of all streams in a particular
// state, in sorted order for reproducibility.
func (ps *PersistentState) StreamsInState(modes ...string) []string {
	var streams []string
	for id, tableState := range ps.Streams {
		for _, mode := range modes {
			if tableState.Mode == mode {
				streams = append(streams, id)
				break
			}
		}
	}
	sort.Strings(streams)
	return streams
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
//	Backfill: The table's rows are being backfilled and replication events will only be emitted for the already-backfilled portion.
//	KeylessBackfill: The table's rows are being backfilled with a non-primary-key based strategy and all replication events will be emitted.
//	Active: The table finished backfilling and replication events are emitted for the entire table.
const (
	TableModeIgnore          = "Ignore"
	TableModePending         = "Pending"
	TableModeBackfill        = "Backfill"
	TableModeKeylessBackfill = "KeylessBackfill"
	TableModeActive          = "Active"
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
	nonexistentWatermark   = "nonexistent-watermark" // The watermark which will be used for the final "tailing" stream call.
	streamIdleWarning      = 60 * time.Second        // After `streamIdleWarning` has elapsed since the last replication event, we log a warning.
	streamProgressInterval = 60 * time.Second        // After `streamProgressInterval` the replication streaming code may log a progress report.
)

// Run is the top level entry point of the capture process.
func (c *Capture) Run(ctx context.Context) (err error) {
	// Notify Flow that we're ready and would like to receive acknowledgements.
	if err := c.Output.Ready(true); err != nil {
		return err
	}

	// Perform discovery and cache the result. This is used at startup when
	// updating the state to reflect catalog changes, and then later it is
	// plumbed through so that value translation can take column types into
	// account.
	logrus.Info("discovering tables")
	c.discovery, err = c.Database.DiscoverTables(ctx)
	if err != nil {
		return fmt.Errorf("error discovering database tables: %w", err)
	}

	if err := c.updateState(ctx); err != nil {
		return fmt.Errorf("error updating capture state: %w", err)
	}

	replStream, err := c.Database.ReplicationStream(ctx, c.State.Cursor)
	if err != nil {
		return fmt.Errorf("error creating replication stream: %w", err)
	}
	for streamID, state := range c.State.Streams {
		if state.Mode == TableModeBackfill || state.Mode == TableModeActive {
			if err := replStream.ActivateTable(ctx, streamID, state.KeyColumns, c.discovery[streamID], state.Metadata); err != nil {
				return fmt.Errorf("error activating table %q: %w", streamID, err)
			}
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
	for _, streamID := range c.State.StreamsInState(TableModePending) {
		var binding = c.Bindings[streamID]
		if binding == nil {
			return fmt.Errorf("internal error: no binding information for stream %q", streamID)
		}

		logrus.WithFields(logrus.Fields{"stream": streamID, "mode": binding.Resource.Mode}).Info("activating replication for stream")

		var state = c.State.Streams[streamID]
		switch binding.Resource.Mode {
		case BackfillModeNormal:
			state.Mode = TableModeBackfill
		case BackfillModeOnlyChanges:
			state.Mode = TableModeActive
		case BackfillModeWithoutKey:
			state.Mode = TableModeKeylessBackfill
		default:
			return fmt.Errorf("invalid backfill mode %q for stream %q", binding.Resource.Mode, streamID)
		}
		state.dirty = true
		c.State.Streams[streamID] = state

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
	for _, streamID := range c.State.StreamsInState(TableModeBackfill) {
		if !c.Database.ShouldBackfill(streamID) {
			var state = c.State.Streams[streamID]
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
			c.State.Streams[streamID] = state
		}
	}

	// Backfill any tables which require it
	for c.State.StreamsInState(TableModeBackfill, TableModeKeylessBackfill) != nil {
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
	logrus.Debug("no tables currently require backfilling")
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
		c.State.Streams = make(map[string]*TableState)
	}

	// Streams may be added to the catalog at various times. We need to
	// initialize new state entries for these streams, and while we're at
	// it this is a good time to sanity-check the primary key configuration.
	for streamID, binding := range c.Bindings {
		var discoveryInfo = c.discovery[streamID]
		if discoveryInfo == nil {
			return fmt.Errorf("table %q is a configured binding of this capture, but doesn't exist or isn't visible with current permissions", streamID)
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
		var streamState, ok = c.State.Streams[streamID]
		if !ok || streamState.Mode == TableModeIgnore {
			c.State.Streams[streamID] = &TableState{Mode: TableModePending, KeyColumns: primaryKey, dirty: true}
			continue
		}

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
	for streamID := range c.State.Streams {
		var _, streamExistsInCatalog = c.Bindings[streamID]
		if !streamExistsInCatalog {
			logrus.WithField("stream", streamID).Info("stream removed from catalog")
			c.State.Streams[streamID] = &TableState{Mode: TableModeIgnore, dirty: true}
		}
	}

	// Emit the new state to stdout. This isn't strictly necessary but it helps to make
	// the emitted sequence of state updates a lot more readable.
	return c.emitState()
}

// This code is all about logging enough information to give insight into
// connector replication progress without being unusably spammy. These logs
// take two forms:
//
//   - A warning/info message when the stream has been sitting idle
//     for a while since the last replication event. This is a warning
//     when we expect to catch up to a watermark, and informational when
//     that watermark is "nonexistent-watermark".
//   - Periodic progress updates, just to show that streaming is ongoing.
//
// Neither of these can occur more frequently than their respective timeout
// values, so the impact of the additional logging should be minimal.
type streamProgressReport struct {
	eventCount   int
	nextProgress time.Time
	idleTimeout  *time.Timer
}

func startProgressReport(idleIsBad bool) *streamProgressReport {
	return &streamProgressReport{
		nextProgress: time.Now().Add(streamProgressInterval),
		idleTimeout: time.AfterFunc(streamIdleWarning, func() {
			if idleIsBad {
				logrus.WithField("timeout", streamIdleWarning.String()).Warn("replication stream idle")
			} else {
				logrus.WithField("timeout", streamIdleWarning.String()).Info("replication stream idle")
			}
		}),
	}
}

func (spr *streamProgressReport) Progress() {
	spr.eventCount++
	if time.Now().After(spr.nextProgress) {
		logrus.WithField("count", spr.eventCount).Info("replication stream progress")
		spr.nextProgress = time.Now().Add(streamProgressInterval)
	}
	spr.idleTimeout.Reset(streamIdleWarning)
}

func (spr *streamProgressReport) Stop() {
	spr.idleTimeout.Stop()
}

func (c *Capture) streamCatchup(ctx context.Context, replStream ReplicationStream, watermark string) error {
	return c.streamToWatermarkWithOptions(ctx, replStream, watermark, true)
}

func (c *Capture) streamToWatermark(ctx context.Context, replStream ReplicationStream, watermark string) error {
	return c.streamToWatermarkWithOptions(ctx, replStream, watermark, false)
}

func (c *Capture) streamForever(ctx context.Context, replStream ReplicationStream) error {
	return c.streamToWatermarkWithOptions(ctx, replStream, nonexistentWatermark, true)
}

// streamToWatermarkWithOptions implements three very similar operations:
//
//   - streamCatchup: Processes replication events until the watermark is reached
//     and then stops. Whenever a database transaction commit is reported, a Flow
//     state checkpoint update is emitted.
//   - streamForever: Like streamCatchup but the watermark doesn't actually exist
//     and so will never be reached.
//   - streamToWatermark: Like streamCatchup except that no state checkpoints are
//     emitted during replication processing. This is done during backfills so that
//     a chunk of backfilled rows plus all relevant replication events will be
//     committed into the same Flow transaction.
//
// All three operations are implemented as a single function here with parameters
// controlling its behavior because the three operations are so similar.
func (c *Capture) streamToWatermarkWithOptions(ctx context.Context, replStream ReplicationStream, watermark string, reportFlush bool) error {
	logrus.WithField("watermark", watermark).Info("streaming to watermark")
	var watermarksTable = c.Database.WatermarksTable()
	var watermarkReached = false

	var progressReport = startProgressReport(watermark != nonexistentWatermark)
	defer progressReport.Stop()

	for event := range replStream.Events() {
		progressReport.Progress()

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
	if watermark == nonexistentWatermark {
		return fmt.Errorf("replication stream closed")
	}
	return fmt.Errorf("replication stream closed before reaching watermark")
}

func (c *Capture) handleReplicationEvent(event DatabaseEvent) error {
	// Metadata events update the per-table metadata and dirty flag.
	// They have no other effect, the new metadata will only be written
	// as part of a subsequent state checkpoint.
	if event, ok := event.(*MetadataEvent); ok {
		var streamID = event.StreamID
		if state, ok := c.State.Streams[streamID]; ok {
			logrus.WithField("stream", streamID).Trace("stream metadata updated")
			state.Metadata = event.Metadata
			state.dirty = true
			c.State.Streams[streamID] = state
		}
		return nil
	}

	// Any other events processed here must be ChangeEvents.
	if _, ok := event.(*ChangeEvent); !ok {
		return fmt.Errorf("unhandled replication event %q", event.String())
	}
	var change = event.(*ChangeEvent)
	var streamID = change.Source.Common().StreamID()

	// Handle the easy cases: Events on ignored or fully-active tables.
	var tableState = c.State.Streams[streamID]
	if tableState == nil || tableState.Mode == "" || tableState.Mode == TableModeIgnore {
		logrus.WithFields(logrus.Fields{
			"stream": streamID,
			"op":     change.Operation,
		}).Debug("ignoring stream")
		return nil
	}
	if tableState.Mode == TableModeActive || tableState.Mode == TableModeKeylessBackfill {
		if err := c.emitChange(change); err != nil {
			return fmt.Errorf("error handling replication event for %q: %w", streamID, err)
		}
		return nil
	}
	if tableState.Mode != TableModeBackfill {
		return fmt.Errorf("table %q in invalid mode %q", streamID, tableState.Mode)
	}

	// While a table is being backfilled, events occurring *before* the current scan point
	// will be emitted, while events *after* that point will be patched (or ignored) into
	// the buffered resultSet.
	if compareTuples(change.RowKey, tableState.Scanned) <= 0 {
		if err := c.emitChange(change); err != nil {
			return fmt.Errorf("error handling replication event for %q: %w", streamID, err)
		}
	}
	return nil
}

func (c *Capture) backfillStreams(ctx context.Context) error {
	var streams = c.State.StreamsInState(TableModeBackfill, TableModeKeylessBackfill)

	// Select one stream at random to backfill at a time. On average this works
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
	var streamState = c.State.Streams[streamID]

	discoveryInfo, ok := c.discovery[streamID]
	if !ok {
		return fmt.Errorf("unknown table %q", streamID)
	}

	// Process backfill query results as a callback-driven stream.
	var lastRowKey = streamState.Scanned
	var eventCount int
	var err = c.Database.ScanTableChunk(ctx, discoveryInfo, streamState, func(event *ChangeEvent) error {
		if compareTuples(lastRowKey, event.RowKey) > 0 {
			// Sanity check that the DB must always return rows whose serialized
			// key value is greater than the previous cursor value. This together
			// with the check in resultset.go ensures that the scan key always
			// increases (or else the capture will fail).
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
	var state = c.State.Streams[streamID]
	if eventCount == 0 {
		state.Mode = TableModeActive
		state.Scanned = nil
	} else {
		state.Scanned = lastRowKey
		state.BackfilledCount += eventCount
	}
	state.dirty = true
	c.State.Streams[streamID] = state
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
	var streams = make(map[string]*TableState)
	for streamID, state := range c.State.Streams {
		if state.dirty {
			state.dirty = false
			streams[streamID] = state
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
