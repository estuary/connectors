package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// TODO(wgd): Contemplate https://github.com/debezium/debezium/blob/main/debezium-connector-sqlserver/src/main/java/io/debezium/connector/sqlserver/SqlServerStreamingChangeEventSource.java#L51
// and the schema upgrade process described there.

var replicationBufferSize = 4 * 1024 // Assuming change events average ~2kB then 4k * 2kB = 8MB

const (
	cdcPollingWorkers     = 4                      // Number of parallel worker threads to execute CDC polling operations
	cdcCleanupWorkers     = 16                     // Number of parallel worker threads to execute table cleanup operations
	cdcPollingInterval    = 500 * time.Millisecond // How frequently to perform CDC polling
	cdcCleanupInterval    = 15 * time.Second       // How frequently to perform CDC table cleanup
	cdcManagementInterval = 30 * time.Second       // How frequently to perform CDC instance management

	// streamToFenceWatchdogTimeout is the length of time after which a stream-to-fence
	// operation will error out if no further events are received when there ought to be
	// some. This should never be hit in normal operation, and exists only so that certain
	// rare failure modes produce an error rather than blocking forever.
	streamToFenceWatchdogTimeout = 5 * time.Minute

	// establishFenceTimeout is the length of time after with an establish-fence-position
	// operation will error out. Since the CDC worker should be running on the database
	// every 5 seconds by default, it should never take much longer than that for us to
	// establish a valid fence position.
	//
	// Technically it's possible for the CDC agent polling interval to be manually set as
	// high as 24 hours, so the logic might be more robust if we scaled our timeout based
	// on the actual `pollinginterval` setting in `cdc.dbo_jobs`. I suspect this will never
	// actually be an issue.
	establishFenceTimeout = 5 * time.Minute
)

// LSN is just a type alias for []byte to make the code that works with LSN values a bit clearer.
type LSN = []byte

// sqlserverSourceInfo is source metadata for data capture events.
type sqlserverSourceInfo struct {
	sqlcapture.SourceCommon

	LSN        LSN    `json:"lsn" jsonschema:"description=The LSN at which a CDC event occurred. Only set for CDC events, not backfills."`
	SeqVal     []byte `json:"seqval" jsonschema:"description=Sequence value used to order changes to a row within a transaction. Only set for CDC events, not backfills."`
	UpdateMask any    `json:"updateMask,omitempty" jsonschema:"description=A bit mask with a bit corresponding to each captured column identified for the capture instance. Only set for CDC events, not backfills."`
}

func (si *sqlserverSourceInfo) Common() sqlcapture.SourceCommon {
	return si.SourceCommon
}

// ReplicationStream constructs a new ReplicationStream object, from which
// a neverending sequence of change events can be read.
func (db *sqlserverDatabase) ReplicationStream(ctx context.Context, startCursorJSON json.RawMessage) (sqlcapture.ReplicationStream, error) {
	var stream = &sqlserverReplicationStream{db: db, conn: db.conn, cfg: db.config}
	if err := stream.open(ctx); err != nil {
		return nil, fmt.Errorf("error opening replication stream: %w", err)
	}

	// Decode start cursor from a JSON quoted string into its actual string contents
	startCursor, err := unmarshalJSONString(startCursorJSON)
	if err != nil {
		return nil, fmt.Errorf("invalid start cursor JSON: %w", err)
	}

	// If we have no resume cursor but we do have an initial backfill cursor, use that as the start position.
	if startCursor == "" && db.initialBackfillCursor != "" {
		log.WithField("cursor", db.initialBackfillCursor).Info("using initial backfill cursor as start position")
		startCursor = db.initialBackfillCursor
	}

	// If the `force_reset_cursor=XYZ` hackery flag is set, use that as the start position regardless of anything else.
	if db.forceResetCursor != "" {
		log.WithField("cursor", db.forceResetCursor).Info("forcibly modified resume cursor")
		startCursor = db.forceResetCursor
	}

	if startCursor == "" {
		var maxLSN, err = cdcGetMaxLSN(ctx, db.conn)
		if err != nil {
			return nil, err
		}
		stream.fromLSN = maxLSN
	} else {
		var resumeLSN, err = base64.StdEncoding.DecodeString(startCursor)
		if err != nil {
			return nil, fmt.Errorf("error decoding resume cursor: %w", err)
		}
		stream.fromLSN = resumeLSN
	}
	stream.fenceLSN = stream.fromLSN

	return stream, nil
}

type sqlserverReplicationStream struct {
	db   *sqlserverDatabase
	conn *sql.DB
	cfg  *Config

	isReplica bool // True if the connected database is a read-only replica

	cancel context.CancelFunc            // Cancel function for the replication goroutine's context
	errCh  chan error                    // Error channel for the final exit status of the replication goroutine
	events chan sqlcapture.DatabaseEvent // Change event channel from the replication goroutine to the main thread

	fromLSN LSN // The LSN from which we will request changes on the next polling cycle

	fenceLSN LSN // The latest fence position, updated at the end of each StreamToFence cycle.

	maxTransactionsPerScanSession int

	tables struct {
		sync.RWMutex
		info map[string]*tableReplicationInfo
	}

	cleanup struct {
		sync.RWMutex
		ackLSN LSN // The latest LSN to be acknowledged by Flow
	}
}

type tableReplicationInfo struct {
	KeyColumns      []string
	Columns         map[string]sqlcapture.ColumnInfo
	ColumnTypes     map[string]any
	ComputedColumns []string // List of the names of computed columns in this table, in no particular order.
}

func (rs *sqlserverReplicationStream) open(ctx context.Context) error {
	var (
		dbName     string
		cdcEnabled bool
	)
	const query = `SELECT name, is_cdc_enabled FROM sys.databases WHERE name = db_name()`
	if err := rs.conn.QueryRowContext(ctx, query).Scan(&dbName, &cdcEnabled); err != nil {
		return fmt.Errorf("error querying cdc state: %w", err)
	}
	if !cdcEnabled {
		return fmt.Errorf("CDC is not enabled on database %q", dbName)
	}

	// Check replica status. Some operations work differently on replicas.
	isReplica, err := isReplicaDatabase(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error determining if database is a replica: %w", err)
	}
	rs.isReplica = isReplica

	rs.errCh = make(chan error)
	rs.events = make(chan sqlcapture.DatabaseEvent)
	rs.tables.info = make(map[string]*tableReplicationInfo)
	return nil
}

func (rs *sqlserverReplicationStream) ActivateTable(ctx context.Context, streamID string, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
	log.WithField("table", streamID).Trace("activate table")

	var columnTypes = make(map[string]any)
	for columnName, columnInfo := range discovery.Columns {
		columnTypes[columnName] = columnInfo.DataType
	}

	var computedColumns []string
	if details, ok := discovery.ExtraDetails.(*sqlserverTableDiscoveryDetails); ok {
		computedColumns = details.ComputedColumns
	}

	rs.tables.Lock()
	rs.tables.info[streamID] = &tableReplicationInfo{
		KeyColumns:      keyColumns,
		Columns:         discovery.Columns,
		ColumnTypes:     columnTypes,
		ComputedColumns: computedColumns,
	}
	rs.tables.Unlock()
	log.WithFields(log.Fields{"stream": streamID}).Debug("activated table")
	return nil
}

func (rs *sqlserverReplicationStream) deactivateTable(streamID string) error {
	rs.tables.Lock()
	defer rs.tables.Unlock()
	delete(rs.tables.info, streamID)
	return nil
}

func (rs *sqlserverReplicationStream) StartReplication(ctx context.Context, discovery map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	// Activate replication for the watermarks table (if this isn't a read-only capture).
	if !rs.db.featureFlags["read_only"] {
		var watermarks = rs.db.config.Advanced.WatermarksTable
		var watermarksInfo = discovery[watermarks]
		if watermarksInfo == nil {
			return fmt.Errorf("error activating replication for watermarks table %q: table missing from latest autodiscovery", watermarks)
		}
		if err := rs.ActivateTable(ctx, watermarks, watermarksInfo.PrimaryKey, watermarksInfo, nil); err != nil {
			return fmt.Errorf("error activating replication for watermarks table %q: %w", watermarks, err)
		}
	}

	var streamCtx, streamCancel = context.WithCancel(ctx)
	rs.events = make(chan sqlcapture.DatabaseEvent, replicationBufferSize)
	rs.errCh = make(chan error)
	rs.cancel = streamCancel

	go func() {
		var err = rs.run(streamCtx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		close(rs.events)
		rs.errCh <- err
	}()
	return nil
}

func (rs *sqlserverReplicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	// We support three different fence mechanisms for SQL Server, which are appropriate
	// in different conditions:
	//  - streamToWatermarkFence: Legacy mechanism. Only works on the master DB. Requires a
	//    writable watermarks table.
	//  - streamToPositionalFence: Works on the master DB. Uses the CDC log scan sessions view
	//    to establish a fence LSN. Requires VIEW DATABASE STATE permission.
	//  - streamToReplicaFence: Works on any DB, including read replicas. Provides slightly
	//    weaker causality guarantees (but the fully-reduced capture output is still always
	//    correct once fully caught up).
	//
	// We only ever use streamToWatermarkFence for legacy captures with 'no_read_only' set,
	// and we only use streamToReplicaFence if we're connected to a replica or the user
	// explicitly requested it by setting the 'replica_fencing' flag. But we could decide
	// in the future to simplify things and just always use streamToReplicaFence, if the
	// looser correctness guarantee is acceptable.
	if !rs.db.featureFlags["read_only"] {
		return rs.streamToWatermarkFence(ctx, fenceAfter, callback)
	}
	if rs.isReplica || rs.db.featureFlags["replica_fencing"] {
		return rs.streamToReplicaFence(ctx, fenceAfter, callback)
	}
	return rs.streamToPositionalFence(ctx, fenceAfter, callback)
}

// streamToReplicaFence implements the stream-to-fence operation without watermark writes,
// using a mechanism that works even against a read replica (unlike streamToPositionalFence)
// but which doesn't provide quite as strict of causality guarantees as streamToPositionalFence.
//
// Put simply: this implementation can only guarantee that you're caught up to the latest CDC
// events available, but it can't guarantee that you've reached a point in the WAL after the
// start of the operation. It could lag behind in various cases, including:
// - You may be a few seconds out of date just because of the CDC polling interval.
// - The CDC worker itself could have just run but could be lagging due to the maxtrans*maxscans limit.
// - The CDC worker might not be running at all (which is basically just an infinite lag).
//
// I keep going back and forth on whether these caveats actually _matter_ in any real sense.
// On the one hand it's nice to provide as strong of correctness guarantees as we can. But on
// the other hand there is only one actual downside and it's pretty tiny. Consider a row which
// goes through states A -> B -> C just as our backfill reaches it. If we backfill the row at B,
// then the captured sequence of changes might go '[Insert(B)], [Insert(A), Update(B), Update(C)]'
// when using this fence policy, whereas streamToPositionalFence can guarantee that the whole
// sequence of changes '[Insert(B), Insert(A), Update(B), Update(C)]' is emitted within a single
// Flow transaction and thus (when not using history mode) is reduced together into 'Insert(C)'
// from the perspective of any data consumers.
//
// For now I've opted to just implement this mechanism side-by-side with streamToPositionalFence
// and automatically select between them depending on whether we're connected to a replica. But
// in the future  we might consider removing the extra complexity and just always using this one.
func (rs *sqlserverReplicationStream) streamToReplicaFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Time-based event streaming until the fenceAfter duration is reached.
	var latestFlushLSN = rs.fenceLSN
	var timedEventsSinceFlush int
	if fenceAfter > 0 {
		log.WithField("cursor", fmt.Sprintf("%X", latestFlushLSN)).Debug("beginning timed streaming phase")
		var deadline = time.NewTimer(fenceAfter)
		defer deadline.Stop()
	timedLoop:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline.C:
				break timedLoop
			case event, ok := <-rs.events:
				if !ok {
					return sqlcapture.ErrFenceNotReached
				} else if err := callback(event); err != nil {
					return err
				}
				timedEventsSinceFlush++
				if event, ok := event.(*sqlcapture.FlushEvent); ok {
					var eventCursor, err = unmarshalJSONString(event.Cursor)
					if err != nil {
						return fmt.Errorf("internal error: failed to decode flush event cursor: %w", err)
					}
					eventLSN, err := base64.StdEncoding.DecodeString(eventCursor)
					if err != nil {
						return fmt.Errorf("internal error: failed to parse flush event cursor value %q", event.Cursor)
					}
					latestFlushLSN = eventLSN
					timedEventsSinceFlush = 0
				}
			}
		}
	}
	log.WithField("cursor", fmt.Sprintf("%X", latestFlushLSN)).Debug("finished timed streaming phase")

	// Establish the fence position by querying the latest CDC time mapping LSN.
	var nextFenceLSN LSN
	if currentLSN, err := cdcGetMaxLSN(ctx, rs.conn); err != nil {
		return fmt.Errorf("error getting current LSN for replica fence: %w", err)
	} else {
		nextFenceLSN = currentLSN
	}

	// Early-exit fast path for when the database has been idle since the last commit event.
	if bytes.Compare(nextFenceLSN, latestFlushLSN) <= 0 {
		log.WithField("cursor", fmt.Sprintf("%X", latestFlushLSN)).WithField("target", fmt.Sprintf("%X", nextFenceLSN)).Debug("fenced streaming phased exited via idle fast-path")

		// As an internal sanity check, we assert that it should never be possible
		// to hit this early exit unless the database has been idle since the last
		// flush event we observed.
		if timedEventsSinceFlush > 0 {
			return fmt.Errorf("internal error: sanity check failed: already at fence after processing %d changes during timed phase", timedEventsSinceFlush)
		}

		// Mark the position of the flush event as the latest fence before returning.
		rs.fenceLSN = latestFlushLSN

		// Since we're still at a valid flush position and those are always between
		// transactions, we can safely emit a synthetic FlushEvent here. This means
		// that every StreamToFence operation ends in a flush, and is helpful since
		// there's a lot of implicit assumptions of regular events / flushes.
		return callback(&sqlcapture.FlushEvent{
			Cursor: marshalJSONString(base64.StdEncoding.EncodeToString(latestFlushLSN)),
		})
	}

	// Given that the early-exit fast path was not taken, there must be further data for
	// us to read. Thus if we sit idle for a nontrivial length of time without reaching
	// our fence position, something is wrong and we should error out instead of blocking
	// forever.
	var fenceWatchdog = time.NewTimer(streamToFenceWatchdogTimeout)

	// Stream replication events until the fence is reached.
	log.WithField("cursor", fmt.Sprintf("%X", latestFlushLSN)).WithField("target", fmt.Sprintf("%X", nextFenceLSN)).Debug("beginning fenced streaming phase")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fenceWatchdog.C:
			return fmt.Errorf("replication became idle while streaming from %X to an established fence at %X", latestFlushLSN, nextFenceLSN)
		case event, ok := <-rs.events:
			fenceWatchdog.Reset(streamToFenceWatchdogTimeout)
			if !ok {
				return sqlcapture.ErrFenceNotReached
			} else if err := callback(event); err != nil {
				return err
			}

			// Mark the fence as reached when we observe a commit event whose cursor value
			// is greater than or equal to the fence LSN.
			if event, ok := event.(*sqlcapture.FlushEvent); ok {
				// It might be a bit inefficient to re-parse every flush cursor here, but
				// realistically it's probably not a significant slowdown and it would be
				// a bit more work to preserve the position as a typed struct.
				var eventCursor, err = unmarshalJSONString(event.Cursor)
				if err != nil {
					return fmt.Errorf("internal error: failed to decode flush event cursor: %w", err)
				}
				eventLSN, err := base64.StdEncoding.DecodeString(eventCursor)
				if err != nil {
					return fmt.Errorf("internal error: failed to parse flush event cursor value %q", event.Cursor)
				}
				if bytes.Compare(eventLSN, nextFenceLSN) >= 0 {
					log.WithField("eventLSN", fmt.Sprintf("%X", eventLSN)).Debug("finished fenced streaming phase")
					rs.fenceLSN = bytes.Clone(eventLSN)
					return nil
				}
			}
		}
	}
}

// streamToPositionalFence implements the stream-to-fence operation without watermark writes,
// using a mechanism that only works on the master DB but is exactly correct. This mechanism is
// based on observing the dm_cdc_log_scan_sessions view to determine when the CDC worker has
// finished processing all changes up to a certain point, and then querying the latest CDC LSN
// after that occurs.
//
// Since we know that the worker has just finished catching up to the current end of the WAL,
// and the CDC LSN reflects the latest CDC-eligible change it processed, we can use that LSN
// as our fence position and know that it is definitely not-before the start of the operation.
//
// This mechanism requires the VIEW DATABASE STATE permission on the database.
func (rs *sqlserverReplicationStream) streamToPositionalFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Keep track of the most recent scan session when we started this stream-to-fence cycle.
	// This will be used to ensure that the fence position we establish occurs after anything
	// prior to this function invocation.
	var mostRecentSession, err = latestCDCLogScanSession(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error establishing fence position: %w", err)
	}

	// Stream change events until the fenceAfter duration is reached and then a valid fence
	// position is able to be established. Note that because of how the SQL Server CDC scan
	// worker runs asynchronously, if fenceAfter>0 it is possible for the resulting fence LSN
	// temporally slightly earlier than the instant when the fenceAfter duration we reached,
	// but this is okay because the core invariants we need to maintain here are just that the
	// fence LSN be causally *after* the point at which a stream-to-fence operation began and
	// that the operation takes at least `fenceAfter` to complete.
	//
	// The behavior of 'establishFenceTimer' is a little bit complicated here. Initially it's
	// set to the 'fenceAfter' duration so we'll run for at least that long before checking if
	// we can establish a new fence position. After that initial duration is reached, we reset
	// it to one second for each retry.
	var timedEventsSinceFlush int
	var latestFlushLSN = rs.fenceLSN
	var nextFenceLSN LSN

	var establishFenceTimer = time.NewTimer(fenceAfter)
	var establishFenceDeadline = time.NewTimer(fenceAfter + establishFenceTimeout)
	defer establishFenceTimer.Stop()
	defer establishFenceDeadline.Stop()

	log.WithField("cursor", fmt.Sprintf("%X", latestFlushLSN)).Debug("beginning timed streaming phase")
loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-establishFenceDeadline.C:
			log.WithField("afterTime", mostRecentSession.EndTime.Format(time.RFC3339)).Error("failed to establish fence position")
			return fmt.Errorf("failed to establish a valid fence position after %s, the CDC worker may not be running", establishFenceTimeout.String())
		case <-establishFenceTimer.C:
			// Try to establish a new fence LSN. If no error is encountered but a new LSN couldn't
			// be established then both values will be nil and we'll retry after a short interval.
			if lsn, err := rs.tryToEstablishFencePosition(ctx, mostRecentSession.EndTime); err != nil {
				return fmt.Errorf("error establishing fence position: %w", err)
			} else if lsn != nil {
				nextFenceLSN = lsn
				break loop
			} else {
				establishFenceTimer.Reset(1 * time.Second)
				continue
			}
		case event, ok := <-rs.events:
			if !ok {
				return sqlcapture.ErrFenceNotReached
			} else if err := callback(event); err != nil {
				return err
			}
			timedEventsSinceFlush++
			if event, ok := event.(*sqlcapture.FlushEvent); ok {
				var eventCursor, err = unmarshalJSONString(event.Cursor)
				if err != nil {
					return fmt.Errorf("internal error: failed to decode flush event cursor: %w", err)
				}
				eventLSN, err := base64.StdEncoding.DecodeString(eventCursor)
				if err != nil {
					return fmt.Errorf("internal error: failed to parse flush event cursor value %q", event.Cursor)
				}
				latestFlushLSN = eventLSN
				timedEventsSinceFlush = 0
			}
		}
	}
	log.WithField("cursor", fmt.Sprintf("%X", latestFlushLSN)).Debug("finished timed streaming phase")

	// Early-exit fast path for when the database has been idle since the last commit event.
	if bytes.Compare(nextFenceLSN, latestFlushLSN) <= 0 {
		log.WithField("cursor", fmt.Sprintf("%X", latestFlushLSN)).WithField("target", fmt.Sprintf("%X", nextFenceLSN)).Debug("fenced streaming phased exited via idle fast-path")

		// As an internal sanity check, we assert that it should never be possible
		// to hit this early exit unless the database has been idle since the last
		// flush event we observed.
		if timedEventsSinceFlush > 0 {
			return fmt.Errorf("internal error: sanity check failed: already at fence after processing %d changes during timed phase", timedEventsSinceFlush)
		}

		// Mark the position of the flush event as the latest fence before returning.
		rs.fenceLSN = latestFlushLSN

		// Since we're still at a valid flush position and those are always between
		// transactions, we can safely emit a synthetic FlushEvent here. This means
		// that every StreamToFence operation ends in a flush, and is helpful since
		// there's a lot of implicit assumptions of regular events / flushes.
		return callback(&sqlcapture.FlushEvent{
			Cursor: marshalJSONString(base64.StdEncoding.EncodeToString(latestFlushLSN)),
		})
	}

	// Given that the early-exit fast path was not taken, there must be further data for
	// us to read. Thus if we sit idle for a nontrivial length of time without reaching
	// our fence position, something is wrong and we should error out instead of blocking
	// forever.
	var fenceWatchdog = time.NewTimer(streamToFenceWatchdogTimeout)

	// Stream replication events until the fence is reached.
	log.WithField("cursor", fmt.Sprintf("%X", latestFlushLSN)).WithField("target", fmt.Sprintf("%X", nextFenceLSN)).Debug("beginning fenced streaming phase")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fenceWatchdog.C:
			return fmt.Errorf("replication became idle while streaming from %X to an established fence at %X", latestFlushLSN, nextFenceLSN)
		case event, ok := <-rs.events:
			fenceWatchdog.Reset(streamToFenceWatchdogTimeout)
			if !ok {
				return sqlcapture.ErrFenceNotReached
			} else if err := callback(event); err != nil {
				return err
			}

			// Mark the fence as reached when we observe a commit event whose cursor value
			// is greater than or equal to the fence LSN.
			if event, ok := event.(*sqlcapture.FlushEvent); ok {
				// It might be a bit inefficient to re-parse every flush cursor here, but
				// realistically it's probably not a significant slowdown and it would be
				// a bit more work to preserve the position as a typed struct.
				var eventCursor, err = unmarshalJSONString(event.Cursor)
				if err != nil {
					return fmt.Errorf("internal error: failed to decode flush event cursor: %w", err)
				}
				eventLSN, err := base64.StdEncoding.DecodeString(eventCursor)
				if err != nil {
					return fmt.Errorf("internal error: failed to parse flush event cursor value %q", event.Cursor)
				}
				if bytes.Compare(eventLSN, nextFenceLSN) >= 0 {
					log.WithField("eventLSN", fmt.Sprintf("%X", eventLSN)).Debug("finished fenced streaming phase")
					rs.fenceLSN = bytes.Clone(eventLSN)
					return nil
				}
			}
		}
	}
}

// tryToEstablishFencePosition queries the 'sys.dm_cdc_log_scan_sessions' database management view
// to check whether a new CDC scan session has completed.
//
// If there's an error doing this, an error is returned. If there's no error but a new fence LSN
// couldn't be established, a nil LSN is returned with a nil error and we need to try again.
func (rs *sqlserverReplicationStream) tryToEstablishFencePosition(ctx context.Context, afterTime time.Time) (LSN, error) {
	// In general we can assume that any time we see the ending timestamp of the most recent
	// CDC scan session increase it means that we're fully caught-up. The exception is when
	// there's a large burst of changes in the WAL such that the CDC agent is still working
	// on getting caught up itself.
	//
	// Since there's an explicit limit to how many transactions the CDC agent will copy out
	// of the WAL per scan session, we can infer that if the number of transactions in the
	// most recent scan session is less than that limit then we're definitely caught up as
	// of the moment that scan session ended.
	//
	// We try to get that limit by querying msdb.dbo.cdc_jobs for the actual setting, but
	// fall back to assuming it's the default value of 500 if we don't have permission to
	// access that table. In either case the resulting value is cached for the lifetime of
	// the current capture task run.
	//
	// TODO(wgd): Consider making it a hard failure if we lack this permission?
	if rs.maxTransactionsPerScanSession == 0 {
		jobConfig, err := cdcQueryCaptureJobConfig(ctx, rs.conn)
		if err == nil {
			log.WithFields(log.Fields{
				"jobID":      jobConfig.JobID,
				"databaseID": jobConfig.DatabaseID,
				"maxScans":   jobConfig.MaxScans,
				"maxTrans":   jobConfig.MaxTrans,
				"continuous": jobConfig.Continuous,
				"poll":       jobConfig.PollInterval,
			}).Debug("queried CDC job config")
			rs.maxTransactionsPerScanSession = jobConfig.MaxTrans
		} else {
			log.WithField("err", err).Warn("failed to query CDC job config")
			rs.maxTransactionsPerScanSession = 500 // Default is 500 transactions per log scan session
		}
	}

	// Obtain information about the latest CDC log scan session and the current maximum CDC LSN.
	// The order in which we fetch these is important, because we need to know that the CDC max-LSN
	// value is causally not-before the log scan session we examine.
	session, err := latestCDCLogScanSession(ctx, rs.conn)
	if err != nil {
		return nil, err
	}
	cdcMaxLSN, err := cdcGetMaxLSN(ctx, rs.conn)
	if err != nil {
		return nil, err
	}

	if session.EndTime.After(afterTime) && session.TransactionCount < rs.maxTransactionsPerScanSession {
		log.WithField("fenceLSN", fmt.Sprintf("%X", cdcMaxLSN)).Debug("established new CDC fence position")
		return cdcMaxLSN, nil
	}
	return nil, nil // No error, but we'll need to retry to get an LSN
}

type logScanSession struct {
	SessionID        int
	EndTime          time.Time
	TransactionCount int
}

// Return the timestamp of the latest completed CDC log scan session.
//
// The 'sys.dm_cdc_log_scan_sessions' management view requires either the 'VIEW DATABASE STATE'
// permission, or in SQL Server 2022+ the 'VIEW DATABASE PERFORMANCE STATE' permission is also
// acceptable. To grant the required permission, run one of the following queries:
//
//	GRANT VIEW DATABASE PERFORMANCE STATE TO flow_capture
//	GRANT VIEW DATABASE STATE TO flow_capture
func latestCDCLogScanSession(ctx context.Context, conn *sql.DB) (*logScanSession, error) {
	const query = `SELECT session_id, end_time, tran_count FROM sys.dm_cdc_log_scan_sessions WHERE scan_phase = 'Done' ORDER BY session_id DESC OFFSET 0 ROWS FETCH FIRST 1 ROW ONLY;`
	var s logScanSession
	if err := conn.QueryRowContext(ctx, query).Scan(&s.SessionID, &s.EndTime, &s.TransactionCount); err != nil {
		return nil, fmt.Errorf("error querying sys.dm_cdc_log_scan_sessions: %w", err)
	}
	log.WithFields(log.Fields{
		"sessionID": s.SessionID,
		"endTime":   s.EndTime.Format(time.RFC3339),
		"tranCount": s.TransactionCount,
	}).Debug("queried latest CDC log scan information")
	return &s, nil
}

// streamToWatermarkFence implements the stream-to-fence operation using a watermark table
// write and the later observation of that write to establish when we're caught up. This is
// the legacy behavior which we retain for backwards compatibility, only because the other
// two implementations both have at least one downside:
//   - streamToPositionalFence requires the VIEW DATABASE STATE permission, which legacy captures
//     often haven't granted to the capture user.
//   - streamToReplicaFence doesn't provide as strong of causality guarantees as the watermark mechanism.
//
// If we decided that streamToReplicaFence was adequate we'd probably remove this outright.
func (rs *sqlserverReplicationStream) streamToWatermarkFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Time-based event streaming until the fenceAfter duration is reached.
	if fenceAfter > 0 {
		var deadline = time.NewTimer(fenceAfter)
		defer deadline.Stop()

	loop:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline.C:
				break loop
			case event, ok := <-rs.events:
				if !ok {
					return sqlcapture.ErrFenceNotReached
				} else if err := callback(event); err != nil {
					return err
				}
			}
		}
	}

	// Establish a watermark-based fence position.
	var fenceWatermark = uuid.New().String()
	if err := rs.db.WriteWatermark(ctx, fenceWatermark); err != nil {
		return fmt.Errorf("error establishing watermark fence: %w", err)
	}

	// Stream replication events until the fence is reached.
	var fenceReached = false
	var watermarkStreamID = rs.db.WatermarksTable()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-rs.events:
			if !ok {
				return sqlcapture.ErrFenceNotReached
			} else if err := callback(event); err != nil {
				return err
			}

			// Mark the fence as reached when we observe a change event on the watermarks stream
			// with the expected value.
			if event, ok := event.(*sqlcapture.ChangeEvent); ok {
				if event.Operation != sqlcapture.DeleteOp && event.Source.Common().StreamID() == watermarkStreamID {
					var actual = event.After["watermark"]
					if actual == nil {
						actual = event.After["WATERMARK"]
					}
					log.WithFields(log.Fields{"expected": fenceWatermark, "actual": actual}).Debug("watermark change")
					if actual == fenceWatermark {
						fenceReached = true
					}
				}
			}

			// The flush event following the watermark change ends the stream-to-fence operation.
			if _, ok := event.(*sqlcapture.FlushEvent); ok && fenceReached {
				return nil
			}
		}
	}
}

func (rs *sqlserverReplicationStream) Acknowledge(ctx context.Context, cursorJSON json.RawMessage) error {
	var cursor, err = unmarshalJSONString(cursorJSON)
	if err != nil {
		return fmt.Errorf("error unmarshalling cursor: %w", err)
	}

	log.WithField("cursor", cursor).Debug("acknowledged up to cursor")
	if cursor == "" {
		return nil
	}

	cursorLSN, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return fmt.Errorf("error decoding cursor %q: %w", cursor, err)
	}

	rs.cleanup.Lock()
	rs.cleanup.ackLSN = cursorLSN
	rs.cleanup.Unlock()
	return nil
}

func (rs *sqlserverReplicationStream) Close(ctx context.Context) error {
	log.Debug("replication stream close requested")
	rs.cancel()
	return <-rs.errCh
}

func (rs *sqlserverReplicationStream) run(ctx context.Context) error {
	// Run capture instance management once at startup before entering the main
	// loop. This helps ensure that alteration handling is quick and reliable in
	// tests, and in production it doesn't really matter at all.
	if err := rs.manageCaptureInstances(ctx); err != nil {
		log.WithField("err", err).Error("error managing capture instances")
		return fmt.Errorf("error managing capture instances: %w", err)
	}

	var poll = time.NewTicker(cdcPollingInterval)
	var cleanup = time.NewTicker(cdcCleanupInterval)
	var manage = time.NewTicker(cdcManagementInterval)
	defer poll.Stop()
	defer cleanup.Stop()
	defer manage.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
			if err := rs.pollChanges(ctx); err != nil {
				log.WithField("err", err).Error("error polling change tables")
				return fmt.Errorf("error requesting CDC events: %w", err)
			}
		case <-cleanup.C:
			if err := rs.cleanupChangeTables(ctx); err != nil {
				log.WithField("err", err).Error("error cleaning up change tables")
				return fmt.Errorf("error cleaning up change tables: %w", err)
			}
		case <-manage.C:
			if err := rs.manageCaptureInstances(ctx); err != nil {
				log.WithField("err", err).Error("error managing capture instances")
				return fmt.Errorf("error managing capture instances: %w", err)
			}
		}
	}
}

func (rs *sqlserverReplicationStream) pollChanges(ctx context.Context) error {
	var toLSN, err = cdcGetMaxLSN(ctx, rs.conn)
	if err != nil {
		return err
	}

	if bytes.Equal(rs.fromLSN, toLSN) {
		// Emit a redundant checkpoint even if we've established that there are no new
		// changes. This ensures that the positional stream-to-fence logic will always
		// get an opportunity to observe the latest position.
		var cursor = base64.StdEncoding.EncodeToString(toLSN)
		log.WithField("cursor", cursor).Trace("server lsn hasn't advanced, not polling any tables")
		rs.events <- &sqlcapture.FlushEvent{Cursor: marshalJSONString(cursor)}
		return nil
	}

	// Enumerate all capture instances currently available on the server, then query
	// the database for the highest LSN in every instance's change table.
	captureInstances, err := cdcListCaptureInstances(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error listing CDC instances: %w", err)
	}
	var captureInstanceNames []string
	for _, instancesForStream := range captureInstances {
		for _, instance := range instancesForStream {
			captureInstanceNames = append(captureInstanceNames, instance.Name)
		}
	}
	captureInstanceMaxLSNs, err := cdcGetInstanceMaxLSNs(ctx, rs.conn, captureInstanceNames)
	if err != nil {
		return fmt.Errorf("failed to query maximum LSNs for capture instances: %w", err)
	}

	// Put together a work queue of CDC polling operations to perform
	var queue []*tablePollInfo
	var failed []sqlcapture.StreamID
	rs.tables.RLock()
	for streamID, info := range rs.tables.info {
		// Figure out which capture instance to use for the current polling cycle.
		var instance = newestValidInstance(captureInstances[streamID], rs.fromLSN, rs.db.featureFlags["tolerate_missed_changes"])
		if instance == nil {
			failed = append(failed, streamID)
			continue
		}

		// Skip polling a particular capture instance if its maximum LSN is less than or
		// equal to our 'fromLSN' value for this polling operation, as this means that it
		// doesn't have any new changes for us.
		//
		// As documented on the `cdcGetInstanceMaxLSNs` function, a completely empty change
		// table will have a nil value in the max LSNs map, and the nil byte string compares
		// lexicographically before any other value, so the comparison here also covers the
		// case of a CDC instance which doesn't need to be polled because it has no changes
		// at all.
		var instanceMaxLSN, ok = captureInstanceMaxLSNs[instance.Name]
		if ok && bytes.Compare(instanceMaxLSN, rs.fromLSN) <= 0 {
			log.WithFields(log.Fields{
				"instance":       instance.Name,
				"instanceMaxLSN": instanceMaxLSN,
				"pollFromLSN":    rs.fromLSN,
			}).Trace("skipping poll for unchanged capture instance")
			continue
		}

		queue = append(queue, &tablePollInfo{
			StreamID:        streamID,
			SchemaName:      instance.TableSchema,
			TableName:       instance.TableName,
			InstanceName:    instance.Name,
			KeyColumns:      info.KeyColumns,
			Columns:         info.Columns,
			ColumnTypes:     info.ColumnTypes,
			ComputedColumns: info.ComputedColumns,
			FromLSN:         rs.fromLSN,
			ToLSN:           toLSN,
		})
	}
	rs.tables.RUnlock()

	// For any streams without a valid capture instance to use, emit TableDropEvents and deactivate.
	for _, streamID := range failed {
		log.WithFields(log.Fields{"stream": streamID, "pollFromLSN": fmt.Sprintf("%X", rs.fromLSN)}).Info("dropping stream with no valid capture instance(s)")
		if err := rs.emitEvent(ctx, &sqlcapture.TableDropEvent{
			StreamID: streamID,
			Cause:    fmt.Sprintf("table %q has no (valid) capture instances", streamID),
		}); err != nil {
			return err
		} else if err := rs.deactivateTable(streamID); err != nil {
			return err
		}
	}

	// Execute all polling operations in the work queue
	var workers, workerContext = errgroup.WithContext(ctx)
	workers.SetLimit(cdcPollingWorkers)
	for _, item := range queue {
		var item = item // Copy to avoid loop+closure issues
		workers.Go(func() error {
			if err := rs.pollTable(workerContext, item); err != nil {
				return fmt.Errorf("error polling changes for table %q: %w", item.StreamID, err)
			}
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return err
	}

	var cursor = base64.StdEncoding.EncodeToString(toLSN)
	log.WithField("cursor", cursor).Debug("checkpoint at cursor")
	rs.events <- &sqlcapture.FlushEvent{Cursor: marshalJSONString(cursor)}
	rs.fromLSN = toLSN

	return nil
}

// newestValidInstance selects the newest (according to the `create_date` column of
// the system table `cdc.change_tables`) capture instance which has a `start_lsn`
// lower bound less than (or equal to) the specified LSN.
//
// The logic is that in general we always want to use the newest capture instance
// for polling, but if the newest instance was created more recently than our last
// polling cycle we can't quite use it yet, so we have to wait until our `fromLSN`
// for polling advances past the point where the capture instance was created.
func newestValidInstance(instances []*captureInstanceInfo, fromLSN LSN, tolerateMissedChanges bool) *captureInstanceInfo {
	var selected *captureInstanceInfo
	for _, candidate := range instances {
		if bytes.Compare(candidate.StartLSN, fromLSN) > 0 {
			continue // Can't use this one yet, the StartLSN is newer than our current FromLSN
		}
		if selected == nil || candidate.CreateDate.After(selected.CreateDate) {
			selected = candidate // Retain the newest valid instance
		}
	}

	// If we're tolerating missed changes and nothing passed the StartLSN>fromLSN check,
	// use the instance with the earliest StartLSN instead.
	if selected == nil && tolerateMissedChanges {
		for _, candidate := range instances {
			if selected == nil || bytes.Compare(candidate.StartLSN, selected.StartLSN) < 0 {
				selected = candidate
			}
		}
	}

	return selected
}

// manageCaptureInstances handles creating new capture instances in response to alterations of source
// tables, and deleting old capture instances once they are no longer needed.
func (rs *sqlserverReplicationStream) manageCaptureInstances(ctx context.Context) error {
	// If the user hasn't requested automatic capture instance management, do nothing
	if !rs.cfg.Advanced.AutomaticCaptureInstances {
		return nil
	}

	// Query system tables for the latest instance list and any DDL events of note.
	captureInstances, err := cdcListCaptureInstances(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error listing CDC instances: %w", err)
	}
	ddlHistory, err := cdcGetDDLHistory(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error fetching DDL history: %w", err)
	}

	// See whether any DDL events require us to create new capture instances. Since
	// our polling logic will seamlessly switch to the latest instance at the right
	// time and we clean up after ourselves, it is okay to err on the side of doing
	// this more often than necessary. So we consider any DDL events to require new
	// capture intance creation.
	var createInstances []*ddlHistoryEntry // (Ab)using the DDL history events to track this because they hold the schema and table name
	rs.tables.RLock()
	for streamID := range rs.tables.info {
		if len(captureInstances[streamID]) != 1 {
			continue // Ignore any streams which don't have exactly one capture instance, we can't do anything about those now.
		}

		// Since `ddl_history` entries are deleted when the corresponding capture instance is
		// deleted, and we've just checked to make sure there's only one capture instance for
		// this table, we can assume any nonzero number of DDL events means that changes have
		// occurred since the current instance was created.
		if events := ddlHistory[streamID]; len(events) > 0 {
			for _, event := range events {
				log.WithFields(log.Fields{
					"stream":  streamID,
					"command": event.Command,
					"lsn":     fmt.Sprintf("%X", event.LSN),
					"ts":      event.Time.UTC().Format(time.RFC3339),
				}).Debug("observed DDL event on stream")
			}

			// Queue a new capture instance creation for this table
			createInstances = append(createInstances, events[len(events)-1])
		}
	}
	rs.tables.RUnlock()

	// Queue for deletion any capture instances (on tables which we're responsible for)
	// where there is a newer instance for the same table which it's valid to use.
	var deleteInstances []*captureInstanceInfo
	rs.tables.RLock()
	for streamID := range rs.tables.info {
		if len(captureInstances[streamID]) == 1 {
			// No deletions possible when there's only one. Strictly speaking this is
			// probably not required as all the other logic should correctly identify
			// that this instance is not eligible for deletion, but it's useful to be
			// explicit about that.
			continue
		}
		var activeInstance = newestValidInstance(captureInstances[streamID], rs.fromLSN, rs.db.featureFlags["tolerate_missed_changes"])
		for _, candidate := range captureInstances[streamID] {
			// Any instance which is valid (startLSN <= fromLSN) but not the newest one is eligible for deletion.
			if bytes.Compare(candidate.StartLSN, rs.fromLSN) <= 0 && candidate != activeInstance {
				deleteInstances = append(deleteInstances, candidate)
			}
		}
	}
	rs.tables.RUnlock()

	// Perform all instance creations
	for _, create := range createInstances {
		var instanceName, err = cdcCreateCaptureInstance(ctx, rs.conn, create.TableSchema, create.TableName, rs.cfg.User, rs.cfg.Advanced.Filegroup, rs.cfg.Advanced.RoleName)
		if err != nil {
			return err
		}
		log.WithFields(log.Fields{
			"schema":   create.TableSchema,
			"table":    create.TableName,
			"instance": instanceName,
		}).Info("created new capture instance for table")
	}

	// Perform all instance deletions
	for _, delete := range deleteInstances {
		if err := cdcDeleteCaptureInstance(ctx, rs.conn, delete.TableSchema, delete.TableName, delete.Name); err != nil {
			return err
		}
		log.WithFields(log.Fields{
			"schema":   delete.TableSchema,
			"table":    delete.TableName,
			"instance": delete.Name,
		}).Info("deleted obsolete capture instance")
	}

	return nil
}

// cleanupChangeTables handles pruning change tables of events which have been persisted into Flow.
func (rs *sqlserverReplicationStream) cleanupChangeTables(ctx context.Context) error {
	// If the user hasn't requested automatic change table cleanup, do nothing
	if !rs.cfg.Advanced.AutomaticChangeTableCleanup {
		return nil
	}

	// Read the latest cleanup metadata
	rs.cleanup.RLock()
	var ackedLSN = rs.cleanup.ackLSN
	rs.cleanup.RUnlock()

	// Enumerate all capture instances currently available on the server.
	captureInstances, err := cdcListCaptureInstances(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error listing CDC instances: %w", err)
	}

	// Make a list of capture instances from our streams which can be usefully cleaned up
	var cleanupInstances []string
	rs.tables.RLock()
	for streamID := range rs.tables.info {
		for _, instance := range captureInstances[streamID] {
			if bytes.Compare(instance.StartLSN, ackedLSN) < 0 {
				cleanupInstances = append(cleanupInstances, instance.Name)
			}
		}
	}
	rs.tables.RUnlock()

	// Execute cleanup operations in parallel
	var workers, workerContext = errgroup.WithContext(ctx)
	workers.SetLimit(cdcCleanupWorkers)
	for _, instanceName := range cleanupInstances {
		var instanceName = instanceName // Copy to avoid loop+closure issues
		workers.Go(func() error {
			return cdcCleanupChangeTable(workerContext, rs.conn, instanceName, ackedLSN)
		})
	}
	if err := workers.Wait(); err != nil {
		return err
	}
	return nil
}

type tablePollInfo struct {
	StreamID        sqlcapture.StreamID
	SchemaName      string
	TableName       string
	InstanceName    string
	KeyColumns      []string
	Columns         map[string]sqlcapture.ColumnInfo
	ColumnTypes     map[string]any
	ComputedColumns []string // List of the names of computed columns in this table, in no particular order.
	FromLSN         LSN
	ToLSN           LSN
}

func (rs *sqlserverReplicationStream) pollTable(ctx context.Context, info *tablePollInfo) error {
	log.WithFields(log.Fields{
		"stream":   info.StreamID,
		"instance": info.InstanceName,
		"fromLSN":  info.FromLSN,
		"toLSN":    info.ToLSN,
	}).Trace("polling stream")

	var query = fmt.Sprintf(`SELECT * FROM cdc.fn_cdc_get_all_changes_%s(@p1, @p2, N'all');`, info.InstanceName)
	rows, err := rs.conn.QueryContext(ctx, query, info.FromLSN, info.ToLSN)
	if err != nil {
		return fmt.Errorf("error requesting changes: %w", err)
	}
	defer rows.Close()

	cnames, err := rows.Columns()
	if err != nil {
		return err
	}

	var vals = make([]any, len(cnames))
	var vptrs = make([]any, len(vals))
	for idx := range vals {
		vptrs[idx] = &vals[idx]
	}

	for rows.Next() {
		if err := rows.Scan(vptrs...); err != nil {
			return fmt.Errorf("error scanning result row: %w", err)
		}
		var fields = make(map[string]interface{})
		for idx, name := range cnames {
			fields[name] = vals[idx]
		}
		for _, name := range info.ComputedColumns {
			// Computed columns always have a value of null in the change table so we should never capture them.
			// One wonders why they're present in the change table at all, but this is a documented fact about SQL Server CDC.
			delete(fields, name)
		}

		log.WithFields(log.Fields{"stream": info.StreamID, "data": fields}).Trace("got change")

		// Extract the event's LSN and remove it from the record fields, since
		// it will be included in the source metadata already.
		var changeLSN, ok = fields["__$start_lsn"].(LSN)
		if !ok || changeLSN == nil {
			return fmt.Errorf("invalid '__$start_lsn' value (capture user may not have correct permissions): %v", changeLSN)
		}
		if bytes.Compare(changeLSN, info.FromLSN) <= 0 {
			log.WithFields(log.Fields{
				"stream":    info.StreamID,
				"changeLSN": changeLSN,
				"fromLSN":   info.FromLSN,
			}).Trace("filtered change <= fromLSN")
			continue
		}
		delete(fields, "__$start_lsn")

		// Extract the change type (Insert/Update/Delete) and remove it from the record data.
		var operation sqlcapture.ChangeOp
		opval, ok := fields["__$operation"]
		if !ok {
			return fmt.Errorf("internal error: cdc operation unspecified ('__$operation' unset)")
		}
		opcode, ok := opval.(int64)
		if !ok {
			return fmt.Errorf("internal error: cdc operation code %T(%#v) isn't an int64", opval, opval)
		}
		switch opcode {
		case 1:
			operation = sqlcapture.DeleteOp
		case 2:
			operation = sqlcapture.InsertOp
		case 4:
			operation = sqlcapture.UpdateOp
		default:
			return fmt.Errorf("invalid change operation: %d", opcode)
		}
		delete(fields, "__$operation")

		// Move the '__$seqval' and '__$update_mask' columns into metadata as well.
		seqval, ok := fields["__$seqval"].(LSN)
		if !ok || seqval == nil {
			return fmt.Errorf("invalid '__$seqval' value: %v", seqval)
		}
		delete(fields, "__$seqval")

		var updateMask = fields["__$update_mask"]
		delete(fields, "__$update_mask")

		var rowKey, err = sqlcapture.EncodeRowKey(info.KeyColumns, fields, info.ColumnTypes, encodeKeyFDB)
		if err != nil {
			return fmt.Errorf("error encoding stream %q row key: %w", info.StreamID, err)
		}
		if err := rs.db.translateRecordFields(info.ColumnTypes, fields); err != nil {
			return fmt.Errorf("error translating stream %q change event: %w", info.StreamID, err)
		}

		// For inserts and updates the change event records the state after the
		// change, and for deletions the change event (partially) records the state
		// before the change.
		var before, after map[string]any
		switch operation {
		case sqlcapture.InsertOp, sqlcapture.UpdateOp:
			after = fields
		case sqlcapture.DeleteOp:
			// Sometimes SQL Server column nullability is screwy for deletions. In generaly we should get values
			// for all columns on deletes, with a few exceptions. Since SQL Server CDC stores change events in a
			// table, missing values are represented as nulls in the change table, and since the corresponding
			// source table might have a non-nullable column type we need to ensure that we don't emit those as
			// a literal null value which would fail validation.
			for colName, val := range fields {
				// As an added safety check we also make sure this logic doesn't apply to key columns.
				if val == nil && !slices.Contains(info.KeyColumns, colName) {
					if colInfo, ok := info.Columns[colName]; ok {
						if textType, ok := colInfo.DataType.(*sqlserverTextColumnType); ok && (textType.Type == "text" || textType.Type == "ntext") {
							// According to MS documentation, text and ntext columns are always omitted (null) in delete events.
							delete(fields, colName)
						} else if colInfo.DataType == "image" {
							// According to MS documentation, image columns are always omitted (null) in delete events.
							delete(fields, colName)
						} else if !colInfo.IsNullable {
							// Sometimes, in direct violation of the documented behavior, we have seen null values on
							// other columns which should definitely be non-nullable. For some reason it seems to mostly
							// happen on a specific BIT NOT NULL column of tables with multiple, and we suspect this is
							// a SQL Server bug (possibly related to how bit fields are packed in WAL events?)
							//
							// Regardless of the reasons, this is a catch-all case which should protect us from emitting
							// null values for non-nullable source columns if it happens again.
							delete(fields, colName)
						}
					}
				}
			}
			before = fields
		}

		var event = &sqlcapture.ChangeEvent{
			Operation: operation,
			RowKey:    rowKey,
			Source: &sqlserverSourceInfo{
				SourceCommon: sqlcapture.SourceCommon{
					Schema: info.SchemaName,
					Table:  info.TableName,
				},
				LSN:        changeLSN,
				SeqVal:     seqval,
				UpdateMask: updateMask,
			},
			Before: before,
			After:  after,
		}
		if err := rs.emitEvent(ctx, event); err != nil {
			return err
		}
	}

	return nil
}

func (rs *sqlserverReplicationStream) emitEvent(ctx context.Context, event sqlcapture.DatabaseEvent) error {
	select {
	case rs.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cdcGetMaxLSN(ctx context.Context, conn *sql.DB) (LSN, error) {
	var maxLSN LSN
	const query = `SELECT sys.fn_cdc_get_max_lsn();`
	if err := conn.QueryRowContext(ctx, query).Scan(&maxLSN); err != nil {
		return nil, fmt.Errorf("error querying database maximum LSN: %w", err)
	}
	if len(maxLSN) == 0 {
		return nil, fmt.Errorf("invalid result from 'sys.fn_cdc_get_max_lsn()', agent process likely not running")
	}
	return maxLSN, nil
}

type captureInstanceInfo struct {
	Name                   string
	TableSchema, TableName string
	RoleName               string
	StartLSN               LSN
	CreateDate             time.Time
}

// cdcListCaptureInstances queries SQL Server system tables and returns a map from stream IDs
// to capture instance information.
func cdcListCaptureInstances(ctx context.Context, conn *sql.DB) (map[sqlcapture.StreamID][]*captureInstanceInfo, error) {
	// This query will enumerate all "capture instances" currently present, along with the
	// schema/table names identifying the source table.
	const query = `SELECT ct.capture_instance, sch.name, tbl.name, ct.start_lsn, ct.create_date, ct.role_name
	                 FROM cdc.change_tables AS ct
					 JOIN sys.tables AS tbl ON ct.source_object_id = tbl.object_id
					 JOIN sys.schemas AS sch ON tbl.schema_id = sch.schema_id;`
	var rows, err = conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	// Process result rows from the above query
	var captureInstances = make(map[sqlcapture.StreamID][]*captureInstanceInfo)
	for rows.Next() {
		var info captureInstanceInfo
		var roleName *string
		if err := rows.Scan(&info.Name, &info.TableSchema, &info.TableName, &info.StartLSN, &info.CreateDate, &roleName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		if roleName != nil {
			info.RoleName = *roleName
		} else {
			info.RoleName = ""
		}

		// In SQL Server, every source table may have up to two "capture instances" associated with it.
		// If a capture instance exists which satisfies the configured naming pattern, then that's one
		// that we should use (and thus if the pattern is "flow_<schema>_<table>" we can be fairly sure
		// not to collide with any other uses of CDC on this database).
		var streamID = sqlcapture.JoinStreamID(info.TableSchema, info.TableName)
		log.WithFields(log.Fields{
			"stream":   streamID,
			"instance": info.Name,
		}).Trace("discovered capture instance")
		captureInstances[streamID] = append(captureInstances[streamID], &info)
	}
	return captureInstances, rows.Err()
}

type ddlHistoryEntry struct {
	TableSchema, TableName string    // The schema and name of the source table to which the DDL change was applied.
	RequiredColumnUpdate   bool      // Indicates that the data type of a captured column was modified in the source table. This modification altered the column in the change table.
	Command                string    // The DDL statement applied to the source table.
	LSN                    LSN       // The LSN associated with the DDL statement commit.
	Time                   time.Time // The timestamp at which the DDL change was made to the source table.
}

// cdcGetDDLHistory queries the `cdc.ddl_history` system table and returns the results
// as a map from the associated source table stream ID to an ordered list of changes.
func cdcGetDDLHistory(ctx context.Context, conn *sql.DB) (map[sqlcapture.StreamID][]*ddlHistoryEntry, error) {
	const query = `SELECT sch.name, tbl.name, hist.required_column_update, hist.ddl_command, hist.ddl_lsn, hist.ddl_time
				     FROM cdc.ddl_history AS hist
					 JOIN sys.tables AS tbl ON hist.source_object_id = tbl.object_id
					 JOIN sys.schemas AS sch ON tbl.schema_id = sch.schema_id
					 ORDER BY hist.ddl_lsn;`
	var rows, err = conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	var ddlHistory = make(map[string][]*ddlHistoryEntry)
	for rows.Next() {
		var entry ddlHistoryEntry
		if err := rows.Scan(&entry.TableSchema, &entry.TableName, &entry.RequiredColumnUpdate, &entry.Command, &entry.LSN, &entry.Time); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(entry.TableSchema, entry.TableName)
		ddlHistory[streamID] = append(ddlHistory[streamID], &entry)
	}
	return ddlHistory, rows.Err()
}

// cdcGetInstanceMaxLSNs queries the maximum change event LSN for each listed capture instance.
//
// It does this by generating a hairy union query which simply selects `MAX(__$start_lsn)` for
// the underlying change table of each instance and combines the results. I have verified that
// this specific query executes as a cheap backwards index scan taking the first result for each
// change table, so this should be reasonably efficient.
//
// In the event that a capture instance contains no changes at all, the results map will contain
// a nil value for that instance LSN.
//
// The generated query has contains a distinct `SELECT` statement for every named capture instance,
// but I have verified in practice that this works for querying at least 1k capture instances at a
// time. If we ever hit a scaling limit here we can implement chunking, but so far that doesn't
// appear to be necessary.
func cdcGetInstanceMaxLSNs(ctx context.Context, conn *sql.DB, instanceNames []string) (map[string]LSN, error) {
	var subqueries []string
	for idx, instanceName := range instanceNames {
		// TODO(wgd): It would probably be a good idea to properly quote the CDC table name instead of just splatting
		// the instance name into the query string. But currently we don't do that for backfills or for the actual
		// replication polling query, so it would be premature to worry about it only here.
		var subquery = fmt.Sprintf("SELECT %d AS idx, MAX(__$start_lsn) AS lsn FROM [cdc].[%s_CT]", idx, instanceName)
		subqueries = append(subqueries, subquery)
	}
	var query = strings.Join(subqueries, " UNION ALL ")

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	var results = make(map[string]LSN)
	for rows.Next() {
		var index int
		var instanceMaxLSN LSN
		if err := rows.Scan(&index, &instanceMaxLSN); err != nil {
			return nil, fmt.Errorf("error scanning result: %w", err)
		}
		if index < 0 || index >= len(instanceNames) {
			return nil, fmt.Errorf("internal error: result index %d out of range for instance list %q", index, len(instanceNames))
		}
		var instanceName = instanceNames[index]
		log.WithFields(log.Fields{
			"instance": instanceName,
			"lsn":      instanceMaxLSN,
		}).Trace("queried max LSN for capture instance")
		results[instanceName] = instanceMaxLSN
	}
	return results, rows.Err()
}

func cdcCleanupChangeTable(ctx context.Context, conn *sql.DB, instanceName string, belowLSN LSN) error {
	log.WithFields(log.Fields{"instance": instanceName, "lsn": fmt.Sprintf("%X", belowLSN)}).Debug("cleaning change table")

	var query = fmt.Sprintf("EXEC sys.sp_cdc_cleanup_change_table @capture_instance = @p1, @low_water_mark = 0x%X; ", belowLSN)
	if _, err := conn.ExecContext(ctx, query, instanceName); err != nil {
		return fmt.Errorf("error cleaning capture instance %q below LSN %X: %w", instanceName, belowLSN, err)
	}
	return nil
}

func cdcCreateCaptureInstance(ctx context.Context, conn *sql.DB, schema, table, username, filegroup, roleName string) (string, error) {
	// SQL Server table names may be up to 128 characters, but capture instance names must
	// be at most 100 characters and we have other information to cram in there. The names
	// must be unique, but other than that they might as well be random strings, the logic
	// around capture instance selection is based on the (schema, table, instance) info in
	// `cdc.change_tables` and doesn't care about the names.
	//
	// So this logic is just trying to give them a reasonably unique name which also tells
	// us some useful facts, like as much of the name as we can accomodate and when it was
	// created. The timestamp also helps ensure that successive capture instances from the
	// same table are always unique.

	// Human-readable prefix of the name, truncated to at most 64 bytes.
	var prefix = unicodeTruncate(schema+"_"+table, 64)

	// A 32-bit hash of the full name provides more uniqueness if the truncated prefixes match for multiple tables.
	var hash = crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s_%s", schema, table)))

	// The full instance name is the 64 bytes prefix, 8 bytes of hash in hex, 10 bytes of Unix timestamp
	// as a decimal number, and two underscores for a total of 84 bytes, well under the limit of 100.
	var instanceName = fmt.Sprintf("%s_%08X_%d", prefix, hash, time.Now().Unix())

	// According to https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql
	// the `sys.sp_cdc_enable_table` procedure "Requires membership in the db_owner fixed database role."
	//
	// Thus unless we're running as the superuser or the 'flow_capture' user has been explicitly granted
	// this role we should expect this to fail. We always try to create capture instances during validation
	// because it hurts nothing if we're rejected, and if the relevant table-alteration handling option is
	// enabled we verify as a prerequisite that we have 'db_owner' so this shouldn't fail there.
	var args = []any{schema, table, instanceName}
	var query = "EXEC sys.sp_cdc_enable_table @source_schema = @p1, @source_name = @p2, @capture_instance = @p3"
	if roleName != "" {
		args = append(args, roleName)
	} else {
		args = append(args, username)
	}
	query += fmt.Sprintf(", @role_name = @p%d", len(args))
	if filegroup != "" {
		args = append(args, filegroup)
		query += fmt.Sprintf(", @filegroup_name = @p%d", len(args))
	}
	query += ";"
	log.WithFields(log.Fields{
		"schema":     schema,
		"table":      table,
		"instance":   instanceName,
		"query":      query,
		"query_args": args,
	}).Debug("creating new capture instance")
	if _, err := conn.ExecContext(ctx, query, args...); err != nil {
		log.WithField("err", err).Debug("failed to create capture instance")
		return "", fmt.Errorf("error creating capture instance %q: %w", instanceName, err)
	}
	return instanceName, nil
}

// unicodeTruncate returns the longest prefix of the input string which is
// not longer than the specified limit in bytes and which is made entirely
// out of complete Unicode code points.
func unicodeTruncate(str string, limit int) (truncated string) {
	for _, r := range str {
		if len(truncated)+len(string(r)) > limit {
			break
		}
		truncated += string(r)
	}
	return truncated
}

func cdcDeleteCaptureInstance(ctx context.Context, conn *sql.DB, schema, table, instance string) error {
	const query = `EXEC sys.sp_cdc_disable_table @source_schema = @p1, @source_name = @p2, @capture_instance = @p3;`
	if _, err := conn.ExecContext(ctx, query, schema, table, instance); err != nil {
		return fmt.Errorf("error deleting capture instance %q: %w", instance, err)
	}
	return nil
}

type cdcCaptureJobConfig struct {
	JobID        string // A unique ID associated with the job.
	DatabaseID   int    // The ID of the database in which the job is run.
	MaxTrans     int    // The maximum number of transactions to process in each scan cycle.
	MaxScans     int    // The maximum number of scan cycles to execute in order to extract all rows from the log.
	Continuous   int    // A flag indicating whether the capture job is to run continuously (1), or run in one-time mode (0).
	PollInterval int    // The number of seconds between log scan cycles.
}

func cdcQueryCaptureJobConfig(ctx context.Context, conn *sql.DB) (*cdcCaptureJobConfig, error) {
	var query = `
	    SELECT job_id, database_id, maxtrans, maxscans, continuous, pollinginterval
	        FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID()`
	var x cdcCaptureJobConfig
	if err := conn.QueryRowContext(ctx, query).Scan(&x.JobID, &x.DatabaseID, &x.MaxTrans, &x.MaxScans, &x.Continuous, &x.PollInterval); err != nil {
		return nil, fmt.Errorf("error querying CDC job config: %w", err)
	}
	return &x, nil
}

// isReplicaDatabase determines whether the current database connection is to a read-only replica.
// This function specifically checks for Always On Availability Groups, which is the modern
// and recommended high availability solution in SQL Server.
func isReplicaDatabase(ctx context.Context, conn *sql.DB) (bool, error) {
	const query = `
    SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM sys.dm_hadr_database_replica_states 
            WHERE is_local = 1 AND is_primary_replica = 0
        ) THEN 1
        ELSE 0
    END AS is_replica;`

	var isReplica bool
	if err := conn.QueryRowContext(ctx, query).Scan(&isReplica); err != nil {
		log.WithError(err).Warn("error determining replica status, assuming primary")
		return false, nil
	}
	return isReplica, nil
}

func (db *sqlserverDatabase) ReplicationDiagnostics(ctx context.Context) error {
	var query = func(q string) {
		var logEntry = log.WithField("query", q)
		logEntry.Info("running diagnostics query")

		var rows, err = db.conn.QueryContext(ctx, q)
		if err != nil {
			logEntry.WithField("err", err).Error("unable to execute diagnostics query")
			return
		}
		defer rows.Close()

		cnames, err := rows.Columns()
		if err != nil {
			logEntry.WithField("err", err).Error("error processing query result")
			return
		}
		var vals = make([]any, len(cnames))
		var vptrs = make([]any, len(vals))
		for idx := range vals {
			vptrs[idx] = &vals[idx]
		}

		var numResults int
		for rows.Next() {
			numResults++
			if err := rows.Scan(vptrs...); err != nil {
				logEntry.WithField("err", err).Error("error scanning result row")
				continue
			}
			var logFields = log.Fields{}
			for idx, name := range cnames {
				logFields[name] = vals[idx]
			}
			log.WithFields(logFields).Info("got diagnostic row")
		}
		if numResults == 0 {
			logEntry.Info("no results")
		}
	}

	query("SELECT * FROM sys.dm_server_services;")
	query("SELECT * FROM sys.dm_cdc_log_scan_sessions;")
	query("SELECT * FROM sys.dm_cdc_errors;")
	query("EXEC msdb.dbo.sp_help_job;")
	query("EXEC sys.sp_cdc_help_jobs;")
	query("SELECT * FROM msdb.dbo.cdc_jobs;")
	query("EXEC sys.sp_cdc_help_change_data_capture;")
	return nil
}

// marshalJSONString returns the serialized JSON representation of the provided string.
//
// A Go string can always be successfully serialized into JSON.
func marshalJSONString(str string) json.RawMessage {
	var bs, err = json.Marshal(str)
	if err != nil {
		panic(fmt.Errorf("internal error: failed to marshal string %q to JSON: %w", str, err))
	}
	return bs
}

func unmarshalJSONString(bs json.RawMessage) (string, error) {
	if bs == nil {
		return "", nil
	}
	var str string
	if err := json.Unmarshal(bs, &str); err != nil {
		return "", err
	}
	return str, nil
}
