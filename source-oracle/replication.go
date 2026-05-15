package main

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// Specify default formats for DATE, TIMESTAMP and TIMESTAMP_TZ types so
// their formats are predictable when we receive SQL statements from Logminer
const ORACLE_DATE_FORMAT = "YYYY-MM-DD HH24:MI:SS"
const ORACLE_TS_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF"
const ORACLE_TSTZ_FORMAT = `YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM`

const PARSE_DATE_FORMAT = "2006-01-02 15:04:05"
const PARSE_TS_FORMAT = "2006-01-02 15:04:05.999999999"
const PARSE_TSTZ_FORMAT = time.RFC3339Nano

const OUT_DATE_FORMAT = "2006-01-02T15:04:05"
const OUT_TS_FORMAT = "2006-01-02T15:04:05.999999999"
const OUT_TSTZ_FORMAT = time.RFC3339Nano

func (db *oracleDatabase) ReplicationStream(ctx context.Context, cpJSON json.RawMessage) (sqlcapture.ReplicationStream, error) {
	var dbConn, err = sql.Open("oracle", db.config.ToURI(db.config.Advanced.IncrementalChunkSize+1))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}
	conn, err := dbConn.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET NLS_DATE_FORMAT = '%s'", ORACLE_DATE_FORMAT)); err != nil {
		return nil, fmt.Errorf("set NLS_DATE_FORMAT: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '%s'", ORACLE_TS_FORMAT)); err != nil {
		return nil, fmt.Errorf("set NLS_TIMESTAMP_FORMAT: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = '%s'", ORACLE_TSTZ_FORMAT)); err != nil {
		return nil, fmt.Errorf("set NLS_TIMESTAMP_TZ_FORMAT: %w", err)
	}

	// Logminer cannot run on PDB instances, so we switch to the CDB
	if db.pdbName != "" {
		if _, err := conn.ExecContext(ctx, "ALTER SESSION SET CONTAINER=CDB$ROOT"); err != nil {
			return nil, fmt.Errorf("switching to CDB: %w", err)
		}
	}

	// Decode start cursor from a JSON quoted string into its actual string contents
	var cp checkpoint
	if len(cpJSON) > 0 {
		if err := json.Unmarshal(cpJSON, &cp); err != nil {
			var oldCheckpoint string
			if err2 := json.Unmarshal(cpJSON, &oldCheckpoint); err2 != nil {
				return nil, fmt.Errorf("decoding checkpoint: %w and %w", err, err2)
			}
			if oldCheckpoint != "" {
				cp.SCN, err = strconv.ParseInt(oldCheckpoint, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("parsing old cursor: %w", err)
				}
			}
		}
	}

	// Initialize the pending transactions map if not restored from checkpoint
	if cp.PendingTransactions == nil {
		cp.PendingTransactions = make(map[XID]*checkpointTx)
	}

	var startSCN SCN
	if cp.SCN != 0 {
		startSCN = cp.SCN
	} else {
		var row = db.conn.QueryRowContext(ctx, "SELECT current_scn FROM V$DATABASE")
		if err := row.Scan(&startSCN); err != nil {
			return nil, fmt.Errorf("fetching current SCN: %w", err)
		}
	}

	logrus.WithFields(logrus.Fields{
		"startSCN": startSCN,
	}).Info("starting replication")

	// This query checks for XIDs which either have a rollback, or have neither rollback and commit
	// in the given SCN range. Rolled back transactions are then excluded from processing completely.
	// Pending transactions are tracked in the pendingTransactions map until they are committed
	// or rolled back
	transactionsStmt, err := conn.PrepareContext(ctx, `
  SELECT
    XID,
    SUM(CASE WHEN OPERATION_CODE=36 THEN 1 ELSE 0 END) AS ROLLBACKS,
    SUM(CASE WHEN OPERATION_CODE=7 THEN 1 ELSE 0 END) AS COMMITS,
    MIN(SCN) AS STARTSCN,
    COUNT(*) AS COUNT
  FROM V$LOGMNR_CONTENTS
  WHERE SCN >= :startSCN AND SCN <= :endSCN
  GROUP BY XID
  HAVING
    SUM(CASE WHEN OPERATION_CODE=36 THEN 1 ELSE 0 END) > 0
    OR (
        SUM(CASE WHEN OPERATION_CODE=36 THEN 1 ELSE 0 END) = 0
        AND SUM(CASE WHEN OPERATION_CODE=7 THEN 1 ELSE 0 END) = 0
        AND COUNT(*) > 0
    )`)
	if err != nil {
		return nil, fmt.Errorf("preparing transactions query: %w", err)
	}

	var smartMode = db.config.Advanced.DictionaryMode == DictionaryModeSmart
	var dictionaryMode = db.config.Advanced.DictionaryMode
	if smartMode {
		dictionaryMode = DictionaryModeOnline
	}
	var stream = &replicationStream{
		db:   db,
		conn: conn,

		lastTxnEndSCN: startSCN,

		smartMode:      smartMode,
		dictionaryMode: dictionaryMode,

		pendingTransactions: cp.PendingTransactions,

		transactionsStmt: transactionsStmt,
	}

	stream.tables.active = make(map[sqlcapture.StreamID]struct{})
	stream.tables.keyColumns = make(map[sqlcapture.StreamID][]string)
	stream.tables.discovery = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo)
	return stream, nil
}

func (s *replicationStream) endLogminer(ctx context.Context) error {
	if _, err := s.conn.ExecContext(ctx, "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR; END;"); err != nil {
		// ORA-01307: no LogMiner session is currently active
		if strings.Contains(err.Error(), "ORA-01307") {
			return nil
		}
		return fmt.Errorf("ending logminer session: %w", err)
	}

	return nil
}

type redoFile struct {
	Status      string
	Name        string
	Sequence    int
	FirstChange SCN
	NextChange  SCN
	DictStart   string
	DictEnd     string
	Bytes       int
	Thread      int
}

var MaxReplicationLogFilesSize = 2 * 1024 * 1024 * 1024

func (s *replicationStream) addLogFiles(ctx context.Context, startSCN, maxSCN SCN) (SCN, error) {
	logrus.WithFields(logrus.Fields{
		"startSCN": startSCN,
		"maxSCN":   maxSCN,
	}).Info("logminer: selecting redo log files")

	// See DBMS_LOGMNR_D.BUILD reference:
	// https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOGMNR_D.html#GUID-20E210F3-A566-46F1-B817-486723069AF4
	if s.dictionaryMode == DictionaryModeExtract {
		if _, err := s.conn.ExecContext(ctx, "BEGIN SYS.DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;"); err != nil {
			return 0, fmt.Errorf("extracting dictionary from logfile: %w", err)
		}
	}
	// We only add the local version of archived log files to avoid duplicates
	var row = s.conn.QueryRowContext(ctx, "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE TYPE='LOCAL' AND STATUS='VALID' AND ROWNUM=1")
	var localDestID int
	if err := row.Scan(&localDestID); err != nil {
		return 0, fmt.Errorf("querying archive log files destination: %w", err)
	}

	var findEarliestLogSCNQuery = "SELECT MAX(NEXT_CHANGE#) FROM V$ARCHIVED_LOG WHERE NEXT_CHANGE# <= :scn"

	// In order to make sure we don't hit dictionary mismatches in extract mode, we need to find the most recent archive log before
	// or at our SCN range that begins a dictionary, so that early rows of this range are not missing their dictionary
	if s.dictionaryMode == DictionaryModeExtract {
		findEarliestLogSCNQuery += " AND DICTIONARY_BEGIN='YES'"
	}
	row = s.conn.QueryRowContext(ctx, findEarliestLogSCNQuery, startSCN)
	var minArchiveSCNScan sql.NullInt64
	if err := row.Scan(&minArchiveSCNScan); err != nil {
		return 0, fmt.Errorf("querying latest archive log to contain the dictionary for the SCN range: %w", err)
	}

	var minArchiveSCN = minArchiveSCNScan.Int64
	// If there are no archive log files with a dictionary file, start with the first archive log file
	// that covers the SCN
	if !minArchiveSCNScan.Valid {
		minArchiveSCN = startSCN
	}
	logrus.WithFields(logrus.Fields{
		"minArchiveSCN": minArchiveSCN,
	}).Debug("starting SCN for log files based on dictionary starting point")

	var liveLogFiles = `SELECT L.STATUS as STATUS, MIN(LF.MEMBER) as NAME, L.SEQUENCE#, L.FIRST_CHANGE#, CASE WHEN L.STATUS = 'CURRENT' THEN 0 ELSE L.NEXT_CHANGE# END as NEXT_CHANGE#, 'NO' as DICT_START, 'NO' as DICT_END, MAX(L.BYTES), L.THREAD# FROM V$LOGFILE LF, V$LOG L
		LEFT JOIN V$ARCHIVED_LOG A ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE#
    WHERE (A.STATUS <> 'A' OR A.FIRST_CHANGE# IS NULL) AND L.GROUP# = LF.GROUP# AND L.STATUS <> 'UNUSED'
		GROUP BY LF.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD#`

	var archivedLogFiles = `SELECT 'ARCHIVED' as STATUS, A.NAME AS NAME, A.SEQUENCE#, A.FIRST_CHANGE#, A.NEXT_CHANGE#, A.DICTIONARY_BEGIN as DICT_START, A.DICTIONARY_END as DICT_END, A.BLOCKS*A.BLOCK_SIZE as BYTES, A.THREAD# FROM V$ARCHIVED_LOG A
    WHERE A.NAME IS NOT NULL AND A.ARCHIVED = 'YES' AND A.STATUS = 'A' AND A.NEXT_CHANGE# >= :1 AND
    DEST_ID IN (` + strconv.Itoa(localDestID) + `)`

	var fullQuery = liveLogFiles + " UNION " + archivedLogFiles
	rows, err := s.conn.QueryContext(ctx, fullQuery, minArchiveSCN)
	if err != nil {
		return 0, fmt.Errorf("fetching log file list: %w", err)
	}
	defer rows.Close()

	var candidates []redoFile
	for rows.Next() {
		var f redoFile
		if err := rows.Scan(&f.Status, &f.Name, &f.Sequence, &f.FirstChange, &f.NextChange, &f.DictStart, &f.DictEnd, &f.Bytes, &f.Thread); err != nil {
			return 0, fmt.Errorf("scanning log file record: %w", err)
		}
		candidates = append(candidates, f)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	candidates, err = resolveDuplicateSequences(candidates)
	if err != nil {
		return 0, err
	}
	// Sort by FIRST_CHANGE# so that any prefix of the slice gives each thread a
	// contiguous chain (within a thread, FIRST_CHANGE# is monotonic and equivalent
	// to SEQUENCE# order). The Thread and Sequence tiebreakers are just for
	// deterministic ordering when two threads happen to have files starting at
	// the same SCN.
	slices.SortFunc(candidates, func(a, b redoFile) int {
		if c := cmp.Compare(a.FirstChange, b.FirstChange); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Thread, b.Thread); c != 0 {
			return c
		}
		return cmp.Compare(a.Sequence, b.Sequence)
	})

	// Every thread that appears in the candidate set has produced redo in the
	// SCN range we care about and must contribute to coverage. The cluster-wide
	// covered SCN is the min across threads of their per-thread tails, but only
	// once every expected thread has at least one file included; before that, we
	// have no meaningful global coverage.
	var expectedThreads = make(map[int]struct{})
	for _, f := range candidates {
		expectedThreads[f.Thread] = struct{}{}
	}

	var totalSizeBytes int
	var endSCN SCN
	var redoFiles []redoFile
	var threadTails = make(map[int]SCN)
	for _, f := range candidates {
		redoFiles = append(redoFiles, f)
		totalSizeBytes += f.Bytes

		// Track this thread's tail. A CURRENT file is open-ended (NextChange == 0
		// sentinel); represent that as math.MaxInt64 so it doesn't drag min() down.
		if f.NextChange == 0 {
			threadTails[f.Thread] = math.MaxInt64
		} else {
			threadTails[f.Thread] = f.NextChange
		}

		// globalCoveredSCN is the highest SCN through which every expected thread
		// has contiguous coverage. Undefined (0) until every thread has a tail.
		var globalCoveredSCN SCN
		if len(threadTails) == len(expectedThreads) {
			globalCoveredSCN = math.MaxInt64
			for _, tail := range threadTails {
				if tail < globalCoveredSCN {
					globalCoveredSCN = tail
				}
			}
		}

		logrus.WithFields(logrus.Fields{
			"name":             f.Name,
			"status":           f.Status,
			"sequence":         f.Sequence,
			"thread":           f.Thread,
			"firstChange":      f.FirstChange,
			"nextChange":       f.NextChange,
			"bytes":            f.Bytes,
			"dictStart":        f.DictStart,
			"dictEnd":          f.DictEnd,
			"totalSizeBytes":   totalSizeBytes,
			"globalCoveredSCN": globalCoveredSCN,
		}).Info("logminer: added redo log file")

		var reachedSizeLimit = totalSizeBytes >= MaxReplicationLogFilesSize
		var reachedMaxSCN = globalCoveredSCN >= maxSCN
		// advancesCursor guards against stopping before global coverage has
		// progressed past startSCN (e.g. early in iteration before all threads
		// have contributed, or when included files only cover SCNs at or below
		// startSCN). Setting endSCN to such a value would make the next poll
		// iteration re-fetch the same files and spin forever.
		var advancesCursor = globalCoveredSCN > startSCN
		if reachedMaxSCN || (reachedSizeLimit && advancesCursor) {
			// In extract mode we only stop on a file whose DICTIONARY_END is 'YES' to
			// keep the included files self-contained dictionary-wise. Online mode has
			// no such constraint.
			if s.dictionaryMode == DictionaryModeOnline || f.DictEnd == "YES" {
				endSCN = min(globalCoveredSCN, maxSCN)
				var reason string
				switch {
				case reachedSizeLimit && reachedMaxSCN:
					reason = "size cap reached and coverage reaches maxSCN"
				case reachedSizeLimit:
					reason = "size cap reached"
				default:
					reason = "coverage reaches maxSCN"
				}
				logrus.WithFields(logrus.Fields{
					"reason":         reason,
					"totalSizeBytes": totalSizeBytes,
					"files":          len(redoFiles),
					"endSCN":         endSCN,
					"maxSCN":         maxSCN,
				}).Info("logminer: stopping log file selection")
				break
			}
		}
	}

	// If the loop exhausted all candidates without breaking, endSCN is still
	// zero. Default to maxSCN so coverage validation has a concrete value to
	// check; validation will fail loudly if the included files don't actually
	// cover that far.
	if endSCN == 0 {
		endSCN = maxSCN
	}

	if err := validateLogFileCoverage(redoFiles, startSCN, endSCN); err != nil {
		return 0, err
	}

	for _, f := range redoFiles {
		if _, err := s.conn.ExecContext(ctx, "BEGIN SYS.DBMS_LOGMNR.ADD_LOGFILE(:filename); END;", f.Name); err != nil {
			return 0, fmt.Errorf("adding logfile %q (%s, %d, %s, %s) to logminer: %w", f.Name, f.Status, f.FirstChange, f.DictStart, f.DictEnd, err)
		}
	}

	return endSCN, nil
}

// validateLogFileCoverage verifies that the selected redo log files provide
// continuous SCN coverage of [startSCN, endSCN] for every thread that appears
// in the file set. Oracle's redo logs partition SCN space per-thread such that
// adjacent files within a thread satisfy prev.NEXT_CHANGE# == next.FIRST_CHANGE#.
// In RAC, each instance writes its own thread of redo, and SCN ranges across
// threads overlap arbitrarily; the adjacency invariant only holds within a
// single thread. A gap in available files (most commonly because archive log
// retention deleted a log we need) would otherwise cause LogMiner to silently
// return only the events present in the files actually added, dropping events
// from the missing log(s). Fail loudly so the cause is visible rather than
// silently losing data.
func validateLogFileCoverage(redoFiles []redoFile, startSCN, endSCN SCN) error {
	if len(redoFiles) == 0 {
		return fmt.Errorf("no redo log files available to cover SCN range [%d, %d]", startSCN, endSCN)
	}

	// Partition by thread and validate each thread's chain independently. We
	// rely on the caller having sorted by FIRST_CHANGE#, which means within
	// each thread the files are in SEQUENCE# order.
	var byThread = make(map[int][]redoFile)
	for _, f := range redoFiles {
		byThread[f.Thread] = append(byThread[f.Thread], f)
	}

	for thread, files := range byThread {
		if files[0].FirstChange > startSCN {
			return fmt.Errorf("redo log gap at start of SCN range for thread %d: first file %q begins at FIRST_CHANGE#=%d, after startSCN=%d", thread, files[0].Name, files[0].FirstChange, startSCN)
		}
		for i := 1; i < len(files); i++ {
			var prev, curr = files[i-1], files[i]
			// NextChange=0 is our sentinel for the CURRENT online log, which is open
			// ended and so must be the final file in its thread.
			if prev.NextChange == 0 {
				return fmt.Errorf("redo log file %q (FIRST_CHANGE#=%d) follows CURRENT-status file %q in thread %d, which has no NEXT_CHANGE#", curr.Name, curr.FirstChange, prev.Name, thread)
			}
			if curr.FirstChange != prev.NextChange {
				return fmt.Errorf("redo log SCN gap in thread %d between %q (NEXT_CHANGE#=%d) and %q (FIRST_CHANGE#=%d)", thread, prev.Name, prev.NextChange, curr.Name, curr.FirstChange)
			}
		}
		var last = files[len(files)-1]
		if last.NextChange != 0 && last.NextChange < endSCN {
			return fmt.Errorf("redo log gap at end of SCN range for thread %d: last file %q ends at NEXT_CHANGE#=%d, before endSCN=%d", thread, last.Name, last.NextChange, endSCN)
		}
	}

	return nil
}

// resolveDuplicateSequences collapses cases where the same (THREAD#, SEQUENCE#)
// pair appears as both a live entry (CURRENT or ACTIVE) and an ARCHIVED entry,
// which happens during the window where an ACTIVE log has been copied to archive
// but not yet retired. The live entry is preferred. If two entries share a key
// and neither is live (or both are), we treat that as a structural error. In
// RAC, SEQUENCE# values are per-thread independent counters, so we must include
// THREAD# in the key to avoid collapsing unrelated files from different threads.
func resolveDuplicateSequences(files []redoFile) ([]redoFile, error) {
	type threadSeq struct{ Thread, Sequence int }
	var isLive = func(f redoFile) bool { return f.Status == "CURRENT" || f.Status == "ACTIVE" }
	var byKey = make(map[threadSeq]redoFile)
	for _, f := range files {
		var key = threadSeq{f.Thread, f.Sequence}
		var existing, ok = byKey[key]
		if !ok {
			byKey[key] = f
			continue
		}
		var kept, dropped redoFile
		switch {
		case isLive(f) && !isLive(existing):
			kept, dropped = f, existing
		case isLive(existing) && !isLive(f):
			kept, dropped = existing, f
		default:
			return nil, fmt.Errorf("found two log files for thread %d sequence %d, cannot disambiguate: %+v, %+v", f.Thread, f.Sequence, existing, f)
		}
		logrus.WithFields(logrus.Fields{
			"kept":    fmt.Sprintf("%+v", kept),
			"dropped": fmt.Sprintf("%+v", dropped),
		}).Debug("found two log files with the same thread and sequence number, keeping CURRENT or ACTIVE")
		byKey[key] = kept
	}
	var out = make([]redoFile, 0, len(byKey))
	for _, f := range byKey {
		out = append(out, f)
	}
	return out, nil
}

func (s *replicationStream) startLogminer(ctx context.Context, startSCN, endSCN SCN) error {
	var dictionaryOption = ""
	if s.dictionaryMode == DictionaryModeExtract {
		dictionaryOption = "DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING"
	} else if s.dictionaryMode == DictionaryModeOnline {
		dictionaryOption = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"
	}

	logrus.WithFields(logrus.Fields{
		"startSCN": startSCN,
		"endSCN":   endSCN,
	}).Debug("starting logminer")
	var startQuery = fmt.Sprintf("BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN=>:scn,ENDSCN=>:end,OPTIONS=>%s); END;", dictionaryOption)
	if _, err := s.conn.ExecContext(ctx, startQuery, startSCN, endSCN); err != nil {
		return fmt.Errorf("starting logminer: %w", err)
	}

	return nil
}

// oracleSource is source metadata for data capture events.
type oracleSource struct {
	sqlcapture.SourceCommon

	// System Change Number, available for incremental changes only
	SCN SCN `json:"scn,omitempty" jsonschema:"description=SCN of this event, only present for incremental changes"`

	RowID string `json:"row_id" jsonschema:"description=ROWID of the document"`

	RSID string `json:"rs_id" jsonschema:"description=Record Set ID of the logical change"`

	SSN int `json:"ssn" jsonschema:"description=SQL sequence number of the logical change"`

	Tag string `json:"tag,omitempty" jsonschema:"description=Optional 'Source Tag' property as defined in the endpoint configuration."`
}

func (s *oracleSource) Common() sqlcapture.SourceCommon {
	return s.SourceCommon
}

// A replicationStream represents the process of receiving Oracle
// logminer events, and translating changes into a more friendly representation.
type replicationStream struct {
	db   *oracleDatabase
	conn *sql.Conn // The Oracle connection

	cancel context.CancelFunc            // Cancel function for the replication goroutine's context
	errCh  chan error                    // Error channel for the final exit status of the replication goroutine
	events chan sqlcapture.DatabaseEvent // The channel to which replication events will be written

	// Is connector configured to be smart about dictionary mode
	smartMode bool

	// One of online or extract, the smart mode will switch between these two values on the replication stream
	dictionaryMode string

	// maximum last DDL SCN of all active tables, we need to stay on extract mode until this SCN has passed
	lastDDLSCN SCN

	lastTxnEndSCN SCN // End SCN (record + 1) of the last completed transaction.

	// Statement for querying pending and rolled back transactions
	transactionsStmt *sql.Stmt

	// Transactions being tracked, a mapping of XIDs to starting SCN and message count of transactions which have not yet finished (commit or rollback)
	pendingTransactions map[XID]*checkpointTx

	// The 'active tables' set, guarded by a mutex so it can be modified from
	// the main goroutine while it's read by the replication goroutine.
	tables struct {
		sync.RWMutex
		active     map[sqlcapture.StreamID]struct{}
		keyColumns map[sqlcapture.StreamID][]string
		discovery  map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo
	}
}

type checkpointTx struct {
	StartSCN     SCN
	MessageCount int
}

type checkpoint struct {
	SCN                 SCN
	PendingTransactions map[XID]*checkpointTx
}

func (s *replicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
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
			case event, ok := <-s.events:
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
	if err := s.db.WriteWatermark(ctx, fenceWatermark); err != nil {
		return fmt.Errorf("error establishing watermark fence: %w", err)
	}
	// Query the post-write current SCN to give a rough lower bound on when in the
	// replication stream the fence should appear. This is best-effort: a failure
	// here doesn't fail the call.
	var postWriteSCN SCN
	if err := s.db.conn.QueryRowContext(ctx, "SELECT current_scn FROM V$DATABASE").Scan(&postWriteSCN); err != nil {
		logrus.WithField("err", err).Warn("error fetching current SCN after watermark write")
	}
	logrus.WithFields(logrus.Fields{
		"fenceWatermark": fenceWatermark,
		"postWriteSCN":   postWriteSCN,
	}).Info("wrote watermark fence")

	// Stream replication events until the fence is reached.
	var fenceReached = false
	var watermarkStreamID = s.db.WatermarksTable()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-s.events:
			if !ok {
				return sqlcapture.ErrFenceNotReached
			} else if err := callback(event); err != nil {
				return err
			}

			// Mark the fence as reached when we observe a change event on the watermarks stream
			// with the expected value.
			if event, ok := event.(*sqlcapture.OldChangeEvent); ok {
				if event.Operation != sqlcapture.DeleteOp && event.Source.Common().StreamID() == watermarkStreamID {
					var actual = event.After["watermark"]
					if actual == nil {
						actual = event.After["WATERMARK"]
					}
					var changeSCN SCN
					if src, ok := event.Source.(*oracleSource); ok {
						changeSCN = src.SCN
					}
					logrus.WithFields(logrus.Fields{
						"expected": fenceWatermark,
						"actual":   actual,
						"scn":      changeSCN,
					}).Info("watermark change observed")
					if actual == fenceWatermark {
						fenceReached = true
					}
				}
			}

			// The flush event following the watermark change ends the stream-to-fence operation.
			if _, ok := event.(*sqlcapture.OldFlushEvent); ok && fenceReached {
				return nil
			}
		}
	}
}

// replicationBufferSize controls how many change events can be buffered in the
// replicationStream before it stops receiving further events from Oracle.
// In normal use it's a constant, it's just a variable so that tests are more
// likely to exercise blocking sends and backpressure.
// This buffer has been set to a fairly small value, because larger buffers can
// cause OOM kills when the incoming data rate exceeds the rate at which we're
// serializing data and getting it into Gazette journals.
var replicationBufferSize = 16

func (s *replicationStream) StartReplication(ctx context.Context, discovery map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	// Activate replication for the watermarks table.
	var watermarks = s.db.WatermarksTable()
	var watermarksInfo = discovery[watermarks]
	if watermarksInfo == nil {
		return fmt.Errorf("error activating replication for watermarks table %q: table missing from latest autodiscovery", watermarks)
	}
	if err := s.ActivateTable(ctx, watermarks, watermarksInfo.PrimaryKey, watermarksInfo, nil); err != nil {
		return fmt.Errorf("error activating replication for watermarks table %q: %w", watermarks, err)
	}

	// Confirm that the watermarks table is present in tableObjectMapping. If it isn't,
	// its DATA_OBJ#/DATA_OBJD# won't appear in the logminer predicate and fence
	// watermark writes will never be observed.
	var watermarksFound = false
	for _, mapping := range s.db.tableObjectMapping {
		if mapping.streamID == watermarks {
			logrus.WithFields(logrus.Fields{
				"watermarksTable": watermarks,
				"objectID":        mapping.objectID,
				"dataObjectID":    mapping.dataObjectID,
			}).Info("watermarks table covered by logminer predicate")
			watermarksFound = true
			break
		}
	}
	if !watermarksFound {
		logrus.WithFields(logrus.Fields{
			"watermarksTable":   watermarks,
			"objectMappingSize": len(s.db.tableObjectMapping),
		}).Warn("watermarks table not in tableObjectMapping; logminer query predicate will not return watermark changes")
	}

	var streamCtx, streamCancel = context.WithCancel(ctx)
	s.events = make(chan sqlcapture.DatabaseEvent, replicationBufferSize)
	s.errCh = make(chan error)
	s.cancel = streamCancel

	go func() {
		err := s.run(streamCtx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		// Context cancellation typically occurs only in tests, and for test stability
		// it should be considered a clean shutdown and not necessarily an error.
		close(s.events)
		s.transactionsStmt.Close()
		s.conn.Close()
		s.errCh <- err
	}()

	return nil
}

const pollInterval = 10 * time.Second

// run is the main loop of the replicationStream which loops message
// receiving and relaying
func (s *replicationStream) run(ctx context.Context) error {
	var poll = time.NewTicker(pollInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
		}

		var err = s.poll(ctx, s.lastTxnEndSCN, math.MaxInt64, nil)
		if err != nil {
			// ORA-01013: user requested cancel of current operation
			// this means a cancellation of context happened, which we don't consider an error
			// since it should only happen in tests
			if strings.Contains(err.Error(), "ORA-01013") {
				continue
			}
			return fmt.Errorf("failed to poll messages: %w", err)
		}
	}
}

// poll logminer for changes in [startSCN, endSCN]
// if transactions is not empty, only messages for the given transactions are processed
func (s *replicationStream) poll(ctx context.Context, minSCN, maxSCN SCN, transactions []string) error {
	var startSCN = minSCN
	logrus.WithFields(logrus.Fields{
		"minSCN":       minSCN,
		"maxSCN":       maxSCN,
		"transactions": transactions,
	}).Info("logminer: poll")

	for {
		if startSCN > maxSCN {
			return nil
		}

		var currentSCN SCN
		var row = s.conn.QueryRowContext(ctx, "SELECT current_scn FROM V$DATABASE")
		if err := row.Scan(&currentSCN); err != nil {
			return fmt.Errorf("fetching current SCN: %w", err)
		}

		var endSCN SCN

		var iterationMaxSCN = maxSCN + 1
		if currentSCN < maxSCN {
			iterationMaxSCN = currentSCN
		}

		if err := s.endLogminer(ctx); err != nil {
			return err
		} else if endSCN, err = s.addLogFiles(ctx, startSCN, iterationMaxSCN); err != nil {
			return err
		}

		logrus.WithFields(logrus.Fields{
			"startSCN":            startSCN,
			"endSCN":              endSCN,
			"currentSCN":          currentSCN,
			"iterationMaxSCN":     iterationMaxSCN,
			"dictionaryMode":      s.dictionaryMode,
			"pendingTransactions": len(s.pendingTransactions),
			"specificXIDs":        len(transactions),
		}).Info("logminer: starting iteration")

		if err := s.startLogminer(ctx, startSCN, endSCN); err != nil {
			return err
		}

		var stmt, debugQuery, err = s.generateLogminerQuery(ctx, transactions)
		if err != nil {
			return err
		}

		// Exclude rollbacked transactions from processing, and keep track of pending transactions
		var rollbackedXIDs map[XID]struct{}
		if len(transactions) == 0 {
			var newPendingTxs map[XID]SCN
			newPendingTxs, rollbackedXIDs, err = s.pendingAndRollbackedTransactions(ctx, startSCN, endSCN)
			if err != nil {
				return err
			}

			for xid, startSCN := range newPendingTxs {
				if _, ok := s.pendingTransactions[xid]; !ok {
					s.pendingTransactions[xid] = &checkpointTx{
						StartSCN:     startSCN,
						MessageCount: 0,
					}
				}
			}

			for xid := range rollbackedXIDs {
				s.pendingTransactions[xid] = nil
			}
		}

		if err := s.receiveMessages(ctx, startSCN, endSCN, stmt, rollbackedXIDs); err != nil {
			// If configured to use smart mode, switch to extract mode and re-try
			// We will switch back to Online mode afterwards
			var dictionaryMismatch = strings.Contains(strings.ToLower(err.Error()), "dictionary mismatch")
			if dictionaryMismatch && s.dictionaryMode == DictionaryModeOnline && s.smartMode {
				logrus.WithField("error", err).Info("smart mode: encountered dictionary mismatch")
				s.dictionaryMode = DictionaryModeExtract
				if lastDDLSCN, err := s.maximumLastDDLSCN(ctx); err != nil {
					return err
				} else {
					s.lastDDLSCN = lastDDLSCN
				}

				logrus.WithField("lastDDLSCN", s.lastDDLSCN).Info("smart mode: switching to extract mode")
				continue
			}

			// TODO: remove this once we have found out what is going on with bad connection errors
			if strings.Contains(err.Error(), "bad connection") {
				logrus.WithField("query", debugQuery).Warn("the query that caused bad connection")
			}
			return fmt.Errorf("receive messages: %w", err)
		}

		stmt.Close()

		if len(transactions) == 0 {
			// Although the query is inclusive, we do not +1 here since
			// it is possible for there to be multiple rows with the same SCN
			// and to be cautious we use the SCN as-is. This may lead to duplicate events being captured
			// on restart, but it saves us from missing events
			s.lastTxnEndSCN = endSCN
		}

		var cp = checkpoint{
			SCN:                 s.lastTxnEndSCN,
			PendingTransactions: s.pendingTransactions,
		}
		if cpJSON, err := json.Marshal(cp); err != nil {
			return fmt.Errorf("marshalling checkpoint: %w", err)
		} else if err := s.emitEvent(ctx, &sqlcapture.OldFlushEvent{Cursor: cpJSON}); err != nil {
			return err
		}

		// Clean up nil entries from the in-memory map after checkpoint emission.
		// These entries were set to nil to signal deletion via JSON merge-patch semantics.
		for xid, tx := range s.pendingTransactions {
			if tx == nil {
				delete(s.pendingTransactions, xid)
			}
		}

		if s.smartMode && s.dictionaryMode == DictionaryModeExtract && int64(s.lastDDLSCN) < endSCN {
			s.dictionaryMode = DictionaryModeOnline
			logrus.WithFields(logrus.Fields{
				"lastDDLSCN": s.lastDDLSCN,
				"endSCN":     endSCN,
			}).Info("smart mode: switching back to online mode")
		}

		startSCN = endSCN
	}
}

func (s *replicationStream) handleTransactionBoundaries(ctx context.Context, msg logminerMessage) error {
	// BEGIN messages are never emitted
	if msg.Op == opStart {
		return nil
	}

	if msg.Op == opCommit {
		if tx, ok := s.pendingTransactions[msg.XID]; ok && tx != nil {
			s.pendingTransactions[msg.XID] = nil
			if tx.MessageCount > 0 {
				return s.capturePendingTransaction(ctx, transaction{XID: msg.XID, StartSCN: tx.StartSCN}, msg)
			} else {
				return nil
			}
		}

		return nil
	}

	if msg.Op == opRollback {
		s.pendingTransactions[msg.XID] = nil
		return nil
	}

	// Emittable messages for pending transactions are ignored until their commit is seen
	if tx, ok := s.pendingTransactions[msg.XID]; ok && tx != nil {
		tx.MessageCount += 1
		return nil
	}

	return s.decodeAndEmitMessage(ctx, msg)
}

func (s *replicationStream) capturePendingTransaction(ctx context.Context, tx transaction, commit logminerMessage) error {
	var startSCN = tx.StartSCN
	var endSCN = commit.SCN

	var transactions = []string{tx.XID}
	logrus.WithFields(logrus.Fields{
		"xid":      tx.XID,
		"startSCN": startSCN,
		"endSCN":   endSCN,
	}).Info("capturing pending transaction")
	return s.poll(ctx, startSCN, endSCN, transactions)
}

func (s *replicationStream) emitEvent(ctx context.Context, event sqlcapture.DatabaseEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *replicationStream) decodeAndEmitMessage(ctx context.Context, msg logminerMessage) error {
	var event, err = s.decodeMessage(msg)
	if err != nil {
		return fmt.Errorf("decode message: %w", err)
	}
	return s.emitEvent(ctx, event)
}

// in WHERE AST, columns are quoted with backticks for some reason. This function
// removes backtick quotes from a string
func unquote(s string) string {
	return strings.TrimSuffix(strings.TrimPrefix(s, "`"), "`")
}

type logminerMessage struct {
	SCN          SCN
	Op           int
	SQL          string
	UndoSQL      string
	TableName    string
	Owner        string
	Timestamp    int64
	Status       int
	Info         string
	RSID         string
	SSN          int
	CSF          int
	ObjectID     int
	DataObjectID int
	XID          string
}

const (
	opInsert   = 1
	opDelete   = 2
	opUpdate   = 3
	opStart    = 6
	opCommit   = 7
	opRollback = 36
)

type XID = string
type SCN = int64

type transaction struct {
	XID      XID
	StartSCN SCN
}

func (s *replicationStream) generateLogminerQuery(ctx context.Context, transactions []string) (*sql.Stmt, string, error) {
	var tableObjectMapping = s.db.tableObjectMapping

	var conditions []string
	for _, mapping := range tableObjectMapping {
		if !s.tableActive(mapping.streamID) {
			continue
		}
		conditions = append(conditions, fmt.Sprintf("(DATA_OBJ# = %d AND DATA_OBJD# = %d)", mapping.objectID, mapping.dataObjectID))
	}
	var opCodes = []int{opInsert, opDelete, opUpdate, opStart, opCommit, opRollback}
	var opPredicate = strings.Trim(strings.Join(strings.Fields(fmt.Sprint(opCodes)), ","), "[]")

	var txValues []string
	for _, tx := range transactions {
		txValues = append(txValues, fmt.Sprintf("utl_encode.base64_decode(utl_raw.cast_to_raw('%s'))", tx))
	}
	var txPredicate = ""
	if len(txValues) > 0 {
		txPredicate = fmt.Sprintf("AND XID IN (%s)", strings.Join(txValues, ","))
	}

	// START and COMMIT operations do not have seg_owner and have data_obj#=0 and data_objd#=0
	var query = fmt.Sprintf(`SELECT SCN, TIMESTAMP, OPERATION_CODE, SQL_REDO, SQL_UNDO, TABLE_NAME, SEG_OWNER, STATUS, INFO, RS_ID, SSN, CSF, DATA_OBJ#, DATA_OBJD#, XID
    FROM V$LOGMNR_CONTENTS
    WHERE OPERATION_CODE IN (%s) AND SCN >= :startSCN AND SCN <= :endSCN AND 
    (SEG_OWNER IS NULL OR
    SEG_OWNER NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS'))
    AND ((DATA_OBJ#=0 AND DATA_OBJD#=0) OR %s) %s`, opPredicate, strings.Join(conditions, " OR "), txPredicate)

	logrus.Debug("generated logminer query")

	stmt, err := s.conn.PrepareContext(ctx, query)

	return stmt, query, err
}

func (s *replicationStream) pendingAndRollbackedTransactions(ctx context.Context, startSCN, endSCN SCN) (map[XID]SCN, map[XID]struct{}, error) {
	// we use maps for O(1) lookup
	var excludeXIDs = make(map[XID]struct{})
	var pendingTXs = make(map[XID]SCN)

	rows, err := s.transactionsStmt.QueryContext(ctx, startSCN, endSCN)
	if err != nil {
		return nil, nil, fmt.Errorf("querying transactions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var xidRaw []byte
		var rollbacks, commits int
		var startSCN SCN
		var count int
		if err := rows.Scan(&xidRaw, &rollbacks, &commits, &startSCN, &count); err != nil {
			return nil, nil, fmt.Errorf("scanning transactions: %w", err)
		}
		var xid = base64.StdEncoding.EncodeToString(xidRaw)

		if rollbacks > 0 {
			excludeXIDs[xid] = struct{}{}
			logrus.WithFields(logrus.Fields{
				"xid":       xid,
				"rollbacks": rollbacks,
				"commits":   commits,
				"count":     count,
			}).Trace("skipping rolled-back transaction")
		} else if commits == 0 {
			pendingTXs[xid] = startSCN
			logrus.WithFields(logrus.Fields{
				"xid":       xid,
				"rollbacks": rollbacks,
				"commits":   commits,
				"count":     count,
				"startSCN":  startSCN,
			}).Trace("found pending transaction")
		} else {
			// sanity check the query, this shouldn't happen
			return nil, nil, fmt.Errorf("unexpected transaction with commit > 0, xid: %s, startSCN: %d, commits: %d, rollbacks: %d", xid, startSCN, commits, rollbacks)
		}
	}

	logrus.WithFields(logrus.Fields{
		"pending":         len(pendingTXs),
		"rollbacks":       len(excludeXIDs),
		"trackedPriorTxs": len(s.pendingTransactions),
		"startSCN":        startSCN,
		"endSCN":          endSCN,
	}).Info("logminer: pending and rolled-back transactions")

	return pendingTXs, excludeXIDs, nil
}

// receiveMessage reads and parses the next replication message from the database,
// blocking until a message is available, the context is cancelled, or an error
// occurs.
func (s *replicationStream) receiveMessages(ctx context.Context, startSCN, endSCN SCN, stmt *sql.Stmt, excludeXIDs map[XID]struct{}) error {
	rows, err := stmt.QueryContext(ctx, startSCN, endSCN)
	if err != nil {
		return fmt.Errorf("logminer query: %w", err)
	}

	var totalMessages = 0
	var fullMessages = 0
	var lastMsg *logminerMessage
	for rows.Next() {
		totalMessages++

		var msg logminerMessage
		var ts time.Time
		var redoSql sql.NullString
		var undoSql sql.NullString
		var tableName sql.NullString
		var owner sql.NullString
		var info sql.NullString
		var xidRaw []byte
		if err := rows.Scan(&msg.SCN, &ts, &msg.Op, &redoSql, &undoSql, &tableName, &owner, &msg.Status, &info, &msg.RSID, &msg.SSN, &msg.CSF, &msg.ObjectID, &msg.DataObjectID, &xidRaw); err != nil {
			return fmt.Errorf("row scan: %w", err)
		}

		msg.XID = base64.StdEncoding.EncodeToString(xidRaw)
		if _, ok := excludeXIDs[msg.XID]; ok {
			continue
		}

		// For some reason RSID comes with a space before and after it
		msg.RSID = strings.TrimSpace(msg.RSID)

		if tableName.Valid {
			msg.TableName = tableName.String
		}

		if owner.Valid {
			msg.Owner = owner.String
		}

		if redoSql.Valid {
			msg.SQL = redoSql.String
		}

		if undoSql.Valid {
			msg.UndoSQL = undoSql.String
		}

		if info.Valid {
			msg.Info = info.String
		}
		msg.Timestamp = ts.UnixMilli()

		logrus.WithFields(logrus.Fields{
			"msg": fmt.Sprintf("%+v", msg),
		}).Trace("logminer: received message")

		// If this change event is on a table we're not capturing, skip doing any
		// further processing on it.
		var streamID = sqlcapture.JoinStreamID(msg.Owner, msg.TableName)
		if !s.tableActive(streamID) && !slices.Contains([]int{opStart, opCommit, opRollback}, msg.Op) {
			logrus.WithFields(logrus.Fields{
				"streamID":     streamID,
				"owner":        msg.Owner,
				"tableName":    msg.TableName,
				"objectID":     msg.ObjectID,
				"dataObjectID": msg.DataObjectID,
				"op":           msg.Op,
				"scn":          msg.SCN,
				"status":       msg.Status,
			}).Info("logminer: received message for unknown table")
			var isKnownTable = false
			// if the table has been dropped, check their object identifier
			if strings.HasPrefix(msg.TableName, "OBJ#") || (strings.HasPrefix(msg.TableName, "BIN$") && strings.HasSuffix(msg.TableName, "==$0") && len(msg.TableName) == 30) {
				if _, ok := s.db.tableObjectMapping[joinObjectID(msg.ObjectID, msg.DataObjectID)]; ok {
					// This is a known table to us, a dictionary mismatch on this row is an error
					isKnownTable = true
				}
			}

			if !isKnownTable {
				continue
			}
		}

		// If logminer can't find the dictionary for a SQL statement (e.g. if using online mode and a schema change has occurred)
		// then we get SQL statements like this:
		// insert into "UNKNOWN"."OBJ# 45522"("COL 1","COL 2","COL 3","COL 4") values (HEXTORAW('45465f4748'),HEXTORAW('546563686e6963616c20577269746572'), HEXTORAW('c229'),HEXTORAW('c3020b'));
		// we additionally get status=2 for these records. Status 2 means the SQL statement is not valid for redoing.
		// Some versions of Oracle report STATUS=2 for LONG column types, but have an empty info column, whereas when
		// status=2 and there is some reason for the error in the info column (usually "Dictionary Mismatch") we consider
		// the case to be one of dictionary mismatch
		if msg.Status == 2 && msg.Info != "" {
			return fmt.Errorf("dictionary mismatch (%s) for table %q: %q", msg.Info, msg.TableName, msg.SQL)
		}

		// last message indicated a continuation of SQL_REDO and SQL_UNDO values
		// so this row's sqls will be appended to the last
		if lastMsg != nil && lastMsg.CSF == 1 {
			// sanity check the (SSN, RSID) tuple matches between the rows
			if lastMsg.SSN != msg.SSN || lastMsg.RSID != msg.RSID {
				return fmt.Errorf("expected SSN and RSID of continued rows to match: (%d, %s) != (%d, %s)", lastMsg.SSN, lastMsg.RSID, msg.SSN, msg.RSID)
			}
			lastMsg.SQL += msg.SQL
			lastMsg.UndoSQL += msg.UndoSQL
			lastMsg.CSF = msg.CSF

			// last message was CSF, this one is not. This is the end of the continuation chain
			if msg.CSF == 0 {
				if err := s.handleTransactionBoundaries(ctx, *lastMsg); err != nil {
					return fmt.Errorf("handle transaction boundaries: %w", err)
				}
				fullMessages++
			}

			continue
		}

		lastMsg = &msg
		if msg.CSF == 1 {
			continue
		}

		if err := s.handleTransactionBoundaries(ctx, msg); err != nil {
			return fmt.Errorf("handle transaction boundaries: %w", err)
		}
		fullMessages++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	} else if err := rows.Close(); err != nil {
		return fmt.Errorf("rows close: %w", err)
	}

	var finalSCN SCN
	if lastMsg != nil {
		finalSCN = lastMsg.SCN
	}
	logrus.WithFields(logrus.Fields{
		"totalMessages": totalMessages,
		"fullMessages":  fullMessages,
		"startSCN":      startSCN,
		"endSCN":        endSCN,
		"finalSCN":      finalSCN,
		"excludedXIDs":  len(excludeXIDs),
	}).Info("logminer: received messages")

	return nil
}

func (s *replicationStream) tableActive(streamID sqlcapture.StreamID) bool {
	s.tables.RLock()
	defer s.tables.RUnlock()
	var _, ok = s.tables.active[streamID]
	return ok
}

func (s *replicationStream) keyColumns(streamID sqlcapture.StreamID) ([]string, bool) {
	s.tables.RLock()
	defer s.tables.RUnlock()
	var keyColumns, ok = s.tables.keyColumns[streamID]
	return keyColumns, ok
}

func (s *replicationStream) ActivateTable(ctx context.Context, streamID sqlcapture.StreamID, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
	s.tables.Lock()
	defer s.tables.Unlock()
	s.tables.active[streamID] = struct{}{}
	s.tables.keyColumns[streamID] = keyColumns
	s.tables.discovery[streamID] = discovery

	logrus.WithField("id", streamID).Info("activating table")

	return nil
}

// This function is a no-op if there is no PDB name configured
func (s *replicationStream) switchToCDB(ctx context.Context) error {
	if s.db.pdbName == "" {
		return nil
	}

	if _, err := s.conn.ExecContext(ctx, "ALTER SESSION SET CONTAINER=CDB$ROOT"); err != nil {
		return fmt.Errorf("switching to CDB: %w", err)
	}

	return nil
}

// This function is a no-op if there is no PDB name configured
func (s *replicationStream) switchToPDB(ctx context.Context) error {
	if s.db.pdbName == "" {
		return nil
	}

	if _, err := s.conn.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET CONTAINER=%s", s.db.pdbName)); err != nil {
		return fmt.Errorf("switching to PDB %s: %w", s.db.pdbName, err)
	}

	return nil
}

// Each table has a LAST_DDL_TIME which specifies the last time it was modified
// by a DDL statement. This function returns the maximum of all of those times
// across all tables. When we hit a dictionary mismatch, we stay on Extract mode until
// we have covered all the latest DDLs on all tables before switching back to online mode
func (s *replicationStream) maximumLastDDLSCN(ctx context.Context) (SCN, error) {
	var tablesCondition = ""
	var i = 0
	for _, mapping := range s.db.tableObjectMapping {
		if i > 0 {
			tablesCondition += " OR "
		}
		tablesCondition += fmt.Sprintf("(OBJECT_ID = %d AND DATA_OBJECT_ID = %d)", mapping.objectID, mapping.dataObjectID)
		i++
	}
	var query = fmt.Sprintf(`SELECT TIMESTAMP_TO_SCN(MAX(LAST_DDL_TIME)) last_ddl FROM ALL_OBJECTS WHERE %s`, tablesCondition)

	if err := s.switchToPDB(ctx); err != nil {
		return 0, err
	}

	row := s.conn.QueryRowContext(ctx, query)

	var maximumDDLSCN SCN
	if err := row.Scan(&maximumDDLSCN); err != nil {
		// ORA-08180: no snapshot found based on specified time
		// this is usually because the time is so recent a snapshot for that time has not
		// materialized into a SCN, in these scenarios we use current SCN
		if strings.Contains(err.Error(), "ORA-08180") {
			var currentSCN SCN
			var row = s.conn.QueryRowContext(ctx, "SELECT current_scn FROM V$DATABASE")
			if err := row.Scan(&currentSCN); err != nil {
				return 0, fmt.Errorf("fetching current SCN: %w", err)
			}
			return currentSCN, nil
		}

		return 0, fmt.Errorf("fetching maximum last DDL SCN: %w", err)
	}

	if err := s.switchToCDB(ctx); err != nil {
		return 0, err
	}

	return maximumDDLSCN, nil
}

// Acknowledge informs the ReplicationStream that all messages up to the specified
// SCN have been persisted
func (s *replicationStream) Acknowledge(ctx context.Context, cursor json.RawMessage) error {
	return nil
}

func (s *replicationStream) Close(ctx context.Context) error {
	logrus.Debug("replication stream close requested")
	s.cancel()
	s.transactionsStmt.Close()
	s.conn.Close()
	return <-s.errCh
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
