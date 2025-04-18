package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

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
    MIN(SCN) AS STARTSCN
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

		pendingTransactions: make(map[XID]checkpointTx),

		transactionsStmt: transactionsStmt,
	}

	stream.tables.active = make(map[string]struct{})
	stream.tables.keyColumns = make(map[string][]string)
	stream.tables.discovery = make(map[string]*sqlcapture.DiscoveryInfo)
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
	DictStart   string
	DictEnd     string
}

func (s *replicationStream) addLogFiles(ctx context.Context, startSCN, endSCN SCN) (int, error) {
	logrus.WithFields(logrus.Fields{
		"startSCN": startSCN,
		"endSCN":   endSCN,
		"sequence": s.redoSequence,
	}).Debug("adding log files")

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

	var liveLogFiles = `SELECT L.STATUS as STATUS, MIN(LF.MEMBER) as NAME, L.SEQUENCE#, L.FIRST_CHANGE#,'NO' as DICT_START, 'NO' as DICT_END FROM V$LOGFILE LF, V$LOG L
		LEFT JOIN V$ARCHIVED_LOG A ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE#
    WHERE (A.STATUS <> 'A' OR A.FIRST_CHANGE# IS NULL) AND L.GROUP# = LF.GROUP# AND L.STATUS <> 'UNUSED'
		GROUP BY LF.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD#`

	var archivedLogFiles = `SELECT 'ARCHIVED' as STATUS, A.NAME AS NAME, A.SEQUENCE#, A.FIRST_CHANGE#, A.DICTIONARY_BEGIN as DICT_START, A.DICTIONARY_END as DICT_END FROM V$ARCHIVED_LOG A
    WHERE A.NAME IS NOT NULL AND A.ARCHIVED = 'YES' AND A.STATUS = 'A' AND A.NEXT_CHANGE# >= :1 AND
    DEST_ID IN (` + strconv.Itoa(localDestID) + `)`

	var fullQuery = liveLogFiles + " UNION " + archivedLogFiles + " ORDER BY SEQUENCE#"
	rows, err := s.conn.QueryContext(ctx, fullQuery, minArchiveSCN)
	if err != nil {
		return 0, fmt.Errorf("fetching log file list: %w", err)
	}
	defer rows.Close()

	var redoSequence int
	var redoFiles []redoFile
	for rows.Next() {
		var f redoFile

		if err := rows.Scan(&f.Status, &f.Name, &f.Sequence, &f.FirstChange, &f.DictStart, &f.DictEnd); err != nil {
			return 0, fmt.Errorf("scanning log file record: %w", err)
		}

		logrus.WithField("file", fmt.Sprintf("%+v", f)).Debug("adding log file")

		// The current log file has the same sequence as the last archived log file
		// in this case we prefer reading from the current log file. It is also possible for
		// log files to have an ACTIVE and an ARCHIVED version at the same time, we prefer ACTIVE
		if f.Sequence == redoSequence {
			var last = redoFiles[len(redoFiles)-1]
			logrus.WithFields(logrus.Fields{
				"this": fmt.Sprintf("%+v", f),
				"last": fmt.Sprintf("%+v", last),
			}).Debug("found two log files with the same sequence number, keeping CURRENT or ACTIVE")

			if f.Status == "CURRENT" || f.Status == "ACTIVE" {
				// Remove the last archived log file from the list
				redoFiles = redoFiles[:len(redoFiles)-1]
			} else if last.Status == "CURRENT" || last.Status == "ACTIVE" {
				// Skip this file
				continue
			} else {
				return 0, fmt.Errorf("found two log files with the same sequence number, but neither is CURRENT or ACTIVE: %+v, %+v", f, last)
			}
		}

		if f.Sequence > redoSequence {
			redoSequence = f.Sequence
		}

		redoFiles = append(redoFiles, f)

		if f.FirstChange >= endSCN {
			if s.dictionaryMode == DictionaryModeOnline {
				break
			}

			// If we don't include a dictionary end file, we risk having an incomplete
			// dictionary in extract mode
			if f.DictEnd == "YES" {
				break
			}
		}
	}

	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, f := range redoFiles {
		if _, err := s.conn.ExecContext(ctx, "BEGIN SYS.DBMS_LOGMNR.ADD_LOGFILE(:filename); END;", f.Name); err != nil {
			return 0, fmt.Errorf("adding logfile %q (%s, %d, %s, %s) to logminer: %w", f.Name, f.Status, f.FirstChange, f.DictStart, f.DictEnd, err)
		}
	}

	return redoSequence, nil
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

	// sequence# of the last redo log file read, used to check if new log files have appeared
	redoSequence int

	lastTxnEndSCN SCN // End SCN (record + 1) of the last completed transaction.

	// Statement for querying pending and rolled back transactions
	transactionsStmt *sql.Stmt

	// Transactions being tracked, a mapping of XIDs to starting SCN and message count of transactions which have not yet finished (commit or rollback)
	pendingTransactions map[XID]checkpointTx

	// The 'active tables' set, guarded by a mutex so it can be modified from
	// the main goroutine while it's read by the replication goroutine.
	tables struct {
		sync.RWMutex
		active     map[string]struct{}
		keyColumns map[string][]string
		discovery  map[string]*sqlcapture.DiscoveryInfo
	}
}

type checkpointTx struct {
	StartSCN     SCN
	MessageCount int
}

type checkpoint struct {
	SCN                 SCN
	PendingTransactions map[XID]checkpointTx
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
			if event, ok := event.(*sqlcapture.ChangeEvent); ok {
				if event.Operation != sqlcapture.DeleteOp && event.Source.Common().StreamID() == watermarkStreamID {
					var actual = event.After["watermark"]
					if actual == nil {
						actual = event.After["WATERMARK"]
					}
					logrus.WithFields(logrus.Fields{"expected": fenceWatermark, "actual": actual}).Debug("watermark change")
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

	var eg, egCtx = errgroup.WithContext(ctx)
	var streamCtx, streamCancel = context.WithCancel(egCtx)
	s.events = make(chan sqlcapture.DatabaseEvent, replicationBufferSize)
	s.errCh = make(chan error)
	s.cancel = streamCancel

	eg.Go(func() error {
		return s.run(streamCtx)
	})

	go func() {
		if err := eg.Wait(); err != nil {
			// Context cancellation typically occurs only in tests, and for test stability
			// it should be considered a clean shutdown and not necessarily an error.
			if errors.Is(err, context.Canceled) {
				err = nil
			}
			close(s.events)
			s.transactionsStmt.Close()
			s.conn.Close()
			s.errCh <- err
		}
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
		// TODO: this value should adapt to the number of log files we find: if we find
		// too many log files, we need to reduce this value automatically to avoid a timeout
		// if we receive too few log files, we can try a larger value to progress faster.
		// One challenge that makes this task more difficult is the need to have dictionary boundaries
		// around a SCN range that we capture. Sometimes it is hard to find a dictionary bound range
		// that is small enough to not exceed the limit of files we need
		var endSCN = startSCN + int64(s.db.config.Advanced.IncrementalSCNRange)

		if maxSCN < endSCN {
			endSCN = maxSCN + 1
		}

		if currentSCN < endSCN {
			endSCN = currentSCN
		}

		// If redo files have switched, we need to restart and add log files
		// over again, otherwise we just start a new session with a different SCN range
		var redoSequence int
		if switched, err := s.redoFileSwitched(ctx); err != nil {
			return err
		} else if switched {
			if err := s.endLogminer(ctx); err != nil {
				return err
			} else if redoSequence, err = s.addLogFiles(ctx, startSCN, endSCN); err != nil {
				return err
			} else if err := s.startLogminer(ctx, startSCN, endSCN); err != nil {
				return err
			}
		} else {
			if err := s.startLogminer(ctx, startSCN, endSCN); err != nil {
				return err
			}
		}

		var stmt, err = s.generateLogminerQuery(ctx, transactions)
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
					s.pendingTransactions[xid] = checkpointTx{
						StartSCN:     startSCN,
						MessageCount: 0,
					}
				}
			}
		}

		if err := s.receiveMessages(ctx, startSCN, endSCN, stmt, rollbackedXIDs); err != nil {
			// If configured to use smart mode, switch to extract mode and re-try
			// We will switch back to Online mode afterwards
			var dictionaryMismatch = strings.Contains(strings.ToLower(err.Error()), "dictionary mismatch")
			if dictionaryMismatch && s.dictionaryMode == DictionaryModeOnline && s.smartMode {
				s.dictionaryMode = DictionaryModeExtract
				if lastDDLSCN, err := s.maximumLastDDLSCN(ctx); err != nil {
					return err
				} else {
					s.lastDDLSCN = lastDDLSCN
				}

				logrus.WithField("lastDDLSCN", s.lastDDLSCN).Info("smart mode: encountered dictionary mismatch, switching to extract mode")
				continue
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
		} else {
			s.events <- &sqlcapture.FlushEvent{Cursor: cpJSON}
		}

		if s.smartMode && s.dictionaryMode == DictionaryModeExtract && int64(s.lastDDLSCN) < endSCN {
			s.dictionaryMode = DictionaryModeOnline
			logrus.WithFields(logrus.Fields{
				"lastDDLSCN": s.lastDDLSCN,
				"endSCN":     endSCN,
			}).Info("smart mode: switching back to online mode")
		}

		s.redoSequence = redoSequence

		startSCN = endSCN
	}
}

func (s *replicationStream) handleTransactionBoundaries(ctx context.Context, msg logminerMessage) error {
	// BEGIN messages are never emitted
	if msg.Op == opStart {
		return nil
	}

	if msg.Op == opCommit {
		if tx, ok := s.pendingTransactions[msg.XID]; ok {
			delete(s.pendingTransactions, msg.XID)
			if tx.MessageCount > 0 {
				return s.capturePendingTransaction(ctx, transaction{XID: msg.XID, StartSCN: tx.StartSCN}, msg)
			} else {
				return nil
			}
		}

		return nil
	}

	if msg.Op == opRollback {
		delete(s.pendingTransactions, msg.XID)
		return nil
	}

	// Emittable messages for pending transactions are ignored until their commit is seen
	if tx, ok := s.pendingTransactions[msg.XID]; ok {
		tx.MessageCount += 1
		s.pendingTransactions[msg.XID] = tx
		return nil
	}

	return s.decodeAndEmitMessage(ctx, msg)
}

func (s *replicationStream) capturePendingTransaction(ctx context.Context, tx transaction, commit logminerMessage) error {
	var startSCN = tx.StartSCN
	var endSCN = commit.SCN

	var transactions = []string{tx.XID}
	logrus.WithFields(logrus.Fields{
		"tx": tx.XID,
	}).Debug("capturing pending transaction")
	return s.poll(ctx, startSCN, endSCN, transactions)
}

func (s *replicationStream) decodeAndEmitMessage(ctx context.Context, msg logminerMessage) error {
	var event, err = s.decodeMessage(msg)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.events <- event:
		return nil
	}
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

func (s *replicationStream) generateLogminerQuery(ctx context.Context, transactions []string) (*sql.Stmt, error) {
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

	// START and COMMIT operations do not seg_owner and have data_obj#=0 and data_objd#=0
	var query = fmt.Sprintf(`SELECT SCN, TIMESTAMP, OPERATION_CODE, SQL_REDO, SQL_UNDO, TABLE_NAME, SEG_OWNER, STATUS, INFO, RS_ID, SSN, CSF, DATA_OBJ#, DATA_OBJD#, XID
    FROM V$LOGMNR_CONTENTS
    WHERE OPERATION_CODE IN (%s) AND SCN >= :startSCN AND SCN <= :endSCN AND 
    (SEG_OWNER IS NULL OR
    SEG_OWNER NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS'))
    AND ((DATA_OBJ#=0 AND DATA_OBJD#=0) OR %s) %s`, opPredicate, strings.Join(conditions, " OR "), txPredicate)

	return s.conn.PrepareContext(ctx, query)
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
		if err := rows.Scan(&xidRaw, &rollbacks, &commits, &startSCN); err != nil {
			return nil, nil, fmt.Errorf("scanning transactions: %w", err)
		}
		var xid = base64.StdEncoding.EncodeToString(xidRaw)

		if rollbacks > 0 {
			excludeXIDs[xid] = struct{}{}
			logrus.WithFields(logrus.Fields{
				"xid": xid,
			}).Debug("skipping rolled-back transaction")
		} else if commits == 0 {
			pendingTXs[xid] = startSCN
			logrus.WithFields(logrus.Fields{
				"xid":      xid,
				"startSCN": startSCN,
			}).Debug("found pending transaction")
		}
	}

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
			return err
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
			logrus.WithField("streamID", streamID).Debug("logminer: received message for unknown table")
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
					return err
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
			return err
		}
		fullMessages++
	}

	if err := rows.Err(); err != nil {
		return err
	} else if err := rows.Close(); err != nil {
		return err
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
	}).Debug("received messages")

	return nil
}

func (s *replicationStream) tableActive(streamID string) bool {
	s.tables.RLock()
	defer s.tables.RUnlock()
	var _, ok = s.tables.active[streamID]
	return ok
}

func (s *replicationStream) keyColumns(streamID string) ([]string, bool) {
	s.tables.RLock()
	defer s.tables.RUnlock()
	var keyColumns, ok = s.tables.keyColumns[streamID]
	return keyColumns, ok
}

func (s *replicationStream) ActivateTable(ctx context.Context, streamID string, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
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
	s.transactionsStmt.Close()
	s.conn.Close()
	s.cancel()
	return <-s.errCh
}

func (s *replicationStream) redoFileSwitched(ctx context.Context) (bool, error) {
	row := s.conn.QueryRowContext(ctx, "SELECT SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT' ORDER BY SEQUENCE# FETCH NEXT 1 ROW ONLY")

	var newSequence int
	if err := row.Scan(&newSequence); err != nil {
		return false, fmt.Errorf("fetching latest redo log sequence number: %w", err)
	}

	return newSequence > s.redoSequence, nil
}

func (db *oracleDatabase) ReplicationDiagnostics(ctx context.Context) error {
	var query = func(q string) {
		logrus.WithField("query", q).Info("replication diagnostics")
		var rows, err = db.conn.QueryContext(ctx, q)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"query": q,
				"err":   err,
			}).Error("unable to execute diagnostics query")
			return
		}
		defer rows.Close()

		var numResults int
		cols, err := rows.Columns()
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"query": q,
				"err":   err,
			}).Error("unable to execute diagnostics query")
		}
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"query": q,
				"err":   err,
			}).Error("unable to execute diagnostics query")
		}
		for rows.Next() {
			numResults++
			// Scan the row values and copy into the equivalent map
			var fields = make(map[string]any)
			var fieldsPtr = make([]any, len(cols))
			for idx, col := range cols {
				fields[col] = reflect.New(colTypes[idx].ScanType()).Interface()
				fieldsPtr[idx] = fields[col]
			}
			if err := rows.Scan(fieldsPtr...); err != nil {
				logrus.WithFields(logrus.Fields{
					"query": q,
					"err":   err,
				}).Error("unable to scan diagnostics query")
			}

			logrus.WithField("result", fields).Info("replication diagnostics")
		}
		if numResults == 0 {
			logrus.WithField("query", q).Info("replication diagnostics: no results")
		}
	}

	query("SELECT * FROM " + db.config.Advanced.WatermarksTable)
	query("SELECT current_scn from V$DATABASE")
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
