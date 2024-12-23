package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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

func (db *oracleDatabase) ReplicationStream(ctx context.Context, startCursor string) (sqlcapture.ReplicationStream, error) {
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

	var startSCN int64
	if startCursor != "" {
		startSCN, err = strconv.ParseInt(startCursor, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing start cursor: %w", err)
		}
	} else {
		var row = db.conn.QueryRowContext(ctx, "SELECT current_scn FROM V$DATABASE")
		if err := row.Scan(&startSCN); err != nil {
			return nil, fmt.Errorf("fetching current SCN: %w", err)
		}
	}

	logrus.WithFields(logrus.Fields{
		"startSCN": startSCN,
	}).Info("starting replication")

	var logminerQuery = generateLogminerQuery(db.tableObjectMapping)
	logrus.WithFields(logrus.Fields{
		"query": logminerQuery,
	}).Info("logminer contents query")

	stmt, err := conn.PrepareContext(ctx, logminerQuery)
	if err != nil {
		return nil, fmt.Errorf("preparing logminer query: %w", err)
	}

	var stream = &replicationStream{
		db:   db,
		conn: conn,

		lastTxnEndSCN: startSCN,
		logminerStmt:  stmt,
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
	FirstChange int64
	DictStart   string
	DictEnd     string
}

func (s *replicationStream) addLogFiles(ctx context.Context, startSCN, endSCN int64) error {
	logrus.WithFields(logrus.Fields{
		"startSCN": startSCN,
		"endSCN":   endSCN,
		"sequence": s.redoSequence,
	}).Debug("adding log files")

	// See DBMS_LOGMNR_D.BUILD reference:
	// https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOGMNR_D.html#GUID-20E210F3-A566-46F1-B817-486723069AF4
	if s.db.config.Advanced.DictionaryMode == DictionaryModeExtract {
		if _, err := s.conn.ExecContext(ctx, "BEGIN SYS.DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;"); err != nil {
			return fmt.Errorf("extracting dictionary from logfile: %w", err)
		}
	}
	// We only add the local version of archived log files to avoid duplicates
	var row = s.conn.QueryRowContext(ctx, "SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE TYPE='LOCAL' AND STATUS='VALID' AND ROWNUM=1")
	var localDestID int
	if err := row.Scan(&localDestID); err != nil {
		return fmt.Errorf("querying archive log files destination: %w", err)
	}

	// In order to make sure we don't hit dictionary mismatches, we need to find the most recent archive log before
	// or at our SCN range that begins a dictionary, so that early rows of this range are not missing their dictionary
	row = s.conn.QueryRowContext(ctx, "SELECT MAX(NEXT_CHANGE#) FROM V$ARCHIVED_LOG WHERE DICTIONARY_BEGIN='YES' AND NEXT_CHANGE# <= :scn", startSCN)
	var minArchiveSCNScan sql.NullInt64
	if err := row.Scan(&minArchiveSCNScan); err != nil {
		return fmt.Errorf("querying latest archive log to contain the dictionary for the SCN range: %w", err)
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
		return fmt.Errorf("fetching log file list: %w", err)
	}
	defer rows.Close()

	var redoSequence int
	var redoFiles []redoFile
	for rows.Next() {
		var f redoFile

		if err := rows.Scan(&f.Status, &f.Name, &f.Sequence, &f.FirstChange, &f.DictStart, &f.DictEnd); err != nil {
			return fmt.Errorf("scanning log file record: %w", err)
		}

		logrus.WithField("file", fmt.Sprintf("%+v", f)).Debug("adding log file")

		if f.Sequence > redoSequence {
			redoSequence = f.Sequence
		}

		redoFiles = append(redoFiles, f)

		// once we hit a log file that has passed endSCN and it signifies a dictEnd, we know we don't
		// need any more log files. If we don't include a dictionary end file, we risk having an incomplete
		// dictionary
		if f.FirstChange >= endSCN && f.DictEnd == "YES" {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for _, f := range redoFiles {
		if _, err := s.conn.ExecContext(ctx, "BEGIN SYS.DBMS_LOGMNR.ADD_LOGFILE(:filename); END;", f.Name); err != nil {
			return fmt.Errorf("adding logfile %q (%s, %d, %s, %s) to logminer: %w", f.Name, f.Status, f.FirstChange, f.DictStart, f.DictEnd, err)
		}
	}

	s.redoSequence = redoSequence

	return err
}

func (s *replicationStream) startLogminer(ctx context.Context, startSCN, endSCN int64) error {
	var dictionaryOption = ""
	if s.db.config.Advanced.DictionaryMode == DictionaryModeExtract {
		dictionaryOption = "+ DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING"
	} else if s.db.config.Advanced.DictionaryMode == DictionaryModeOnline {
		dictionaryOption = "+ DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"
	}

	logrus.WithFields(logrus.Fields{
		"startSCN": startSCN,
		"endSCN":   endSCN,
	}).Debug("starting logminer")
	var startQuery = fmt.Sprintf("BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN=>:scn,ENDSCN=>:end,OPTIONS=>DBMS_LOGMNR.COMMITTED_DATA_ONLY %s); END;", dictionaryOption)
	if _, err := s.conn.ExecContext(ctx, startQuery, startSCN, endSCN); err != nil {
		return fmt.Errorf("starting logminer: %w", err)
	}

	return nil
}

// oracleSource is source metadata for data capture events.
type oracleSource struct {
	sqlcapture.SourceCommon

	// System Change Number, available for incremental changes only
	SCN int `json:"scn,omitempty" jsonschema:"description=SCN of this event, only present for incremental changes"`

	RowID string `json:"row_id" jsonschema:"description=ROWID of the document"`
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

	// sequence# of the last redo log file read, used to check if new log files have appeared
	redoSequence int

	lastTxnEndSCN int64 // End SCN (record + 1) of the last completed transaction.

	logminerStmt *sql.Stmt

	// The 'active tables' set, guarded by a mutex so it can be modified from
	// the main goroutine while it's read by the replication goroutine.
	tables struct {
		sync.RWMutex
		active     map[string]struct{}
		keyColumns map[string][]string
		discovery  map[string]*sqlcapture.DiscoveryInfo
	}
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
			s.logminerStmt.Close()
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

		var err = s.poll(ctx)
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

func (s *replicationStream) poll(ctx context.Context) error {
	for {
		var startSCN = s.lastTxnEndSCN
		var currentSCN int64
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

		if currentSCN < endSCN {
			endSCN = currentSCN
		}

		// If redo files have switched, we need to restart and add log files
		// over again, otherwise we just start a new session with a different SCN range
		if switched, err := s.redoFileSwitched(ctx); err != nil {
			return err
		} else if switched {
			if err := s.endLogminer(ctx); err != nil {
				return err
			} else if err := s.addLogFiles(ctx, startSCN, endSCN); err != nil {
				return err
			} else if err := s.startLogminer(ctx, startSCN, endSCN); err != nil {
				return err
			}
		} else {
			if err := s.startLogminer(ctx, startSCN, endSCN); err != nil {
				return err
			}
		}

		if err := s.receiveMessages(ctx, startSCN, endSCN); err != nil {
			return fmt.Errorf("receive messages: %w", err)
		}

		// Although the query is inclusive, we do not +1 here since
		// it is possible for there to be multiple rows with the same SCN
		// and to be cautious we use the SCN as-is. This may lead to duplicate events being captured
		// on restart, but it saves us from missing events
		s.lastTxnEndSCN = endSCN

		s.events <- &sqlcapture.FlushEvent{Cursor: strconv.FormatInt(s.lastTxnEndSCN, 10)}
	}
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
	SCN          int
	EndSCN       int
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
}

const (
	opInsert = 1
	opDelete = 2
	opUpdate = 3
)

func generateLogminerQuery(tableObjectMapping map[string]tableObject) string {
	var tablesCondition = ""
	var i = 0
	for _, mapping := range tableObjectMapping {
		if i > 0 {
			tablesCondition += " OR "
		}
		tablesCondition += fmt.Sprintf("(DATA_OBJ# = %d AND DATA_OBJD# = %d)", mapping.objectID, mapping.dataObjectID)
		i++
	}
	return fmt.Sprintf(`SELECT SCN, TIMESTAMP, OPERATION_CODE, SQL_REDO, SQL_UNDO, TABLE_NAME, SEG_OWNER, STATUS, INFO, RS_ID, SSN, CSF, DATA_OBJ#, DATA_OBJD#
    FROM V$LOGMNR_CONTENTS
    WHERE OPERATION_CODE IN (1, 2, 3) AND SCN >= :startSCN AND SCN <= :endSCN AND
    SEG_OWNER NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS')
    AND (%s)`, tablesCondition)
}

// receiveMessage reads and parses the next replication message from the database,
// blocking until a message is available, the context is cancelled, or an error
// occurs.
func (s *replicationStream) receiveMessages(ctx context.Context, startSCN, endSCN int64) error {
	rows, err := s.logminerStmt.QueryContext(ctx, startSCN, endSCN)
	if err != nil {
		return fmt.Errorf("logminer query: %w", err)
	}

	var totalMessages = 0
	var relevantMessages = 0
	var lastMsg *logminerMessage
	for rows.Next() {
		totalMessages++

		var msg logminerMessage
		var ts time.Time
		var undoSql sql.NullString
		var info sql.NullString
		if err := rows.Scan(&msg.SCN, &ts, &msg.Op, &msg.SQL, &undoSql, &msg.TableName, &msg.Owner, &msg.Status, &info, &msg.RSID, &msg.SSN, &msg.CSF, &msg.ObjectID, &msg.DataObjectID); err != nil {
			return err
		}

		if undoSql.Valid {
			msg.UndoSQL = undoSql.String
		}

		if info.Valid {
			msg.Info = info.String
		}
		msg.Timestamp = ts.UnixMilli()

		logrus.WithFields(logrus.Fields{
			"msg": msg,
		}).Trace("received message")

		// If this change event is on a table we're not capturing, skip doing any
		// further processing on it.
		var streamID = sqlcapture.JoinStreamID(msg.Owner, msg.TableName)
		if !s.tableActive(streamID) {
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
				if err := s.decodeAndEmitMessage(ctx, *lastMsg); err != nil {
					return err
				}
				relevantMessages++
			}

			continue
		}

		lastMsg = &msg
		if msg.CSF == 1 {
			continue
		}

		if err := s.decodeAndEmitMessage(ctx, msg); err != nil {
			return err
		}
		relevantMessages++
	}

	if err := rows.Err(); err != nil {
		return err
	} else if err := rows.Close(); err != nil {
		return err
	}

	var finalSCN = 0
	if lastMsg != nil {
		finalSCN = lastMsg.SCN
	}
	logrus.WithFields(logrus.Fields{
		"totalMessages":    totalMessages,
		"relevantMessages": relevantMessages,
		"startSCN":         startSCN,
		"endSCN":           endSCN,
		"finalSCN":         finalSCN,
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
	return nil
}

// Acknowledge informs the ReplicationStream that all messages up to the specified
// SCN have been persisted
func (s *replicationStream) Acknowledge(ctx context.Context, cursor string) error {
	return nil
}

func (s *replicationStream) Close(ctx context.Context) error {
	logrus.Debug("replication stream close requested")
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
