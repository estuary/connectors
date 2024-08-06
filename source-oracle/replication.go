package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf16"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/vitess/go/vt/sqlparser"
	"github.com/sirupsen/logrus"
)

type redoFile struct {
	status   string
	file     string
	sequence int
}

func (db *oracleDatabase) HeartbeatWatermarkInterval() time.Duration {
	return 60 * time.Second
}

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
	conn, err := db.conn.Conn(ctx)
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

	var startSCN int
	if startCursor != "" {
		startSCN, err = strconv.Atoi(startCursor)
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

	var stream = &replicationStream{
		db:   db,
		conn: conn,

		ackSCN:        uint64(startSCN),
		lastTxnEndSCN: startSCN + 1,
	}

	if err = stream.startLogminer(ctx, startSCN); err != nil {
		return nil, fmt.Errorf("starting logminer: %w", err)
	}

	stream.tables.active = make(map[string]struct{})
	stream.tables.keyColumns = make(map[string][]string)
	stream.tables.discovery = make(map[string]*sqlcapture.DiscoveryInfo)
	return stream, nil
}

func (s *replicationStream) endLogminer(ctx context.Context) error {
	if _, err := s.conn.ExecContext(ctx, "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR; END;"); err != nil {
		return fmt.Errorf("ending logminer session: %w", err)
	}

	return nil
}

func (s *replicationStream) addLogFiles(ctx context.Context, startSCN int) error {
	logrus.WithField("startSCN", startSCN).Debug("adding log files")

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

	var liveLogFiles = `SELECT L.STATUS as STATUS, MIN(LF.MEMBER) as NAME, L.SEQUENCE# FROM V$LOGFILE LF, V$LOG L
		LEFT JOIN V$ARCHIVED_LOG A ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE#
		WHERE (A.STATUS <> 'A' OR A.FIRST_CHANGE# IS NULL) AND L.GROUP# = LF.GROUP# AND L.STATUS <> 'UNUSED' AND L.NEXT_CHANGE# >= :1 AND LF.MEMBER NOT IN (SELECT FILENAME FROM V$LOGMNR_LOGS)
		GROUP BY LF.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE#, L.THREAD#`

	var archivedLogFiles = `SELECT 'ARCHIVED' as STATUS, A.NAME AS NAME, A.SEQUENCE# FROM V$ARCHIVED_LOG A
    WHERE A.NAME IS NOT NULL AND A.ARCHIVED = 'YES' AND A.STATUS = 'A' AND A.NEXT_CHANGE# >= :1 AND
    A.NAME NOT IN (SELECT FILENAME FROM V$LOGMNR_LOGS) AND DEST_ID IN (` + strconv.Itoa(localDestID) + `)`

	var fullQuery = liveLogFiles + " UNION " + archivedLogFiles + " ORDER BY SEQUENCE#"
	rows, err := s.conn.QueryContext(ctx, fullQuery, startSCN)
	if err != nil {
		return fmt.Errorf("fetching log file list: %w", err)
	}
	defer rows.Close()

	var redoSequence int
	var redoFiles []redoFile
	for rows.Next() {
		var f redoFile
		if err := rows.Scan(&f.status, &f.file, &f.sequence); err != nil {
			return fmt.Errorf("scanning log file record: %w", err)
		}

		logrus.WithFields(logrus.Fields{
			"status":   f.status,
			"file":     f.file,
			"sequence": f.sequence,
		}).Debug("log file")

		if f.sequence > redoSequence {
			redoSequence = f.sequence
		}

		redoFiles = append(redoFiles, f)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for _, f := range redoFiles {
		if _, err := s.conn.ExecContext(ctx, "BEGIN SYS.DBMS_LOGMNR.ADD_LOGFILE(:filename); END;", f.file); err != nil {
			return fmt.Errorf("adding logfile %q (%s) to logminer: %w", f.file, f.status, err)
		}
	}

	s.redoFiles = redoFiles
	s.redoSequence = redoSequence

	return nil
}

func (s *replicationStream) startLogminer(ctx context.Context, startSCN int) error {
	logrus.WithField("startSCN", startSCN).Debug("starting logminer")

	var dictionaryOption = ""
	if s.db.config.Advanced.DictionaryMode == DictionaryModeExtract {
		dictionaryOption = "+ DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING"
	} else if s.db.config.Advanced.DictionaryMode == DictionaryModeOnline {
		dictionaryOption = "+ DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"
	}

	if err := s.addLogFiles(ctx, startSCN); err != nil {
		return err
	}

	var startQuery = fmt.Sprintf("BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN=>:scn,OPTIONS=>DBMS_LOGMNR.COMMITTED_DATA_ONLY %s); END;", dictionaryOption)
	if _, err := s.conn.ExecContext(ctx, startQuery, startSCN); err != nil {
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

	cancel   context.CancelFunc            // Cancel function for the replication goroutine's context
	errCh    chan error                    // Error channel for the final exit status of the replication goroutine
	events   chan sqlcapture.DatabaseEvent // The channel to which replication events will be written
	eventBuf []sqlcapture.DatabaseEvent    // A buffer used in between 'receiveMessage' and the output channel

	redoFiles    []redoFile // list of redo files
	redoSequence int

	ackSCN        uint64 // The most recently Ack'd SCN, passed to startReplication or updated via CommitSCN.
	lastTxnEndSCN int    // End SCN (record + 1) of the last completed transaction.

	// The 'active tables' set, guarded by a mutex so it can be modified from
	// the main goroutine while it's read by the replication goroutine.
	tables struct {
		sync.RWMutex
		active     map[string]struct{}
		keyColumns map[string][]string
		discovery  map[string]*sqlcapture.DiscoveryInfo
	}
}

const pollInterval = 10 * time.Second

// replicationBufferSize controls how many change events can be buffered in the
// replicationStream before it stops receiving further events from Oracle.
// In normal use it's a constant, it's just a variable so that tests are more
// likely to exercise blocking sends and backpressure.
// This buffer has been set to a fairly small value, because larger buffers can
// cause OOM kills when the incoming data rate exceeds the rate at which we're
// serializing data and getting it into Gazette journals.
var replicationBufferSize = 16

func (s *replicationStream) Events() <-chan sqlcapture.DatabaseEvent {
	return s.events
}

func (s *replicationStream) StartReplication(ctx context.Context) error {
	var streamCtx, streamCancel = context.WithCancel(ctx)
	s.events = make(chan sqlcapture.DatabaseEvent, replicationBufferSize)
	s.errCh = make(chan error)
	s.cancel = streamCancel

	go func() {
		var err = s.run(streamCtx)
		// Context cancellation typically occurs only in tests, and for test stability
		// it should be considered a clean shutdown and not necessarily an error.
		if errors.Is(err, context.Canceled) {
			err = nil
		}

		// Always take up to 1 second to notify the database that we're done
		var _, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		s.conn.Close()
		cancel()
		close(s.events)
		s.errCh <- err
	}()
	return nil
}

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
		// If there's already a change event which needs to be sent to the consumer,
		// try to do so until/unless the context expires first.
		if s.eventBuf != nil {
			for _, ev := range s.eventBuf {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case s.events <- ev:
					continue
				}
			}

			s.eventBuf = nil
		}

		switched, err := s.redoFileSwitched(ctx)
		if err != nil {
			return err
		}

		if switched {
			if err := s.endLogminer(ctx); err != nil {
				return err
			} else if err := s.startLogminer(ctx, s.lastTxnEndSCN); err != nil {
				return err
			}
		}

		// In the absence of a buffered message, go try to receive another from
		// the database.
		msgs, err := s.receiveMessages(ctx)
		if err != nil {
			return fmt.Errorf("receive messages: %w", err)
		}

		s.eventBuf = make([]sqlcapture.DatabaseEvent, len(msgs)+1)
		for i, msg := range msgs {
			event, err := s.decodeMessage(msg)
			if err != nil {
				return fmt.Errorf("error decoding message: %w", err)
			}

			// Once a message arrives, decode it and buffer the result until the next
			// time this function is invoked.
			s.eventBuf[i] = event
		}

		s.eventBuf[len(s.eventBuf)-1] = &sqlcapture.FlushEvent{
			Cursor: strconv.Itoa(s.lastTxnEndSCN),
		}
	}
}

// decode the escaped unicode literals in UNISTR() functions
// the unicode literals are in the UCS-2 format
// see https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/UNISTR.html
func decodeUnistr(input string) (string, error) {
	var val string
	for i := 0; i < len(input); i++ {
		var c = input[i]
		// 92 = \ (backslash)
		if c == 92 {
			// two backslashes following each other is just an escaped backslash
			if len(input) > i+1 && input[i+1] == 92 {
				val += string(c)
				i += 1
			} else if len(input) > i+4 {
				var codeStrOne = fmt.Sprintf("%s", input[i+1:i+5])
				var codeOne, err = strconv.ParseInt(codeStrOne, 16, 32)
				if err != nil {
					return "", fmt.Errorf("parsing unicode point at %q (%d): %w", input, i+1, err)
				}
				if utf16.IsSurrogate(rune(codeOne)) {
					var codeStrTwo = fmt.Sprintf("%s", input[i+6:i+10])
					var codeTwo, err = strconv.ParseInt(codeStrTwo, 16, 32)
					if err != nil {
						return "", fmt.Errorf("parsing unicode point at %q (%d): %w", input, i+6, err)
					}
					i += 5

					val += string(utf16.DecodeRune(rune(codeOne), rune(codeTwo)))
				} else {
					val += string(rune(codeOne))
				}

				i += 4
			} else {
				val += string(c)
			}
		} else {
			val += string(c)
		}
	}

	return val, nil
}

// decode SQL values extracted from the sql queries returned by logminer and parsed by sqlparser, into values
// to be used in the document
func decodeValue(colInfo sqlcapture.ColumnInfo, expr sqlparser.Expr, colName string, originalSql string) (any, error) {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		return v.Val, nil
	case *sqlparser.NullVal:
		return nil, nil
	case *sqlparser.OrExpr:
		// Oracle's string concatenation using pipes || is parsed as an OrExpr
		// UNISTR('literal string')
		var leftLiteral = v.Left.(*sqlparser.FuncExpr).Exprs[0].(*sqlparser.Literal).Val
		var left, err = decodeUnistr(leftLiteral)
		if err != nil {
			return nil, err
		}
		var rightLiteral = v.Right.(*sqlparser.FuncExpr).Exprs[0].(*sqlparser.Literal).Val
		right, err := decodeUnistr(rightLiteral)
		if err != nil {
			return nil, err
		}

		return left + right, nil
	case *sqlparser.FuncExpr:
		switch v.Name.String() {
		// Unicode strings (used for NVARCHAR2 literals with unicode characters)
		case "UNISTR":
			var val, err = decodeUnistr(v.Exprs[0].(*sqlparser.Literal).Val)
			if err != nil {
				return "", err
			}
			return val, nil
		// Used for DATE values
		case "TO_DATE":
			var val = v.Exprs[0].(*sqlparser.Literal).Val
			if t, err := time.Parse(PARSE_DATE_FORMAT, val); err != nil {
				return nil, fmt.Errorf("invalid date %q: %w", val, err)
			} else {
				return t.Format(OUT_DATE_FORMAT), nil
			}
			return val, nil
		case "TO_TIMESTAMP":
			var val = v.Exprs[0].(*sqlparser.Literal).Val

			// Timestamp has no fractional seconds, it ends up with a trailing dot
			if strings.HasSuffix(val, ".") {
				val += "0"
			}
			if t, err := time.Parse(PARSE_TS_FORMAT, val); err != nil {
				return nil, fmt.Errorf("invalid timestamp %q: %w", val, err)
			} else {
				return t.Format(OUT_TS_FORMAT), nil
			}
		case "TO_TIMESTAMP_TZ":
			var val = v.Exprs[0].(*sqlparser.Literal).Val
			// Timestamp has no fractional seconds, it ends up with a trailing dot
			if strings.HasSuffix(val, ".") {
				val += "0"
			}
			if t, err := time.ParseInLocation(PARSE_TSTZ_FORMAT, val, time.UTC); err != nil {
				// If the function is TO_TIMESTAMP_TZ but it has been output as TS format, then the value
				// is a local timestamp
				if tlocal, e := time.Parse(PARSE_TS_FORMAT, val); e == nil {
					return tlocal.Format(OUT_TS_FORMAT), nil
				}
				return nil, fmt.Errorf("invalid timestamptz %q: %w", val, err)
			} else {
				return t.UTC().Format(OUT_TSTZ_FORMAT), nil
			}
		case "TO_YMINTERVAL":
			var val = v.Exprs[0].(*sqlparser.Literal).Val
			return val, nil
		case "TO_DSINTERVAL":
			var val = v.Exprs[0].(*sqlparser.Literal).Val
			return val, nil
		case "HEXTORAW":
			var val = v.Exprs[0].(*sqlparser.Literal).Val
			var src = []byte(val)
			var hx = make([]byte, hex.DecodedLen(len(src)))
			if _, err := hex.Decode(hx, src); err != nil {
				return nil, fmt.Errorf("decoding hex %q: %w", val, err)
			}

			var b64 = make([]byte, base64.StdEncoding.EncodedLen(len(hx)))
			base64.StdEncoding.Encode(b64, hx)

			return string(b64), nil
		}
	}

	return nil, fmt.Errorf("unknown expression: %s %+v", reflect.TypeOf(expr).Name(), expr)
}

func (s *replicationStream) decodeMessage(msg logminerMessage) (sqlcapture.DatabaseEvent, error) {
	var streamID = sqlcapture.JoinStreamID(msg.owner, msg.tableName)
	var parser, err = sqlparser.New(sqlparser.Options{})
	if err != nil {
		return nil, err
	}
	ast, err := parser.Parse(msg.sql)
	if err != nil {
		return nil, fmt.Errorf("parsing sql query %q: %w", msg.sql, err)
	}
	undoAST, err := parser.Parse(msg.undoSql)
	if err != nil {
		return nil, fmt.Errorf("parsing undo sql query %q: %w", msg.undoSql, err)
	}
	var after, before map[string]any

	var op sqlcapture.ChangeOp
	switch msg.op {
	case opInsert:
		after = make(map[string]any)
		op = sqlcapture.InsertOp
		insert, ok := ast.(*sqlparser.Insert)
		if !ok {
			return nil, fmt.Errorf("expected INSERT sql statement, instead got: %v", ast)
		}

		values, ok := insert.Rows.(sqlparser.Values)
		if !ok {
			return nil, fmt.Errorf("expected VALUES in INSERT statement, instead got: %v", insert.Rows)
		}
		// sql_redo field of logminer will have a single VALUES set for each insertion
		var vs = values[0]

		for i, col := range insert.Columns {
			var key = col.String()

			value, err := decodeValue(s.tables.discovery[streamID].Columns[key], vs[i], col.String(), msg.sql)
			if err != nil {
				return nil, fmt.Errorf("decoding value %s=%v: %w", key, values[0][i], err)
			}
			after[key] = value
		}

		// sql_redo of insert statements do not include a ROWID, but their SQL_UNDO which
		// is a delete statement does include a ROWID, so we extract the ROWID from the undo SQL
		del, ok := undoAST.(*sqlparser.Delete)
		if !ok {
			return nil, fmt.Errorf("expected DELETE undo sql statement, instead got: %v", undoAST)
		}

		sqlparser.VisitExpr(del.Where.Expr, func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch n := node.(type) {
			case *sqlparser.ComparisonExpr:
				var key = unquote(sqlparser.String(n.Left))
				if key == "ROWID" {
					var value = n.Right.(*sqlparser.Literal).Val
					after[key] = value

					return false, nil
				}
			}

			return true, nil
		})
	case opUpdate:
		before = make(map[string]any)
		after = make(map[string]any)
		op = sqlcapture.UpdateOp
		// construct `after` from the redo sql
		update, ok := ast.(*sqlparser.Update)
		if !ok {
			return nil, fmt.Errorf("expected UPDATE sql statement, instead got: %v", ast)
		}
		sqlparser.VisitExpr(update.Where.Expr, func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch n := node.(type) {
			case *sqlparser.ComparisonExpr:
				var key = unquote(sqlparser.String(n.Left))
				var value = n.Right.(*sqlparser.Literal).Val
				after[key] = value
			}

			return true, nil
		})

		for _, expr := range update.Exprs {
			var key = unquote(sqlparser.String(expr.Name.Name))
			var value = expr.Expr.(*sqlparser.Literal).Val
			after[key] = value
		}

		// construct `before` from the undo sql
		undo, ok := undoAST.(*sqlparser.Update)
		if !ok {
			return nil, fmt.Errorf("expected UPDATE sql statement, instead got: %v", ast)
		}
		sqlparser.VisitExpr(undo.Where.Expr, func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch n := node.(type) {
			case *sqlparser.ComparisonExpr:
				var key = unquote(sqlparser.String(n.Left))
				var value = n.Right.(*sqlparser.Literal).Val
				before[key] = value
			}

			return true, nil
		})

		for _, expr := range undo.Exprs {
			var key = unquote(sqlparser.String(expr.Name.Name))
			var value = expr.Expr.(*sqlparser.Literal).Val
			before[key] = value
		}
	case opDelete:
		before = make(map[string]any)
		op = sqlcapture.DeleteOp
		del, ok := ast.(*sqlparser.Delete)
		if !ok {
			return nil, fmt.Errorf("expected DELETE sql statement, instead got: %v", ast)
		}
		sqlparser.VisitExpr(del.Where.Expr, func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch n := node.(type) {
			case *sqlparser.ComparisonExpr:
				var key = unquote(sqlparser.String(n.Left))
				var value = n.Right.(*sqlparser.Literal).Val
				before[key] = value
			}

			return true, nil
		})
	default:
		return nil, fmt.Errorf("unexpected operation code %d", msg.op)
	}

	discovery, ok := s.tables.discovery[streamID]
	if !ok {
		return nil, fmt.Errorf("unknown discovery info for stream %q", streamID)
	}

	var rowid string
	if after != nil {
		rowid = after["ROWID"].(string)
		if err := translateRecordFields(discovery, after); err != nil {
			return nil, fmt.Errorf("error translating 'after' tuple: %w", err)
		}
	} else if before != nil {
		rowid = before["ROWID"].(string)
		if err := translateRecordFields(discovery, before); err != nil {
			return nil, fmt.Errorf("error translating 'before' tuple: %w", err)
		}
	}

	keyColumns, ok := s.keyColumns(streamID)
	if !ok {
		return nil, fmt.Errorf("unknown key columns for stream %q", streamID)
	}

	var columnTypes = make(map[string]oracleColumnType)
	for name, column := range discovery.Columns {
		columnTypes[name] = column.DataType.(oracleColumnType)
	}

	var rowKey []byte
	if op == sqlcapture.InsertOp || op == sqlcapture.UpdateOp {
		rowKey, err = sqlcapture.EncodeRowKey(keyColumns, after, columnTypes, encodeKeyFDB)
	} else {
		rowKey, err = sqlcapture.EncodeRowKey(keyColumns, before, columnTypes, encodeKeyFDB)
	}
	if err != nil {
		return nil, fmt.Errorf("error encoding row key for %q: %w", streamID, err)
	}
	delete(after, "ROWID")
	delete(before, "ROWID")

	var sourceInfo = &oracleSource{
		SourceCommon: sqlcapture.SourceCommon{
			Millis:   msg.ts,
			Schema:   msg.owner,
			Snapshot: false,
			Table:    msg.tableName,
		},
		SCN:   msg.startSCN,
		RowID: rowid,
	}

	var event = &sqlcapture.ChangeEvent{
		Operation: op,
		RowKey:    rowKey,
		Source:    sourceInfo,
		Before:    before,
		After:     after,
	}

	s.lastTxnEndSCN = msg.startSCN + 1

	return event, nil
}

// in WHERE AST, columns are quoted with backticks for some reason. This function
// removes backtick quotes from a string
func unquote(s string) string {
	return strings.TrimSuffix(strings.TrimPrefix(s, "`"), "`")
}

type logminerMessage struct {
	startSCN  int
	endSCN    int
	op        int
	sql       string
	undoSql   string
	tableName string
	owner     string
	ts        int64
	status    int
	info      string
	rsID      string
	ssn       int
	csf       int
}

const (
	opInsert = 1
	opDelete = 2
	opUpdate = 3
)

// receiveMessage reads and parses the next replication message from the database,
// blocking until a message is available, the context is cancelled, or an error
// occurs.
func (s *replicationStream) receiveMessages(ctx context.Context) ([]logminerMessage, error) {
	var rows, err = s.conn.QueryContext(ctx, `SELECT START_SCN, TIMESTAMP, OPERATION_CODE, SQL_REDO, SQL_UNDO, TABLE_NAME, SEG_OWNER, STATUS, INFO, RS_ID, SSN, CSF FROM V$LOGMNR_CONTENTS
    WHERE OPERATION_CODE IN (1, 2, 3) AND START_SCN >= :scn AND
    SEG_OWNER NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS')`, s.lastTxnEndSCN)
	if err != nil {
		return nil, fmt.Errorf("logminer query: %w", err)
	}
	defer rows.Close()

	var msgs []logminerMessage
	for rows.Next() {
		var lastMsg *logminerMessage

		if len(msgs) > 0 {
			lastMsg = &msgs[len(msgs)-1]
		}

		var msg logminerMessage
		var ts time.Time
		var undoSql sql.NullString
		var info sql.NullString
		if err := rows.Scan(&msg.startSCN, &ts, &msg.op, &msg.sql, &undoSql, &msg.tableName, &msg.owner, &msg.status, &info, &msg.rsID, &msg.ssn, &msg.csf); err != nil {
			return nil, err
		}
		if undoSql.Valid {
			msg.undoSql = undoSql.String
		}

		if info.Valid {
			msg.info = info.String
		}
		msg.ts = ts.UnixMilli()

		if logrus.IsLevelEnabled(logrus.TraceLevel) {
			logrus.WithFields(logrus.Fields{
				"sql":       msg.sql,
				"undoSql":   msg.undoSql,
				"op":        msg.op,
				"tableName": msg.tableName,
				"owner":     msg.owner,
				"ts":        msg.ts,
				"status":    msg.status,
				"info":      msg.info,
				"startSCN":  msg.startSCN,
				"endSCN":    msg.endSCN,
				"ssn":       msg.ssn,
				"rsid":      msg.rsID,
				"csf":       msg.csf,
			}).Trace("received message")
		}

		// last message indicated a continuation of SQL_REDO and SQL_UNDO values
		// so this row's sqls will be appended to the last
		if lastMsg != nil && lastMsg.csf == 1 {
			// sanity check the (SSN, RSID) tuple matches between the rows
			if lastMsg.ssn != msg.ssn || lastMsg.rsID != msg.rsID {
				return nil, fmt.Errorf("expected SSN and RSID of continued rows to match: (%d, %s) != (%d, %s)", lastMsg.ssn, lastMsg.rsID, msg.ssn, msg.rsID)
			}
			lastMsg.sql += msg.sql
			lastMsg.undoSql += msg.undoSql
			lastMsg.csf = msg.csf
			continue
		}

		// If this change event is on a table we're not capturing, skip doing any
		// further processing on it.
		var streamID = sqlcapture.JoinStreamID(msg.owner, msg.tableName)
		if !s.tableActive(streamID) {
			continue
		}

		// If logminer can't find the dictionary for a SQL statement (e.g. if using online mode and a schema change has occurred)
		// then we get SQL statements like this:
		// insert into "UNKNOWN"."OBJ# 45522"("COL 1","COL 2","COL 3","COL 4") values (HEXTORAW('45465f4748'),HEXTORAW('546563686e6963616c20577269746572'), HEXTORAW('c229'),HEXTORAW('c3020b'));
		// we additionally get status=2 for these records. Status 2 means the SQL statement is not valid for redoing.
		// Some versions of Oracle report STATUS=2 for LONG column types, but have an empty info column, whereas when
		// status=2 and there is some reason for the error in the info column (usually "Dictionary Mismatch") we consider
		// the case to be one of dictionary mismatch
		if msg.status == 2 && msg.info != "" {
			return nil, fmt.Errorf("dictionary mismatch (%s) for table %q: %q", msg.info, msg.tableName, msg.sql)
		}

		msgs = append(msgs, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return msgs, nil
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
	logrus.WithField("cursor", cursor).Debug("advancing acknowledged SCN")
	var scn, err = strconv.Atoi(cursor)
	if err != nil {
		return fmt.Errorf("error parsing acknowledge cursor: %w", err)
	}
	atomic.StoreUint64(&s.ackSCN, uint64(scn))
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
