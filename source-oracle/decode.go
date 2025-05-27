package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/vitess/go/vt/sqlparser"
)

// decode SQL values extracted from the sql queries returned by logminer and parsed by sqlparser, into values
// to be used in the document
func decodeValue(expr sqlparser.Expr) (any, error) {
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
			t, err := time.Parse(PARSE_DATE_FORMAT, val)
			if err != nil {
				return nil, fmt.Errorf("invalid date %q: %w", val, err)
			}
			return t.Format(OUT_DATE_FORMAT), nil
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
				var codeStrOne = input[i+1 : i+5]
				var codeOne, err = strconv.ParseInt(codeStrOne, 16, 32)
				if err != nil {
					return "", fmt.Errorf("parsing unicode point at %q (%d): %w", input, i+1, err)
				}
				if utf16.IsSurrogate(rune(codeOne)) {
					var codeStrTwo = input[i+6 : i+10]
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

func (s *replicationStream) decodeMessage(msg logminerMessage) (sqlcapture.DatabaseEvent, error) {
	var streamID = sqlcapture.JoinStreamID(msg.Owner, msg.TableName)

	var parser, err = sqlparser.New(sqlparser.Options{})
	if err != nil {
		return nil, err
	}
	ast, err := parser.Parse(msg.SQL)
	if err != nil {
		return nil, fmt.Errorf("parsing sql query %v: %w", msg, err)
	}
	undoAST, err := parser.Parse(msg.UndoSQL)
	if err != nil {
		return nil, fmt.Errorf("parsing undo sql query %v: %w", msg, err)
	}
	var after, before map[string]any

	var op sqlcapture.ChangeOp
	switch msg.Op {
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

			value, err := decodeValue(vs[i])
			if err != nil {
				return nil, fmt.Errorf("insert sql statement %q, key %q: %w", msg.SQL, key, err)
			}
			after[key] = value
		}

		// sql_redo of insert statements do not include a ROWID, but their SQL_UNDO which
		// is a delete statement does include a ROWID, so we extract the ROWID from the undo SQL
		del, ok := undoAST.(*sqlparser.Delete)
		if !ok {
			return nil, fmt.Errorf("expected DELETE undo sql statement, instead got: %v", undoAST)
		}

		err := sqlparser.VisitExpr(del.Where.Expr, func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch n := node.(type) {
			case *sqlparser.ComparisonExpr:
				var col = n.Left.(*sqlparser.ColName)
				var key = unquote(sqlparser.String(col.Name))
				if key == "ROWID" {
					var value = n.Right.(*sqlparser.Literal).Val
					after[key] = value

					return false, nil
				}
			}

			return true, nil
		})
		if err != nil {
			return nil, err
		}
	case opUpdate:
		before = make(map[string]any)
		after = make(map[string]any)
		op = sqlcapture.UpdateOp
		// construct `after` from the redo sql
		update, ok := ast.(*sqlparser.Update)
		if !ok {
			return nil, fmt.Errorf("expected UPDATE sql statement, instead got: %v", ast)
		}

		err := sqlparser.VisitExpr(update.Where.Expr, func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch n := node.(type) {
			case *sqlparser.ComparisonExpr:
				var col = n.Left.(*sqlparser.ColName)
				var key = unquote(sqlparser.String(col.Name))
				value, err := decodeValue(n.Right)
				if err != nil {
					return false, fmt.Errorf("update sql statement where clause %q, key %q: %w", msg.SQL, key, err)
				}
				after[key] = value
			}

			return true, nil
		})
		if err != nil {
			return nil, err
		}

		for _, expr := range update.Exprs {
			var key = unquote(sqlparser.String(expr.Name.Name))
			value, err := decodeValue(expr.Expr)
			if err != nil {
				return nil, fmt.Errorf("update sql statement %q, key %q: %w", msg.SQL, key, err)
			}
			after[key] = value
		}

		// construct `before` from the undo sql
		undo, ok := undoAST.(*sqlparser.Update)
		if !ok {
			return nil, fmt.Errorf("expected UPDATE sql statement, instead got: %v", ast)
		}
		err = sqlparser.VisitExpr(undo.Where.Expr, func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch n := node.(type) {
			case *sqlparser.ComparisonExpr:
				var col = n.Left.(*sqlparser.ColName)
				var key = unquote(sqlparser.String(col.Name))
				value, err := decodeValue(n.Right)
				if err != nil {
					return false, fmt.Errorf("update sql undo statement where clause %q, key %q: %w", msg.UndoSQL, key, err)
				}
				before[key] = value
			}

			return true, nil
		})
		if err != nil {
			return nil, err
		}

		for _, expr := range undo.Exprs {
			var key = unquote(sqlparser.String(expr.Name.Name))
			value, err := decodeValue(expr.Expr)
			if err != nil {
				return nil, fmt.Errorf("update sql undo statement %q, key %q: %w", msg.UndoSQL, key, err)
			}
			before[key] = value
		}
	case opDelete:
		before = make(map[string]any)
		op = sqlcapture.DeleteOp
		del, ok := ast.(*sqlparser.Delete)
		if !ok {
			return nil, fmt.Errorf("expected DELETE sql statement, instead got: %v", ast)
		}
		err := sqlparser.VisitExpr(del.Where.Expr, func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch n := node.(type) {
			case *sqlparser.ComparisonExpr:
				var col = n.Left.(*sqlparser.ColName)
				var key = unquote(sqlparser.String(col.Name))
				value, err := decodeValue(n.Right)
				if err != nil {
					return false, fmt.Errorf("delete sql statement %q, key %q: %w", msg.SQL, key, err)
				}
				before[key] = value
			}

			return true, nil
		})
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected operation code %d", msg.Op)
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
	}

	if before != nil {
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
			Millis:   msg.Timestamp,
			Schema:   msg.Owner,
			Snapshot: false,
			Table:    msg.TableName,
		},
		SCN:   msg.SCN,
		RowID: rowid,
		RSID:  msg.RSID,
		SSN:   msg.SSN,
	}

	var event = &sqlcapture.ChangeEvent{
		Operation: op,
		RowKey:    rowKey,
		Source:    sourceInfo,
		Before:    before,
		After:     after,
	}

	return event, nil
}
