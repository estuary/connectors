package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	go_ibm_db "github.com/ibmdb/go_ibm_db"
	"github.com/ibmdb/go_ibm_db/api"
	"github.com/stretchr/testify/require"
)

func decodeTestLOB(t *testing.T, data []byte, name string) driver.Value {
	t.Helper()
	var stype, ctype api.SQLSMALLINT = api.SQL_CLOB, api.SQL_C_CHAR
	if name == "BLOB" {
		stype, ctype = api.SQL_BLOB, api.SQL_C_BINARY
	} else if name == "XML" {
		stype, ctype = api.SQL_XML, api.SQL_C_BINARY
	}
	var column = go_ibm_db.NewVariableWidthColumn(&go_ibm_db.BaseColumn{SType: stype}, ctype, api.SQLULEN(len(data)+1)).(*go_ibm_db.BindableColumn)
	column.IsBound = true
	column.Len = go_ibm_db.BufferLen(copy(column.Buffer, data))
	if data == nil {
		column.Len = api.SQL_NULL_DATA
	}
	var handle api.SQLHSTMT
	var value, err = column.Value(handle, 0)
	require.NoError(t, err)
	return value
}

func TestLOBRecoveryUnicode(t *testing.T) {
	for _, name := range []string{"CLOB", "DBCLOB"} {
		t.Run(name, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var data = []byte(strings.Repeat("🌍", 16383) + "漢")
			require.Len(t, data, 65535)
			var values = []any{decodeTestLOB(t, data, name)}
			require.Nil(t, values[0])
			var expr = `"CONTENT"`
			if name == "DBCLOB" {
				expr = "CLOB(" + expr + ")"
			}
			for i, start := range []int{1, 4097, 8193, 12289} {
				var piece = strings.Repeat("🌍", 4096)
				if i == 3 {
					piece = strings.Repeat("🌍", 4095) + "漢"
				}
				var query = fmt.Sprintf(`SELECT SUBSTRING(%s, %d, 4096, CODEUNITS32) FROM "DB2INST1"."ORDERS" WHERE "ID" = ?`, expr, start)
				mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(int64(123)).
					WillReturnRows(sqlmock.NewRows([]string{"PIECE"}).AddRow(decodeTestLOB(t, []byte(piece), "CLOB"))).RowsWillBeClosed()
			}
			var columns = []db2LOBColumn{{Name: "CONTENT", Index: 0, Type: db2ColumnType{Name: name}}}
			var lengths = []sql.NullInt64{{Int64: 16384, Valid: true}}
			require.NoError(t, recoverLOBs(context.Background(), db.conn, `"DB2INST1"."ORDERS" WHERE "ID" = ?`, []any{int64(123)}, columns, lengths, values, false))
			require.Equal(t, data, values[0])
		})
	}
}

func TestBackfillLOBBoundaries(t *testing.T) {
	for _, name := range []string{"CLOB", "BLOB", "DBCLOB", "XML"} {
		var sizes = []int{-1, 0, 65534, 65535, 65536, 131070, 131071, 131072, 196606, 196607, 196608}
		if name == "XML" {
			sizes[1] = 4 // An empty XML element is <x/>.
		}
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%s/%d", name, size), func(t *testing.T) {
				var db, mock = testDatabase(t)
				var info = testOrdersInfo()
				info.ColumnNames = []string{"ID", "CONTENT"}
				info.Columns["CONTENT"] = sqlcapture.ColumnInfo{Name: "CONTENT", DataType: db2ColumnType{Name: name}}
				var data []byte
				if size >= 0 {
					data = []byte(strings.Repeat("a", size))
					if name == "BLOB" {
						for i := range data {
							data[i] = byte(i)
						}
					} else if name == "DBCLOB" {
						data = []byte(strings.Repeat("漢", size/3) + strings.Repeat("a", size%3))
					} else if name == "XML" {
						if size == 4 {
							data = []byte("<x/>")
						} else {
							data = []byte("<x>" + strings.Repeat("漢", (size-7)/3) + strings.Repeat("a", (size-7)%3) + "</x>")
						}
					}
					require.Len(t, data, size)
				}
				var value = decodeTestLOB(t, data, name)
				var length any
				var units = len(data)
				if name != "BLOB" {
					units = utf8.RuneCount(data)
				}
				if data != nil {
					length = units
				}
				var lengthExpression = `LENGTH\(.*`
				if name == "XML" {
					lengthExpression = regexp.QuoteMeta(`LENGTH(XMLSERIALIZE("CONTENT" AS CLOB(2G)), CODEUNITS32)`)
				}
				mock.ExpectQuery(`SELECT "ID", .*` + lengthExpression + ` AS "__FLOW_LOB_LENGTH_1" FROM "DB2INST1"\."ORDERS" AS T ORDER BY T."ID" FETCH FIRST 50000 ROWS ONLY$`).
					WillReturnRows(sqlmock.NewRows([]string{"ID", "CONTENT", "LENGTH"}).AddRow(7, value, length)).RowsWillBeClosed()
				if value == nil && data != nil {
					for start := 0; start < units; start += 4096 {
						var count = min(4096, units-start)
						var expr = `"CONTENT"`
						var suffix = ", CODEUNITS32"
						var piece []byte
						if name == "BLOB" {
							suffix, piece = "", data[start:start+count]
						} else {
							piece = []byte(string([]rune(string(data))[start : start+count]))
							if name == "DBCLOB" {
								expr = "CLOB(" + expr + ")"
							} else if name == "XML" {
								expr = "XMLSERIALIZE(" + expr + " AS CLOB(2G))"
							}
						}
						var pieceType = "CLOB"
						if name == "BLOB" {
							pieceType = "BLOB"
						}
						var query = fmt.Sprintf(`SELECT SUBSTRING(%s, %d, %d%s) FROM "DB2INST1"."ORDERS" WHERE "ID" = ?`, expr, start+1, count, suffix)
						mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(int64(7)).
							WillReturnRows(sqlmock.NewRows([]string{"PIECE"}).AddRow(decodeTestLOB(t, piece, pieceType))).RowsWillBeClosed()
					}
				}
				var documents []map[string]any
				var complete, cursor, err = db.ScanTableChunk(context.Background(), info, &sqlcapture.TableState{KeyColumns: []string{"ID"}}, "", func(event sqlcapture.ChangeEvent) error {
					var data, err = event.AppendJSON(nil)
					require.NoError(t, err)
					var doc map[string]any
					require.NoError(t, json.Unmarshal(data, &doc))
					documents = append(documents, doc)
					return nil
				})
				require.NoError(t, err)
				require.True(t, complete)
				require.Equal(t, tuple.Tuple{7}.Pack(), cursor)
				require.Len(t, documents, 1)
				require.Len(t, documents[0], 3, "companion lengths must not be emitted")
				var want any
				if data != nil {
					want = string(data)
					if name == "BLOB" {
						want = base64.StdEncoding.EncodeToString(data)
					}
				}
				require.Equal(t, want, documents[0]["CONTENT"])
			})
		}
	}
}

func TestBackfillLOBKey(t *testing.T) {
	for _, mode := range []string{"CollectionKey", "KeylessWithPrimaryKey", "KeylessWithoutKey"} {
		t.Run(mode, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var info = testOrdersInfo()
			info.ColumnNames = []string{"CONTENT", `Part"Name`, "ID"}
			info.Columns["CONTENT"] = sqlcapture.ColumnInfo{Name: "CONTENT", DataType: db2ColumnType{Name: "CLOB"}}
			info.Columns[`Part"Name`] = sqlcapture.ColumnInfo{Name: `Part"Name`, DataType: db2ColumnType{Name: "VARCHAR", Codepage: 1208}}
			var keys = []string{"ID", `Part"Name`}
			var state = &sqlcapture.TableState{KeyColumns: keys}
			var columns = []string{"CONTENT", `Part"Name`, "ID", "LENGTH"}
			var values = []driver.Value{nil, []byte("é"), 7, 65535}
			var cursor = tuple.Tuple{7, "é"}.Pack()
			if mode != "CollectionKey" {
				state.Mode, state.KeyColumns = sqlcapture.TableStateKeylessBackfill, nil
				info.PrimaryKey = keys
				columns, values = append(columns, "RID"), append(values, 123)
				cursor = tuple.Tuple{123}.Pack()
			}
			if mode == "KeylessWithoutKey" {
				info.PrimaryKey = nil
			}
			mock.ExpectQuery(`FROM "DB2INST1"\."ORDERS" AS T .* FETCH FIRST 50000 ROWS ONLY$`).
				WillReturnRows(sqlmock.NewRows(columns).AddRow(values...)).RowsWillBeClosed()
			var from = `"DB2INST1"."ORDERS" WHERE "ID" = ? AND "Part""Name" = ?`
			var key = []driver.Value{int64(7), "é"}
			if mode != "CollectionKey" {
				from, key = `"DB2INST1"."ORDERS" AS T WHERE RID(T) = ?`, []driver.Value{int64(123)}
			}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT SUBSTRING("CONTENT", 1, 4096, CODEUNITS32) FROM `+from) + "$").
				WithArgs(key...).WillReturnRows(sqlmock.NewRows([]string{"PIECE"}).AddRow("changed"))
			var emitted int
			var complete, resume, err = db.ScanTableChunk(context.Background(), info, state, "", func(event sqlcapture.ChangeEvent) error {
				emitted++
				require.Equal(t, "changed", event.(*sqlcapture.OldChangeEvent).After["CONTENT"])
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 1, emitted)
			require.True(t, complete)
			require.Equal(t, cursor, resume)
		})
	}
}

func TestBackfillKeylessLOBBoundary(t *testing.T) {
	for _, mode := range []string{"WithoutKey", "NonUniqueCollectionKey"} {
		t.Run(mode, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var info = testOrdersInfo()
			info.PrimaryKey, info.ColumnNames = nil, []string{"ID", "CONTENT"}
			info.Columns["CONTENT"] = sqlcapture.ColumnInfo{Name: "CONTENT", DataType: db2ColumnType{Name: "CLOB"}}
			var state = &sqlcapture.TableState{}
			if mode == "NonUniqueCollectionKey" {
				state.Mode, state.KeyColumns = sqlcapture.TableStateKeylessBackfill, []string{"ID"}
			}
			var contents = []string{strings.Repeat("a", 65535), strings.Repeat("b", 65535)}
			var rows = sqlmock.NewRows([]string{"ID", "CONTENT", "LENGTH", "RID"})
			for i, content := range contents {
				var value = decodeTestLOB(t, []byte(content), "CLOB")
				require.Nil(t, value)
				rows.AddRow(7, value, len(content), 123+i)
			}
			mock.ExpectQuery(`FROM "DB2INST1"\."ORDERS" AS T ORDER BY RID\(T\) FETCH FIRST 50000 ROWS ONLY$`).
				WillReturnRows(rows).RowsWillBeClosed()
			for i, content := range contents {
				for start := 0; start < len(content); start += 4096 {
					var count = min(4096, len(content)-start)
					var query = fmt.Sprintf(`SELECT SUBSTRING("CONTENT", %d, %d, CODEUNITS32) FROM "DB2INST1"."ORDERS" AS T WHERE RID(T) = ?`, start+1, count)
					mock.ExpectQuery(regexp.QuoteMeta(query) + "$").WithArgs(int64(123 + i)).
						WillReturnRows(sqlmock.NewRows([]string{"PIECE"}).AddRow(content[start : start+count])).RowsWillBeClosed()
				}
			}
			var emitted []string
			var complete, cursor, err = db.ScanTableChunk(context.Background(), info, state, "", func(event sqlcapture.ChangeEvent) error {
				require.Equal(t, tuple.Tuple{123 + len(emitted)}.Pack(), event.GetRowKey())
				emitted = append(emitted, event.(*sqlcapture.OldChangeEvent).After["CONTENT"].(string))
				return nil
			})
			require.NoError(t, err)
			require.True(t, complete)
			require.Equal(t, tuple.Tuple{124}.Pack(), cursor)
			require.Equal(t, contents, emitted)
		})
	}
}

func TestBackfillLOBChangesBetweenPieces(t *testing.T) {
	for _, change := range []string{"Deleted", "Null", "Empty", "Shorter"} {
		t.Run(change, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var from = `"DB2INST1"."ORDERS" WHERE "ID" = ?`
			var prefix = strings.Repeat("a", 4096)
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT SUBSTRING("CONTENT", 1, 4096, CODEUNITS32) FROM ` + from)).
				WithArgs(7).WillReturnRows(sqlmock.NewRows([]string{"PIECE"}).AddRow(prefix))
			var rows = sqlmock.NewRows([]string{"PIECE"})
			var want any
			switch change {
			case "Null":
				rows.AddRow(nil)
			case "Empty":
				rows.AddRow("")
				want = []byte(prefix)
			case "Shorter":
				rows.AddRow("é")
				want = []byte(prefix + "é")
			}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT SUBSTRING("CONTENT", 4097, 4096, CODEUNITS32) FROM ` + from)).
				WithArgs(7).WillReturnRows(rows)
			var columns = []db2LOBColumn{{Name: "CONTENT", Index: 0, Type: db2ColumnType{Name: "CLOB"}}}
			var lengths = []sql.NullInt64{{Int64: 65535, Valid: true}}
			var values = []any{nil}
			require.NoError(t, recoverLOBs(context.Background(), db.conn, from, []any{7}, columns, lengths, values, true))
			require.Equal(t, want, values[0])
		})
	}
}

func TestReplicationLOBBoundaries(t *testing.T) {
	for _, name := range []string{"CLOB", "BLOB", "DBCLOB", "XML"} {
		t.Run(name, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var rs = testReplicationStream(t, db, "01")
			var info = testOrdersInfo()
			info.ColumnNames = []string{"ID", "CONTENT"}
			info.Columns["CONTENT"] = sqlcapture.ColumnInfo{Name: "CONTENT", DataType: db2ColumnType{Name: name}}
			var columns = append(append([]string{}, cdMetadataColumns...), "ID", "CONTENT", "XID", "XCONTENT")
			expectCDInspection(mock, info, columns, map[string]string{"CONTENT": name, "XCONTENT": name})
			require.NoError(t, rs.ActivateTable(context.Background(), sqlcapture.JoinStreamID(info.Schema, info.Name), []string{"ID"}, info, nil))
			expectFrontier(mock, []byte{2})
			var after, before = strings.Repeat("a", 65535), strings.Repeat("b", 131071)
			var lengths = `LENGTH\(.*`
			if name == "XML" {
				after, before = "<x>"+after[7:]+"</x>", "<x>"+before[7:]+"</x>"
				lengths = regexp.QuoteMeta(`LENGTH(XMLSERIALIZE("CONTENT" AS CLOB(2G)), CODEUNITS32) AS "__FLOW_LOB_LENGTH_4", LENGTH(XMLSERIALIZE("XCONTENT" AS CLOB(2G)), CODEUNITS32) AS "__FLOW_LOB_LENGTH_6"`)
			}
			mock.ExpectQuery(`SELECT .*`+lengths+` FROM "DB2INST1"\."CDORDERS" WHERE .* FETCH FIRST 1000 ROWS ONLY$`).WithArgs([]byte{1}, []byte{2}).
				WillReturnRows(sqlmock.NewRows(append(columns, "__FLOW_LOB_LENGTH_4", "__FLOW_LOB_LENGTH_6")).
					AddRow([]byte{2}, []byte{1}, "U", 7, decodeTestLOB(t, []byte(after), name), 7, decodeTestLOB(t, []byte(before), name), len(after), len(before))).RowsWillBeClosed()
			for i, data := range []string{after, before} {
				var column = []string{"CONTENT", "XCONTENT"}[i]
				var expr, suffix = quoteIdentifier(column), ", CODEUNITS32"
				if name == "BLOB" {
					suffix = ""
				} else if name == "DBCLOB" {
					expr = "CLOB(" + expr + ")"
				} else if name == "XML" {
					expr = "XMLSERIALIZE(" + expr + " AS CLOB(2G))"
				}
				var pieceType = "CLOB"
				if name == "BLOB" {
					pieceType = "BLOB"
				}
				for start := 0; start < len(data); start += 4096 {
					var count = min(4096, len(data)-start)
					var query = fmt.Sprintf(`SELECT SUBSTRING(%s, %d, %d%s) FROM "DB2INST1"."CDORDERS" WHERE IBMSNAP_COMMITSEQ = ? AND IBMSNAP_INTENTSEQ = ?`, expr, start+1, count, suffix)
					mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs([]byte{2}, []byte{1}).
						WillReturnRows(sqlmock.NewRows([]string{"PIECE"}).AddRow(decodeTestLOB(t, []byte(data[start:start+count]), pieceType)))
				}
			}
			var changes int
			require.NoError(t, rs.pollChanges(context.Background(), func(event sqlcapture.DatabaseEvent) error {
				if change, ok := event.(*sqlcapture.OldChangeEvent); ok {
					changes++
					require.Equal(t, sqlcapture.UpdateOp, change.Operation)
					var data, err = change.AppendJSON(nil)
					require.NoError(t, err)
					var doc map[string]any
					require.NoError(t, json.Unmarshal(data, &doc))
					if name == "BLOB" {
						require.Equal(t, []byte(before), change.Before["CONTENT"])
						require.Equal(t, base64.StdEncoding.EncodeToString([]byte(after)), doc["CONTENT"])
					} else {
						require.Equal(t, before, change.Before["CONTENT"])
						require.Equal(t, after, doc["CONTENT"])
					}
				}
				return nil
			}))
			require.Equal(t, 1, changes)
			require.Equal(t, []byte{2}, rs.fromSeq)
		})
	}
}

func TestLOBRecoveryChangesAndFailures(t *testing.T) {
	for _, path := range []string{"Backfill", "CDC"} {
		for _, failure := range []string{"QueryError", "MissingRow", "NullPiece", "ShortPiece"} {
			t.Run(path+"/"+failure, func(t *testing.T) {
				var db, mock = testDatabase(t)
				var info = testOrdersInfo()
				info.ColumnNames = []string{"ID", "CONTENT"}
				info.Columns["CONTENT"] = sqlcapture.ColumnInfo{Name: "CONTENT", DataType: db2ColumnType{Name: "CLOB"}}
				var rs *db2ReplicationStream
				if path == "CDC" {
					rs = testReplicationStream(t, db, "01")
					var columns = append(append([]string{}, cdMetadataColumns...), "ID", "CONTENT", "XID")
					expectCDInspection(mock, info, columns, map[string]string{"CONTENT": "CLOB"})
					require.NoError(t, rs.ActivateTable(context.Background(), sqlcapture.JoinStreamID(info.Schema, info.Name), []string{"ID"}, info, nil))
					expectFrontier(mock, []byte{2})
					mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE`).WillReturnRows(sqlmock.NewRows(append(columns, "__FLOW_LOB_LENGTH_4")).
						AddRow([]byte{2}, []byte{1}, "I", 7, nil, nil, 65535)).RowsWillBeClosed()
				} else {
					mock.ExpectQuery(`FROM "DB2INST1"\."ORDERS" AS T`).WillReturnRows(sqlmock.NewRows([]string{"ID", "CONTENT", "LENGTH"}).
						AddRow(7, nil, 65535)).RowsWillBeClosed()
				}
				var query = mock.ExpectQuery(`SELECT SUBSTRING\("CONTENT", 1, 4096, CODEUNITS32\)`)
				switch failure {
				case "QueryError":
					query.WillReturnError(fmt.Errorf("SQL0551N not authorized"))
				case "MissingRow":
					query.WillReturnRows(sqlmock.NewRows([]string{"PIECE"}))
				case "NullPiece":
					query.WillReturnRows(sqlmock.NewRows([]string{"PIECE"}).AddRow(nil))
				case "ShortPiece":
					query.WillReturnRows(sqlmock.NewRows([]string{"PIECE"}).AddRow("short"))
				}
				var emitted int
				var err error
				if path == "CDC" {
					err = rs.pollChanges(context.Background(), func(sqlcapture.DatabaseEvent) error { emitted++; return nil })
					require.Equal(t, []byte{1}, rs.fromSeq)
				} else {
					var complete bool
					var cursor []byte
					var content any
					complete, cursor, err = db.ScanTableChunk(context.Background(), info, &sqlcapture.TableState{KeyColumns: []string{"ID"}}, "", func(event sqlcapture.ChangeEvent) error {
						emitted++
						content = event.(*sqlcapture.OldChangeEvent).After["CONTENT"]
						return nil
					})
					if failure != "QueryError" {
						require.NoError(t, err)
						require.True(t, complete)
						require.Equal(t, tuple.Tuple{7}.Pack(), cursor)
						require.Equal(t, 1, emitted)
						if failure == "ShortPiece" {
							require.Equal(t, "short", content)
						} else {
							require.Nil(t, content)
						}
						return
					}
					require.False(t, complete)
					require.Nil(t, cursor)
				}
				require.Zero(t, emitted)
				require.ErrorContains(t, err, `non-null LOB column "CONTENT"`)
				require.ErrorContains(t, err, "ORDERS")
				require.ErrorContains(t, err, "key")
			})
		}
	}
}
