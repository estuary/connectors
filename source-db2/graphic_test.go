package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	go_ibm_db "github.com/ibmdb/go_ibm_db"
	"github.com/ibmdb/go_ibm_db/api"
	"github.com/stretchr/testify/require"
)

func TestGraphicStrings(t *testing.T) {
	for _, name := range []string{"GRAPHIC", "VARGRAPHIC", "LONG VARGRAPHIC"} {
		for _, tc := range []struct {
			name  string
			value any
		}{
			{"Empty", ""},
			{"EmbeddedNUL", "A\x00B"},
			{"Unicode", "é漢🌍"},
			{"Padding", "A   "},
			{"Null", nil},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				var info = testOrdersInfo()
				info.ColumnNames, info.PrimaryKey = []string{"CONTENT"}, []string{"CONTENT"}
				info.Columns = map[string]sqlcapture.ColumnInfo{
					"CONTENT": {Name: "CONTENT", DataType: db2ColumnType{Name: name, Codepage: 1200}, IsNullable: tc.value == nil},
				}
				// CLOB projections use the actual driver's length-aware SQL_C_CHAR path.
				var column = go_ibm_db.NewVariableWidthColumn(&go_ibm_db.BaseColumn{SType: api.SQL_CLOB}, api.SQL_C_CHAR, 100).(*go_ibm_db.BindableColumn)
				column.IsBound = true
				if tc.value == nil {
					column.Len = api.SQL_NULL_DATA
				} else {
					column.Len = go_ibm_db.BufferLen(copy(column.Buffer, tc.value.(string)))
				}
				var handle api.SQLHSTMT
				var value, err = column.Value(handle, 0)
				require.NoError(t, err)
				var keyValue = tc.value
				var extraProjection string
				var row = []driver.Value{value}
				var cdRow = []driver.Value{[]byte{2}, []byte{1}, "U", value, value}
				if tc.value == nil {
					info.PrimaryKey = []string{"ID"}
					info.ColumnNames = append(info.ColumnNames, "ID")
					info.Columns["ID"] = sqlcapture.ColumnInfo{Name: "ID", DataType: db2ColumnType{Name: "INTEGER"}}
					keyValue, extraProjection = int64(1), `, "ID"`
					row = append(row, int64(1))
					cdRow = append(cdRow, int64(1), int64(1))
				}
				var checkDocument = func(t *testing.T, event sqlcapture.ChangeEvent) {
					t.Helper()
					var data, err = event.AppendJSON(nil)
					require.NoError(t, err)
					var document map[string]any
					require.NoError(t, json.Unmarshal(data, &document))
					require.Contains(t, document, "CONTENT")
					require.Equal(t, tc.value, document["CONTENT"])
					require.Equal(t, tuple.Tuple{keyValue}.Pack(), event.GetRowKey())
				}
				t.Run("Backfill", func(t *testing.T) {
					var db, mock = testDatabase(t)
					mock.ExpectQuery(regexp.QuoteMeta(`SELECT CLOB("CONTENT") AS "CONTENT"` + extraProjection + ` FROM "DB2INST1"."ORDERS" AS T`)).
						WillReturnRows(sqlmock.NewRows(info.ColumnNames).AddRow(row...))
					var changes int
					var complete, _, err = db.ScanTableChunk(context.Background(), info, &sqlcapture.TableState{KeyColumns: info.PrimaryKey}, "", func(event sqlcapture.ChangeEvent) error {
						changes++
						checkDocument(t, event)
						return nil
					})
					require.NoError(t, err)
					require.True(t, complete)
					require.Equal(t, 1, changes)
				})
				t.Run("CDC", func(t *testing.T) {
					var db, mock = testDatabase(t)
					var rs = testReplicationStream(t, db, "01")
					var columns = append(append([]string{}, cdMetadataColumns...), "CONTENT", "XCONTENT")
					var extraProjection string
					if tc.value == nil {
						columns = append(columns, "ID", "XID")
						extraProjection = `, "ID", "XID"`
					}
					activateTestTable(t, rs, mock, info, columns)
					expectFrontier(mock, []byte{2})
					mock.ExpectQuery(regexp.QuoteMeta(`SELECT "IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", CLOB("CONTENT") AS "CONTENT", CLOB("XCONTENT") AS "XCONTENT"`+extraProjection+` FROM "DB2INST1"."CDORDERS" WHERE`)).
						WithArgs([]byte{1}, []byte{2}).WillReturnRows(sqlmock.NewRows(columns).
						AddRow(cdRow...))
					var changes int
					require.NoError(t, rs.pollChanges(context.Background(), func(event sqlcapture.DatabaseEvent) error {
						if change, ok := event.(*sqlcapture.OldChangeEvent); ok {
							changes++
							require.Equal(t, sqlcapture.UpdateOp, change.Operation)
							require.Contains(t, change.Before, "CONTENT")
							require.Equal(t, tc.value, change.Before["CONTENT"])
							checkDocument(t, change)
						}
						return nil
					}))
					require.Equal(t, 1, changes)
				})
			})
		}
	}
}
