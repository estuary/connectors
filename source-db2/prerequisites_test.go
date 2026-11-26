package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/estuary/connectors/sqlcapture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestNullableEffectiveKey(t *testing.T) {
	for _, path := range []string{"Validation", "Activation"} {
		t.Run(path, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var info = testOrdersInfo()
			var key = []string{"PRICE", "ID"}
			var err error
			if path == "Validation" {
				require.NoError(t, db.setCapture(&pf.CaptureSpec{Bindings: []*pf.CaptureSpec_Binding{{
					ResourceConfigJson: json.RawMessage(`{"namespace":"DB2INST1","stream":"ORDERS"}`),
					Collection:         pf.CollectionSpec{Key: []string{"/PRICE", "/ID"}},
				}}}))
				mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).WillReturnRows(sqlmock.NewRows(tableDetailColumns).
					AddRow(info.Schema, info.Name, "T", "Y", "DB2INST1", "CDORDERS", "A", "N", "X"))
				mock.ExpectQuery(`FROM SYSCAT.COLUMNS C LEFT JOIN`).WillReturnRows(sqlmock.NewRows(columnDetailColumns).
					AddRow(info.Schema, info.Name, "ID", 0, "N", "INTEGER", 4, 0, 0, nil).
					AddRow(info.Schema, info.Name, "PRICE", 1, "Y", "DECIMAL", 10, 2, 0, nil))
				mock.ExpectQuery(`JOIN SYSCAT.TABCONST`).WillReturnRows(sqlmock.NewRows(primaryKeyColumns).
					AddRow(info.Schema, info.Name, "ID", 1))
				var table = sqlcapture.TableID{Schema: info.Schema, Table: info.Name}
				err = db.SetupTablePrerequisites(context.Background(), []sqlcapture.TableID{table})[table]
			} else {
				var rs = testReplicationStream(t, db, "01")
				err = rs.ActivateTable(context.Background(), sqlcapture.JoinStreamID(info.Schema, info.Name), key, info, nil)
				require.Empty(t, rs.tables)
			}
			require.ErrorContains(t, err, `key column "PRICE"`)
			require.ErrorContains(t, err, "DB2INST1")
			require.ErrorContains(t, err, "ORDERS")
			require.ErrorContains(t, err, "NOT NULL")
		})
	}
}

func TestArchiveLoggingPrerequisite(t *testing.T) {
	for _, tc := range []struct {
		method string
		valid  bool
	}{
		{"OFF", false},
		{"OFF   ", false},
		{"DISK:/archive", true},
		{"LOGRETAIN", true},
	} {
		t.Run(tc.method, func(t *testing.T) {
			var db, mock = testDatabase(t)
			db.config.Database = "TESTDB"
			mock.ExpectQuery(`SELECT VALUE FROM SYSIBMADM.DBCFG WHERE NAME = 'logarchmeth1'`).
				WillReturnRows(sqlmock.NewRows([]string{"VALUE"}).AddRow(tc.method))
			var err = db.prerequisiteArchiveLogging(context.Background())
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "UPDATE DB CFG FOR TESTDB USING LOGARCHMETH1")
				require.ErrorContains(t, err, "BACKUP DB TESTDB")
			}
		})
	}
}

func TestTableRegistrationPrerequisite(t *testing.T) {
	var cases = []struct {
		name         string
		dataCapture  bool
		registration *captureRegistration
		error        string
	}{
		{"NoDataCapture", false, nil, "ALTER TABLE"},
		{"NoRegistration", true, nil, "CREATE REGISTRATION"},
		{"Inactive", true, &captureRegistration{State: "I"}, "SET STATE = 'A'"},
		{"Active", true, &captureRegistration{State: "A"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var db, _ = testDatabase(t)
			var err = db.checkTableRegistration(&sqlcapture.DiscoveryInfo{
				Schema: "DB2INST1", Name: "ORDERS", BaseTable: true,
				ExtraDetails: &db2TableDiscoveryDetails{DataCaptureChanges: tc.dataCapture, Registration: tc.registration},
			})
			if tc.error == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.error)
			}
		})
	}
}

func TestOldKeySafety(t *testing.T) {
	var cases = []struct {
		name    string
		pairs   bool
		prefix  string
		columns []string
		valid   bool
	}{
		{"DeleteInsert", true, "", []string{"ORDER_ID", "LINE_NO"}, true},
		{"BeforeImages", false, "X", []string{"ORDER_ID", "LINE_NO", "XORDER_ID", "XLINE_NO"}, true},
		{"MissingOneKey", false, "X", []string{"ORDER_ID", "LINE_NO", "XORDER_ID"}, false},
		{"NoPrefix", false, "", []string{"ORDER_ID", "LINE_NO"}, false},
		{"CaseMismatch", false, "X", []string{"ORDER_ID", "LINE_NO", "Xorder_id", "Xline_no"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var info = &sqlcapture.DiscoveryInfo{
				Schema: "DB2INST1", Name: "LINES", PrimaryKey: []string{"ORDER_ID", "LINE_NO"},
				ExtraDetails: &db2TableDiscoveryDetails{Registration: &captureRegistration{
					CDTable: "CDLINES", UpdateAsDeleteInsert: tc.pairs, BeforeImagePrefix: tc.prefix,
				}},
			}
			var err = checkOldKeyColumns(info, info.PrimaryKey, tc.columns)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "cannot safely capture key changes")
				require.ErrorContains(t, err, "UPDATE AS DELETE INSERT ON")
			}
		})
	}
}

func TestWatermarksPrerequisite(t *testing.T) {
	t.Run("Writable", func(t *testing.T) {
		var db, mock = testDatabase(t)
		mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).WithArgs("DB2INST1", "FLOW_WATERMARKS").
			WillReturnRows(sqlmock.NewRows(tableDetailColumns).
				AddRow("DB2INST1", "FLOW_WATERMARKS", "T", "Y", "DB2INST1", "CDFLOW_WATERMARKS", "A", "N", ""))
		expectCDInspection(mock, testWatermarksInfo(), append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK"), nil)
		mock.ExpectExec(`UPDATE "DB2INST1"\."FLOW_WATERMARKS" SET WATERMARK = WATERMARK WHERE 1 = 0`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`INSERT INTO "DB2INST1"\."FLOW_WATERMARKS" .* FROM SYSIBM.SYSDUMMY1 WHERE 1 = 0`).WillReturnResult(sqlmock.NewResult(0, 0))
		require.NoError(t, db.prerequisiteWatermarksTable(context.Background()))
	})
	t.Run("Missing", func(t *testing.T) {
		var db, mock = testDatabase(t)
		mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).WithArgs("DB2INST1", "FLOW_WATERMARKS").WillReturnRows(sqlmock.NewRows(tableDetailColumns))
		require.ErrorContains(t, db.prerequisiteWatermarksTable(context.Background()), `CREATE TABLE "DB2INST1"."FLOW_WATERMARKS"`)
	})
	t.Run("ReadOnly", func(t *testing.T) {
		var db, mock = testDatabase(t)
		mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).WithArgs("DB2INST1", "FLOW_WATERMARKS").
			WillReturnRows(sqlmock.NewRows(tableDetailColumns).
				AddRow("DB2INST1", "FLOW_WATERMARKS", "T", "Y", "DB2INST1", "CDFLOW_WATERMARKS", "A", "N", ""))
		expectCDInspection(mock, testWatermarksInfo(), append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK"), nil)
		mock.ExpectExec(`UPDATE .* WHERE 1 = 0`).WillReturnError(fmt.Errorf("SQL0551N not authorized"))
		require.ErrorContains(t, db.prerequisiteWatermarksTable(context.Background()), "GRANT SELECT, INSERT, UPDATE")
	})
}

func TestCDColumnComparison(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		current, added, removed []string
	}{
		{"Unchanged", []string{"ID", "NAME"}, nil, nil},
		{"Reordered", []string{"NAME", "ID"}, nil, nil},
		{"Added", []string{"ID", "NAME", "QTY"}, []string{"QTY"}, nil},
		{"Removed", []string{"ID"}, nil, []string{"NAME"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var added, removed = compareCDColumns([]string{"ID", "NAME"}, tc.current)
			require.Equal(t, tc.added, added)
			require.Equal(t, tc.removed, removed)
		})
	}
}

func TestInspectionBlocksUnusableKeys(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     []string
		columns []string
		types   map[string]string
		failure string
	}{
		{"MissingKey", []string{"ID"}, nil, nil, `required column "ID"`},
		{"MissingBeforeImage", []string{"ID", "NAME"}, []string{"ID", "NAME", "XID"}, nil, `missing before-image column for "NAME"`},
		{"LOBFlagKey", []string{"NOTE"}, []string{"NOTE", "XNOTE"}, map[string]string{"NOTE": "CHAR", "XNOTE": "CHAR"}, `required column "NOTE"`},
		{"LOBFlagBeforeImage", []string{"NOTE"}, []string{"NOTE", "XNOTE"}, map[string]string{"NOTE": "CLOB", "XNOTE": "CHAR"}, `missing before-image column for "NOTE"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var info = testOrdersInfo()
			info.Columns["NOTE"] = sqlcapture.ColumnInfo{Name: "NOTE", DataType: db2ColumnType{Name: "CLOB"}}
			var name = info.Columns["NAME"]
			name.IsNullable = false
			info.Columns["NAME"] = name
			expectCDCatalog(mock, info, append(append([]string{}, cdMetadataColumns...), tc.columns...), tc.types)
			var _, err = db.inspectTable(context.Background(), info, tc.key)
			require.ErrorContains(t, err, tc.failure)
		})
	}
}

func TestWatermarksCDAccess(t *testing.T) {
	var db, mock = testDatabase(t)
	mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).WithArgs("DB2INST1", "FLOW_WATERMARKS").
		WillReturnRows(sqlmock.NewRows(tableDetailColumns).AddRow("DB2INST1", "FLOW_WATERMARKS", "T", "Y", "DB2INST1", "CDFLOW_WATERMARKS", "A", "N", ""))
	expectCDCatalog(mock, testWatermarksInfo(), append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK"), nil)
	mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE 1 = 0`).WillReturnError(fmt.Errorf("SQL0551N not authorized"))
	require.ErrorContains(t, db.prerequisiteWatermarksTable(context.Background()), `GRANT SELECT ON TABLE "DB2INST1"."CDFLOW_WATERMARKS"`)
}

func TestAddedCDColumns(t *testing.T) {
	for _, cast := range []bool{false, true} {
		t.Run(fmt.Sprint(cast), func(t *testing.T) {
			var db, mock = testDatabase(t)
			var rs = testReplicationStream(t, db, "01")
			var info = testOrdersInfo()
			var columns = append(append([]string{}, cdMetadataColumns...), "ID", "XID")
			if cast {
				columns = append(columns, "CREATED", "XCREATED")
			}
			activateTestTable(t, rs, mock, info, columns)
			var table = rs.tables[sqlcapture.JoinStreamID(info.Schema, info.Name)]
			var projection = table.CDProjection
			require.NotEqual(t, "*", projection)
			db.registrationChecksAfter = time.Time{}
			mock.ExpectQuery(`SELECT STATE FROM`).WillReturnRows(sqlmock.NewRows([]string{"STATE"}).AddRow("A"))
			expectCDCatalog(mock, info, append(append([]string{}, columns...), "NEW_COLUMN"), nil)
			require.NoError(t, db.PeriodicChecks(context.Background()))
			require.Contains(t, table.CatalogColumns, "NEW_COLUMN")
			require.Equal(t, columns, table.CDColumns)
			require.Equal(t, projection, table.CDProjection)
			expectFrontier(mock, []byte{2})
			mock.ExpectQuery(regexp.QuoteMeta("SELECT " + projection + ` FROM "DB2INST1"."CDORDERS" WHERE`)).
				WillReturnRows(sqlmock.NewRows(columns))
			require.NoError(t, rs.pollChanges(context.Background(), func(sqlcapture.DatabaseEvent) error { return nil }))
			require.Equal(t, []byte{2}, rs.fromSeq)
		})
	}
}
