package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/estuary/connectors/go/common"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/stretchr/testify/require"
)

func testDatabase(t *testing.T) (*db2Database, sqlmock.Sqlmock) {
	t.Helper()
	var conn, mock, err = sqlmock.New()
	require.NoError(t, err)
	var lowercase = sqlcapture.LowercaseStreamIDs
	sqlcapture.LowercaseStreamIDs = false
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		conn.Close()
		sqlcapture.LowercaseStreamIDs = lowercase
	})
	var cfg = &Config{User: "db2inst1"}
	cfg.SetDefaults()
	return &db2Database{
		config: cfg,
		now:    time.Now,
		conn:   conn,
		featureFlags: map[string]bool{
			"use_schema_inference": true,
			"emit_sourced_schemas": true,
		},
	}, mock
}

var tableDetailColumns = []string{"TABSCHEMA", "TABNAME", "TYPE", "DATACAPTURE", "CD_OWNER", "CD_TABLE", "STATE", "CHG_UPD_TO_DEL_INS", "BEFORE_IMG_PREFIX"}
var columnDetailColumns = []string{"TABSCHEMA", "TABNAME", "COLNAME", "COLNO", "NULLS", "TYPENAME", "LENGTH", "SCALE", "CODEPAGE", "SOURCENAME"}
var primaryKeyColumns = []string{"TABSCHEMA", "TABNAME", "COLNAME", "COLSEQ"}

func TestDiscoverCatalog(t *testing.T) {
	var db, mock = testDatabase(t)
	mock.ExpectQuery(`FROM SYSCAT.TABLES T JOIN "ASN"\."IBMSNAP_REGISTER" R .* WHERE T.TYPE IN \('T', 'U'\) AND R.STATE = 'A' AND NOT EXISTS`).
		WillReturnRows(sqlmock.NewRows([]string{"TABSCHEMA", "TABNAME"}).
			AddRow("ASN", "IBMSNAP_REGISTER").
			AddRow("DB2INST1", "FLOW_WATERMARKS").
			AddRow("DB2INST1", "LINES").
			AddRow("DB2INST1", "ORDERS").
			AddRow("SYSCAT", "TABLES"))
	var args = []driver.Value{"DB2INST1", "LINES", "DB2INST1", "ORDERS", "DB2INST1", "FLOW_WATERMARKS"}
	mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN "ASN"\."IBMSNAP_REGISTER"`).WithArgs(args...).
		WillReturnRows(sqlmock.NewRows(tableDetailColumns).
			AddRow("DB2INST1", "LINES", "T", "Y", "DB2INST1", "CDLINES", "A", "Y", nil).
			AddRow("DB2INST1", "ORDERS", "T", "Y", "DB2INST1   ", "CDORDERS   ", "A", "N", "X ").
			AddRow("DB2INST1", "FLOW_WATERMARKS", "T", "Y", "DB2INST1", "CDFLOW_WATERMARKS", "A", "N", ""))
	mock.ExpectQuery(`FROM SYSCAT.COLUMNS C LEFT JOIN SYSCAT.DATATYPES D .* WHERE`).WithArgs(args...).
		WillReturnRows(sqlmock.NewRows(columnDetailColumns).
			AddRow("DB2INST1", "FLOW_WATERMARKS", "SLOT", 0, "N", "VARCHAR", 1000, 0, 1208, nil).
			AddRow("DB2INST1", "FLOW_WATERMARKS", "WATERMARK", 1, "Y", "VARCHAR", 1000, 0, 1208, nil).
			AddRow("DB2INST1", "LINES", "ORDER_ID", 0, "N", "INTEGER", 4, 0, 0, nil).
			AddRow("DB2INST1", "LINES", "LINE_NO", 1, "N", "SMALLINT", 2, 0, 0, nil).
			AddRow("DB2INST1", "LINES", "SKU", 2, "Y", "CHARACTER", 8, 0, 1208, nil).
			AddRow("DB2INST1", "LINES", "QTY", 3, "Y", "INTEGER", 4, 0, 0, nil).
			AddRow("DB2INST1", "ORDERS", "ID", 0, "N", "INTEGER", 4, 0, 0, nil).
			AddRow("DB2INST1", "ORDERS", "NAME", 1, "Y", "VARCHAR", 50, 0, 1208, nil).
			AddRow("DB2INST1", "ORDERS", "QTY", 2, "Y", "INTEGER", 4, 0, 0, nil).
			AddRow("DB2INST1", "ORDERS", "PRICE", 3, "Y", "DECIMAL", 10, 2, 0, nil).
			AddRow("DB2INST1", "ORDERS", "CREATED", 4, "Y", "TIMESTAMP", 10, 6, 0, nil).
			AddRow("DB2INST1", "ORDERS", "NOTE", 5, "Y", "VARCHAR", 200, 0, 1208, nil))
	mock.ExpectQuery(`JOIN SYSCAT.TABCONST C ON C.TABSCHEMA = K.TABSCHEMA AND C.TABNAME = K.TABNAME AND C.CONSTNAME = K.CONSTNAME WHERE C.TYPE = 'P'`).WithArgs(args...).
		WillReturnRows(sqlmock.NewRows(primaryKeyColumns).
			AddRow("DB2INST1", "FLOW_WATERMARKS", "SLOT", 1).
			AddRow("DB2INST1", "LINES", "ORDER_ID", 1).
			AddRow("DB2INST1", "LINES", "LINE_NO", 2).
			AddRow("DB2INST1", "ORDERS", "ID", 1))

	var bindings, err = sqlcapture.DiscoverCatalog(context.Background(), db)
	require.NoError(t, err)
	require.Len(t, bindings, 2)
	var keys = make(map[string][]string)
	for _, binding := range bindings {
		var resource sqlcapture.Resource
		require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &resource))
		require.Equal(t, "DB2INST1", resource.Namespace)
		keys[resource.Stream] = binding.Key
		var schema struct {
			Definitions map[string]struct {
				Properties map[string]map[string]any `json:"properties"`
			} `json:"$defs"`
		}
		require.NoError(t, json.Unmarshal(binding.DocumentSchemaJson, &schema))
		require.Contains(t, schema.Definitions, resource.Namespace+resource.Stream)
		var columns = schema.Definitions[resource.Namespace+resource.Stream].Properties
		require.Contains(t, columns, "QTY")
		require.NotContains(t, columns, "qty")
		require.NotContains(t, columns, "IBMSNAP_COMMITSEQ")
		if resource.Stream == "ORDERS" {
			require.JSONEq(t, `{"namespace":"DB2INST1","stream":"ORDERS"}`, string(binding.ResourceConfigJson))
			require.Equal(t, "integer", columns["ID"]["type"])
			require.ElementsMatch(t, []any{"string", "null"}, columns["PRICE"]["type"])
			require.Equal(t, "number", columns["PRICE"]["format"])
			require.Equal(t, "date-time", columns["CREATED"]["format"])
			require.Contains(t, columns, "NOTE")
			require.NotContains(t, columns, "XID")
		}
	}
	require.Equal(t, map[string][]string{"ORDERS": {"/ID"}, "LINES": {"/ORDER_ID", "/LINE_NO"}}, keys)
}

func TestCaseSensitiveDiscoverCatalog(t *testing.T) {
	var lowercaseStreams, lowercaseNames = sqlcapture.LowercaseStreamIDs, sqlcapture.LowercaseRecommendedNames
	t.Cleanup(func() {
		sqlcapture.LowercaseStreamIDs, sqlcapture.LowercaseRecommendedNames = lowercaseStreams, lowercaseNames
	})
	sqlcapture.LowercaseStreamIDs, sqlcapture.LowercaseRecommendedNames = true, true
	// Apply the connector's naming settings, then fail before opening a connection.
	var ctx, cancel = context.WithCancel(context.Background())
	cancel()
	var _, err = connectDB2(ctx, "test", json.RawMessage(`{"address":"localhost:50000","database":"TESTDB","user":"db2inst1","password":"test"}`), common.CreatedAt{})
	require.ErrorIs(t, err, context.Canceled)
	var db, mock = testDatabase(t)
	mock.ExpectQuery(`FROM SYSCAT.TABLES T JOIN`).WillReturnRows(sqlmock.NewRows([]string{"TABSCHEMA", "TABNAME"}).
		AddRow("Mixed", "Orders").AddRow("Mixed", "ORDERS"))
	var args = []driver.Value{"Mixed", "Orders", "Mixed", "ORDERS", "DB2INST1", "FLOW_WATERMARKS"}
	mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).WithArgs(args...).WillReturnRows(sqlmock.NewRows(tableDetailColumns).
		AddRow("Mixed", "Orders", "T", "Y", "Mixed", "CDOrders", "A", "N", "X").
		AddRow("Mixed", "ORDERS", "T", "Y", "Mixed", "CDORDERS", "A", "Y", nil))
	mock.ExpectQuery(`FROM SYSCAT.COLUMNS C LEFT JOIN`).WithArgs(args...).WillReturnRows(sqlmock.NewRows(columnDetailColumns).
		AddRow("Mixed", "Orders", "Id", 0, "N", "VARCHAR", 100, 0, 1208, nil).
		AddRow("Mixed", "ORDERS", "ID", 0, "N", "INTEGER", 4, 0, 0, nil))
	mock.ExpectQuery(`FROM SYSCAT.KEYCOLUSE`).WithArgs(args...).WillReturnRows(sqlmock.NewRows(primaryKeyColumns).
		AddRow("Mixed", "Orders", "Id", 1).AddRow("Mixed", "ORDERS", "ID", 1))
	var bindings, discoverErr = sqlcapture.DiscoverCatalog(context.Background(), db)
	require.NoError(t, discoverErr)
	require.Len(t, bindings, 2)
	var names = make(map[string]string)
	for _, binding := range bindings {
		var resource sqlcapture.Resource
		require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &resource))
		require.Equal(t, "Mixed", resource.Namespace)
		names[resource.Stream] = binding.RecommendedName
	}
	require.Equal(t, map[string]string{"Orders": "Mixed/Orders", "ORDERS": "Mixed/ORDERS"}, names)
}

func TestDiscoveryFilters(t *testing.T) {
	var db, _ = testDatabase(t)
	var cases = []struct {
		schema, table string
		visible       bool
	}{
		{"DB2INST1", "ORDERS", true},
		{"DB2INST1", "LINES", true},
		{"DB2INST1", "FLOW_WATERMARKS", false},
		{"OTHER", "FLOW_WATERMARKS", true},
		{"ASN", "TABLE", false},
		{"SYSIBM", "TABLE", false},
		{"SYSSTAT", "TABLE", false},
		{"SYSCAT", "TABLE", false},
		{"SQLJ", "TABLE", false},
	}
	for _, tc := range cases {
		t.Run(tc.schema+"."+tc.table, func(t *testing.T) {
			require.Equal(t, tc.visible, db.discoverableTable(sqlcapture.TableID{Schema: tc.schema, Table: tc.table}))
		})
	}
	db.config.Advanced.CaptureSchema = "REPL"
	require.False(t, db.discoverableTable(sqlcapture.TableID{Schema: "REPL", Table: "TABLE"}))
	db.config.Advanced.DiscoverSchemas = []string{"MixedCase"}
	require.True(t, db.discoverableTable(sqlcapture.TableID{Schema: "MixedCase", Table: "ORDERS"}))
	require.False(t, db.discoverableTable(sqlcapture.TableID{Schema: "MIXEDCASE", Table: "ORDERS"}))
	require.False(t, db.discoverableTable(sqlcapture.TableID{Schema: "DB2INST1", Table: "ORDERS"}))
}

func TestCaseSensitiveDiscovery(t *testing.T) {
	var db, mock = testDatabase(t)
	var args = []driver.Value{"Mixed", "Orders", "Mixed", "ORDERS", "DB2INST1", "FLOW_WATERMARKS"}
	mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).WithArgs(args...).WillReturnRows(sqlmock.NewRows(tableDetailColumns).
		AddRow("Mixed", "Orders", "T", "Y", "Mixed", "CDOrders", "A", "N", "X ").
		AddRow("Mixed", "ORDERS", "T", "Y", "Mixed", "CDORDERS", "A", "Y", nil))
	mock.ExpectQuery(`FROM SYSCAT.COLUMNS C LEFT JOIN SYSCAT.DATATYPES D .* WHERE`).WithArgs(args...).WillReturnRows(sqlmock.NewRows(columnDetailColumns).
		AddRow("Mixed", "ORDERS", "ID", 0, "N", "INTEGER", 4, 0, 0, nil).
		AddRow("Mixed", "Orders", "Id/~", 0, "N", "VARCHAR", 100, 0, 1208, nil))
	mock.ExpectQuery(`FROM SYSCAT.KEYCOLUSE`).WithArgs(args...).WillReturnRows(sqlmock.NewRows(primaryKeyColumns).
		AddRow("Mixed", "ORDERS", "ID", 1).
		AddRow("Mixed", "Orders", "Id/~", 1))
	var tables, err = db.DiscoverTableDetails(context.Background(), []sqlcapture.TableID{{Schema: "Mixed", Table: "Orders"}, {Schema: "Mixed", Table: "ORDERS"}})
	require.NoError(t, err)
	require.Len(t, tables, 2)
	var mixed = tables[sqlcapture.StreamID{Schema: "Mixed", Table: "Orders"}]
	require.NotNil(t, mixed)
	require.Equal(t, []string{"Id/~"}, mixed.ColumnNames)
	require.Equal(t, []string{"Id/~"}, mixed.PrimaryKey)
	require.True(t, mixed.UnpredictableKeyOrdering)
	require.Equal(t, "X", mixed.ExtraDetails.(*db2TableDiscoveryDetails).Registration.BeforeImagePrefix)
	var upper = tables[sqlcapture.StreamID{Schema: "Mixed", Table: "ORDERS"}]
	require.NotNil(t, upper)
	require.False(t, upper.UnpredictableKeyOrdering)
	require.True(t, upper.ExtraDetails.(*db2TableDiscoveryDetails).Registration.UpdateAsDeleteInsert)
}

func TestDataCaptureChanges(t *testing.T) {
	for _, dataCapture := range []string{"N", "Y", "L"} {
		t.Run(dataCapture, func(t *testing.T) {
			var db, mock = testDatabase(t)
			mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).WithArgs("DB2INST1", "ORDERS", "DB2INST1", "FLOW_WATERMARKS").
				WillReturnRows(sqlmock.NewRows(tableDetailColumns).
					AddRow("DB2INST1", "ORDERS", "T", dataCapture, "DB2INST1", "CDORDERS", "A", "Y", "").
					AddRow("DB2INST1", "FLOW_WATERMARKS", "T", dataCapture, "DB2INST1", "CDFLOW_WATERMARKS", "A", "N", ""))
			var tables, err = db.getTables(context.Background(), []sqlcapture.TableID{
				{Schema: "DB2INST1", Table: "ORDERS"}, {Schema: "DB2INST1", Table: "FLOW_WATERMARKS"},
			})
			require.NoError(t, err)
			require.Len(t, tables, 2)
			for _, table := range tables {
				if dataCapture == "N" {
					require.False(t, table.ExtraDetails.(*db2TableDiscoveryDetails).DataCaptureChanges)
					require.ErrorContains(t, db.checkTableRegistration(table), "DATA CAPTURE CHANGES is not enabled")
				} else {
					require.True(t, table.ExtraDetails.(*db2TableDiscoveryDetails).DataCaptureChanges)
					require.NoError(t, db.checkTableRegistration(table))
				}
			}
		})
	}
}

func TestDistinctTypeValues(t *testing.T) {
	var timestamp = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		column   db2ColumnType
		value    driver.Value
		expected string
		schema   string
	}{
		{"EMAIL", db2ColumnType{Name: "VARCHAR", Length: 100, Codepage: 1208}, []byte("a@b.c"), "a@b.c", `{"type":"string","maxLength":100}`},
		{"MONEY", db2ColumnType{Name: "DECIMAL", Length: 20, Scale: 2}, []byte("123456789012345678.90"), "123456789012345678.90", `{"type":"string","format":"number"}`},
		{"BIRTHDATE", db2ColumnType{Name: "DATE", Length: 4}, timestamp, "2026-01-02", `{"type":"string","format":"date"}`},
		{"EVENT_TIME", db2ColumnType{Name: "TIMESTAMP", Length: 13, Scale: 12}, []byte("2026-01-02-03.04.05.123456789001"), "2026-01-02T03:04:05.123456789001-07:00", `{"type":"string","format":"date-time"}`},
		{"BINARY_DATA", db2ColumnType{Name: "VARCHAR", Length: 100}, []byte{0, 1, 255}, "AAH/", `{"type":"string","contentEncoding":"base64"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var db, mock = testDatabase(t)
			db.datetimeLocation = time.FixedZone("test", -7*60*60)
			mock.ExpectQuery(regexp.QuoteMeta(`FROM SYSCAT.COLUMNS C
  LEFT JOIN SYSCAT.DATATYPES D ON D.TYPESCHEMA = C.TYPESCHEMA AND D.TYPENAME = C.TYPENAME
    AND D.METATYPE = 'D' AND D.SOURCESCHEMA = 'SYSIBM'
  WHERE ((C.TABSCHEMA = ? AND C.TABNAME = ?))
  ORDER BY C.TABSCHEMA, C.TABNAME, C.COLNO`)).WithArgs("DB2INST1", "ORDERS").
				WillReturnRows(sqlmock.NewRows(columnDetailColumns).
					AddRow("DB2INST1", "ORDERS", "ID", 0, "N", "INTEGER", 4, 0, 0, nil).
					AddRow("DB2INST1", "ORDERS", "VALUE", 1, "N", tc.name, tc.column.Length, tc.column.Scale, tc.column.Codepage, tc.column.Name+" "))
			var columns, err = getColumns(context.Background(), db.conn, []sqlcapture.TableID{{Schema: "DB2INST1", Table: "ORDERS"}})
			require.NoError(t, err)
			require.Len(t, columns, 2)
			require.Equal(t, tc.column, columns[1].DataType)
			var schema, schemaErr = db.TranslateDBToJSONType(columns[1], false)
			require.NoError(t, schemaErr)
			var encoded, marshalErr = json.Marshal(schema)
			require.NoError(t, marshalErr)
			require.JSONEq(t, tc.schema, string(encoded))

			var info = testOrdersInfo()
			info.ColumnNames, info.Columns = nil, make(map[string]sqlcapture.ColumnInfo)
			for _, column := range columns {
				info.ColumnNames = append(info.ColumnNames, column.Name)
				info.Columns[column.Name] = column
			}
			var checkDocument = func(t *testing.T, event sqlcapture.ChangeEvent) {
				t.Helper()
				var data, err = event.AppendJSON(nil)
				require.NoError(t, err)
				var document map[string]any
				require.NoError(t, json.Unmarshal(data, &document))
				require.Equal(t, tc.expected, document["VALUE"])
			}
			t.Run("Backfill", func(t *testing.T) {
				mock.ExpectQuery(`FROM "DB2INST1"\."ORDERS" AS T`).
					WillReturnRows(sqlmock.NewRows(info.ColumnNames).AddRow(1, tc.value))
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
				var rs = testReplicationStream(t, db, "01")
				var cdColumns = append(append([]string{}, cdMetadataColumns...), "ID", "VALUE", "XID", "XVALUE")
				activateTestTable(t, rs, mock, info, cdColumns)
				expectFrontier(mock, []byte{2})
				mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE`).WithArgs([]byte{1}, []byte{2}).
					WillReturnRows(sqlmock.NewRows(cdColumns).AddRow([]byte{2}, []byte{1}, "U", 1, tc.value, 1, tc.value))
				var changes int
				require.NoError(t, rs.pollChanges(context.Background(), func(event sqlcapture.DatabaseEvent) error {
					if change, ok := event.(*sqlcapture.OldChangeEvent); ok {
						changes++
						require.Equal(t, sqlcapture.UpdateOp, change.Operation)
						checkDocument(t, change)
						require.Equal(t, change.After["VALUE"], change.Before["VALUE"])
					}
					return nil
				}))
				require.Equal(t, 1, changes)
			})
		})
	}
}

func TestDiscoveryErrors(t *testing.T) {
	t.Run("ControlTablePermission", func(t *testing.T) {
		var db, mock = testDatabase(t)
		mock.ExpectQuery(`FROM SYSCAT.TABLES`).WillReturnError(fmt.Errorf("SQL0551N not authorized"))
		var _, err = db.ListTables(context.Background())
		require.ErrorContains(t, err, `GRANT SELECT ON TABLE "ASN"."IBMSNAP_REGISTER"`)
	})
	t.Run("IterationError", func(t *testing.T) {
		var db, mock = testDatabase(t)
		mock.ExpectQuery(`FROM SYSCAT.TABLES`).WillReturnRows(sqlmock.NewRows([]string{"TABSCHEMA", "TABNAME"}).
			AddRow("DB2INST1", "ORDERS").RowError(0, fmt.Errorf("connection interrupted")))
		var tables, err = db.ListTables(context.Background())
		require.ErrorContains(t, err, "connection interrupted")
		require.Nil(t, tables)
	})
}
