package main

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/google/uuid"
	go_ibm_db "github.com/ibmdb/go_ibm_db"
	"github.com/ibmdb/go_ibm_db/api"
	"github.com/stretchr/testify/require"
)

func testOrdersInfo() *sqlcapture.DiscoveryInfo {
	var info = &sqlcapture.DiscoveryInfo{
		Schema: "DB2INST1", Name: "ORDERS", BaseTable: true, PrimaryKey: []string{"ID"},
		ColumnNames: []string{"ID", "NAME", "QTY", "PRICE", "CREATED", "NOTE"},
		Columns:     make(map[string]sqlcapture.ColumnInfo),
		ExtraDetails: &db2TableDiscoveryDetails{DataCaptureChanges: true, Registration: &captureRegistration{
			CDSchema: "DB2INST1", CDTable: "CDORDERS", State: "A", BeforeImagePrefix: "X",
		}},
	}
	for i, ct := range []db2ColumnType{
		{Name: "INTEGER"}, {Name: "VARCHAR", Codepage: 1208}, {Name: "INTEGER"},
		{Name: "DECIMAL", Length: 10, Scale: 2}, {Name: "TIMESTAMP"}, {Name: "VARCHAR", Codepage: 1208},
	} {
		var name = info.ColumnNames[i]
		info.Columns[name] = sqlcapture.ColumnInfo{Name: name, Index: i, DataType: ct, IsNullable: name != "ID"}
	}
	return info
}

func testLinesInfo() *sqlcapture.DiscoveryInfo {
	var info = &sqlcapture.DiscoveryInfo{
		Schema: "DB2INST1", Name: "LINES", BaseTable: true, PrimaryKey: []string{"ORDER_ID", "LINE_NO"},
		ColumnNames: []string{"ORDER_ID", "LINE_NO", "SKU", "QTY"},
		Columns:     make(map[string]sqlcapture.ColumnInfo),
		ExtraDetails: &db2TableDiscoveryDetails{DataCaptureChanges: true, Registration: &captureRegistration{
			CDSchema: "DB2INST1", CDTable: "CDLINES", State: "A", UpdateAsDeleteInsert: true,
		}},
	}
	for i, ct := range []db2ColumnType{{Name: "INTEGER"}, {Name: "SMALLINT"}, {Name: "CHARACTER", Codepage: 1208}, {Name: "INTEGER"}} {
		var name = info.ColumnNames[i]
		info.Columns[name] = sqlcapture.ColumnInfo{Name: name, Index: i, DataType: ct, IsNullable: i >= 2}
	}
	return info
}

func testWatermarksInfo() *sqlcapture.DiscoveryInfo {
	return &sqlcapture.DiscoveryInfo{
		Schema: "DB2INST1", Name: "FLOW_WATERMARKS", BaseTable: true, OmitBinding: true,
		PrimaryKey: []string{"SLOT"}, ColumnNames: []string{"SLOT", "WATERMARK"},
		Columns: map[string]sqlcapture.ColumnInfo{
			"SLOT":      {Name: "SLOT", DataType: db2ColumnType{Name: "VARCHAR", Codepage: 1208}},
			"WATERMARK": {Name: "WATERMARK", DataType: db2ColumnType{Name: "VARCHAR", Codepage: 1208}},
		},
		ExtraDetails: &db2TableDiscoveryDetails{DataCaptureChanges: true, Registration: &captureRegistration{
			CDSchema: "DB2INST1", CDTable: "CDFLOW_WATERMARKS", State: "A",
		}},
	}
}

func TestBackfill(t *testing.T) {
	var db, mock = testDatabase(t)
	db.config.Advanced.BackfillChunkSize = 2
	db.config.Advanced.SourceTag = "fixture"
	var info = testOrdersInfo()
	var state = &sqlcapture.TableState{Mode: sqlcapture.TableStatePreciseBackfill, KeyColumns: info.PrimaryKey}
	var created = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT "ID", "NAME", "QTY", "PRICE", "CREATED", "NOTE" FROM "DB2INST1"."ORDERS" AS T ORDER BY T."ID" FETCH FIRST 2 ROWS ONLY`)).
		WillReturnRows(sqlmock.NewRows(info.ColumnNames).
			AddRow(1, []byte("apple"), 1, []byte("1.50"), created, []byte("first")).
			AddRow(2, []byte("pear"), 2, []byte("2.25"), created.Add(time.Second), nil)).RowsWillBeClosed()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT "ID", "NAME", "QTY", "PRICE", "CREATED", "NOTE" FROM "DB2INST1"."ORDERS" AS T WHERE ((T."ID" > ?)) ORDER BY T."ID" FETCH FIRST 2 ROWS ONLY`)).
		WithArgs(int64(2)).WillReturnRows(sqlmock.NewRows(info.ColumnNames).
		AddRow(3, []byte("plum"), 3, []byte("3.00"), created.Add(2*time.Second), []byte("third"))).RowsWillBeClosed()

	var documents []json.RawMessage
	var emit = func(event sqlcapture.ChangeEvent) error {
		var doc, err = event.AppendJSON(nil)
		documents = append(documents, doc)
		return err
	}
	var complete, cursor, err = db.ScanTableChunk(context.Background(), info, state, "", emit)
	require.NoError(t, err)
	require.False(t, complete)
	require.Equal(t, tuple.Tuple{int64(2)}.Pack(), cursor)
	state.Scanned, state.BackfilledCount = cursor, 2

	// Resume from the serialized state, as after a checkpoint and restart.
	var encoded, marshalErr = json.Marshal(state)
	require.NoError(t, marshalErr)
	state = new(sqlcapture.TableState)
	require.NoError(t, json.Unmarshal(encoded, state))
	complete, _, err = db.ScanTableChunk(context.Background(), info, state, "", emit)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, documents, 3)
	require.JSONEq(t, `{"ID":1,"NAME":"apple","QTY":1,"PRICE":"1.50","CREATED":"2026-01-02T03:04:05Z","NOTE":"first","_meta":{"op":"c","source":{"schema":"DB2INST1","table":"ORDERS","snapshot":true,"cursor":"","intent":"0000000000000000","tag":"fixture"}}}`, string(documents[0]))
	require.JSONEq(t, `{"ID":2,"NAME":"pear","QTY":2,"PRICE":"2.25","CREATED":"2026-01-02T03:04:06Z","NOTE":null,"_meta":{"op":"c","source":{"schema":"DB2INST1","table":"ORDERS","snapshot":true,"cursor":"","intent":"0000000000000001","tag":"fixture"}}}`, string(documents[1]))
	require.JSONEq(t, `{"ID":3,"NAME":"plum","QTY":3,"PRICE":"3.00","CREATED":"2026-01-02T03:04:07Z","NOTE":"third","_meta":{"op":"c","source":{"schema":"DB2INST1","table":"ORDERS","snapshot":true,"cursor":"","intent":"0000000000000002","tag":"fixture"}}}`, string(documents[2]))
}

func TestBackfillCompoundKey(t *testing.T) {
	var db, mock = testDatabase(t)
	db.config.Advanced.BackfillChunkSize = 2
	var info = testLinesInfo()
	var state = &sqlcapture.TableState{Mode: sqlcapture.TableStatePreciseBackfill, KeyColumns: info.PrimaryKey}
	mock.ExpectQuery(`FROM "DB2INST1"\."LINES" AS T ORDER BY T."ORDER_ID", T."LINE_NO" FETCH FIRST 2 ROWS ONLY`).
		WillReturnRows(sqlmock.NewRows(info.ColumnNames).AddRow(1, 1, []byte("SKU-A   "), 5).AddRow(1, 2, []byte("SKU-B   "), 6))
	mock.ExpectQuery(regexp.QuoteMeta(`WHERE ((T."ORDER_ID" > ?) OR (T."ORDER_ID" = ? AND T."LINE_NO" > ?)) ORDER BY T."ORDER_ID", T."LINE_NO" FETCH FIRST 2 ROWS ONLY`)).
		WithArgs(int64(1), int64(1), int64(2)).WillReturnRows(sqlmock.NewRows(info.ColumnNames).AddRow(2, 1, []byte("SKU-C   "), 7))
	var keys [][]byte
	var quantities []any
	var emit = func(event sqlcapture.ChangeEvent) error {
		keys = append(keys, event.GetRowKey())
		var fields = event.(*sqlcapture.OldChangeEvent).After
		require.IsType(t, "", fields["SKU"])
		quantities = append(quantities, fields["QTY"])
		return nil
	}
	var complete, cursor, err = db.ScanTableChunk(context.Background(), info, state, "", emit)
	require.NoError(t, err)
	require.False(t, complete)
	state.Scanned, state.BackfilledCount = cursor, 2
	complete, _, err = db.ScanTableChunk(context.Background(), info, state, "", emit)
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, [][]byte{tuple.Tuple{1, 1}.Pack(), tuple.Tuple{1, 2}.Pack(), tuple.Tuple{2, 1}.Pack()}, keys)
	require.Equal(t, []any{int64(5), int64(6), int64(7)}, quantities)
}

func testTimeInfo() *sqlcapture.DiscoveryInfo {
	var info = testOrdersInfo()
	info.PrimaryKey, info.ColumnNames = []string{"AT"}, []string{"AT"}
	info.Columns = map[string]sqlcapture.ColumnInfo{
		"AT": {Name: "AT", DataType: db2ColumnType{Name: "TIME"}},
	}
	return info
}

func TestBackfillEndOfDayTime(t *testing.T) {
	var db, mock = testDatabase(t)
	db.config.Advanced.BackfillChunkSize = 1
	var info = testTimeInfo()
	var state = &sqlcapture.TableState{Mode: sqlcapture.TableStateUnfilteredBackfill, KeyColumns: info.PrimaryKey}
	var projection = `SELECT CAST("AT" AS VARCHAR(8)) AS "AT" FROM "DB2INST1"."ORDERS" AS T`
	var column = go_ibm_db.BaseColumn{SType: api.SQL_VARCHAR, CType: api.SQL_C_CHAR}
	var noon, err = column.Value([]byte("12.00.00"))
	require.NoError(t, err)
	endOfDay, err := column.Value([]byte("24.00.00"))
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta(projection + ` ORDER BY T."AT" FETCH FIRST 1 ROWS ONLY`)).
		WillReturnRows(sqlmock.NewRows(info.ColumnNames).AddRow(noon))
	mock.ExpectQuery(regexp.QuoteMeta(projection + ` WHERE ((T."AT" > ?)) ORDER BY T."AT" FETCH FIRST 1 ROWS ONLY`)).
		WithArgs("12:00:00").WillReturnRows(sqlmock.NewRows(info.ColumnNames).AddRow(endOfDay))
	mock.ExpectQuery(regexp.QuoteMeta(projection + ` WHERE ((T."AT" > ?)) ORDER BY T."AT" FETCH FIRST 1 ROWS ONLY`)).
		WithArgs("24:00:00").WillReturnRows(sqlmock.NewRows(info.ColumnNames))
	var times []any
	var keys [][]byte
	for i, resume := range []string{"12:00:00", "24:00:00", "24:00:00"} {
		var complete, cursor, err = db.ScanTableChunk(context.Background(), info, state, "", func(event sqlcapture.ChangeEvent) error {
			var data, err = event.AppendJSON(nil)
			require.NoError(t, err)
			var document map[string]any
			require.NoError(t, json.Unmarshal(data, &document))
			times = append(times, document["AT"])
			keys = append(keys, event.GetRowKey())
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, i == 2, complete)
		require.Equal(t, tuple.Tuple{resume}.Pack(), cursor)
		state.Scanned, state.BackfilledCount = cursor, len(times)
		var encoded, marshalErr = json.Marshal(state)
		require.NoError(t, marshalErr)
		state = new(sqlcapture.TableState)
		require.NoError(t, json.Unmarshal(encoded, state))
	}
	require.Equal(t, []any{"12:00:00", "24:00:00"}, times)
	require.Equal(t, [][]byte{tuple.Tuple{"12:00:00"}.Pack(), tuple.Tuple{"24:00:00"}.Pack()}, keys)
}

func testTimestampInfo() *sqlcapture.DiscoveryInfo {
	var info = testOrdersInfo()
	info.PrimaryKey = []string{"CREATED"}
	info.ColumnNames = []string{"CREATED"}
	info.Columns = map[string]sqlcapture.ColumnInfo{
		"CREATED": {Name: "CREATED", DataType: db2ColumnType{Name: "TIMESTAMP", Scale: 12}},
	}
	return info
}

func TestBackfillTimestampPrecision(t *testing.T) {
	var db, mock = testDatabase(t)
	db.config.Advanced.BackfillChunkSize = 1
	var info = testTimestampInfo()
	var state = &sqlcapture.TableState{Mode: sqlcapture.TableStateUnfilteredBackfill, KeyColumns: info.PrimaryKey}
	var projection = `SELECT CAST("CREATED" AS VARCHAR(32)) AS "CREATED" FROM "DB2INST1"."ORDERS" AS T`
	mock.ExpectQuery(regexp.QuoteMeta(projection + ` ORDER BY T."CREATED" FETCH FIRST 1 ROWS ONLY`)).
		WillReturnRows(sqlmock.NewRows(info.ColumnNames).AddRow([]byte("2026-01-02-03.04.05.123456789001")))
	mock.ExpectQuery(regexp.QuoteMeta(projection + ` WHERE ((T."CREATED" > ?)) ORDER BY T."CREATED" FETCH FIRST 1 ROWS ONLY`)).
		WithArgs("2026-01-02 03:04:05.123456789001").
		WillReturnRows(sqlmock.NewRows(info.ColumnNames).AddRow("2026-01-02-03.04.05.123456789002"))
	mock.ExpectQuery(regexp.QuoteMeta(projection + ` WHERE ((T."CREATED" > ?)) ORDER BY T."CREATED" FETCH FIRST 1 ROWS ONLY`)).
		WithArgs("2026-01-02 03:04:05.123456789002").WillReturnRows(sqlmock.NewRows(info.ColumnNames))
	var keys [][]byte
	var documents []any
	var emit = func(event sqlcapture.ChangeEvent) error {
		keys = append(keys, event.GetRowKey())
		documents = append(documents, event.(*sqlcapture.OldChangeEvent).After["CREATED"])
		return nil
	}
	for i := 0; i < 3; i++ {
		var complete, cursor, err = db.ScanTableChunk(context.Background(), info, state, "", emit)
		require.NoError(t, err)
		require.Equal(t, i == 2, complete)
		state.Scanned = cursor
		var encoded, marshalErr = json.Marshal(state)
		require.NoError(t, marshalErr)
		require.NoError(t, json.Unmarshal(encoded, state))
	}
	require.Equal(t, []any{"2026-01-02T03:04:05.123456789001Z", "2026-01-02T03:04:05.123456789002Z"}, documents)
	require.Equal(t, [][]byte{
		tuple.Tuple{"2026-01-02 03:04:05.123456789001"}.Pack(),
		tuple.Tuple{"2026-01-02 03:04:05.123456789002"}.Pack(),
	}, keys)
}

func TestBackfillDBCLOB(t *testing.T) {
	var db, mock = testDatabase(t)
	var info = testOrdersInfo()
	info.ColumnNames = []string{"ID", "CONTENT"}
	info.Columns["CONTENT"] = sqlcapture.ColumnInfo{Name: "CONTENT", DataType: db2ColumnType{Name: "DBCLOB", Codepage: 1200}}
	var texts = []string{"A", "é", "漢", "🌍", ""}
	var rows = sqlmock.NewRows(append(append([]string{}, info.ColumnNames...), "__FLOW_LOB_LENGTH_1"))
	// Exercise the actual driver's CLOB decoder, which receives UTF-8 after conversion.
	var column = go_ibm_db.BaseColumn{SType: api.SQL_CLOB, CType: api.SQL_C_CHAR}
	for i, text := range texts {
		var value, err = column.Value([]byte(text))
		require.NoError(t, err)
		rows.AddRow(i, value, utf8.RuneCountInString(text))
	}
	rows.AddRow(len(texts), nil, nil)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT "ID", CLOB("CONTENT") AS "CONTENT", LENGTH(CLOB("CONTENT"), CODEUNITS32) AS "__FLOW_LOB_LENGTH_1" FROM "DB2INST1"."ORDERS" AS T ORDER BY T."ID" FETCH FIRST 50000 ROWS ONLY`)).WillReturnRows(rows)
	var contents []any
	var complete, _, err = db.ScanTableChunk(context.Background(), info, &sqlcapture.TableState{KeyColumns: info.PrimaryKey}, "", func(event sqlcapture.ChangeEvent) error {
		var data, err = event.AppendJSON(nil)
		require.NoError(t, err)
		var document map[string]any
		require.NoError(t, json.Unmarshal(data, &document))
		contents = append(contents, document["CONTENT"])
		return nil
	})
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, []any{"A", "é", "漢", "🌍", "", nil}, contents)
}

func TestBackfillDBCLOBIntegration(t *testing.T) {
	var cfg = integrationConfig(t)
	cfg.Advanced.BackfillChunkSize = 1
	var db = &db2Database{config: cfg}
	var ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, db.connect(ctx))
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	var info = &sqlcapture.DiscoveryInfo{
		Schema: strings.ToUpper(cfg.User), Name: "FLOW_DBCLOB_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		ColumnNames: []string{"ID", "CONTENT"},
		Columns: map[string]sqlcapture.ColumnInfo{
			"ID":      {Name: "ID", DataType: db2ColumnType{Name: "INTEGER"}},
			"CONTENT": {Name: "CONTENT", DataType: db2ColumnType{Name: "DBCLOB", Codepage: 1200}},
		},
	}
	var table = quoteTableName(info.Schema, info.Name)
	_, err := db.conn.ExecContext(ctx, "CREATE TABLE "+table+" (ID INTEGER NOT NULL PRIMARY KEY, CONTENT DBCLOB(1M))")
	require.NoError(t, err)
	t.Cleanup(func() {
		var _, err = db.conn.ExecContext(context.Background(), "DROP TABLE "+table)
		require.NoError(t, err)
	})
	var texts = []any{"A", "é", "漢", "🌍", "", nil}
	for i, text := range texts {
		_, err = db.conn.ExecContext(ctx, "INSERT INTO "+table+" VALUES (?, DBCLOB(CAST(? AS VARCHAR(100))))", i, text)
		require.NoError(t, err)
	}
	var state = &sqlcapture.TableState{Mode: sqlcapture.TableStatePreciseBackfill, KeyColumns: []string{"ID"}}
	var contents []any
	for {
		var complete, cursor, err = db.ScanTableChunk(ctx, info, state, "", func(event sqlcapture.ChangeEvent) error {
			var data, err = event.AppendJSON(nil)
			require.NoError(t, err)
			var document map[string]any
			require.NoError(t, json.Unmarshal(data, &document))
			contents = append(contents, document["CONTENT"])
			return nil
		})
		require.NoError(t, err)
		if complete {
			break
		}
		state.Scanned, state.BackfilledCount = cursor, len(contents)
	}
	require.Equal(t, texts, contents)
}

func TestBackfillTimestampQuery(t *testing.T) {
	var info = testTimestampInfo()
	var query, args, err = buildBackfillQuery(info, &sqlcapture.TableState{
		KeyColumns: info.PrimaryKey, Scanned: tuple.Tuple{"2026-01-02 03:04:05.123456789001"}.Pack(),
	}, 100, "")
	require.NoError(t, err)
	require.Equal(t, `SELECT CAST("CREATED" AS VARCHAR(32)) AS "CREATED" FROM "DB2INST1"."ORDERS" AS T WHERE ((T."CREATED" > ?)) ORDER BY T."CREATED" FETCH FIRST 100 ROWS ONLY`, query)
	require.Equal(t, []any{"2026-01-02 03:04:05.123456789001"}, args)
}

func TestBackfillEmptyAndFailures(t *testing.T) {
	var failure = errors.New("interrupted")
	for _, name := range []string{"Empty", "QueryError", "ReadError", "CallbackError"} {
		t.Run(name, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var info = testOrdersInfo()
			var expected = mock.ExpectQuery(`FROM "DB2INST1"\."ORDERS"`)
			var rows = sqlmock.NewRows(info.ColumnNames)
			switch name {
			case "Empty":
				expected.WillReturnRows(rows)
			case "QueryError":
				expected.WillReturnError(failure)
			case "ReadError":
				expected.WillReturnRows(rows.AddRow(1, "apple", 1, "1.50", nil, nil).RowError(0, failure))
			case "CallbackError":
				expected.WillReturnRows(rows.AddRow(1, "apple", 1, "1.50", nil, nil))
			}
			var complete, _, err = db.ScanTableChunk(context.Background(), info, &sqlcapture.TableState{KeyColumns: info.PrimaryKey}, "", func(sqlcapture.ChangeEvent) error { return failure })
			if name == "Empty" {
				require.NoError(t, err)
				require.True(t, complete)
			} else {
				require.ErrorIs(t, err, failure)
				require.False(t, complete)
			}
		})
	}
}

func TestBackfillQuery(t *testing.T) {
	var info = testOrdersInfo()
	info.Schema, info.Name = `Mixed"Schema`, `Order"Table`
	var query, args, err = buildBackfillQuery(info, &sqlcapture.TableState{KeyColumns: info.PrimaryKey, Scanned: tuple.Tuple{4}.Pack()}, 10, `"QTY" > 1 OR "NOTE" IS NULL`)
	require.NoError(t, err)
	require.Contains(t, query, `FROM "Mixed""Schema"."Order""Table"`)
	require.Contains(t, query, `WHERE ((T."ID" > ?)) AND ("QTY" > 1 OR "NOTE" IS NULL)`)
	require.Equal(t, []any{int64(4)}, args)
	_, _, err = buildBackfillQuery(info, &sqlcapture.TableState{KeyColumns: info.PrimaryKey, Scanned: []byte{0xff}}, 10, "")
	require.ErrorContains(t, err, "invalid backfill resume key")
	query, args, err = buildBackfillQuery(info, &sqlcapture.TableState{Mode: sqlcapture.TableStateKeylessBackfill, BackfilledCount: 20}, 10, "")
	require.NoError(t, err)
	require.Contains(t, query, "ORDER BY RID(T) FETCH FIRST 10 ROWS ONLY")
	require.NotContains(t, query, "OFFSET")
	require.Empty(t, args)
	query, args, err = buildBackfillQuery(info, &sqlcapture.TableState{
		Mode: sqlcapture.TableStateKeylessBackfill, BackfilledCount: 20, Scanned: tuple.Tuple{int64(123456)}.Pack(),
	}, 10, `"QTY" > 1`)
	require.NoError(t, err)
	require.Contains(t, query, `WHERE RID(T) > ? AND ("QTY" > 1) ORDER BY RID(T) FETCH FIRST 10 ROWS ONLY`)
	require.Equal(t, []any{int64(123456)}, args)
	for _, cursor := range [][]byte{[]byte("B0000000000000000019"), tuple.Tuple{"123456"}.Pack(), tuple.Tuple{1, 2}.Pack()} {
		_, _, err = buildBackfillQuery(info, &sqlcapture.TableState{Mode: sqlcapture.TableStateKeylessBackfill, Scanned: cursor}, 10, "")
		require.ErrorContains(t, err, "re-backfill the binding")
	}
}
