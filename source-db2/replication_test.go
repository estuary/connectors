package main

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var cdMetadataColumns = []string{"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION"}

func testReplicationStream(t *testing.T, db *db2Database, cursor string) *db2ReplicationStream {
	t.Helper()
	var encoded, err = json.Marshal(cursor)
	require.NoError(t, err)
	stream, err := db.ReplicationStream(context.Background(), encoded)
	require.NoError(t, err)
	db.registrationChecksAfter = db.now().Add(time.Minute)
	return stream.(*db2ReplicationStream)
}

func activateTestTable(t *testing.T, rs *db2ReplicationStream, mock sqlmock.Sqlmock, info *sqlcapture.DiscoveryInfo, columns []string) {
	t.Helper()
	expectCDInspection(mock, info, columns, nil)

	require.NoError(t, rs.ActivateTable(context.Background(), sqlcapture.JoinStreamID(info.Schema, info.Name), info.PrimaryKey, info, nil))
}

func expectCDCatalog(mock sqlmock.Sqlmock, info *sqlcapture.DiscoveryInfo, columns []string, types map[string]string) {
	var registration = info.ExtraDetails.(*db2TableDiscoveryDetails).Registration
	var rows = sqlmock.NewRows(columnDetailColumns)
	for index, name := range columns {
		rows.AddRow(registration.CDSchema, registration.CDTable, name, index, "Y", types[name], 0, 0, 0, nil)
	}
	mock.ExpectQuery(`FROM SYSCAT.COLUMNS C LEFT JOIN`).
		WithArgs(registration.CDSchema, registration.CDTable).WillReturnRows(rows).RowsWillBeClosed()
}

func expectCDInspection(mock sqlmock.Sqlmock, info *sqlcapture.DiscoveryInfo, columns []string, types map[string]string) {
	expectCDCatalog(mock, info, columns, types)
	var registration = info.ExtraDetails.(*db2TableDiscoveryDetails).Registration
	mock.ExpectQuery(`SELECT .* FROM ` + regexp.QuoteMeta(quoteTableName(registration.CDSchema, registration.CDTable)) + ` WHERE 1 = 0`).
		WillReturnRows(sqlmock.NewRows(columns)).RowsWillBeClosed()
}

func columnProjection(columns []string) string {
	var quoted []string
	for _, name := range columns {
		quoted = append(quoted, quoteIdentifier(name))
	}
	return strings.Join(quoted, ", ")
}

func expectFrontier(mock sqlmock.Sqlmock, frontier []byte) {
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT SYNCHPOINT FROM "ASN"."IBMSNAP_REGISTER" WHERE GLOBAL_RECORD = 'Y'`)).
		WillReturnRows(sqlmock.NewRows([]string{"SYNCHPOINT"}).AddRow(frontier))
}

func expectRegistrationChecks(mock sqlmock.Sqlmock, tables ...*sqlcapture.DiscoveryInfo) {
	for _, info := range tables {
		var registration = info.ExtraDetails.(*db2TableDiscoveryDetails).Registration
		mock.ExpectQuery(`SELECT STATE FROM "ASN"\."IBMSNAP_REGISTER" WHERE`).
			WithArgs(info.Schema, info.Name, registration.CDSchema, registration.CDTable).
			WillReturnRows(sqlmock.NewRows([]string{"STATE"}).AddRow("A"))
		var columns = append(append([]string{}, cdMetadataColumns...), info.ColumnNames...)
		if registration.BeforeImagePrefix != "" {
			for _, name := range info.ColumnNames {
				columns = append(columns, registration.BeforeImagePrefix+name)
			}
		}
		expectCDCatalog(mock, info, columns, nil)
	}
}

func TestReplicationMerge(t *testing.T) {
	var db, mock = testDatabase(t)
	var rs = testReplicationStream(t, db, "01")
	rs.batchSize = 2
	var linesColumns = append(append([]string{}, cdMetadataColumns...), "ORDER_ID", "LINE_NO", "SKU", "QTY")
	var ordersColumns = append(append([]string{}, cdMetadataColumns...), "ID", "NAME", "QTY", "XID", "XNAME", "XQTY")
	activateTestTable(t, rs, mock, testLinesInfo(), linesColumns)
	activateTestTable(t, rs, mock, testOrdersInfo(), ordersColumns)
	expectFrontier(mock, []byte{5})
	var firstRange = ` WHERE IBMSNAP_COMMITSEQ > ? AND IBMSNAP_COMMITSEQ <= ? ORDER BY IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ FETCH FIRST 2 ROWS ONLY`
	var nextRange = ` WHERE IBMSNAP_COMMITSEQ >= ? AND IBMSNAP_COMMITSEQ <= ? AND (IBMSNAP_COMMITSEQ > ? OR IBMSNAP_INTENTSEQ > ?) ORDER BY IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ FETCH FIRST 2 ROWS ONLY`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT `+columnProjection(linesColumns)+` FROM "DB2INST1"."CDLINES"`+firstRange)).WithArgs([]byte{1}, []byte{5}).
		WillReturnRows(sqlmock.NewRows(linesColumns).
			AddRow([]byte{3}, []byte{1}, "D", 1, 2, []byte("SKU-B   "), 6).
			AddRow([]byte{3}, []byte{2}, "I", 1, 2, []byte("SKU-B   "), 60)).RowsWillBeClosed()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT `+columnProjection(ordersColumns)+` FROM "DB2INST1"."CDORDERS"`+firstRange)).WithArgs([]byte{1}, []byte{5}).
		WillReturnRows(sqlmock.NewRows(ordersColumns).
			AddRow([]byte{2}, []byte{2}, "I", 4, "kiwi", 4, nil, nil, nil).
			AddRow([]byte{3}, []byte{3}, "U", 1, "apple", 10, 1, "apple", 1)).RowsWillBeClosed()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT `+columnProjection(linesColumns)+` FROM "DB2INST1"."CDLINES"`+nextRange)).WithArgs([]byte{3}, []byte{5}, []byte{3}, []byte{2}).
		WillReturnRows(sqlmock.NewRows(linesColumns).AddRow([]byte{4}, []byte{1}, "I", 4, 1, []byte("SKU-D   "), 8))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT `+columnProjection(ordersColumns)+` FROM "DB2INST1"."CDORDERS"`+nextRange)).WithArgs([]byte{3}, []byte{5}, []byte{3}, []byte{3}).
		WillReturnRows(sqlmock.NewRows(ordersColumns).
			AddRow([]byte{3}, []byte{4}, "U", 20, "pear2", 2, 2, "pear", 2).
			AddRow([]byte{4}, []byte{2}, "D", 3, "plum", 3, nil, nil, nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT `+columnProjection(ordersColumns)+` FROM "DB2INST1"."CDORDERS"`+nextRange)).WithArgs([]byte{4}, []byte{5}, []byte{4}, []byte{2}).
		WillReturnRows(sqlmock.NewRows(ordersColumns))

	var events []string
	var documents []map[string]any
	require.NoError(t, rs.pollChanges(context.Background(), func(event sqlcapture.DatabaseEvent) error {
		if change, ok := event.(*sqlcapture.OldChangeEvent); ok {
			var bs, err = change.AppendJSON(nil)
			require.NoError(t, err)
			var doc map[string]any
			require.NoError(t, json.Unmarshal(bs, &doc))
			documents = append(documents, doc)
			events = append(events, fmt.Sprintf("%s %s %v %v", change.Operation, change.StreamID().Table, doc["ID"], doc["QTY"]))
		} else {
			events = append(events, event.String())
		}
		return nil
	}))
	require.Equal(t, []string{
		"c ORDERS 4 4", "Commit(02)", "u LINES <nil> 60", "u ORDERS 1 10", "d ORDERS 2 2", "c ORDERS 20 2", "Commit(03)",
		"c LINES <nil> 8", "d ORDERS 3 3", "Commit(04)", "Commit(05)",
	}, events)
	require.Equal(t, []byte{5}, rs.fromSeq)
	require.NotContains(t, documents[0], "NOTE", "unregistered columns must be omitted rather than synthesized as null")
	require.NotContains(t, documents[0], "XID")
}

func TestReplicationPairAcrossBatches(t *testing.T) {
	for _, commit := range []byte{2, 3} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			var db, mock = testDatabase(t)
			var rs = testReplicationStream(t, db, "01")
			rs.batchSize = 1
			var columns = append(append([]string{}, cdMetadataColumns...), "ORDER_ID", "LINE_NO", "SKU", "QTY")
			activateTestTable(t, rs, mock, testLinesInfo(), columns)
			expectFrontier(mock, []byte{commit})
			mock.ExpectQuery(`FROM "DB2INST1"\."CDLINES" .* FETCH FIRST 1 ROWS ONLY`).WithArgs([]byte{1}, []byte{commit}).
				WillReturnRows(sqlmock.NewRows(columns).AddRow([]byte{2}, []byte{1}, "D", 1, 2, "SKU-B", 6))
			mock.ExpectQuery(`FROM "DB2INST1"\."CDLINES" .* FETCH FIRST 1 ROWS ONLY`).WithArgs([]byte{2}, []byte{commit}, []byte{2}, []byte{1}).
				WillReturnRows(sqlmock.NewRows(columns).AddRow([]byte{commit}, []byte{2}, "I", 1, 2, "SKU-B", 60))
			mock.ExpectQuery(`FROM "DB2INST1"\."CDLINES" .* FETCH FIRST 1 ROWS ONLY`).WithArgs([]byte{commit}, []byte{commit}, []byte{commit}, []byte{2}).
				WillReturnRows(sqlmock.NewRows(columns))
			var events []string
			require.NoError(t, rs.pollChanges(context.Background(), func(event sqlcapture.DatabaseEvent) error {
				events = append(events, event.String())
				if change, ok := event.(*sqlcapture.OldChangeEvent); ok && change.Operation != sqlcapture.DeleteOp {
					require.EqualValues(t, 60, change.After["QTY"])
				}
				return nil
			}))
			if commit == 2 {
				require.Equal(t, []string{"Update(DB2INST1.LINES)", "Commit(02)"}, events)
			} else {
				require.Equal(t, []string{"Delete(DB2INST1.LINES)", "Commit(02)", "Insert(DB2INST1.LINES)", "Commit(03)"}, events)
			}
		})
	}
}

func TestReplicationBatchRange(t *testing.T) {
	var db, mock = testDatabase(t)
	var rs = testReplicationStream(t, db, "01")
	rs.batchSize = 2
	var info = testLinesInfo()
	var streamID = sqlcapture.JoinStreamID(info.Schema, info.Name)
	var columns = append(append([]string{}, cdMetadataColumns...), "ORDER_ID", "LINE_NO", "SKU", "QTY")
	activateTestTable(t, rs, mock, info, columns)
	var cursor = &cdTableCursor{
		stream: rs, table: rs.tables[streamID], fromSeq: []byte{1}, toSeq: []byte{9}, commit: []byte{7}, intent: []byte{3},
	}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT `+columnProjection(columns)+` FROM "DB2INST1"."CDLINES" WHERE IBMSNAP_COMMITSEQ >= ? AND IBMSNAP_COMMITSEQ <= ? AND (IBMSNAP_COMMITSEQ > ? OR IBMSNAP_INTENTSEQ > ?) ORDER BY IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ FETCH FIRST 2 ROWS ONLY`)).
		WithArgs([]byte{7}, []byte{9}, []byte{7}, []byte{3}).
		WillReturnRows(sqlmock.NewRows(columns).
			AddRow([]byte{7}, []byte{4}, "I", 1, 1, "SKU", 1).
			AddRow([]byte{8}, []byte{1}, "I", 1, 2, "SKU", 2))
	require.NoError(t, cursor.fetch(context.Background()))
	require.Len(t, cursor.batch, 2)
	require.Equal(t, []byte{8}, cursor.commit)
	require.Equal(t, []byte{1}, cursor.intent)
	require.False(t, cursor.done)
}

func TestReplicationIntentGap(t *testing.T) {
	for _, tc := range []struct {
		name        string
		first, next []byte
		newKey      int
		commit      byte
		operations  []sqlcapture.ChangeOp
	}{
		{"Consecutive", []byte{1}, []byte{2}, 7, 2, []sqlcapture.ChangeOp{sqlcapture.UpdateOp}},
		{"Gap", []byte{1}, []byte{3}, 7, 2, []sqlcapture.ChangeOp{sqlcapture.DeleteOp, sqlcapture.InsertOp}},
		{"ChangedKey", []byte{1}, []byte{2}, 8, 2, []sqlcapture.ChangeOp{sqlcapture.DeleteOp, sqlcapture.InsertOp}},
		{"Carry", []byte{0, 255}, []byte{1, 0}, 7, 2, []sqlcapture.ChangeOp{sqlcapture.UpdateOp}},
		{"GapAcrossCarry", []byte{0, 255}, []byte{1, 1}, 7, 2, []sqlcapture.ChangeOp{sqlcapture.DeleteOp, sqlcapture.InsertOp}},
		{"TransactionBoundary", []byte{1}, []byte{2}, 7, 3, []sqlcapture.ChangeOp{sqlcapture.DeleteOp, sqlcapture.InsertOp}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var db, _ = testDatabase(t)
			var rs = testReplicationStream(t, db, "01")
			var info = testLinesInfo()
			var table = &tableReplicationInfo{StreamID: sqlcapture.JoinStreamID(info.Schema, info.Name), Discovery: info,
				Registration: info.ExtraDetails.(*db2TableDiscoveryDetails).Registration, KeyColumns: info.PrimaryKey}
			var old = &db2ChangeRow{Table: table, CommitSeq: []byte{2}, IntentSeq: tc.first, Operation: "D",
				Fields: map[string]any{"ORDER_ID": 7, "LINE_NO": 1, "SKU": []byte("old")}}
			var next = &db2ChangeRow{Table: table, CommitSeq: []byte{tc.commit}, IntentSeq: tc.next, Operation: "I",
				Fields: map[string]any{"ORDER_ID": tc.newKey, "LINE_NO": 1, "SKU": []byte("new")}}
			var pair = updatePairer{stream: rs}
			var operations []sqlcapture.ChangeOp
			for _, row := range []*db2ChangeRow{old, next, nil} {
				events, err := pair.pairUpdates(row)
				require.NoError(t, err)
				for _, event := range events {
					operations = append(operations, event.Operation)
					var metadata = next
					if event.Operation == sqlcapture.DeleteOp {
						metadata = old
					}
					require.Equal(t, hex.EncodeToString(metadata.IntentSeq), event.Source.(db2SourceInfo).Intent)
					require.Equal(t, hex.EncodeToString(metadata.CommitSeq), event.Source.(db2SourceInfo).Cursor)
				}
			}
			require.Equal(t, tc.operations, operations)
			require.Equal(t, []byte("old"), old.Fields["SKU"], "pairing must not translate input maps in place")
			require.Equal(t, []byte("new"), next.Fields["SKU"])
		})
	}
}

func TestReplicationTimestampPrecision(t *testing.T) {
	var db, mock = testDatabase(t)
	var rs = testReplicationStream(t, db, "01")
	var info = testTimestampInfo()
	var columns = append(append([]string{}, cdMetadataColumns...), "CREATED", "XCREATED")
	activateTestTable(t, rs, mock, info, columns)
	expectFrontier(mock, []byte{2})
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT "IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", CAST("CREATED" AS VARCHAR(32)) AS "CREATED", CAST("XCREATED" AS VARCHAR(32)) AS "XCREATED" FROM "DB2INST1"."CDORDERS"`)).
		WithArgs([]byte{1}, []byte{2}).WillReturnRows(sqlmock.NewRows(columns).
		AddRow([]byte{2}, []byte{1}, "U", []byte("2026-01-02-03.04.05.123456789002"), []byte("2026-01-02-03.04.05.123456789001")))
	var changes []*sqlcapture.OldChangeEvent
	require.NoError(t, rs.pollChanges(context.Background(), func(event sqlcapture.DatabaseEvent) error {
		if change, ok := event.(*sqlcapture.OldChangeEvent); ok {
			changes = append(changes, change)
		}
		return nil
	}))
	require.Len(t, changes, 2)
	require.Equal(t, sqlcapture.DeleteOp, changes[0].Operation)
	require.Equal(t, sqlcapture.InsertOp, changes[1].Operation)
	require.Equal(t, "2026-01-02T03:04:05.123456789001Z", changes[0].Before["CREATED"])
	require.Equal(t, "2026-01-02T03:04:05.123456789002Z", changes[1].After["CREATED"])
	require.Equal(t, tuple.Tuple{"2026-01-02 03:04:05.123456789001"}.Pack(), changes[0].RowKey)
	require.Equal(t, tuple.Tuple{"2026-01-02 03:04:05.123456789002"}.Pack(), changes[1].RowKey)
}

func TestReplicationEndOfDayTime(t *testing.T) {
	for _, paired := range []bool{false, true} {
		t.Run(fmt.Sprintf("DeleteInsert=%t", paired), func(t *testing.T) {
			var db, mock = testDatabase(t)
			var rs = testReplicationStream(t, db, "01")
			var info = testTimeInfo()
			var registration = info.ExtraDetails.(*db2TableDiscoveryDetails).Registration
			registration.UpdateAsDeleteInsert = paired
			var columns = append(append([]string{}, cdMetadataColumns...), "AT")
			var projection = `SELECT "IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", CAST("AT" AS VARCHAR(8)) AS "AT"`
			if paired {
				registration.BeforeImagePrefix = ""
			} else {
				columns = append(columns, "XAT")
				projection += `, CAST("XAT" AS VARCHAR(8)) AS "XAT"`
			}
			activateTestTable(t, rs, mock, info, columns)
			expectFrontier(mock, []byte{2})
			var rows = sqlmock.NewRows(columns)
			if paired {
				rows.AddRow([]byte{2}, []byte{1}, "D", []byte("24.00.00"))
				rows.AddRow([]byte{2}, []byte{2}, "I", []byte("00.00.00"))
				rows.AddRow([]byte{2}, []byte{3}, "D", []byte("00.00.00"))
				rows.AddRow([]byte{2}, []byte{4}, "I", []byte("24.00.00"))
				rows.AddRow([]byte{2}, []byte{5}, "D", []byte("24.00.00"))
				rows.AddRow([]byte{2}, []byte{6}, "I", []byte("24.00.00"))
			} else {
				rows.AddRow([]byte{2}, []byte{1}, "U", []byte("00.00.00"), []byte("24.00.00"))
				rows.AddRow([]byte{2}, []byte{2}, "U", []byte("24.00.00"), []byte("00.00.00"))
				rows.AddRow([]byte{2}, []byte{3}, "U", []byte("24.00.00"), []byte("24.00.00"))
			}
			mock.ExpectQuery(regexp.QuoteMeta(projection+` FROM "DB2INST1"."CDORDERS" WHERE`)).
				WithArgs([]byte{1}, []byte{2}).WillReturnRows(rows)
			var changes []*sqlcapture.OldChangeEvent
			require.NoError(t, rs.pollChanges(context.Background(), func(event sqlcapture.DatabaseEvent) error {
				if change, ok := event.(*sqlcapture.OldChangeEvent); ok {
					changes = append(changes, change)
				}
				return nil
			}))
			require.Len(t, changes, 5)
			for i, expected := range []struct {
				op   sqlcapture.ChangeOp
				time string
			}{
				{sqlcapture.DeleteOp, "24:00:00"}, {sqlcapture.InsertOp, "00:00:00"},
				{sqlcapture.DeleteOp, "00:00:00"}, {sqlcapture.InsertOp, "24:00:00"},
				{sqlcapture.UpdateOp, "24:00:00"},
			} {
				require.Equal(t, expected.op, changes[i].Operation)
				require.Equal(t, tuple.Tuple{expected.time}.Pack(), changes[i].RowKey)
				var data, err = changes[i].AppendJSON(nil)
				require.NoError(t, err)
				var document map[string]any
				require.NoError(t, json.Unmarshal(data, &document))
				require.Equal(t, expected.time, document["AT"])
			}
			require.Equal(t, "24:00:00", changes[4].Before["AT"])
		})
	}
}

func TestReplicationCursor(t *testing.T) {
	t.Run("Fresh", func(t *testing.T) {
		var db, mock = testDatabase(t)
		expectFrontier(mock, []byte{0, 0xff})
		var stream, err = db.ReplicationStream(context.Background(), nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0, 0xff}, stream.(*db2ReplicationStream).fromSeq)
	})
	t.Run("ResumeAndIdle", func(t *testing.T) {
		var db, mock = testDatabase(t)
		var rs = testReplicationStream(t, db, "00ff")
		expectFrontier(mock, []byte{0, 0xff})
		require.NoError(t, rs.pollChanges(context.Background(), func(sqlcapture.DatabaseEvent) error {
			t.Fatal("idle frontier must not replay changes")
			return nil
		}))
	})
	for _, cursor := range []string{`123`, `"not-hex"`, `"123"`} {
		t.Run(cursor, func(t *testing.T) {
			var db, _ = testDatabase(t)
			var _, err = db.ReplicationStream(context.Background(), json.RawMessage(cursor))
			require.Error(t, err)
		})
	}
	t.Run("CallbackFailure", func(t *testing.T) {
		var db, mock = testDatabase(t)
		var rs = testReplicationStream(t, db, "01")
		expectFrontier(mock, []byte{2})
		var failure = errors.New("checkpoint failed")
		require.ErrorIs(t, rs.pollChanges(context.Background(), func(sqlcapture.DatabaseEvent) error { return failure }), failure)
		require.Equal(t, []byte{1}, rs.fromSeq)
	})
}

// publishWatermark returns the generated UUID through the mocked CD table, just as asncap does.
type publishWatermark struct {
	rows      *sqlmock.Rows
	slot      string
	commit    []byte
	published bool
}

func (w *publishWatermark) Match(value driver.Value) bool {
	var watermark, ok = value.(string)
	if !ok {
		return false
	}
	if _, err := uuid.Parse(watermark); err != nil {
		return false
	}
	if w.rows != nil && !w.published {
		w.rows.AddRow(w.commit, []byte{1}, []byte("U"), []byte(w.slot), []byte(watermark))
		w.published = true
	}
	return true
}

func TestWatermarkFence(t *testing.T) {
	var db, mock = testDatabase(t)
	db.name = "fixture/capture"
	db.config.Advanced.PollingInterval = "1ms"
	var rs = testReplicationStream(t, db, "01")
	db.registrationChecksAfter = time.Time{}
	var columns = append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK")
	var info = testWatermarksInfo()
	expectCDInspection(mock, info, columns, nil)
	require.NoError(t, rs.StartReplication(context.Background(), map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo{db.WatermarksTable(): info}))
	var orderColumns = append(append([]string{}, cdMetadataColumns...), "ID", "QTY", "XID")
	activateTestTable(t, rs, mock, testOrdersInfo(), orderColumns)
	var changes = sqlmock.NewRows(columns)
	mock.ExpectExec(`MERGE INTO "DB2INST1"\."FLOW_WATERMARKS" AS T .* WHEN MATCHED THEN UPDATE .* WHEN NOT MATCHED THEN INSERT`).
		WithArgs(db.name, &publishWatermark{rows: changes, slot: db.name, commit: []byte{2}}).WillReturnResult(sqlmock.NewResult(0, 1))
	expectRegistrationChecks(mock, testWatermarksInfo(), testOrdersInfo())
	expectFrontier(mock, []byte{2})
	mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE IBMSNAP_COMMITSEQ > \? AND IBMSNAP_COMMITSEQ <= \? ORDER BY`).
		WithArgs([]byte{1}, []byte{2}).WillReturnRows(changes)
	mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE IBMSNAP_COMMITSEQ > \? AND IBMSNAP_COMMITSEQ <= \? ORDER BY`).
		WithArgs([]byte{1}, []byte{2}).WillReturnRows(sqlmock.NewRows(orderColumns).AddRow([]byte{2}, []byte{2}, "I", 4, 4, nil))
	var events []string
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, rs.StreamToFence(ctx, 0, func(event sqlcapture.DatabaseEvent) error {
		events = append(events, event.String())
		return nil
	}))
	require.Equal(t, []string{"Update(DB2INST1.FLOW_WATERMARKS)", "Insert(DB2INST1.ORDERS)", "Commit(02)"}, events)
	require.Equal(t, "02", hex.EncodeToString(rs.fromSeq))
}

func TestWatermarkDeadline(t *testing.T) {
	var db, _ = testDatabase(t)
	var now = time.Unix(1000, 0)
	db.now = func() time.Time { return now }
	var rs = testReplicationStream(t, db, "01")
	rs.watermarkWritten, rs.frontierChanged = now, now
	for _, tc := range []struct {
		name                     string
		started, finished, moved time.Duration
		idle, expired            bool
	}{
		{"BeforeDeadline", 4 * time.Minute, 4 * time.Minute, 0, true, false},
		{"AtDeadline", 5 * time.Minute, 5 * time.Minute, 5 * time.Minute, false, true},
		{"SlowDrain", 4 * time.Minute, 8 * time.Minute, 4 * time.Minute, false, false},
		{"IdleFrontier", 4 * time.Minute, 5 * time.Minute, 0, true, true},
		{"RecentProgress", 4 * time.Minute, 6 * time.Minute, 4 * time.Minute, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now = rs.watermarkWritten.Add(tc.finished)
			rs.frontierChanged = rs.watermarkWritten.Add(tc.moved)
			require.Equal(t, tc.expired, rs.watermarkExpired(rs.watermarkWritten.Add(tc.started), tc.idle))
		})
	}
}

func TestWatermarkCancellation(t *testing.T) {
	var db, mock = testDatabase(t)
	var rs = testReplicationStream(t, db, "01")
	mock.ExpectExec(`MERGE INTO`).WillReturnResult(sqlmock.NewResult(0, 1))
	var ctx, cancel = context.WithCancel(context.Background())
	// Cancel at the write so no wall-clock deadline determines the outcome.
	db.now = func() time.Time {
		cancel()
		return time.Unix(1000, 0)
	}
	require.ErrorIs(t, rs.StreamToFence(ctx, 0, func(sqlcapture.DatabaseEvent) error { return nil }), context.Canceled)
}

func TestMissingWatermarkWithAdvancingFrontier(t *testing.T) {
	var db, mock = testDatabase(t)
	db.config.Advanced.PollingInterval = "1ms"
	var rs = testReplicationStream(t, db, "01")
	db.registrationChecksAfter = time.Time{}
	var now = time.Unix(1000, 0)
	db.now = func() time.Time { return now }
	var watermarks, orders = testWatermarksInfo(), testOrdersInfo()
	var watermarkColumns = append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK")
	var orderColumns = append(append([]string{}, cdMetadataColumns...), "ID", "QTY", "XID")
	activateTestTable(t, rs, mock, watermarks, watermarkColumns)
	activateTestTable(t, rs, mock, orders, orderColumns)
	mock.ExpectExec(`MERGE INTO "DB2INST1"\."FLOW_WATERMARKS"`).WithArgs(db.name, &publishWatermark{}).WillReturnResult(sqlmock.NewResult(0, 1))
	expectRegistrationChecks(mock, watermarks, orders)
	for seq := byte(2); seq <= 3; seq++ {
		expectFrontier(mock, []byte{seq})
		mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE`).WithArgs([]byte{seq - 1}, []byte{seq}).
			WillReturnRows(sqlmock.NewRows(watermarkColumns).AddRow([]byte{seq}, []byte{1}, "U", db.name, "older watermark"))
		mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE`).WithArgs([]byte{seq - 1}, []byte{seq}).
			WillReturnRows(sqlmock.NewRows(orderColumns).AddRow([]byte{seq}, []byte{2}, "I", seq, seq, nil))
	}
	expectFrontier(mock, []byte{3})
	mock.ExpectQuery(`SELECT STATE FROM`).WillReturnRows(sqlmock.NewRows([]string{"STATE"}).AddRow("A"))
	var commits int
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err = rs.StreamToFence(ctx, 0, func(event sqlcapture.DatabaseEvent) error {
		if _, ok := event.(sqlcapture.CommitEvent); ok {
			commits++
			now = now.Add(6 * time.Minute)
			db.registrationChecksAfter = now.Add(time.Minute)
		}
		return nil
	})
	require.ErrorContains(t, err, "ASN Capture (asncap) does not appear to be running or is not capturing DB2INST1.FLOW_WATERMARKS")
	require.ErrorContains(t, err, "saved cursor=03, global SYNCHPOINT=03")
	require.ErrorContains(t, err, `STATE="A"`)
	require.ErrorContains(t, err, "watermark written 12m0s ago, frontier last moved 6m0s ago")
	require.Equal(t, 2, commits, "a poll started before the timeout must finish, then a fresh frontier must be drained")
	require.Equal(t, []byte{3}, rs.fromSeq)
}

func TestRegistrationCheckInterval(t *testing.T) {
	var db, mock = testDatabase(t)
	var now = time.Unix(1000, 0)
	db.now = func() time.Time { return now }
	var rs = testReplicationStream(t, db, "01")
	var info = testWatermarksInfo()
	var columns = append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK")
	activateTestTable(t, rs, mock, info, columns)
	db.registrationChecksAfter = time.Time{}
	for _, elapsed := range []time.Duration{0, 59 * time.Second, time.Minute} {
		now = time.Unix(1000, 0).Add(elapsed)
		if elapsed != 59*time.Second {
			expectRegistrationChecks(mock, info)
		}
		require.NoError(t, db.PeriodicChecks(context.Background()))
	}
	require.Equal(t, now.Add(time.Minute), db.registrationChecksAfter)
}

func TestInactiveWatermarkRegistrationDuringFence(t *testing.T) {
	var db, mock = testDatabase(t)
	db.config.Advanced.PollingInterval = "1ms"
	var rs = testReplicationStream(t, db, "01")
	db.registrationChecksAfter = time.Time{}
	var columns = append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK")
	activateTestTable(t, rs, mock, testWatermarksInfo(), columns)
	mock.ExpectExec(`MERGE INTO "DB2INST1"\."FLOW_WATERMARKS"`).WithArgs(db.name, &publishWatermark{}).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT STATE FROM "ASN"\."IBMSNAP_REGISTER" WHERE`).
		WithArgs("DB2INST1", "FLOW_WATERMARKS", "DB2INST1", "CDFLOW_WATERMARKS").
		WillReturnRows(sqlmock.NewRows([]string{"STATE"}).AddRow("I"))
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err = rs.StreamToFence(ctx, 0, func(sqlcapture.DatabaseEvent) error {
		t.Fatal("an inactive registration must stop cursor advancement")
		return nil
	})
	require.ErrorContains(t, err, `registration for DB2INST1.FLOW_WATERMARKS is missing or inactive (STATE "I")`)
	require.ErrorIs(t, err, db.registrationError)
	require.Equal(t, []byte{1}, rs.fromSeq)
}

func TestWatermarkDuringSlowProcessing(t *testing.T) {
	for _, frontier := range []byte{2, 3} {
		t.Run(fmt.Sprint(frontier), func(t *testing.T) {
			var db, mock = testDatabase(t)
			db.config.Advanced.PollingInterval = "1ms"
			var rs = testReplicationStream(t, db, "01")
			db.registrationChecksAfter = time.Time{}
			rs.batchSize = 1
			var now = time.Unix(1000, 0)
			db.now = func() time.Time { return now }
			var watermarkColumns = append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK")
			var orderColumns = append(append([]string{}, cdMetadataColumns...), "ID", "QTY", "XID")
			activateTestTable(t, rs, mock, testWatermarksInfo(), watermarkColumns)
			activateTestTable(t, rs, mock, testOrdersInfo(), orderColumns)
			var watermarkRows = sqlmock.NewRows(watermarkColumns)
			mock.ExpectExec(`MERGE INTO "DB2INST1"\."FLOW_WATERMARKS"`).
				WithArgs(db.name, &publishWatermark{rows: watermarkRows, slot: db.name, commit: []byte{3}}).WillReturnResult(sqlmock.NewResult(0, 1))
			expectRegistrationChecks(mock, testWatermarksInfo(), testOrdersInfo())
			expectFrontier(mock, []byte{frontier})
			var firstWatermarkRows = watermarkRows
			if frontier < 3 {
				firstWatermarkRows = sqlmock.NewRows(watermarkColumns)
			}
			mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE`).WillReturnRows(firstWatermarkRows)
			for i := byte(1); i <= 2; i++ {
				mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE`).
					WillReturnRows(sqlmock.NewRows(orderColumns).AddRow([]byte{2}, []byte{i}, "I", i, i, nil))
			}
			mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE`).WillReturnRows(sqlmock.NewRows(orderColumns))
			if frontier < 3 {
				expectFrontier(mock, []byte{3})
				mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE`).WillReturnRows(watermarkRows)
				mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE`).WillReturnRows(sqlmock.NewRows(orderColumns))
			}
			mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE`).WillReturnRows(sqlmock.NewRows(watermarkColumns))
			var events []string
			var ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			require.NoError(t, rs.StreamToFence(ctx, 0, func(event sqlcapture.DatabaseEvent) error {
				if change, ok := event.(*sqlcapture.OldChangeEvent); ok && change.StreamID().Table == "ORDERS" {
					now = now.Add(6 * time.Minute)
					db.registrationChecksAfter = now.Add(time.Minute)
				}
				events = append(events, event.String())
				return nil
			}))
			require.Equal(t, []string{"Insert(DB2INST1.ORDERS)", "Insert(DB2INST1.ORDERS)", "Commit(02)", "Update(DB2INST1.FLOW_WATERMARKS)", "Commit(03)"}, events)
			require.Equal(t, []byte{3}, rs.fromSeq)
		})
	}
}

func TestStalledFrontierWithPendingWatermark(t *testing.T) {
	var db, mock = testDatabase(t)
	db.config.Advanced.PollingInterval = "1ms"
	var rs = testReplicationStream(t, db, "01")
	db.registrationChecksAfter = time.Time{}
	var now = time.Unix(1000, 0)
	db.now = func() time.Time { return now }
	rs.frontierChanged = now.Add(-10 * time.Minute)
	var columns = append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK")
	activateTestTable(t, rs, mock, testWatermarksInfo(), columns)
	var changes = sqlmock.NewRows(columns)
	mock.ExpectExec(`MERGE INTO "DB2INST1"\."FLOW_WATERMARKS"`).
		WithArgs(db.name, &publishWatermark{rows: changes, slot: db.name, commit: []byte{2}}).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT STATE FROM "ASN"\."IBMSNAP_REGISTER" WHERE`).
		WithArgs("DB2INST1", "FLOW_WATERMARKS", "DB2INST1", "CDFLOW_WATERMARKS").
		WillReturnRows(sqlmock.NewRows([]string{"STATE"}).AddRow("A"))
	expectCDCatalog(mock, testWatermarksInfo(), columns, nil)
	expectFrontier(mock, []byte{1})
	expectFrontier(mock, []byte{2})
	mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE`).WithArgs([]byte{1}, []byte{2}).WillReturnRows(changes)
	var writtenAfter = now
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, rs.StreamToFence(ctx, 0, func(sqlcapture.DatabaseEvent) error { return nil }))
	require.Equal(t, writtenAfter, rs.watermarkWritten, "the timeout starts at the watermark write")
	require.Equal(t, []byte{2}, rs.fromSeq)
}

func TestCDColumnFilter(t *testing.T) {
	for _, name := range []string{"CLOB", "BLOB", "DBCLOB", "XML"} {
		t.Run(name, func(t *testing.T) {
			var source = db2ColumnType{Name: name}
			require.True(t, cdColumnHasData(source, name))
			for _, flagType := range []string{"CHARACTER", "CHAR", "VARCHAR", "", "INTEGER"} {
				require.False(t, cdColumnHasData(source, flagType))
			}
		})
	}
	require.True(t, cdColumnHasData(db2ColumnType{Name: "VARCHAR"}, "CHARACTER"))
}

func TestReplicationLOBFlags(t *testing.T) {
	var db, mock = testDatabase(t)
	var rs = testReplicationStream(t, db, "01")
	var info = testOrdersInfo()
	info.ColumnNames = []string{"ID", "TEXT", "BINARY", "GRAPHIC", "XML", "CONTENT"}
	info.Columns = map[string]sqlcapture.ColumnInfo{"ID": info.Columns["ID"]}
	for name, columnType := range map[string]string{"TEXT": "CLOB", "BINARY": "BLOB", "GRAPHIC": "DBCLOB", "XML": "XML", "CONTENT": "CLOB"} {
		info.Columns[name] = sqlcapture.ColumnInfo{Name: name, DataType: db2ColumnType{Name: columnType}}
	}
	var columns = append(append([]string{}, cdMetadataColumns...), "ID", "TEXT", "BINARY", "GRAPHIC", "XML", "CONTENT", "XID", "XTEXT", "XCONTENT")
	var types = map[string]string{"CONTENT": "CLOB", "XCONTENT": "CLOB"}
	expectCDInspection(mock, info, columns, types)
	require.NoError(t, rs.ActivateTable(context.Background(), sqlcapture.JoinStreamID(info.Schema, info.Name), info.PrimaryKey, info, nil))
	expectFrontier(mock, []byte{2})
	mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE`).WithArgs([]byte{1}, []byte{2}).
		WillReturnRows(sqlmock.NewRows(append(append([]string{}, cdMetadataColumns...), "ID", "CONTENT", "XID", "XCONTENT", "__FLOW_LOB_LENGTH_4", "__FLOW_LOB_LENGTH_6")).AddRow([]byte{2}, []byte{1}, "U", 7, "new content", 7, "old content", 11, 11))
	var changes int
	require.NoError(t, rs.pollChanges(context.Background(), func(event sqlcapture.DatabaseEvent) error {
		if change, ok := event.(*sqlcapture.OldChangeEvent); ok {
			changes++
			require.Equal(t, sqlcapture.UpdateOp, change.Operation)
			require.Equal(t, map[string]any{"ID": int64(7), "CONTENT": "new content"}, change.After)
			require.Equal(t, map[string]any{"ID": int64(7), "CONTENT": "old content"}, change.Before)
		}
		return nil
	}))
	require.Equal(t, 1, changes)
}

func TestReplicationDBCLOB(t *testing.T) {
	var db, mock = testDatabase(t)
	var rs = testReplicationStream(t, db, "01")
	var info = testOrdersInfo()
	info.ColumnNames = []string{"ID", "CONTENT"}
	info.Columns = map[string]sqlcapture.ColumnInfo{
		"ID": info.Columns["ID"], "CONTENT": {Name: "CONTENT", DataType: db2ColumnType{Name: "DBCLOB", Codepage: 1200}},
	}
	var columns = append(append([]string{}, cdMetadataColumns...), "ID", "CONTENT", "XID", "XCONTENT")
	expectCDInspection(mock, info, columns, map[string]string{"CONTENT": "DBCLOB", "XCONTENT": "DBCLOB"})
	require.NoError(t, rs.ActivateTable(context.Background(), sqlcapture.JoinStreamID(info.Schema, info.Name), info.PrimaryKey, info, nil))
	expectFrontier(mock, []byte{2})
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT "IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "ID", CLOB("CONTENT") AS "CONTENT", "XID", CLOB("XCONTENT") AS "XCONTENT", LENGTH(CLOB("CONTENT"), CODEUNITS32) AS "__FLOW_LOB_LENGTH_4", LENGTH(CLOB("XCONTENT"), CODEUNITS32) AS "__FLOW_LOB_LENGTH_6" FROM "DB2INST1"."CDORDERS" WHERE`)).
		WithArgs([]byte{1}, []byte{2}).WillReturnRows(sqlmock.NewRows(append(columns, "__FLOW_LOB_LENGTH_4", "__FLOW_LOB_LENGTH_6")).AddRow([]byte{2}, []byte{1}, "U", 1, []byte("é漢🌍"), 1, []byte("A"), 3, 1))
	var changes int
	require.NoError(t, rs.pollChanges(context.Background(), func(event sqlcapture.DatabaseEvent) error {
		if change, ok := event.(*sqlcapture.OldChangeEvent); ok {
			changes++
			require.Equal(t, "é漢🌍", change.After["CONTENT"])
			require.Equal(t, "A", change.Before["CONTENT"])
		}
		return nil
	}))
	require.Equal(t, 1, changes)
}

func TestActivateCollectionKey(t *testing.T) {
	for _, beforeImage := range []bool{false, true} {
		t.Run(fmt.Sprint(beforeImage), func(t *testing.T) {
			var db, mock = testDatabase(t)
			var rs = testReplicationStream(t, db, "01")
			var info = testOrdersInfo()
			var name = info.Columns["NAME"]
			name.IsNullable = false
			info.Columns["NAME"] = name
			var columns = append(append([]string{}, cdMetadataColumns...), "ID", "NAME", "XID")
			if beforeImage {
				columns = append(columns, "XNAME")
			}
			if beforeImage {
				expectCDInspection(mock, info, columns, nil)
			} else {
				expectCDCatalog(mock, info, columns, nil)
			}
			var err = rs.ActivateTable(context.Background(), sqlcapture.JoinStreamID(info.Schema, info.Name), []string{"ID", "NAME"}, info, nil)
			if beforeImage {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, `missing before-image column for "NAME"`)
				require.ErrorContains(t, err, "UPDATE AS DELETE INSERT ON")
				require.Empty(t, rs.tables)
			}
		})
	}
}

func TestPeriodicRegistrationChecks(t *testing.T) {
	for _, state := range []string{"A", "A ", "S", "I", "missing"} {
		t.Run(state, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var rs = testReplicationStream(t, db, "01")
			db.registrationChecksAfter = time.Time{}
			var columns = append(append([]string{}, cdMetadataColumns...), "ID", "XID")
			activateTestTable(t, rs, mock, testOrdersInfo(), columns)
			var rows = sqlmock.NewRows([]string{"STATE"})
			if state != "missing" {
				rows.AddRow(state)
			}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT STATE FROM "ASN"."IBMSNAP_REGISTER" WHERE SOURCE_OWNER = ? AND SOURCE_TABLE = ? AND CD_OWNER = ? AND CD_TABLE = ?`)).
				WithArgs("DB2INST1", "ORDERS", "DB2INST1", "CDORDERS").WillReturnRows(rows)
			if strings.TrimSpace(state) == "A" {
				expectCDCatalog(mock, testOrdersInfo(), columns, nil)
			}
			var err = db.PeriodicChecks(context.Background())
			if state == "A" || state == "A " {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "DB2INST1.ORDERS")
				require.ErrorContains(t, err, "reactivate the registration and re-backfill the binding")
				require.ErrorIs(t, rs.pollChanges(context.Background(), func(sqlcapture.DatabaseEvent) error {
					t.Fatal("an inactive registration must stop cursor advancement")
					return nil
				}), err)
				require.Equal(t, []byte{1}, rs.fromSeq)
			}
		})
	}
}
