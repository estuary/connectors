package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/estuary/connectors/go/common"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// Catalog and privilege checks have separate tests. This adapter exercises the
// capture protocol, backfill, watermark fencing, and committed state together.
type captureTestDatabase struct {
	*db2Database
	discovery map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo
}

func (db *captureTestDatabase) DiscoverTableDetails(ctx context.Context, tables []sqlcapture.TableID) (map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, error) {
	if db.discovery == nil {
		return db.db2Database.DiscoverTableDetails(ctx, tables)
	}
	return db.discovery, nil
}

func (db *captureTestDatabase) SetupPrerequisites(context.Context) []error { return nil }

func (db *captureTestDatabase) SetupTablePrerequisites(context.Context, []sqlcapture.TableID) map[sqlcapture.TableID]error {
	return nil
}

func (db *captureTestDatabase) EstimatedRowCounts(context.Context, []sqlcapture.TableID) (map[sqlcapture.TableID]int, error) {
	return nil, nil
}

func TestCaptureBackfill(t *testing.T) {
	// The common capture test helper is opt-in even when its database is mocked.
	t.Setenv("TEST_DATABASE", "yes")
	t.Setenv("SHUTDOWN_AFTER_POLLING", "yes")
	var db, mock = testDatabase(t)
	db.name = "fixture/capture"
	db.config.Advanced.PollingInterval = "1ms"
	mock.MatchExpectationsInOrder(false)
	var orders, lines, watermarks = testOrdersInfo(), testLinesInfo(), testWatermarksInfo()
	var discovery = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo)
	for _, info := range []*sqlcapture.DiscoveryInfo{orders, lines, watermarks} {
		discovery[sqlcapture.JoinStreamID(info.Schema, info.Name)] = info
	}
	var drv = *db2Driver
	drv.Connect = func(context.Context, string, json.RawMessage, common.CreatedAt) (sqlcapture.Database, error) {
		return &captureTestDatabase{db2Database: db, discovery: discovery}, nil
	}
	var spec = &st.CaptureSpec{Driver: &drv, EndpointSpec: db.config, Validator: &st.SortedCaptureValidator{}}
	for _, info := range []*sqlcapture.DiscoveryInfo{orders, lines} {
		var resource, err = json.Marshal(sqlcapture.Resource{Namespace: info.Schema, Stream: info.Name})
		require.NoError(t, err)
		var key []string
		for _, name := range info.PrimaryKey {
			key = append(key, "/"+name)
		}
		spec.Bindings = append(spec.Bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: resource, ResourcePath: []string{info.Schema, info.Name}, StateKey: info.Name,
			Collection: pf.CollectionSpec{Name: pf.Collection("fixture/" + info.Name), Key: key},
		})
	}
	var watermarkColumns = append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK")
	var orderColumns = append(append([]string{}, cdMetadataColumns...), "ID", "NAME", "QTY", "XID")
	var lineColumns = append(append([]string{}, cdMetadataColumns...), lines.ColumnNames...)
	expectFrontier(mock, []byte{1})
	expectCDInspection(mock, testWatermarksInfo(), watermarkColumns, nil)
	expectCDInspection(mock, testOrdersInfo(), orderColumns, nil)
	expectCDInspection(mock, testLinesInfo(), lineColumns, nil)
	expectRegistrationChecks(mock, watermarks)

	// One catch-up fence, then a fence after each table's backfill.
	for seq := byte(2); seq <= 4; seq++ {
		var changes = sqlmock.NewRows(watermarkColumns)
		mock.ExpectExec(`MERGE INTO "DB2INST1"\."FLOW_WATERMARKS"`).
			WithArgs(db.name, &publishWatermark{rows: changes, slot: db.name, commit: []byte{seq}}).WillReturnResult(sqlmock.NewResult(0, 1))
		expectFrontier(mock, []byte{seq})
		mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE IBMSNAP_COMMITSEQ >`).
			WithArgs([]byte{seq - 1}, []byte{seq}).WillReturnRows(changes)
		if seq > 2 {
			mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE IBMSNAP_COMMITSEQ >`).
				WithArgs([]byte{seq - 1}, []byte{seq}).WillReturnRows(sqlmock.NewRows(orderColumns))
			mock.ExpectQuery(`FROM "DB2INST1"\."CDLINES" WHERE IBMSNAP_COMMITSEQ >`).
				WithArgs([]byte{seq - 1}, []byte{seq}).WillReturnRows(sqlmock.NewRows(lineColumns))
		}
	}
	var created = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	mock.ExpectQuery(`FROM "DB2INST1"\."ORDERS" AS T ORDER BY T."ID" FETCH FIRST 50000 ROWS ONLY`).
		WillReturnRows(sqlmock.NewRows(orders.ColumnNames).
			AddRow(1, []byte("apple"), 1, []byte("1.50"), created, []byte("first")).
			AddRow(2, []byte("pear"), 2, []byte("2.25"), created.Add(time.Second), nil).
			AddRow(3, []byte("plum"), 3, []byte("3.00"), created.Add(2*time.Second), []byte("third")))
	mock.ExpectQuery(`FROM "DB2INST1"\."LINES" AS T ORDER BY T."ORDER_ID", T."LINE_NO" FETCH FIRST 50000 ROWS ONLY`).
		WillReturnRows(sqlmock.NewRows(lines.ColumnNames).
			AddRow(1, 1, []byte("SKU-A   "), 5).AddRow(1, 2, []byte("SKU-B   "), 6).AddRow(2, 1, []byte("SKU-C   "), 7))
	mock.ExpectClose()

	var documents []map[string]any
	var ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var observe = func(data json.RawMessage) {
		var doc map[string]any
		require.NoError(t, json.Unmarshal(data, &doc))
		if _, ok := doc["_meta"]; ok {
			documents = append(documents, doc)
		}
	}
	spec.Capture(ctx, t, observe)
	require.Empty(t, spec.Errors)
	require.Len(t, documents, 6)
	var quantities = make(map[string][]float64)
	for _, doc := range documents {
		var meta = doc["_meta"].(map[string]any)
		require.Equal(t, "c", meta["op"])
		var source = meta["source"].(map[string]any)
		require.Equal(t, true, source["snapshot"])
		var table = source["table"].(string)
		quantities[table] = append(quantities[table], doc["QTY"].(float64))
	}
	require.Equal(t, map[string][]float64{"ORDERS": {1, 2, 3}, "LINES": {5, 6, 7}}, quantities)
	var state sqlcapture.PersistentState
	require.NoError(t, json.Unmarshal(spec.Checkpoint, &state))
	require.JSONEq(t, `"04"`, string(state.Cursor))
	for _, name := range []string{"ORDERS", "LINES"} {
		require.Equal(t, sqlcapture.TableStateActive, state.Streams[boilerplate.StateKey(name)].Mode)
		require.Equal(t, 3, state.Streams[boilerplate.StateKey(name)].BackfilledCount)
		require.Nil(t, state.Streams[boilerplate.StateKey(name)].Scanned)
	}
	require.NoError(t, mock.ExpectationsWereMet())

	// Reopen from the committed checkpoint: neither source table should be scanned again.
	db, mock = testDatabase(t)
	db.name = "fixture/capture"
	db.config.Advanced.PollingInterval = "1ms"
	mock.MatchExpectationsInOrder(false)
	expectCDInspection(mock, testWatermarksInfo(), watermarkColumns, nil)
	expectCDInspection(mock, testOrdersInfo(), orderColumns, nil)
	expectCDInspection(mock, testLinesInfo(), lineColumns, nil)
	expectRegistrationChecks(mock, watermarks, lines, orders)

	var changes = sqlmock.NewRows(watermarkColumns)
	mock.ExpectExec(`MERGE INTO "DB2INST1"\."FLOW_WATERMARKS"`).
		WithArgs(db.name, &publishWatermark{rows: changes, slot: db.name, commit: []byte{5}}).WillReturnResult(sqlmock.NewResult(0, 1))
	expectFrontier(mock, []byte{5})
	mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE IBMSNAP_COMMITSEQ >`).
		WithArgs([]byte{4}, []byte{5}).WillReturnRows(changes)
	mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE IBMSNAP_COMMITSEQ >`).
		WithArgs([]byte{4}, []byte{5}).WillReturnRows(sqlmock.NewRows(orderColumns))
	mock.ExpectQuery(`FROM "DB2INST1"\."CDLINES" WHERE IBMSNAP_COMMITSEQ >`).
		WithArgs([]byte{4}, []byte{5}).WillReturnRows(sqlmock.NewRows(lineColumns))
	mock.ExpectClose()
	spec.Reset()
	documents = nil
	spec.Capture(ctx, t, observe)
	require.Empty(t, spec.Errors)
	require.Empty(t, documents, "resuming a completed backfill must not emit previously captured rows")
	require.NoError(t, json.Unmarshal(spec.Checkpoint, &state))
	require.JSONEq(t, `"05"`, string(state.Cursor))
}

func TestCaptureCollectionKeyOrdering(t *testing.T) {
	t.Setenv("TEST_DATABASE", "yes")
	t.Setenv("SHUTDOWN_AFTER_POLLING", "yes")
	var shutdown = sqlcapture.TestShutdownAfterBackfill
	sqlcapture.TestShutdownAfterBackfill = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterBackfill = shutdown })
	var spec = &st.CaptureSpec{
		Validator: &st.SortedCaptureValidator{},
		Bindings: []*pf.CaptureSpec_Binding{{
			ResourceConfigJson: json.RawMessage(`{"namespace":"DB2INST1","stream":"ORDERS"}`),
			ResourcePath:       []string{"DB2INST1", "ORDERS"}, StateKey: "ORDERS",
			Collection: pf.CollectionSpec{Name: "fixture/orders", Key: []string{"/PRICE", "/ID"}},
		}},
	}
	var watermarkColumns = append(append([]string{}, cdMetadataColumns...), "SLOT", "WATERMARK")
	var orderColumns = append(append([]string{}, cdMetadataColumns...), "ID", "PRICE", "XID", "XPRICE")
	var prices []string
	var modes []string
	for attempt := 0; attempt < 3; attempt++ {
		var db, mock = testDatabase(t)
		db.name = "fixture/capture"
		db.config.Advanced.BackfillChunkSize = 1
		db.config.Advanced.PollingInterval = "1ms"
		mock.MatchExpectationsInOrder(false)
		var drv = *db2Driver
		drv.Connect = func(context.Context, string, json.RawMessage, common.CreatedAt) (sqlcapture.Database, error) {
			return &captureTestDatabase{db2Database: db}, nil
		}
		spec.Driver, spec.EndpointSpec = &drv, db.config
		mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).
			WillReturnRows(sqlmock.NewRows(tableDetailColumns).
				AddRow("DB2INST1", "ORDERS", "T", "Y", "DB2INST1", "CDORDERS", "A", "N", "X").
				AddRow("DB2INST1", "FLOW_WATERMARKS", "T", "Y", "DB2INST1", "CDFLOW_WATERMARKS", "A", "N", ""))
		mock.ExpectQuery(`FROM SYSCAT.COLUMNS C LEFT JOIN SYSCAT.DATATYPES D .* WHERE`).
			WillReturnRows(sqlmock.NewRows(columnDetailColumns).
				AddRow("DB2INST1", "ORDERS", "ID", 0, "N", "INTEGER", 4, 0, 0, nil).
				AddRow("DB2INST1", "ORDERS", "PRICE", 1, "N", "DECIMAL", 10, 2, 0, nil).
				AddRow("DB2INST1", "FLOW_WATERMARKS", "SLOT", 0, "N", "VARCHAR", 1000, 0, 1208, nil).
				AddRow("DB2INST1", "FLOW_WATERMARKS", "WATERMARK", 1, "Y", "VARCHAR", 1000, 0, 1208, nil))
		mock.ExpectQuery(`JOIN SYSCAT.TABCONST`).
			WillReturnRows(sqlmock.NewRows(primaryKeyColumns).
				AddRow("DB2INST1", "ORDERS", "ID", 1).
				AddRow("DB2INST1", "FLOW_WATERMARKS", "SLOT", 1))
		expectCDInspection(mock, testWatermarksInfo(), watermarkColumns, nil)
		expectCDInspection(mock, testOrdersInfo(), orderColumns, nil)
		expectRegistrationChecks(mock, testWatermarksInfo())
		if attempt > 0 {
			expectRegistrationChecks(mock, testOrdersInfo())
		}

		var cursor = byte(1 + 2*attempt)
		if attempt == 0 {
			expectFrontier(mock, []byte{cursor})
		}
		for seq := cursor + 1; seq <= cursor+2; seq++ {
			var changes = sqlmock.NewRows(watermarkColumns)
			mock.ExpectExec(`MERGE INTO "DB2INST1"\."FLOW_WATERMARKS"`).
				WithArgs(db.name, &publishWatermark{rows: changes, slot: db.name, commit: []byte{seq}}).WillReturnResult(sqlmock.NewResult(0, 1))
			expectFrontier(mock, []byte{seq})
			mock.ExpectQuery(`FROM "DB2INST1"\."CDFLOW_WATERMARKS" WHERE IBMSNAP_COMMITSEQ >`).
				WithArgs([]byte{seq - 1}, []byte{seq}).WillReturnRows(changes)
			if seq > 2 {
				mock.ExpectQuery(`FROM "DB2INST1"\."CDORDERS" WHERE IBMSNAP_COMMITSEQ >`).
					WithArgs([]byte{seq - 1}, []byte{seq}).WillReturnRows(sqlmock.NewRows(orderColumns))
			}
		}
		var rows = sqlmock.NewRows([]string{"ID", "PRICE"})
		if attempt < 2 {
			rows.AddRow(attempt+1, []byte([]string{"2.00", "10.00"}[attempt]))
		}
		var scan = mock.ExpectQuery(regexp.QuoteMeta(`SELECT "ID", "PRICE" FROM "DB2INST1"."ORDERS" AS T`)).WillReturnRows(rows)
		if attempt > 0 {
			var previous = []string{"2.00", "10.00"}[attempt-1]
			scan.WithArgs(previous, previous, int64(attempt))
		}
		mock.ExpectClose()
		var ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		spec.Capture(ctx, t, func(data json.RawMessage) {
			var doc map[string]any
			require.NoError(t, json.Unmarshal(data, &doc))
			if price, ok := doc["PRICE"].(string); ok {
				prices = append(prices, price)
			}
		})
		cancel()
		require.Empty(t, spec.Errors)
		var state sqlcapture.PersistentState
		require.NoError(t, json.Unmarshal(spec.Checkpoint, &state))
		var table = state.Streams["ORDERS"]
		modes = append(modes, table.Mode)
		if attempt < 2 {
			require.Equal(t, tuple.Tuple{prices[attempt], int64(attempt + 1)}.Pack(), table.Scanned)
		} else {
			require.Equal(t, 2, table.BackfilledCount)
			require.Nil(t, table.Scanned)
		}
		require.NoError(t, mock.ExpectationsWereMet())
		spec.Reset()
	}
	require.Equal(t, []string{"2.00", "10.00"}, prices)
	require.Equal(t, []string{sqlcapture.TableStateUnfilteredBackfill, sqlcapture.TableStateUnfilteredBackfill, sqlcapture.TableStateActive}, modes)
}

// Stop after the framework resolves the key, before any SQL or backfill work.
type keyTestDatabase struct {
	*captureTestDatabase
	activated []string
	info      *sqlcapture.DiscoveryInfo
}

func (db *keyTestDatabase) ReplicationStream(context.Context, json.RawMessage) (sqlcapture.ReplicationStream, error) {
	return &keyTestStream{db: db}, nil
}

type keyTestStream struct {
	sqlcapture.ReplicationStream
	db *keyTestDatabase
}

func (rs *keyTestStream) StartReplication(context.Context, map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	return nil
}
func (rs *keyTestStream) StreamToFence(context.Context, time.Duration, func(sqlcapture.DatabaseEvent) error) error {
	return nil
}
func (rs *keyTestStream) Close(context.Context) error                        { return nil }
func (rs *keyTestStream) Acknowledge(context.Context, json.RawMessage) error { return nil }
func (rs *keyTestStream) ActivateTable(_ context.Context, _ sqlcapture.StreamID, key []string, info *sqlcapture.DiscoveryInfo, _ json.RawMessage) error {
	rs.db.activated, rs.db.info = key, info
	return fmt.Errorf("stop after activation")
}

func TestEffectiveCollectionKeyMatchesFramework(t *testing.T) {
	t.Setenv("TEST_DATABASE", "yes")
	for _, tc := range []struct {
		name                 string
		collection, override []string
		mode                 sqlcapture.BackfillMode
		unpredictable        bool
	}{
		{"Integer", []string{"/ID"}, nil, "", false},
		{"Decimal", []string{"/PRICE", "/ID"}, nil, "", true},
		{"Temporal", []string{"/CREATED", "/ID"}, nil, "", true},
		{"Text", []string{"/NAME", "/ID"}, nil, "", true},
		{"ResourceDecimal", []string{"/ID"}, []string{"PRICE", "ID"}, "", true},
		{"ResourceInteger", []string{"/PRICE", "/ID"}, []string{"ID"}, "", false},
		{"Escaped", []string{"/A~1B~01", "/ID"}, nil, "", true},
		{"Fallback", []string{"/_meta/source/cursor", "/_meta/source/intent"}, nil, sqlcapture.BackfillModeOnlyChanges, false},
		{"WithoutKey", []string{"/ID"}, []string{"ID"}, sqlcapture.BackfillModeWithoutKey, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var db, mock = testDatabase(t)
			var info = testOrdersInfo()
			info.ColumnNames = append(info.ColumnNames, "A/B~1")
			info.Columns["A/B~1"] = sqlcapture.ColumnInfo{Name: "A/B~1", DataType: db2ColumnType{Name: "VARCHAR", Codepage: 1208}}
			var database = &keyTestDatabase{captureTestDatabase: &captureTestDatabase{db2Database: db}}
			mock.ExpectQuery(`FROM SYSCAT.TABLES T LEFT JOIN`).WillReturnRows(sqlmock.NewRows(tableDetailColumns).
				AddRow("DB2INST1", "ORDERS", "T", "Y", "DB2INST1", "CDORDERS", "A", "N", "X"))
			var columns = sqlmock.NewRows(columnDetailColumns)
			for index, name := range info.ColumnNames {
				var ct = info.Columns[name].DataType.(db2ColumnType)
				columns.AddRow(info.Schema, info.Name, name, index, "N", ct.Name, ct.Length, ct.Scale, ct.Codepage, nil)
			}
			mock.ExpectQuery(`FROM SYSCAT.COLUMNS C LEFT JOIN`).WillReturnRows(columns)
			mock.ExpectQuery(`JOIN SYSCAT.TABCONST`).WillReturnRows(sqlmock.NewRows(primaryKeyColumns).AddRow("DB2INST1", "ORDERS", "NAME", 1))
			var drv = *db2Driver
			drv.Connect = func(context.Context, string, json.RawMessage, common.CreatedAt) (sqlcapture.Database, error) {
				return database, nil
			}
			var resource, err = json.Marshal(sqlcapture.Resource{Namespace: info.Schema, Stream: info.Name, PrimaryKey: tc.override, Mode: tc.mode})
			require.NoError(t, err)
			var spec = &st.CaptureSpec{Driver: &drv, EndpointSpec: db.config, Validator: &st.SortedCaptureValidator{},
				Bindings: []*pf.CaptureSpec_Binding{{ResourceConfigJson: resource, ResourcePath: []string{info.Schema, info.Name}, StateKey: info.Name,
					Collection: pf.CollectionSpec{Name: "fixture/orders", Key: tc.collection}}},
			}
			mock.ExpectClose()
			spec.Capture(context.Background(), t, nil)
			require.Len(t, spec.Errors, 1)
			require.ErrorContains(t, spec.Errors[0], "stop after activation")
			require.Equal(t, database.activated, db.tableKey(database.info), "connector and sqlcapture must resolve the same key")
			require.Equal(t, tc.unpredictable, database.info.UnpredictableKeyOrdering)
			require.Equal(t, []string{"NAME"}, database.info.PrimaryKey)
		})
	}
}
