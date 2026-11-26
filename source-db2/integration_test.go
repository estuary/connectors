package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIntegrationLOBBoundaries(t *testing.T) {
	var cfg = integrationConfig(t)
	cfg.Advanced.BackfillChunkSize = 1
	var db = &db2Database{config: cfg}
	var ctx, cancel = context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	require.NoError(t, db.connect(ctx))
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	var info = &sqlcapture.DiscoveryInfo{
		Schema: strings.ToUpper(cfg.User), Name: "FLOW_LOB_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		ColumnNames: []string{"ID", "TEXT", "BINARY", "GRAPHIC", "XML"},
		Columns: map[string]sqlcapture.ColumnInfo{
			"ID":      {Name: "ID", DataType: db2ColumnType{Name: "INTEGER"}},
			"TEXT":    {Name: "TEXT", DataType: db2ColumnType{Name: "CLOB"}},
			"BINARY":  {Name: "BINARY", DataType: db2ColumnType{Name: "BLOB"}},
			"GRAPHIC": {Name: "GRAPHIC", DataType: db2ColumnType{Name: "DBCLOB", Codepage: 1200}},
			"XML":     {Name: "XML", DataType: db2ColumnType{Name: "XML"}},
		},
	}
	var table = quoteTableName(info.Schema, info.Name)
	var _, err = db.conn.ExecContext(ctx, "CREATE TABLE "+table+` (ID INTEGER NOT NULL PRIMARY KEY, "TEXT" CLOB(1M), "BINARY" BLOB(1M), "GRAPHIC" DBCLOB(1M), "XML" XML)`)
	require.NoError(t, err)
	t.Cleanup(func() {
		var _, err = db.conn.ExecContext(context.Background(), "DROP TABLE "+table)
		require.NoError(t, err)
	})
	var sizes = []int{0, 65534, 65535, 65536, 131070, 131071, 131072, 196606, 196607, 196608}
	for id, size := range sizes {
		var xmlValue = "NULL"
		if size > 0 {
			xmlValue = fmt.Sprintf("XMLPARSE(DOCUMENT CLOB('<x>') || REPEAT(CLOB('a'), %d) || '</x>')", size-7)
		}
		var query = fmt.Sprintf("INSERT INTO %s VALUES (?, REPEAT(CLOB('a'), %d), BLOB(REPEAT(CLOB('b'), %d)), DBCLOB(REPEAT(CLOB('漢'), %d) || REPEAT(CLOB('a'), %d)), %s)", table, size, size, size/3, size%3, xmlValue)
		_, err = db.conn.ExecContext(ctx, query, id)
		require.NoError(t, err)
	}
	for _, mode := range []string{sqlcapture.TableStatePreciseBackfill, sqlcapture.TableStateKeylessBackfill, "KeylessWithoutKey"} {
		t.Run(mode, func(t *testing.T) {
			var state = &sqlcapture.TableState{Mode: mode, KeyColumns: []string{"ID"}}
			if mode == "KeylessWithoutKey" {
				var _, err = db.conn.ExecContext(ctx, "ALTER TABLE "+table+" DROP PRIMARY KEY")
				require.NoError(t, err)
				state.Mode, state.KeyColumns = sqlcapture.TableStateKeylessBackfill, nil
			}
			var seen = make(map[int]bool)
			for {
				var complete, cursor, err = db.ScanTableChunk(ctx, info, state, "", func(event sqlcapture.ChangeEvent) error {
					var data, err = event.AppendJSON(nil)
					require.NoError(t, err)
					var doc map[string]any
					require.NoError(t, json.Unmarshal(data, &doc))
					var id = int(doc["ID"].(float64))
					require.False(t, seen[id], "duplicate row %d", id)
					seen[id] = true
					var size = sizes[id]
					require.Equal(t, strings.Repeat("a", size), doc["TEXT"])
					require.Equal(t, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("b", size))), doc["BINARY"])
					require.Equal(t, strings.Repeat("漢", size/3)+strings.Repeat("a", size%3), doc["GRAPHIC"])
					if size == 0 {
						require.Nil(t, doc["XML"])
					} else {
						var value struct {
							XMLName xml.Name `xml:"x"`
							Text    string   `xml:",chardata"`
						}
						require.IsType(t, "", doc["XML"])
						require.NoError(t, xml.Unmarshal([]byte(doc["XML"].(string)), &value))
						require.Equal(t, strings.Repeat("a", size-7), value.Text)
					}
					return nil
				})
				require.NoError(t, err)
				if complete {
					break
				}
				state.Scanned, state.BackfilledCount = cursor, len(seen)
			}
			require.Len(t, seen, len(sizes))
		})
	}
}

func integrationConfig(t *testing.T) *Config {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skip("set TEST_DATABASE=yes to run against Db2")
	}
	var cfg = &Config{Address: *dbCaptureAddress, User: *dbCaptureUser, Password: os.Getenv("TEST_DB2_PASSWORD"), Database: *dbName}
	require.NotEmpty(t, cfg.Password, "set TEST_DB2_PASSWORD to the fixture database password")
	cfg.SetDefaults()
	return cfg
}

// waitPublished fences preceding writes using a unique watermark and the global
// SYNCHPOINT, so old CD rows cannot leak into a new test's backfill.
func waitPublished(t *testing.T, ctx context.Context, db *db2Database) {
	t.Helper()
	var watermark = uuid.NewString()
	require.NoError(t, db.WriteWatermark(ctx, watermark))
	var table = db.WatermarksTable()
	var tables, err = db.getTables(ctx, []sqlcapture.TableID{{Schema: table.Schema, Table: table.Table}})
	require.NoError(t, err)
	require.Len(t, tables, 1)
	var registration = tables[0].ExtraDetails.(*db2TableDiscoveryDetails).Registration
	require.NotNil(t, registration)
	var query = fmt.Sprintf(`SELECT COUNT(*) FROM %s C, %s R
  WHERE R.GLOBAL_RECORD = 'Y' AND C.SLOT = ? AND C.WATERMARK = ? AND C.IBMSNAP_COMMITSEQ <= R.SYNCHPOINT`,
		quoteTableName(registration.CDSchema, registration.CDTable), quoteTableName(db.config.Advanced.CaptureSchema, "IBMSNAP_REGISTER"))
	var ticker = time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		require.NoError(t, db.conn.QueryRowContext(ctx, query, db.name, watermark).Scan(&count))
		if count > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("ASN Capture did not publish the test watermark: ", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestIntegrationCDC(t *testing.T) {
	var cfg = integrationConfig(t)
	cfg.Advanced.PollingInterval = "100ms"
	var ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var configJSON, err = json.Marshal(cfg)
	require.NoError(t, err)
	database, err := connectDB2(ctx, "db2-integration-"+uuid.NewString(), configJSON, common.CreatedAt{})
	require.NoError(t, err)
	var db = database.(*db2Database)
	defer db.Close(context.Background())
	var tables = []string{"CDC_BI", "CDC_DI"}
	var execute = func(statement string) {
		t.Helper()
		var tx, err = db.conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		for _, table := range tables {
			_, err = tx.ExecContext(ctx, fmt.Sprintf(statement, quoteTableName("DB2INST1", table)))
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
	}
	execute("DELETE FROM %s")
	waitPublished(t, ctx, db)
	execute("INSERT INTO %s VALUES (1, 'backfill', 10)")
	waitPublished(t, ctx, db)

	var spec = &st.CaptureSpec{Driver: db2Driver, EndpointSpec: cfg, Validator: &st.OrderedCaptureValidator{}}
	for _, table := range tables {
		var resource, err = json.Marshal(sqlcapture.Resource{Namespace: "DB2INST1", Stream: table})
		require.NoError(t, err)
		spec.Bindings = append(spec.Bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: resource, ResourcePath: []string{"DB2INST1", table}, StateKey: table,
			Collection: pf.CollectionSpec{Name: pf.Collection("fixture/" + table), Key: []string{"/ID"}},
		})
	}
	_, err = spec.Validate(ctx, t)
	require.NoError(t, err)
	var steps = []struct {
		name, sql string
		documents []string
	}{
		{"Backfill", "", []string{`{"ID":1,"NAME":"backfill","QTY":10,"op":"c"}`}},
		{"Insert", "INSERT INTO %s VALUES (2, 'insert', 20)", []string{`{"ID":2,"NAME":"insert","QTY":20,"op":"c"}`}},
		{"Update", "UPDATE %s SET NAME = 'updated', QTY = 30 WHERE ID = 2", []string{`{"ID":2,"NAME":"updated","QTY":30,"op":"u"}`}},
		{"KeyChangeAfterRestart", "UPDATE %s SET ID = 3 WHERE ID = 2", []string{`{"ID":2,"NAME":"updated","QTY":30,"op":"d"}`, `{"ID":3,"NAME":"updated","QTY":30,"op":"c"}`}},
		{"Delete", "DELETE FROM %s WHERE ID = 3", []string{`{"ID":3,"NAME":"updated","QTY":30,"op":"d"}`}},
		{"TransactionBothTables", "UPDATE %s SET NAME = 'transaction', QTY = 40 WHERE ID = 1", []string{`{"ID":1,"NAME":"transaction","QTY":40,"op":"u"}`}},
	}
	t.Setenv("SHUTDOWN_AFTER_POLLING", "")
	var step int
	for _, stop := range []int{3, len(steps)} {
		var captureCtx, stopCapture = context.WithCancel(ctx)
		var documents = make(map[string][]string)
		t.Log("step:", steps[step].name)
		if step > 0 {
			execute(steps[step].sql)
		}
		spec.Capture(captureCtx, t, func(data json.RawMessage) {
			var doc map[string]any
			require.NoError(t, json.Unmarshal(data, &doc))
			if meta, ok := doc["_meta"].(map[string]any); ok {
				var table = meta["source"].(map[string]any)["table"].(string)
				doc["op"] = meta["op"]
				delete(doc, "_meta")
				normalized, err := json.Marshal(doc)
				require.NoError(t, err)
				documents[table] = append(documents[table], string(normalized))
				return
			}
			if len(documents) == 0 {
				return
			}
			var state sqlcapture.PersistentState
			require.NoError(t, json.Unmarshal(data, &state))
			if step == 0 {
				for _, table := range state.Streams {
					if table.Mode != sqlcapture.TableStateActive {
						return
					}
				}
			}
			// Both tables were changed in one transaction, so both must be in this checkpoint.
			require.Len(t, documents, len(tables), steps[step].name)
			for _, table := range tables {
				require.Equal(t, steps[step].documents, documents[table], "%s/%s", steps[step].name, table)
			}
			documents = make(map[string][]string)
			step++
			if step == stop {
				stopCapture()
				return
			}
			t.Log("step:", steps[step].name)
			execute(steps[step].sql)
		})
		stopCapture()
		require.Empty(t, spec.Errors)
		require.Equal(t, stop, step)
		require.NotEmpty(t, spec.Checkpoint)
		spec.Reset()
	}
	// Drain through another fence: successful resume must emit no old documents.
	t.Setenv("SHUTDOWN_AFTER_POLLING", "yes")
	var repeated int
	spec.Capture(ctx, t, func(data json.RawMessage) {
		var doc map[string]any
		require.NoError(t, json.Unmarshal(data, &doc))
		if _, ok := doc["_meta"]; ok {
			repeated++
		}
	})
	require.Empty(t, spec.Errors)
	require.Zero(t, repeated, "resuming a committed checkpoint must not repeat documents")
}
