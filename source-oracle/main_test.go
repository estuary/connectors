package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/estuary/flow/go/protocols/flow"
	_ "github.com/sijms/go-ora/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const testSchemaName = "admin"

func TestMain(m *testing.M) {
	flag.Parse()

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		level, err := log.ParseLevel(logLevel)
		if err != nil {
			log.WithField("level", logLevel).Fatal("invalid log level")
		}
		log.SetLevel(level)
	} else {
		log.SetLevel(log.DebugLevel)
	}

	os.Exit(m.Run())
}

func oracleTestBackend(t testing.TB) *testBackend {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil
	}

	var ctx = context.Background()
	var sops = exec.CommandContext(ctx, "sops", "--decrypt", "--output-type", "json", "config.yaml")
	var configRaw, err = sops.Output()
	require.NoError(t, err)
	var config Config
	err = json.Unmarshal(configRaw, &config)
	require.NoError(t, err)

	config.Advanced.BackfillChunkSize = 16
	if err := config.Validate(); err != nil {
		t.Fatalf("error validating capture config: %v", err)
	}
	config.SetDefaults("test")

	// Open control connection
	db, err := connectOracle(ctx, "test", configRaw)
	log.WithFields(log.Fields{
		"user": config.User,
		"addr": config.Address,
	}).Info("opening control connection")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close(ctx) })

	var conn = db.(*oracleDatabase).conn
	return &testBackend{control: conn, config: config}
}

type testBackend struct {
	control *sql.DB // The open control connection to use for test setup
	config  Config  // Default capture configuration for test captures
}

func (tb *testBackend) CaptureSpec(ctx context.Context, t testing.TB, streamMatchers ...*regexp.Regexp) *st.CaptureSpec {
	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|-[0-9]+:[0-9]+)"`)
	sanitizers[`"scn":11111111`] = regexp.MustCompile(`"scn":([0-9]+)`)
	sanitizers[`"cursor":"11111111"`] = regexp.MustCompile(`"cursor":"([0-9]+)"`)
	sanitizers[`"row_id":"AAAAAAAAAAAAAAAAAA"`] = regexp.MustCompile(`"row_id":"[^"]+"`)
	sanitizers[`"ts_ms":1111111111111`] = regexp.MustCompile(`"ts_ms":[0-9]+`)

	var cfg = tb.config
	var cs = &st.CaptureSpec{
		Driver:       oracleDriver,
		EndpointSpec: &cfg,
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   sanitizers,
	}
	if len(streamMatchers) > 0 {
		cs.Bindings = tests.DiscoverBindings(ctx, t, tb, streamMatchers...)
	}
	return cs
}

// CreateTable is a test helper for creating a new database table and returning the
// name of the new table. The table is named "test_<testName>", or "test_<testName>_<suffix>"
// if the suffix is non-empty.
func (tb *testBackend) CreateTable(ctx context.Context, t testing.TB, suffix string, tableDef string) string {
	t.Helper()

	var tableName = testSchemaName + "."
	if suffix != "" {
		tableName += fmt.Sprintf(`"t%s"`, suffix)
	} else {
		tableName += fmt.Sprintf(`"%s"`, strings.TrimPrefix(t.Name(), "Test"))
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}
	tableName = strings.ToLower(tableName)

	log.WithFields(log.Fields{"table": tableName, "cols": tableDef}).Debug("creating test table")
	tb.Query(ctx, t, false, fmt.Sprintf(`DROP TABLE %s`, tableName))
	tb.Query(ctx, t, true, fmt.Sprintf(`CREATE TABLE %s %s`, tableName, tableDef))
	t.Cleanup(func() {
		log.WithField("table", tableName).Debug("destroying test table")
		tb.Query(ctx, t, false, fmt.Sprintf(`DROP TABLE %s`, tableName))
	})
	return tableName
}

func (tb *testBackend) Insert(ctx context.Context, t testing.TB, table string, rows [][]any) {
	t.Helper()

	if len(rows) < 1 {
		t.Fatalf("must insert at least one row")
	}
	log.WithFields(log.Fields{"table": table, "count": len(rows), "first": rows[0]}).Debug("inserting data")
	for _, row := range rows {
		log.WithFields(log.Fields{"table": table, "row": row}).Trace("inserting row")
		if len(row) != len(rows[0]) {
			t.Fatalf("incorrect number of values in row %q (expected %d)", row, len(rows[0]))
		}
		var query = fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, argsTuple(row))
		log.WithFields(log.Fields{"query": query}).Debug("inserting data")
		tb.Query(ctx, t, true, query)
	}
}

func (tb *testBackend) Update(ctx context.Context, t testing.TB, table string, whereCol string, whereVal any, setCol string, setVal any) {
	t.Helper()
	tb.Query(ctx, t, true, fmt.Sprintf(`UPDATE %s SET %s = :1 WHERE %s = :2`, table, setCol, whereCol), setVal, whereVal)
}

func (tb *testBackend) Delete(ctx context.Context, t testing.TB, table string, whereCol string, whereVal any) {
	t.Helper()
	tb.Query(ctx, t, true, fmt.Sprintf(`DELETE FROM %s WHERE %s = :1`, table, whereCol), whereVal)
}

func (tb *testBackend) Query(ctx context.Context, t testing.TB, fatal bool, query string, args ...any) {
	t.Helper()
	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing query")
	var rows, err = tb.control.QueryContext(ctx, query, args...)
	if err != nil {
		if fatal {
			t.Fatalf("unable to execute query: %v", err)
		} else {
			return
		}
	}
	defer rows.Close()
	// Log the response, doing a bit of extra work to make it readable
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("unable to get columns: %v", err)
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("unable to get columns: %v", err)
	}
	for rows.Next() {
		var fields = make(map[string]any)
		var fieldsPtr = make([]any, len(cols))
		for idx, col := range cols {
			fields[col] = reflect.New(colTypes[idx].ScanType()).Interface()
			fieldsPtr[idx] = fields[col]
		}
		if err := rows.Scan(fieldsPtr...); err != nil {
			t.Fatalf("error scanning query: %v", err)
		}
		log.WithField("row", fields).Debug("query result row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error running query: %v", err)
	}
}

func argsTuple(row []any) string {
	var tuple = "("
	for idx, value := range row {
		if idx > 0 {
			tuple += ","
		}
		switch v := value.(type) {
		case string:
			tuple += fmt.Sprintf("'%s'", v)
		case int:
			tuple += fmt.Sprintf("%d", v)
		}
	}
	return tuple + ")"
}

// TestGeneric runs the generic sqlcapture test suite.
func TestGeneric(t *testing.T) {
	var tb = oracleTestBackend(t)
	tests.Run(context.Background(), t, tb)
}

func TestCapitalizedTables(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()
	tb.Query(ctx, t, false, fmt.Sprintf(`DROP TABLE "%s"."USERS"`, testSchemaName))
	tb.Query(ctx, t, true, fmt.Sprintf(`CREATE TABLE "%s"."USERS" (id INTEGER PRIMARY KEY, data VARCHAR(2000) NOT NULL)`, testSchemaName))
	var cs = tb.CaptureSpec(ctx, t)
	t.Run("Discover", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(`(?i:users)`))
	})
	var resourceSpecJSON, err = json.Marshal(sqlcapture.Resource{
		Namespace: testSchemaName,
		Stream:    "USERS",
	})
	require.NoError(t, err)
	cs.Bindings = []*flow.CaptureSpec_Binding{{
		// Because we're explicitly constructing the collection spec here this test accidentally
		// exercises the "legacy collection without a /_meta/source/txid property" case, so we
		// may as well leave it like that.
		Collection:         flow.CollectionSpec{Name: flow.Collection("acmeCo/test/users")},
		ResourceConfigJson: resourceSpecJSON,
		ResourcePath:       []string{testSchemaName, "USERS"},
		StateKey:           tests.StateKey([]string{testSchemaName, "USERS"}),
	}}
	t.Run("Validate", func(t *testing.T) {
		var _, err = cs.Validate(ctx, t)
		require.NoError(t, err)
	})
	t.Run("Capture", func(t *testing.T) {
		tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."USERS" VALUES (1, 'Alice'), (2, 'Bob')`, testSchemaName))
		tests.VerifiedCapture(ctx, t, cs)
		t.Run("Replication", func(t *testing.T) {
			tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."USERS" VALUES (3, 'Carol'), (4, 'Dave')`, testSchemaName))
			tests.VerifiedCapture(ctx, t, cs)
		})
	})
}

func TestConfigURI(t *testing.T) {
	for name, cfg := range map[string]Config{
		"Basic": {
			Address:  "example.com",
			User:     "will",
			Password: "secret1234",
			Database: "somedb",
		},
	} {
		t.Run(name, func(t *testing.T) {
			var valid = "config valid"
			if err := cfg.Validate(); err != nil {
				valid = err.Error()
			}
			cfg.SetDefaults("test")
			var uri = cfg.ToURI()
			cupaloy.SnapshotT(t, fmt.Sprintf("%s\n%s", uri, valid))
		})
	}
}
