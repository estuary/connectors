package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture/tests"
	_ "github.com/sijms/go-ora/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var (
	// Config file for tests. Files with ".sops." in the name are decrypted with sops;
	// others are read as plaintext YAML. Default is testdata/config.local.yaml for local Docker testing.
	dbConfig = flag.String("db_config", "testdata/config.local.yaml", "Path to database config file (use .sops. in name for encrypted configs)")
)

func TestMain(m *testing.M) {
	flag.Parse()

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		level, err := log.ParseLevel(logLevel)
		if err != nil {
			log.WithField("level", logLevel).Fatal("invalid log level")
		}
		log.SetLevel(level)
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
	var config Config
	var configJSON []byte
	var configFile = *dbConfig

	if strings.Contains(configFile, ".sops.") {
		// Use sops to decrypt config file to JSON
		var sops = exec.CommandContext(ctx, "sops", "--decrypt", "--output-type", "json", configFile)
		var configRaw, err = sops.Output()
		require.NoError(t, err)
		// Strip _sops suffix from keys
		var jq = exec.CommandContext(ctx, "jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
		jq.Stdin = bytes.NewReader(configRaw)
		configJSON, err = jq.Output()
		require.NoError(t, err)
	} else {
		// Read plaintext YAML config and convert to JSON
		var configRaw, err = os.ReadFile(configFile)
		require.NoError(t, err)
		var generic map[string]any
		err = yaml.Unmarshal(configRaw, &generic)
		require.NoError(t, err)
		configJSON, err = json.Marshal(generic)
		require.NoError(t, err)
	}

	var err = json.Unmarshal(configJSON, &config)
	require.NoError(t, err)

	config.Advanced.BackfillChunkSize = 16
	if err := config.Validate(); err != nil {
		t.Fatalf("error validating capture config: %v", err)
	}
	config.SetDefaults("test")
	// Use online mode to speed up tests, we have a specific test
	// for testing extract mode
	config.Advanced.DictionaryMode = DictionaryModeOnline

	// Open control connection
	db, err := connectOracle(ctx, "test", configJSON)
	t.Logf("opening control connection: addr=%q, user=%q", config.Address, config.User)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close(ctx) })

	var conn = db.(*oracleDatabase).conn
	return &testBackend{control: conn, config: config}
}

type testBackend struct {
	control *sql.DB // The open control connection to use for test setup
	config  Config  // Default capture configuration for test captures
}

func (tb *testBackend) UpperCaseMode() bool { return true }

func (tb *testBackend) CaptureSpec(ctx context.Context, t testing.TB, streamMatchers ...*regexp.Regexp) *st.CaptureSpec {
	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"scn":11111111`] = regexp.MustCompile(`"scn":([0-9]+)`)
	sanitizers[`"row_id":"AAAAAAAAAAAAAAAAAA"`] = regexp.MustCompile(`"row_id":"[^"]+"`)
	sanitizers[`"rs_id":"111111111111111111"`] = regexp.MustCompile(`"rs_id":"[^"]+"`)
	sanitizers[`"ssn":111`] = regexp.MustCompile(`"ssn":[0-9]+`)
	sanitizers[`"ts_ms":1111111111111`] = regexp.MustCompile(`"ts_ms":[0-9]+`)
	sanitizers[`"scanned":"AAAAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"scanned":"[^"]+"`)
	sanitizers[`"AAAAAAA=":{"MessageCount"`] = regexp.MustCompile(`"[^"]+":{"MessageCount"`)
	sanitizers[`"StartSCN":111111111"`] = regexp.MustCompile(`"StartSCN":[0-9]+`)
	sanitizers[`"SCN":111111111"`] = regexp.MustCompile(`"SCN":[0-9]+`)
	sanitizers[`"PendingTransactions":{},"SCN"`] = regexp.MustCompile(`"PendingTransactions":{.*},"SCN"`)

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

	var tableName = fmt.Sprintf("\"%s\".", tb.config.User)
	if suffix != "" {
		tableName += fmt.Sprintf(`"t%s"`, suffix)
	} else {
		tableName += fmt.Sprintf(`"%s"`, strings.TrimPrefix(t.Name(), "Test"))
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}
	tableName = strings.ToUpper(tableName)

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
	var _, err = tb.control.ExecContext(ctx, query, args...)
	if err != nil {
		if fatal {
			t.Fatalf("unable to execute query: %v", err)
		}
	}
}

// A type that passes the string DDL as-is when using argsTuple
type rawTupleValue struct {
	DDL string
}

func NewRawTupleValue(s string) rawTupleValue {
	return rawTupleValue{DDL: s}
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
		case float64:
			tuple += fmt.Sprintf("%f", v)
		case rawTupleValue:
			tuple += v.DDL
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
	tb.Query(ctx, t, false, fmt.Sprintf(`DROP TABLE "%s"."USERS"`, tb.config.User))
	tb.Query(ctx, t, true, fmt.Sprintf(`CREATE TABLE "%s"."USERS" (id INTEGER PRIMARY KEY, data VARCHAR(2000) NOT NULL)`, tb.config.User))
	var cs = tb.CaptureSpec(ctx, t)
	t.Run("Discover", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(`(?i:users)`))
	})
	cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(`(?i:users)`))
	t.Run("Validate", func(t *testing.T) {
		var _, err = cs.Validate(ctx, t)
		require.NoError(t, err)
	})
	t.Run("Capture", func(t *testing.T) {
		tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."USERS" VALUES (1, 'Alice')`, tb.config.User))
		tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."USERS" VALUES (2, 'Bob')`, tb.config.User))
		tests.VerifiedCapture(ctx, t, cs)
		t.Run("Replication", func(t *testing.T) {
			tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."USERS" VALUES (3, 'Carol')`, tb.config.User))
			tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."USERS" VALUES (4, 'Dave')`, tb.config.User))
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
			var uri = cfg.ToURI(1)
			cupaloy.SnapshotT(t, fmt.Sprintf("%s\n%s", uri, valid))
		})
	}
}
