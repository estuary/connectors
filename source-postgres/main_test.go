package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbName = flag.String("db_name", "postgres", "Use the named database for tests")

	dbControlAddress = flag.String("db_control_address", "localhost:5432", "The database server address to use for test setup/control operations")
	dbControlUser    = flag.String("db_control_user", "postgres", "The user for test setup/control operations")
	dbControlPass    = flag.String("db_control_pass", "postgres", "The password the the test setup/control user")

	dbCaptureAddress = flag.String("db_capture_address", "localhost:5432", "The database server address to use for test captures")
	dbCaptureUser    = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass    = flag.String("db_capture_pass", "secret1234", "The password for the capture user")

	readOnlyCapture = flag.Bool("read_only_capture", false, "When true, run test captures in read-only mode")

	testFeatureFlags = flag.String("feature_flags", "", "Feature flags to apply to all test captures.")
)

const testSchemaName = "test"

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

	// Set a 900MiB memory limit, same as we use in production.
	debug.SetMemoryLimit(900 * 1024 * 1024)

	os.Exit(m.Run())
}

func postgresTestBackend(t testing.TB) *testBackend {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil
	}

	// Open control connection
	var ctx = context.Background()
	var controlURI = fmt.Sprintf(`postgres://%s:%s@%s/%s`, *dbControlUser, *dbControlPass, *dbControlAddress, *dbName)
	log.WithFields(log.Fields{
		"user": *dbControlUser,
		"addr": *dbControlAddress,
	}).Info("opening control connection")
	var pool, err = pgxpool.New(ctx, controlURI)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// Construct the capture config
	var captureConfig = Config{
		Address:  *dbCaptureAddress,
		User:     *dbCaptureUser,
		Password: *dbCapturePass,
		Database: *dbName,
	}
	captureConfig.Advanced.FeatureFlags = *testFeatureFlags
	captureConfig.Advanced.BackfillChunkSize = 16
	if *readOnlyCapture {
		captureConfig.Advanced.ReadOnlyCapture = true
	}
	if err := captureConfig.Validate(); err != nil {
		t.Fatalf("error validating capture config: %v", err)
	}
	captureConfig.SetDefaults()

	return &testBackend{control: pool, config: captureConfig}
}

type testBackend struct {
	control *pgxpool.Pool // The open control connection to use for test setup
	config  Config        // Default capture configuration for test captures
}

func (tb *testBackend) UpperCaseMode() bool { return false }

func (tb *testBackend) CaptureSpec(ctx context.Context, t testing.TB, streamMatchers ...*regexp.Regexp) *st.CaptureSpec {
	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"loc":[11111111,11111111,11111111]`] = regexp.MustCompile(`"loc":\[(-1|[0-9]+),[0-9]+,[0-9]+\]`)
	sanitizers[`"cursor":"0/1111111"`] = regexp.MustCompile(`"cursor":"[0-9A-F]+/[0-9A-F]+"`)
	sanitizers[`"ts_ms":1111111111111`] = regexp.MustCompile(`"ts_ms":[0-9]+`)
	sanitizers[`"txid":111111`] = regexp.MustCompile(`"txid":[0-9]+`)

	var cfg = tb.config
	var cs = &st.CaptureSpec{
		Driver:       postgresDriver,
		EndpointSpec: &cfg,
		Validator:    &st.OrderedCaptureValidator{IncludeSourcedSchemas: true},
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

	var tableName = testSchemaName + "." + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}
	tableName = strings.ToLower(tableName)

	log.WithFields(log.Fields{"table": tableName, "cols": tableDef}).Debug("creating test table")
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %s %s;`, tableName, tableDef))
	t.Cleanup(func() {
		log.WithField("table", tableName).Debug("destroying test table")
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE %s;`, tableName))
	})
	return tableName
}

func (tb *testBackend) Insert(ctx context.Context, t testing.TB, table string, rows [][]interface{}) {
	t.Helper()

	if len(rows) < 1 {
		t.Fatalf("must insert at least one row")
	}
	var tx, err = tb.control.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("unable to begin transaction: %v", err)
	}
	log.WithFields(log.Fields{"table": table, "count": len(rows)}).Debug("inserting data")
	var query = fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, argsTuple(len(rows[0])))
	for _, row := range rows {
		log.WithFields(log.Fields{"table": table, "row": row}).Trace("inserting row")
		if len(row) != len(rows[0]) {
			t.Fatalf("incorrect number of values in row %q (expected %d)", row, len(rows[0]))
		}
		var results, err = tx.Query(ctx, query, row...)
		if err != nil {
			t.Fatalf("unable to execute query: %v", err)
		}
		results.Close()
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("unable to commit insert transaction: %v", err)
	}
}

func (tb *testBackend) Update(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}, setCol string, setVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf(`UPDATE %s SET %s = $1 WHERE %s = $2;`, table, setCol, whereCol), setVal, whereVal)
}

func (tb *testBackend) Delete(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf(`DELETE FROM %s WHERE %s = $1;`, table, whereCol), whereVal)
}

func (tb *testBackend) Query(ctx context.Context, t testing.TB, query string, args ...interface{}) {
	t.Helper()
	if log.IsLevelEnabled(log.DebugLevel) {
		log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing query")
	}
	var rows, err = tb.control.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("unable to execute query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var vals, err = rows.Values()
		if err != nil {
			t.Fatalf("error processing query result: %v", err)
		}
		log.WithField("values", vals).Debug("query result row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error running query: %v", err)
	}
}

func argsTuple(argc int) string {
	var tuple = "($1"
	for idx := 1; idx < argc; idx++ {
		tuple += fmt.Sprintf(",$%d", idx+1)
	}
	return tuple + ")"
}

func uniqueTableID(t testing.TB, extra ...string) string {
	t.Helper()
	var h = sha256.New()
	h.Write([]byte(t.Name()))
	for _, x := range extra {
		h.Write([]byte{':'})
		h.Write([]byte(x))
	}
	var x = binary.BigEndian.Uint32(h.Sum(nil)[0:4])
	return fmt.Sprintf("%d", (x%900000)+100000)
}

func setShutdownAfterCaughtUp(t testing.TB, setting bool) {
	t.Helper()
	var prevSetting = sqlcapture.TestShutdownAfterCaughtUp
	sqlcapture.TestShutdownAfterCaughtUp = setting
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = prevSetting })
}

func setResourceBackfillMode(t *testing.T, binding *flow.CaptureSpec_Binding, mode sqlcapture.BackfillMode) {
	t.Helper()
	require.NotNil(t, binding)
	var res sqlcapture.Resource
	require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &res))
	res.Mode = mode
	var bs, err = json.Marshal(&res)
	require.NoError(t, err)
	binding.ResourceConfigJson = bs
}

// TestGeneric runs the generic sqlcapture test suite.
func TestGeneric(t *testing.T) {
	var tb = postgresTestBackend(t)
	tests.Run(context.Background(), t, tb)
}

func TestCapitalizedTables(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."USERS"`, testSchemaName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE "%s"."USERS" (id INTEGER PRIMARY KEY, data TEXT NOT NULL)`, testSchemaName))
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
		tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO "%s"."USERS" VALUES (1, 'Alice'), (2, 'Bob')`, testSchemaName))
		tests.VerifiedCapture(ctx, t, cs)
		t.Run("Replication", func(t *testing.T) {
			tb.Query(ctx, t, fmt.Sprintf(`INSERT INTO "%s"."USERS" VALUES (3, 'Carol'), (4, 'Dave')`, testSchemaName))
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
		"RequireSSL": {
			Address:  "example.com",
			User:     "will",
			Password: "secret1234",
			Database: "somedb",
			Advanced: advancedConfig{
				SSLMode: "verify-full",
			},
		},
		"IncorrectSSL": {
			Address:  "example.com",
			User:     "will",
			Password: "secret1234",
			Database: "somedb",
			Advanced: advancedConfig{
				SSLMode: "whoops-this-isnt-right",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			var valid = "config valid"
			if err := cfg.Validate(); err != nil {
				valid = err.Error()
			}
			cfg.SetDefaults()
			var uri = cfg.ToURI()
			cupaloy.SnapshotT(t, fmt.Sprintf("%s\n%s", uri, valid))
		})
	}
}
