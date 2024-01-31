package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"
)

var (
	dbHost      = flag.String("db_host", "bn92689.us-central1.gcp.snowflakecomputing.com", "The Snowflake host to use for tests")
	dbAccount   = flag.String("db_account", "bn92689", "The Snowflake account ID to use for tests")
	dbName      = flag.String("db_name", "CONNECTOR_TESTING", "The database to use for tests")
	dbWarehouse = flag.String("db_warehouse", "COMPUTE_WH", "The warehouse to execute test queries in")

	dbCaptureUser = flag.String("db_capture_user", "USERNAME", "The user to perform captures as")
	dbCapturePass = flag.String("db_capture_pass", "secret1234", "The password for the capture user")
	dbControlUser = flag.String("db_control_user", "", "The user for test setup/control operations, if different from the capture user")
	dbControlPass = flag.String("db_control_pass", "", "The password the the test setup/control user, if different from the capture password")
)

const (
	snowflakeDefaultSchema = "PUBLIC"
)

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

	// Lower checkpoint spacing constant so tests can exercise this more easily
	partialProgressCheckpointSpacing = 10

	os.Exit(m.Run())
}

type testBackend struct {
	control *sql.DB
	config  config
}

func snowflakeTestBackend(t *testing.T) *testBackend {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil
	}

	// Open control connection
	var controlUser = *dbControlUser
	if controlUser == "" {
		controlUser = *dbCaptureUser
	}
	var controlPass = *dbControlPass
	if controlPass == "" {
		controlPass = *dbCapturePass
	}
	var controlURI = (&config{
		Host:      *dbHost,
		Account:   *dbAccount,
		User:      controlUser,
		Password:  controlPass,
		Database:  *dbName,
		Warehouse: *dbWarehouse,
	}).ToURI()
	log.WithFields(log.Fields{
		"user":     controlUser,
		"addr":     *dbHost,
		"database": *dbName,
	}).Info("opening control connection")
	var conn, err = sql.Open("snowflake", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	// Construct the capture config
	var captureConfig = config{
		Host:      *dbHost,
		Account:   *dbAccount,
		User:      *dbCaptureUser,
		Password:  *dbCapturePass,
		Database:  *dbName,
		Warehouse: *dbWarehouse,
	}
	if err := captureConfig.Validate(); err != nil {
		t.Fatalf("error validating capture config: %v", err)
	}
	captureConfig.SetDefaults()

	// Drop the schema at the start of the tests. Because the connector leaves its most recent
	// set of staging tables around after each invocation, it's possible for a staging table
	// to linger between consecutive runs of the same test and cause problems which can't
	// easily occur in production.
	log.WithField("schema", captureConfig.Advanced.FlowSchema).Info("dropping flow schema")
	_, err = conn.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s;", quoteSnowflakeIdentifier(captureConfig.Advanced.FlowSchema)))
	require.NoError(t, err, "dropping flow schema")

	return &testBackend{control: conn, config: captureConfig}
}

func (tb *testBackend) CaptureSpec(ctx context.Context, t testing.TB, streamMatchers ...*regexp.Regexp) *st.CaptureSpec {
	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"rowid":"ffffffffffffffffffffffffffffffffffffffff"`] = regexp.MustCompile(`"rowid":"[0-9a-fA-F]{32,}"`)
	sanitizers[`"uid":"FFFFFFFFFFFFFFFF_`] = regexp.MustCompile(`"uid":"[0-9a-fA-F]{16}_`)

	var cfg = tb.config
	var cs = &st.CaptureSpec{
		Driver:       new(snowflakeDriver),
		EndpointSpec: &cfg,
		Validator:    &st.OrderedCaptureValidator{},
		Sanitizers:   sanitizers,
	}
	if len(streamMatchers) > 0 {
		cs.Bindings = discoverBindings(ctx, t, tb, streamMatchers...)
	}
	return cs
}

func discoverBindings(ctx context.Context, t testing.TB, tb *testBackend, streamMatchers ...*regexp.Regexp) []*pf.CaptureSpec_Binding {
	t.Helper()
	var cs = tb.CaptureSpec(ctx, t)

	// Perform discovery, expecting to match one stream with each provided regexp.
	var discovery = cs.Discover(ctx, t, streamMatchers...)
	if len(discovery) != len(streamMatchers) {
		t.Fatalf("discovered incorrect number of streams: got %d, expected %d", len(discovery), len(streamMatchers))
	}

	// Translate discovery bindings into capture bindings.
	var bindings []*pf.CaptureSpec_Binding
	for _, b := range discovery {
		var res resource
		require.NoError(t, json.Unmarshal(b.ResourceConfigJson, &res))
		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: b.ResourceConfigJson,
			Collection: pf.CollectionSpec{
				Name:           pf.Collection("acmeCo/test/" + b.RecommendedName),
				ReadSchemaJson: b.DocumentSchemaJson,
				Key:            b.Key,
				// Converting the discovered schema into a list of projections would be quite
				// a task and all we actually need it for is to enable transaction IDs in
				// MySQL and Postgres.
				Projections: []pf.Projection{{Ptr: "/_meta/source/txid"}},
			},

			ResourcePath: []string{res.Schema, res.Table},
			StateKey:     url.QueryEscape(res.Schema + "/" + res.Table),
		})
	}
	return bindings
}

func (tb *testBackend) CreateTable(ctx context.Context, t testing.TB, suffix string, tableDef string) string {
	t.Helper()

	var tableName = "test_" + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}
	var fullTableName = snowflakeObject{snowflakeDefaultSchema, tableName}.QuotedName()

	log.WithFields(log.Fields{"table": fullTableName, "cols": tableDef}).Debug("creating test table")

	tb.Query(ctx, t, fmt.Sprintf("CREATE OR REPLACE TABLE %s%s;", fullTableName, tableDef))

	t.Cleanup(func() {
		log.WithField("table", fullTableName).Debug("destroying test table")
		tb.Query(ctx, t, fmt.Sprintf("DROP TABLE IF EXISTS %s;", fullTableName))
	})

	return fullTableName
}

func argsTuple(argc int) string {
	var tuple = "(?"
	for idx := 1; idx < argc; idx++ {
		tuple += fmt.Sprintf(",?")
	}
	return tuple + ")"
}

func (tb *testBackend) Insert(ctx context.Context, t testing.TB, table string, rows [][]interface{}) {
	t.Helper()
	if len(rows) < 1 {
		t.Fatalf("must insert at least one row")
	}

	// Restructure dataset into columns and wrap into Snowflake arrays
	var cols = make([][]any, len(rows[0]))
	for _, row := range rows {
		require.Equal(t, len(row), len(cols), "incorrect number of values in row")
		for j, val := range row {
			cols[j] = append(cols[j], val)
		}
	}

	var arrs []any
	for idx := range cols {
		arrs = append(arrs, gosnowflake.Array(&cols[idx]))
	}

	var tx, err = tb.control.BeginTx(ctx, nil)
	require.NoErrorf(t, err, "begin transaction")
	log.WithFields(log.Fields{"table": table, "columns": len(cols), "rows": len(cols[0]), "first": rows[0]}).Debug("inserting data")
	var query = fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, argsTuple(len(arrs)))
	log.WithFields(log.Fields{"query": query}).Trace("executing query")
	_, err = tx.ExecContext(ctx, query, arrs...)
	require.NoError(t, err, "insert data")
	log.WithFields(log.Fields{"table": table}).Trace("committing transaction")
	require.NoErrorf(t, tx.Commit(), "commit transaction")
}

func (tb *testBackend) Update(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}, setCol string, setVal interface{}) {
	t.Helper()
	var query = fmt.Sprintf(`UPDATE %s SET %s = ? WHERE %s = ?;`, table, setCol, whereCol)
	log.WithField("query", query).Debug("updating rows")
	tb.Query(ctx, t, query, setVal, whereVal)
}

func (tb *testBackend) Delete(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}) {
	t.Helper()
	var query = fmt.Sprintf(`DELETE FROM %s WHERE %s = ?;`, table, whereCol)
	log.WithField("query", query).Debug("deleting rows")
	tb.Query(ctx, t, query, whereVal)
}

func (tb *testBackend) Query(ctx context.Context, t testing.TB, query string, args ...any) {
	t.Helper()
	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing control query")
	var _, err = tb.control.ExecContext(ctx, query, args...)
	require.NoError(t, err)
}

func TestSpec(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)

	response, err := tb.CaptureSpec(ctx, t).Driver.Spec(ctx, &pc.Request_Spec{})
	require.NoError(t, err)
	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}
