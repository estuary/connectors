package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbName = flag.String("db_name", "testdb", "Connect to the named database for tests")

	dbControlAddress = flag.String("db_control_addr", "localhost:50000", "The database server address to use for test setup/control operations")
	dbControlUser    = flag.String("db_control_user", "db2inst1", "The user for test setup/control operations")
	dbControlPass    = flag.String("db_control_pass", "gf6w6dkD", "The password the the test setup/control user")

	dbCaptureAddress = flag.String("db_capture_addr", "localhost:50000", "The database server address to use for test captures")
	dbCaptureUser    = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass    = flag.String("db_capture_pass", "secret1234", "The password for the capture user")

	testSchemaName   = flag.String("test_schema_name", "TEST", "The schema in which to create test tables.")
	testFeatureFlags = flag.String("feature_flags", "", "Feature flags to apply to all test captures.")
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

func testCaptureSpec(t testing.TB) *st.CaptureSpec {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var endpointSpec = &Config{
		Address:  *dbCaptureAddress,
		User:     *dbCaptureUser,
		Password: *dbCapturePass,
		Database: *dbName,
		Advanced: advancedConfig{
			PollSchedule: "200ms",
			FeatureFlags: *testFeatureFlags,
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"polled":"<TIMESTAMP>"`] = regexp.MustCompile(`"polled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"LastPolled":"<TIMESTAMP>"`] = regexp.MustCompile(`"LastPolled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)

	return &st.CaptureSpec{
		Driver:       db2Driver,
		EndpointSpec: endpointSpec,
		Validator:    &st.OrderedCaptureValidator{IncludeSourcedSchemas: true, PrettyDocuments: true},
		Sanitizers:   sanitizers,
	}
}

func discoverBindings(ctx context.Context, t testing.TB, cs *st.CaptureSpec, matchers ...*regexp.Regexp) []*pf.CaptureSpec_Binding {
	t.Helper()

	var discovery = cs.Discover(ctx, t, matchers...)
	var bindings []*pf.CaptureSpec_Binding
	for _, discovered := range discovery {
		var res Resource
		require.NoError(t, json.Unmarshal(discovered.ResourceConfigJson, &res))
		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: discovered.ResourceConfigJson,
			Collection: pf.CollectionSpec{
				Name:           pf.Collection("acmeCo/test/" + discovered.RecommendedName),
				ReadSchemaJson: discovered.DocumentSchemaJson,
				Key:            discovered.Key,
			},
			ResourcePath: []string{res.Name},
			StateKey:     res.Name,
		})
	}
	return bindings
}

func testControlClient(t testing.TB) *sql.DB {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var controlSpec = &Config{
		Address:  *dbControlAddress,
		User:     *dbControlUser,
		Password: *dbControlPass,
		Database: *dbName,
		Advanced: advancedConfig{
			PollSchedule: "200ms",
			FeatureFlags: *testFeatureFlags,
		},
	}

	var controlURI = controlSpec.ToURI()
	t.Logf("opening control connection: uri=%q", controlURI)

	var conn, err = sql.Open("go_ibm_db", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.Ping())
	return conn
}

func testTableName(t *testing.T, uniqueID string) (name, id string) {
	t.Helper()
	var baseName = strings.ToLower(strings.TrimPrefix(t.Name(), "Test"))
	for _, str := range []string{"/", "=", "(", ")"} {
		baseName = strings.ReplaceAll(baseName, str, "_")
	}
	return fmt.Sprintf("%s.%s_%s", *testSchemaName, baseName, uniqueID), uniqueID
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

func createTestTable(t testing.TB, control *sql.DB, tableName, definition string) {
	t.Helper()
	// DB2 doesn't have DROP TABLE IF EXISTS, so just try to drop and ignore errors
	control.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	executeControlQuery(t, control, fmt.Sprintf("CREATE TABLE %s %s", tableName, definition))
	t.Cleanup(func() { control.Exec(fmt.Sprintf("DROP TABLE %s", tableName)) })
}

func summarizeBindings(t testing.TB, bindings []*pf.CaptureSpec_Binding) string {
	t.Helper()
	var summary = new(strings.Builder)
	for idx, binding := range bindings {
		fmt.Fprintf(summary, "Binding %d:\n", idx)
		bs, err := json.MarshalIndent(binding, "  ", "  ")
		require.NoError(t, err)
		io.Copy(summary, bytes.NewReader(bs))
		fmt.Fprintf(summary, "\n")
	}
	if len(bindings) == 0 {
		fmt.Fprintf(summary, "(no bindings)")
	}
	return summary.String()
}

func executeControlQuery(t testing.TB, client *sql.DB, query string, args ...interface{}) {
	t.Helper()
	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing setup query")
	var _, err = client.Exec(query, args...)
	require.NoError(t, err)
}

func setShutdownAfterQuery(t testing.TB, setting bool) {
	t.Helper()
	var oldSetting = TestShutdownAfterQuery
	TestShutdownAfterQuery = setting
	t.Cleanup(func() { TestShutdownAfterQuery = oldSetting })
}

func setResourceCursor(t testing.TB, binding *pf.CaptureSpec_Binding, cursor ...string) {
	var res Resource
	require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &res))
	res.Cursor = cursor
	var bs, err = json.Marshal(res)
	require.NoError(t, err)
	binding.ResourceConfigJson = bs
}

// TestSpec verifies the connector's response to the Spec RPC against a snapshot.
func TestSpec(t *testing.T) {
	response, err := db2Driver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// TestIncorrectLogin verifies that the connector returns a useful error message
// when the user provides incorrect login credentials.
func TestIncorrectLogin(t *testing.T) {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var ctx = context.Background()
	var cfg = &Config{
		Address:  *dbCaptureAddress,
		User:     "flow_capture",
		Password: "wrong_password",
		Database: *dbName,
	}
	cfg.SetDefaults()

	var _, err = connectDB2(ctx, cfg)
	require.Error(t, err)
	cupaloy.SnapshotT(t, err.Error())
}

// TestSchemaFilter exercises the 'discover_schemas' advanced option.
func TestSchemaFilter(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INT PRIMARY KEY NOT NULL, data CLOB)")

	// Run discovery with several schema filters and snapshot the results
	t.Run("Unfiltered", func(t *testing.T) {
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{}
		cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
	})
	t.Run("FilteredOut", func(t *testing.T) {
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"foo", "bar"}
		cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
	})
	t.Run("FilteredIn", func(t *testing.T) {
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"foo", *testSchemaName}
		cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
	})
}

// TestQueryTemplates exercises the selection and execution of query templates
// for various combinations of resource spec and stream state.
func TestQueryTemplates(t *testing.T) {
	var testCases = []struct {
		name         string
		cursor       []string
		cursorValues []any
	}{
		{name: "SingleCursorFirstQuery", cursor: []string{"updated_at"}},
		{name: "SingleCursorSubsequentQuery", cursor: []string{"updated_at"}, cursorValues: []any{"2024-02-20 12:00:00"}},
		{name: "MultiCursorFirstQuery", cursor: []string{"major", "minor"}},
		{name: "MultiCursorSubsequentQuery", cursor: []string{"major", "minor"}, cursorValues: []any{1, 2}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var resource = &Resource{
				Name:       "test_foobar",
				SchemaName: "test",
				TableName:  "foobar",
				Cursor:     tc.cursor,
			}
			var state = &streamState{
				CursorNames:  tc.cursor,
				CursorValues: tc.cursorValues,
			}
			var query, err = db2Driver.buildQuery(resource, state)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, query)
		})
	}
}

// TestFieldLengthDiscovery exercises discovery of column types with specific field lengths.
func TestFieldLengthDiscovery(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
		id INT PRIMARY KEY NOT NULL,

		char_5 CHAR(5),
		varchar_100 VARCHAR(100),
		varchar_max CLOB,
		nchar_15 CHAR(15),
		nvarchar_max CLOB,
		binary_8 BINARY(8),
		varbinary_16 VARBINARY(16),
		varbinary_max BLOB,

		decimal_10_2 DECIMAL(10,2),
		decimal_31_10 DECIMAL(31,10),
		numeric_5_0 NUMERIC(5,0),
		numeric_18_4 NUMERIC(18,4),
		float_24 FLOAT(24),
		float_53 FLOAT(53)
	)`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings))
}
