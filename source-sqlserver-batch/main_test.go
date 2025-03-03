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
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbName = flag.String("db_name", "test", "Connect to the named database for tests")

	dbControlAddress = flag.String("db_control_addr", "127.0.0.1:1433", "The database server address to use for test setup/control operations")
	dbControlUser    = flag.String("db_control_user", "sa", "The user for test setup/control operations")
	dbControlPass    = flag.String("db_control_pass", "gf6w6dkD", "The password the the test setup/control user")

	dbCaptureAddress = flag.String("db_capture_addr", "127.0.0.1:1433", "The database server address to use for test captures")
	dbCaptureUser    = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass    = flag.String("db_capture_pass", "we2rie1E", "The password for the capture user")

	testSchemaName   = flag.String("test_schema_name", "dbo", "The schema in which to create test tables.")
	testFeatureFlags = flag.String("feature_flags", "", "Feature flags to apply to all test captures.")
)

func TestMain(m *testing.M) {
	flag.Parse()
	if level, err := log.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		log.SetLevel(level)
	} else {
		log.SetLevel(log.InfoLevel)
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
		Driver:       sqlserverDriver,
		EndpointSpec: endpointSpec,
		Validator:    &st.OrderedCaptureValidator{},
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
	log.WithField("uri", controlURI).Debug("opening database control connection")

	var conn, err = sql.Open("sqlserver", controlURI)
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
	executeControlQuery(t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	executeControlQuery(t, control, fmt.Sprintf("CREATE TABLE %s %s", tableName, definition))
	t.Cleanup(func() { executeControlQuery(t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
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
	response, err := sqlserverDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// TestSimpleCapture exercises the simplest use-case of a capture first doing an initial
// backfill and subsequently capturing new rows using an updated_at column for the cursor.
func TestSimpleCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data NVARCHAR(MAX), updated_at DATETIME2)")
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(
				"INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf(
				"INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}
