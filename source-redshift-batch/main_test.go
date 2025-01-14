package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbAddress     = flag.String("db_address", "default-workgroup.123456789012.us-east-1.redshift-serverless.amazonaws.com:5439", "The database server address to use for tests")
	dbName        = flag.String("db_name", "dev", "Use the named database for tests")
	dbCaptureUser = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass = flag.String("db_capture_pass", "secret1234", "The password for the capture user")
	dbControlUser = flag.String("db_control_user", "", "The user for test setup/control operations, if different from the capture user")
	dbControlPass = flag.String("db_control_pass", "", "The password the the test setup/control user, if different from the capture password")
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

func TestSpec(t *testing.T) {
	response, err := redshiftDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestQueryTemplate(t *testing.T) {
	res, err := redshiftDriver.GenerateResource("foobar", "testdata", "foobar", "BASE TABLE")
	require.NoError(t, err)

	tmpl, err := template.New("query").Funcs(templateFuncs).Parse(res.Template)
	require.NoError(t, err)

	for _, tc := range []struct {
		Name    string
		IsFirst bool
		Cursor  []string
	}{
		{Name: "FirstNoCursor", IsFirst: true, Cursor: nil},
		{Name: "SubsequentNoCursor", IsFirst: false, Cursor: nil},
		{Name: "FirstOneCursor", IsFirst: true, Cursor: []string{"`ka`"}},
		{Name: "SubsequentOneCursor", IsFirst: false, Cursor: []string{"`ka`"}},
		{Name: "FirstTwoCursor", IsFirst: true, Cursor: []string{"`ka`", "`kb`"}},
		{Name: "SubsequentTwoCursor", IsFirst: false, Cursor: []string{"`ka`", "`kb`"}},
		{Name: "FirstThreeCursor", IsFirst: true, Cursor: []string{"`ka`", "`kb`", "`kc`"}},
		{Name: "SubsequentThreeCursor", IsFirst: false, Cursor: []string{"`ka`", "`kb`", "`kc`"}},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var buf = new(strings.Builder)
			require.NoError(t, tmpl.Execute(buf, map[string]any{
				"IsFirstQuery": tc.IsFirst,
				"CursorFields": tc.Cursor,
			}))
			cupaloy.SnapshotT(t, buf.String())
		})
	}
}

func TestBasicCapture(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "826935"
	var tableName = fmt.Sprintf("test.basic_capture_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, data TEXT)", tableName))

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { snapshotBindings(t, cs.Bindings) })

	// Edit the discovered binding to use the ID column as a cursor
	var res Resource
	require.NoError(t, json.Unmarshal(cs.Bindings[0].ResourceConfigJson, &res))
	res.Cursor = []string{"id"}
	resourceConfigBytes, err := json.Marshal(res)
	require.NoError(t, err)
	cs.Bindings[0].ResourceConfigJson = resourceConfigBytes

	t.Run("Capture", func(t *testing.T) {
		// Spawn a worker thread which will insert 50 rows of data in parallel with the capture.
		var insertsDone atomic.Bool
		go func() {
			for i := 0; i < 50; i++ {
				time.Sleep(500 * time.Millisecond)
				executeControlQuery(ctx, t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName), i, fmt.Sprintf("Value for row %d", i))
				log.WithField("i", i).Debug("inserted row")
			}
			time.Sleep(10 * time.Second)
			insertsDone.Store(true)
		}()

		// Run the capture over and over for 5 seconds each time until all inserts have finished, then verify results.
		for !insertsDone.Load() {
			var captureCtx, cancelCapture = context.WithCancel(ctx)
			time.AfterFunc(5*time.Second, cancelCapture)
			cs.Capture(captureCtx, t, nil)
		}
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestBasicDatatypes(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "13111208"
	var tableName = fmt.Sprintf("test.basic_datatypes_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, a_real REAL, a_bool BOOL, a_date DATE, a_ts TIMESTAMP, a_tstz TIMESTAMPTZ)", tableName))

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { snapshotBindings(t, cs.Bindings) })

	t.Run("Capture", func(t *testing.T) {
		executeControlQuery(ctx, t, control, fmt.Sprintf("INSERT INTO %s(id, a_real, a_bool, a_date, a_ts, a_tstz) VALUES ($1,$2,$3,$4,$5,$6)", tableName),
			100, -12.34, true, "2024-02-26", time.Date(2024, 02, 26, 12, 34, 56, 00, time.UTC), time.Date(2024, 02, 26, 12, 34, 56, 00, time.UTC))

		// Run the capture for 5 seconds, which should be plenty to pull down a few rows.
		var captureCtx, cancelCapture = context.WithCancel(ctx)
		time.AfterFunc(5*time.Second, cancelCapture)
		cs.Capture(captureCtx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestFloatNaNs(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "10511"
	var tableName = fmt.Sprintf("test.float_nans_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, a_real REAL, a_double DOUBLE PRECISION)", tableName))

	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))

	t.Run("Discovery", func(t *testing.T) { snapshotBindings(t, cs.Bindings) })

	t.Run("Capture", func(t *testing.T) {
		executeControlQuery(ctx, t, control, fmt.Sprintf(`INSERT INTO %s VALUES (0, 2.0, 'NaN'), (1, 'NaN', 3.0), (2, 'Infinity', '-Infinity')`, tableName))

		// Run the capture for 1 second, which should be plenty to pull down a few rows.
		var captureCtx, cancelCapture = context.WithCancel(ctx)
		time.AfterFunc(1*time.Second, cancelCapture)
		cs.Capture(captureCtx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestSchemaFilter(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "22492"
	var tableName = fmt.Sprintf("test.schema_filtering_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, data TEXT)", tableName))

	// Run discovery with several schema filters and snapshot the results
	t.Run("Unfiltered", func(t *testing.T) {
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{}
		snapshotBindings(t, discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID)))
	})
	t.Run("FilteredOut", func(t *testing.T) {
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"foo", "bar"}
		snapshotBindings(t, discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID)))
	})
	t.Run("FilteredIn", func(t *testing.T) {
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"foo", "test"}
		snapshotBindings(t, discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID)))
	})
}

func TestKeyDiscovery(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "329932"
	var tableName = fmt.Sprintf("test.key_discovery_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(k_smallint SMALLINT, k_int INTEGER, k_bigint BIGINT, k_bool BOOLEAN, k_str VARCHAR(8), data TEXT, PRIMARY KEY (k_smallint, k_int, k_bigint, k_bool, k_str))", tableName))

	cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"test"}
	snapshotBindings(t, discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID)))
}

func TestKeylessDiscovery(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "10352"
	var tableName = fmt.Sprintf("test.keyless_discovery_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(v_smallint SMALLINT, v_int INTEGER, v_bigint BIGINT, v_bool BOOLEAN, v_str VARCHAR(8), v_ts TIMESTAMP, v_tstz TIMESTAMP WITH TIME ZONE, v_text TEXT, v_int_notnull INTEGER NOT NULL, v_text_notnull TEXT NOT NULL)", tableName))

	cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"test"}
	snapshotBindings(t, discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID)))
}

func testControlClient(ctx context.Context, t testing.TB) *sql.DB {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var controlUser = *dbControlUser
	if controlUser == "" {
		controlUser = *dbCaptureUser
	}
	var controlPass = *dbControlPass
	if controlPass == "" {
		controlPass = *dbCapturePass
	}
	var controlURI = fmt.Sprintf(`postgres://%s:%s@%s/%s`, controlUser, controlPass, *dbAddress, *dbName)
	log.WithField("uri", controlURI).Debug("opening database control connection")
	var conn, err = sql.Open("pgx", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.PingContext(ctx))
	return conn
}

func executeControlQuery(ctx context.Context, t testing.TB, client *sql.DB, query string, args ...interface{}) {
	t.Helper()
	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing setup query")
	var _, err = client.ExecContext(ctx, query, args...)
	require.NoError(t, err)
}

func testCaptureSpec(t testing.TB) *st.CaptureSpec {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var endpointSpec = &Config{
		Address:  *dbAddress,
		User:     *dbCaptureUser,
		Password: *dbCapturePass,
		Database: *dbName,
		Advanced: advancedConfig{
			PollSchedule: "10s",
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"polled":"<TIMESTAMP>"`] = regexp.MustCompile(`"polled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"LastPolled":"<TIMESTAMP>"`] = regexp.MustCompile(`"LastPolled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)

	return &st.CaptureSpec{
		Driver:       redshiftDriver,
		EndpointSpec: endpointSpec,
		Validator:    &st.OrderedCaptureValidator{},
		Sanitizers:   sanitizers,
	}
}

func discoverStreams(ctx context.Context, t testing.TB, cs *st.CaptureSpec, matchers ...*regexp.Regexp) []*pf.CaptureSpec_Binding {
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

func snapshotBindings(t testing.TB, bindings []*pf.CaptureSpec_Binding) {
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
	cupaloy.SnapshotT(t, summary.String())
}

func TestFeatureFlagUseSchemaInference(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "77244729"
	var tableName = fmt.Sprintf("test.feature_flag_use_schema_inference_%s", uniqueID)

	// Includes some extra junk to make sure the parsing helper logic is doing its job as intended
	cs.EndpointSpec.(*Config).Advanced.FeatureFlags = "this_flag_does_not_exist,use_schema_inference,,,,no_this_flag_also_does_not_exist"

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, data TEXT)", tableName))

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { snapshotBindings(t, cs.Bindings) })
}
