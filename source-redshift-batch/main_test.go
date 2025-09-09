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
	"os/exec"
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
	cfgCapture = flag.String("cfg_capture", "configs/cloud-capture.yaml", "The path to an encrypted capture configuration file to use for test captures.")
	cfgSetup   = flag.String("cfg_setup", "configs/cloud-setup.yaml", "The path to an encrypted configuration file containing the connection settings to use for test control operations.")
	testSchema = flag.String("test_schema", "public", "The schema in which to create test entities.")

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
	// Some tested behaviors (TestDateAndTimeTypes) are timezone-sensitive. This
	// is arguably a bug, but the goal of the current work is just to document
	// via test snapshots the current behavior. So we set the timezone to a known
	// value to avoid test failures due to timezone differences, especially in CI.
	os.Setenv("TZ", "America/New_York")
	os.Exit(m.Run())
}

// stripEncryptedSuffix recursively removes the specified suffix from map keys
func stripEncryptedSuffix(v interface{}, suffix string) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for k, v := range x {
			newKey := strings.TrimSuffix(k, suffix)
			result[newKey] = stripEncryptedSuffix(v, suffix)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(x))
		for i, v := range x {
			result[i] = stripEncryptedSuffix(v, suffix)
		}
		return result
	default:
		return v
	}
}

func parseConfig(t testing.TB, path string) *Config {
	t.Helper()

	// Run sops to decrypt the configuration file into JSON
	var cmd = exec.Command("sops", "-d", "--output-type=json", path)
	var bs, err = cmd.Output()
	require.NoError(t, err, "failed to decrypt config file %q", path)

	// First unmarshal into a generic map and strip the "_sops" suffix from any properties
	var rawConfig map[string]interface{}
	require.NoError(t, json.Unmarshal(bs, &rawConfig), "failed to unmarshal decrypted config")
	var strippedConfig = stripEncryptedSuffix(rawConfig, "_sops")

	// Marshal back to JSON and then parse that as a Config struct
	bs, err = json.Marshal(strippedConfig)
	require.NoError(t, err, "failed to marshal processed config")
	var config Config
	require.NoError(t, json.Unmarshal(bs, &config), "failed to parse stripped config %q", path)
	return &config
}

func testCaptureSpec(t testing.TB) *st.CaptureSpec {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var endpointSpec = parseConfig(t, *cfgCapture)
	endpointSpec.Advanced.PollSchedule = "5s" // Always poll frequently in tests
	endpointSpec.Advanced.FeatureFlags = *testFeatureFlags

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"polled":"<TIMESTAMP>"`] = regexp.MustCompile(`"polled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"LastPolled":"<TIMESTAMP>"`] = regexp.MustCompile(`"LastPolled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)

	return &st.CaptureSpec{
		Driver:       redshiftDriver,
		EndpointSpec: endpointSpec,
		Validator:    &st.OrderedCaptureValidator{IncludeSourcedSchemas: true},
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

	var setupConfig = parseConfig(t, *cfgSetup)
	var controlURI = setupConfig.ToURI()
	t.Logf("opening control connection: addr=%q, user=%q", setupConfig.Address, setupConfig.User)
	var conn, err = sql.Open("pgx", controlURI)
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
	return fmt.Sprintf("%s.%s_%s", *testSchema, baseName, uniqueID), uniqueID
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
	t.Helper()
	var res Resource
	require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &res))
	res.Cursor = cursor
	var bs, err = json.Marshal(res)
	require.NoError(t, err)
	binding.ResourceConfigJson = bs
}

func TestSpec(t *testing.T) {
	response, err := redshiftDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestQueryTemplate(t *testing.T) {
	var res = &Resource{Name: "foobar", SchemaName: "testdata", TableName: "foobar"}

	tmplString, err := redshiftDriver.SelectQueryTemplate(res)
	require.NoError(t, err)

	tmpl, err := template.New("query").Funcs(templateFuncs).Parse(tmplString)
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
				"SchemaName":   res.SchemaName,
				"TableName":    res.TableName,
			}))
			cupaloy.SnapshotT(t, buf.String())
		})
	}
}

func TestBasicCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

	// Ignore SourcedSchema updates as their precise position in the change stream is
	// unpredictable when we're restarting the capture in parallel with changes.
	cs.Validator = &st.OrderedCaptureValidator{IncludeSourcedSchemas: false}

	// Have to sanitize the index within the polling interval because we're running
	// the capture in parallel with changes.
	cs.Sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

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
				executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName), i, fmt.Sprintf("Value for row %d", i))
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
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, a_real REAL, a_bool BOOL, a_date DATE, a_ts TIMESTAMP, a_tstz TIMESTAMPTZ)")

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s(id, a_real, a_bool, a_date, a_ts, a_tstz) VALUES ($1,$2,$3,$4,$5,$6)", tableName),
			100, -12.34, true, "2024-02-26", time.Date(2024, 02, 26, 12, 34, 56, 00, time.UTC), time.Date(2024, 02, 26, 12, 34, 56, 00, time.UTC))

		setShutdownAfterQuery(t, true)
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestFloatNaNs(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, a_real REAL, a_double DOUBLE PRECISION)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s VALUES (0, 2.0, 'NaN'), (1, 'NaN', 3.0), (2, 'Infinity', '-Infinity')`, tableName))

		setShutdownAfterQuery(t, true)
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestSchemaFilter(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

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
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"foo", *testSchema}
		cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
	})
}

func TestKeyDiscovery(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, "(k_smallint SMALLINT, k_int INTEGER, k_bigint BIGINT, k_bool BOOLEAN, k_str VARCHAR(8), data TEXT, PRIMARY KEY (k_smallint, k_int, k_bigint, k_bool, k_str))")
	cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{*testSchema}
	cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
}

func TestKeylessDiscovery(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, "(v_smallint SMALLINT, v_int INTEGER, v_bigint BIGINT, v_bool BOOLEAN, v_str VARCHAR(8), v_ts TIMESTAMP, v_tstz TIMESTAMP WITH TIME ZONE, v_text TEXT, v_int_notnull INTEGER NOT NULL, v_text_notnull TEXT NOT NULL)")
	cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{*testSchema}
	cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
}

func TestFeatureFlagUseSchemaInference(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	// Includes some extra junk to make sure the parsing helper logic is doing its job as intended
	cs.EndpointSpec.(*Config).Advanced.FeatureFlags = "this_flag_does_not_exist,use_schema_inference,,,,no_this_flag_also_does_not_exist"

	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })
}

// TestQueryTemplateOverride exercises a capture configured with an explicit query template
// in the resource spec rather than a blank template and specified table name/schema.
//
// This is the behavior of preexisting bindings which were created before the table/schema
// change in February 2025.
func TestQueryTemplateOverride(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT, updated_at TIMESTAMP)")

	// Create a binding with a query template override instead of table/schema
	var res = Resource{
		Name:     "query_template_override",
		Template: fmt.Sprintf(`SELECT * FROM %[1]s {{if not .IsFirstQuery}} WHERE updated_at > $1 {{end}} ORDER BY updated_at`, tableName),
		Cursor:   []string{"updated_at"},
	}
	bs, err := json.Marshal(res)
	require.NoError(t, err)
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	cs.Bindings[0].ResourceConfigJson = bs

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial rows
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES ($1, $2, $3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// More rows with later timestamps
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES ($1, $2, $3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i+5)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessCapture exercises discovery and capture from a table without
// a defined primary key, but using a user-specified updated_at cursor.
func TestKeylessCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(data TEXT, value INTEGER, updated_at TIMESTAMP)") // No PRIMARY KEY

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial batch of data
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value, updated_at) VALUES ($1, $2, $3)", tableName),
				fmt.Sprintf("Initial row %d", i), i,
				baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// Add more rows with later timestamps
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value, updated_at) VALUES ($1, $2, $3)", tableName),
				fmt.Sprintf("Additional row %d", i), i,
				baseTime.Add(time.Duration(i+5)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// Update some rows with new timestamps
		executeControlQuery(t, control, fmt.Sprintf(
			"UPDATE %s SET data = data || ' (updated)', updated_at = $1 WHERE value >= 3 AND value <= 7",
			tableName), baseTime.Add(15*time.Minute).Format("2006-01-02 15:04:05"))
		cs.Capture(ctx, t, nil)

		// Delete and reinsert some rows
		executeControlQuery(t, control, fmt.Sprintf("DELETE FROM %s WHERE value IN (4, 6)", tableName))
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (data, value, updated_at) VALUES ($1, $2, $3), ($4, $5, $6)",
			tableName),
			"Reinserted row 4", 4, baseTime.Add(20*time.Minute).Format("2006-01-02 15:04:05"),
			"Reinserted row 6", 6, baseTime.Add(20*time.Minute).Format("2006-01-02 15:04:05"))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessFullRefreshCapture exercises discovery and capture from a table
// without a defined primary key, and with the cursor left empty to test
// full-refresh behavior.
func TestKeylessFullRefreshCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(data TEXT, value INTEGER)") // No PRIMARY KEY

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// Initial batch of data
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
				fmt.Sprintf("Initial row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Add more rows - these should appear in the next full refresh
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
				fmt.Sprintf("Additional row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Modify some existing rows - changes should appear in next full refresh
		executeControlQuery(t, control, fmt.Sprintf(
			"UPDATE %s SET data = data || ' (updated)' WHERE value >= 3 AND value <= 7",
			tableName))
		cs.Capture(ctx, t, nil)

		// Delete some rows and add new ones - changes should appear in next full refresh
		executeControlQuery(t, control, fmt.Sprintf("DELETE FROM %s WHERE value IN (4, 6)", tableName))
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (data, value) VALUES ($1, $2), ($3, $4)",
			tableName),
			"New row A", 20,
			"New row B", 21)
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFeatureFlagKeylessRowID exercises discovery and capture from a keyless
// full-refresh table (as in TestKeylessFullRefreshCapture), but with the
// keyless_row_id feature flag explicitly set to true and false in distinct
// subtests.
func TestFeatureFlagKeylessRowID(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag string
	}{
		{"Enabled", "keyless_row_id"},
		{"Disabled", "no_keyless_row_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ctx, control = context.Background(), testControlClient(t)
			var tableName, uniqueID = testTableName(t, uniqueTableID(t))
			createTestTable(t, control, tableName, "(data TEXT, value INTEGER)") // No PRIMARY KEY

			// Create capture spec with specific feature flag
			var cs = testCaptureSpec(t)
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = tc.flag

			// Discover the table and verify discovery snapshot
			cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))

			// Empty cursor forces full refresh behavior
			setResourceCursor(t, cs.Bindings[0])

			t.Run("Discovery", func(t *testing.T) {
				cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings))
			})

			t.Run("Capture", func(t *testing.T) {
				setShutdownAfterQuery(t, true)

				// Initial data batch
				for i := 0; i < 5; i++ {
					executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
						fmt.Sprintf("Initial row %d", i), i)
				}
				cs.Capture(ctx, t, nil)

				// Add more rows
				for i := 5; i < 10; i++ {
					executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
						fmt.Sprintf("Additional row %d", i), i)
				}
				cs.Capture(ctx, t, nil)

				// Modify some existing rows
				executeControlQuery(t, control, fmt.Sprintf(
					"UPDATE %s SET data = data || ' (updated)' WHERE value >= 3 AND value <= 7",
					tableName))
				cs.Capture(ctx, t, nil)

				// Delete and add new rows
				executeControlQuery(t, control, fmt.Sprintf("DELETE FROM %s WHERE value IN (4, 6)", tableName))
				executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
					"New row A", 20)
				cs.Capture(ctx, t, nil)

				cupaloy.SnapshotT(t, cs.Summary())
			})
		})
	}
}

// TestCaptureFromView exercises discovery and capture from a view with an updated_at cursor.
func TestCaptureFromView(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var baseTableName, tableID = testTableName(t, uniqueTableID(t))
	var viewName, viewID = testTableName(t, uniqueTableID(t, "view"))

	// Create base table and view
	executeControlQuery(t, control, fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName))
	createTestTable(t, control, baseTableName, `(
        id INTEGER PRIMARY KEY,
        name TEXT,
        visible BOOLEAN,
        updated_at TIMESTAMP
    )`)
	executeControlQuery(t, control, fmt.Sprintf(`
        CREATE VIEW %s AS
        SELECT id, name, updated_at
        FROM %s
        WHERE visible = true`, viewName, baseTableName))
	t.Cleanup(func() { executeControlQuery(t, control, fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)) })

	// By default views should not be discovered.
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(tableID), regexp.MustCompile(viewID))
	t.Run("DiscoveryWithoutViews", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	// Enable view discovery and re-discover bindings, then set a cursor for capturing the view.
	cs.EndpointSpec.(*Config).Advanced.DiscoverViews = true
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(tableID), regexp.MustCompile(viewID))
	t.Run("DiscoveryWithViews", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	// Update both the base table and the view incrementally using the updated_at column.
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	setResourceCursor(t, cs.Bindings[1], "updated_at")

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// First batch: Insert rows into base table, some visible in view
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, name, visible, updated_at) VALUES ($1, $2, $3, $4)", baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// Second batch: More rows with later timestamps
		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, name, visible, updated_at) VALUES ($1, $2, $3, $4)", baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// Update some rows to change their visibility
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET visible = NOT visible, updated_at = $1
            WHERE id IN (2, 3, 4)`, baseTableName),
			baseTime.Add(20*time.Minute).Format("2006-01-02 15:04:05"))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFeatureFlagEmitSourcedSchemas runs a capture with the `emit_sourced_schemas` feature flag set.
func TestFeatureFlagEmitSourcedSchemas(t *testing.T) {
	var ctx, control = context.Background(), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY, data TEXT)`)

	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, 'hello'), (2, 'world')", tableName))

	for _, tc := range []struct {
		name string
		flag string
	}{
		{"Default", ""},
		{"Enabled", "emit_sourced_schemas"},
		{"Disabled", "no_emit_sourced_schemas"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cs = testCaptureSpec(t)
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = tc.flag
			cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
			setShutdownAfterQuery(t, true)

			cs.Capture(ctx, t, nil)
			cupaloy.SnapshotT(t, cs.Summary())
		})
	}
}

// TestNonexistentCursor exercises the situation of a capture whose configured
// cursor column doesn't actually exist.
func TestNonexistentCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY, data TEXT)`)
	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, 'hello'), (2, 'world')", tableName))

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at") // Nonexistent column

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestSourceTag verifies the output of a capture with /advanced/source_tag set
func TestSourceTag(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY, data TEXT)`)
	cs.EndpointSpec.(*Config).Advanced.SourceTag = "example_source_tag_1234"
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, 'one'), (2, 'two')", tableName))
		cs.Capture(ctx, t, nil)
		executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (3, 'three'), (4, 'four')", tableName))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}
