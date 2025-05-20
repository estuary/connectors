package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"regexp"
	"strings"
	"testing"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
)

var (
	testCredentialsPath = flag.String(
		"creds_path",
		"~/.config/gcloud/application_default_credentials.json",
		"Path to the credentials JSON to use for authentication in tests",
	)
	projectID = flag.String(
		"project_id",
		"estuary-theatre",
		"The project ID to use for tests",
	)
	testDataset = flag.String(
		"test_dataset",
		"testdata",
		"The dataset (schema) to create test tables in",
	)

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

	// Load credentials from disk and construct an endpoint spec
	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credentialsJSON, err := os.ReadFile(credentialsPath)
	require.NoError(t, err)

	var endpointSpec = &Config{
		CredentialsJSON: string(credentialsJSON),
		ProjectID:       *projectID,
		Dataset:         *testDataset,
		Advanced: advancedConfig{
			PollSchedule: "200ms",
			FeatureFlags: *testFeatureFlags,
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"index":99`] = regexp.MustCompile(`"index":\d+`)
	sanitizers[`"row_id":99`] = regexp.MustCompile(`"row_id":\d+`)
	sanitizers[`"polled":"<TIMESTAMP>"`] = regexp.MustCompile(`"polled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"LastPolled":"<TIMESTAMP>"`] = regexp.MustCompile(`"LastPolled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)

	return &st.CaptureSpec{
		Driver:       bigqueryDriver,
		EndpointSpec: endpointSpec,
		Validator:    &st.SortedCaptureValidator{},
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

func testBigQueryClient(t testing.TB) *bigquery.Client {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credentialsJSON, err := os.ReadFile(credentialsPath)
	require.NoError(t, err)

	var clientOpts = []option.ClientOption{
		option.WithCredentialsJSON([]byte(credentialsJSON)),
	}
	client, err := bigquery.NewClient(context.Background(), *projectID, clientOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
}

func testTableName(t *testing.T, uniqueID string) (name, id string) {
	t.Helper()
	const testSchemaName = "testdata"
	var baseName = strings.ToLower(strings.TrimPrefix(t.Name(), "Test"))
	for _, str := range []string{"/", "=", "(", ")"} {
		baseName = strings.ReplaceAll(baseName, str, "_")
	}
	return fmt.Sprintf("%s.%s_%s", testSchemaName, baseName, uniqueID), uniqueID
}

func createTestTable(ctx context.Context, t testing.TB, client *bigquery.Client, tableName, tableDef string) {
	t.Helper()
	require.NoError(t, executeSetupQuery(ctx, t, client, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)))
	require.NoError(t, executeSetupQuery(ctx, t, client, fmt.Sprintf("CREATE TABLE %s%s", tableName, tableDef)))
	// A note on table cleanup:
	//
	// Typically in SQL database tests we drop tables at the end of each test. But we
	// can't reliably do that in BigQuery.
	//
	// BigQuery has a rate limit of 5 table update operations per 10 seconds per table.
	// DML operations (such as test data inserts) count against this limit but aren't
	// subject to it (weird policy, but okay), which means that in general we can't
	// reliably drop a table immediately after running a test. So we don't bother,
	// because we _can_ reliably drop+recreate it at the start of a test, and that
	// is sufficient for our purposes.
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
		fmt.Fprintf(summary, "(no output)")
	}
	return summary.String()
}

func executeSetupQuery(ctx context.Context, t testing.TB, client *bigquery.Client, query string, args ...interface{}) error {
	t.Helper()
	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing setup query")
	var q = client.Query(query)
	var params []bigquery.QueryParameter
	for idx, val := range args {
		params = append(params, bigquery.QueryParameter{
			Name:  fmt.Sprintf("p%d", idx),
			Value: val,
		})
	}
	q.Parameters = params
	var job, err = q.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	return status.Err()
}

func parallelSetupQueries(ctx context.Context, t testing.TB, client *bigquery.Client, query string, args [][]any) error {
	t.Helper()
	var eg, setupCtx = errgroup.WithContext(ctx)
	for idx := range args {
		var idx = idx
		eg.Go(func() error {
			return executeSetupQuery(setupCtx, t, client, query, args[idx]...)
		})
	}
	return eg.Wait()
}

func setShutdownAfterQuery(t testing.TB, setting bool) {
	var oldSetting = TestShutdownAfterQuery
	TestShutdownAfterQuery = setting
	t.Cleanup(func() { TestShutdownAfterQuery = oldSetting })
}

func setCursorColumns(t testing.TB, binding *pf.CaptureSpec_Binding, cursor ...string) {
	var res Resource
	require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &res))
	res.Cursor = cursor
	resourceConfigBytes, err := json.Marshal(res)
	require.NoError(t, err)
	binding.ResourceConfigJson = resourceConfigBytes
}

func setQueryLimit(t testing.TB, binding *pf.CaptureSpec_Binding, limit int) {
	var res Resource
	require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &res))
	res.Template = strings.ReplaceAll(res.Template, ";", fmt.Sprintf(" LIMIT %d;", limit))
	resourceConfigBytes, err := json.Marshal(res)
	require.NoError(t, err)
	binding.ResourceConfigJson = resourceConfigBytes
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

// TestSpec verifies the connector's response to the Spec RPC against a snapshot.
func TestSpec(t *testing.T) {
	response, err := bigqueryDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// TestQueryTemplate is a unit test which verifies that the default query template produces
// the expected output for initial/subsequent polling queries with different cursors.
func TestQueryTemplate(t *testing.T) {
	var res = &Resource{Name: "foobar", SchemaName: "testdata", TableName: "foobar"}

	tmplString, err := bigqueryDriver.SelectQueryTemplate(res)
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

// TestSimpleCapture exercises the simplest use-case of a capture first doing an initial
// backfill and subsequently capturing new rows using ["id"] as the cursor.
func TestSimpleCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(id INTEGER PRIMARY KEY NOT ENFORCED, data STRING)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		require.NoError(t, parallelSetupQueries(ctx, t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1)", tableName), [][]any{
			{1, "Value for row 1"}, {2, "Value for row 2"},
			{3, "Value for row 3"}, {4, "Value for row 4"},
		}))
		cs.Capture(ctx, t, nil)

		require.NoError(t, parallelSetupQueries(ctx, t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1)", tableName), [][]any{
			{5, "Value for row 5"}, {6, "Value for row 6"},
			{7, "Value for row 7"}, {8, "Value for row 8"},
		}))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestIntegerTypes exercises discovery and capture of the integer types
// INT, SMALLINT, INTEGER, BIGINT, TINYINT, and BYTEINT. In BigQuery these
// types are all aliases for each other, but it's worth being thorough.
func TestIntegerTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, `(
        id            INTEGER PRIMARY KEY NOT ENFORCED,
        int_val       INT,
        smallint_val  SMALLINT,
        integer_val   INTEGER,
        bigint_val    BIGINT,
        tinyint_val   TINYINT,
        byteint_val   BYTEINT
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6)", tableName),
			[][]any{
				// Test minimum values
				{1, -9223372036854775808, -9223372036854775808, -9223372036854775808,
					-9223372036854775808, -9223372036854775808, -9223372036854775808},
				// Test maximum values
				{2, 9223372036854775807, 9223372036854775807, 9223372036854775807,
					9223372036854775807, 9223372036854775807, 9223372036854775807},
				// Test zero values
				{3, 0, 0, 0, 0, 0, 0},
				// Test NULL values
				{4, bigquery.NullInt64{}, bigquery.NullInt64{}, bigquery.NullInt64{}, bigquery.NullInt64{}, bigquery.NullInt64{}, bigquery.NullInt64{}},
				// Test some typical values
				{5, 42, 123, -456, 789, -12, 34},
			}))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestNumericTypes exercises discovery and capture of the numeric types
// numeric types NUMERIC, DECIMAL, BIGNUMERIC, BIGDECIMAL, and FLOAT64.
func TestNumericTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, `(
        id              INTEGER PRIMARY KEY NOT ENFORCED,
        numeric_val     NUMERIC,
        decimal_val     DECIMAL,
        bignumeric_val  BIGNUMERIC,
        bigdecimal_val  BIGDECIMAL,
        float64_val     FLOAT64
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// Helper function to create *big.Rat values
		mustRat := func(s string) *big.Rat {
			r, ok := new(big.Rat).SetString(s)
			require.True(t, ok, "Failed to parse rational number: %s", s)
			return r
		}

		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2, @p3, @p4, @p5)", tableName),
			[][]any{
				// Test maximum precision values for NUMERIC/DECIMAL (38 digits, 9 decimal places).
				// There is no convenient way to represent the maximum possible BIGNUMERIC value via
				// the particular way we're feeding in query parameters, so we just use a NUMERIC input.
				{1, mustRat("99999999999999999999999999999.999999999"),
					mustRat("99999999999999999999999999999.999999999"),
					mustRat("99999999999999999999999999999.999999999"),
					mustRat("99999999999999999999999999999.999999999"),
					1.7976931348623157e+308},
				// Test minimum values
				{2, mustRat("-99999999999999999999999999999.999999999"),
					mustRat("-99999999999999999999999999999.999999999"),
					mustRat("-99999999999999999999999999999.999999999"),
					mustRat("-99999999999999999999999999999.999999999"),
					-1.7976931348623157e+308},
				// Test zero values
				{3, new(big.Rat), new(big.Rat), new(big.Rat), new(big.Rat), 0.0},
				// Test some typical values with decimals
				{4, mustRat("123.456"), mustRat("789.012"),
					mustRat("1234.5678"), mustRat("9012.3456"),
					3.14159},
				// Test some whole numbers
				{5, mustRat("42"), mustRat("100"),
					mustRat("1000"), mustRat("10000"),
					12345.0},
			}))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestStringTypes exercises discovery and capture of the string types
// STRING and STRING(L).
func TestStringTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, `(
        id              INTEGER PRIMARY KEY NOT ENFORCED,
        string_val      STRING,
        string_len_val  STRING(50)
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				// Test empty strings
				{1, "", ""},
				// Test NULL values
				{2, bigquery.NullString{}, bigquery.NullString{}},
				// Test strings with special characters
				{3, "Hello, 世界!", "Unicode: ñ, é, ü"},
				// Test strings with quotes and backslashes
				{4, "He said \"Hello\"", "Path: C:\\Program Files\\"},
				// Test strings with newlines and tabs
				{5, "Line 1\nLine 2", "Col1\tCol2\tCol3"},
				// Test long strings
				{6, strings.Repeat("a", 100), strings.Repeat("b", 50)},
			}))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestBinaryTypes exercises discovery and capture of the binary types
// BOOLEAN and BYTES.
func TestBinaryTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, `(
        id            INTEGER PRIMARY KEY NOT ENFORCED,
        bool_val      BOOLEAN,
        bytes_val     BYTES
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				// Test basic boolean values
				{1, true, []byte("hello world")},
				{2, false, []byte{0x00, 0x01, 0x02, 0x03}},
				// Test NULL values
				{3, bigquery.NullBool{}, []byte(nil)},
				// Test empty bytes
				{4, true, []byte{}},
				// Test larger byte array
				{5, false, bytes.Repeat([]byte{0xAA}, 100)},
				// Test UTF-8 encoded bytes
				{6, true, []byte("Hello, 世界!")},
			}))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestTemporalTypes exercises discovery and capture of the temporal types
// DATE, DATETIME, TIME, and TIMESTAMP.
func TestTemporalTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, `(
        id              INTEGER PRIMARY KEY NOT ENFORCED,
        date_val        DATE,
        datetime_val    DATETIME,
        time_val        TIME,
        timestamp_val   TIMESTAMP
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2, @p3, @p4)", tableName),
			[][]any{
				// Test current time values
				{1, civil.Date{Year: 2025, Month: 2, Day: 18},
					"2025-02-18 15:04:05.999999",
					civil.TimeOf(time.Date(0, 0, 0, 15, 4, 5, 999999000, time.UTC)),
					time.Date(2025, 2, 18, 15, 4, 5, 999999000, time.UTC)},
				// Test minimum values (year 1)
				{2, civil.Date{Year: 1, Month: 1, Day: 1},
					"0001-01-01 00:00:00.000000",
					civil.TimeOf(time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)),
					time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
				// Test NULL values
				{3, bigquery.NullDate{}, bigquery.NullDateTime{},
					bigquery.NullTime{}, bigquery.NullTimestamp{}},
				// Test maximum values (year 9999)
				{4, civil.Date{Year: 9999, Month: 12, Day: 31},
					"9999-12-31 23:59:59.999999",
					civil.TimeOf(time.Date(0, 0, 0, 23, 59, 59, 999999000, time.UTC)),
					time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)},
				// Test timezone handling
				{5, civil.Date{Year: 2025, Month: 2, Day: 18},
					"2025-02-18 15:04:05.000000",
					civil.TimeOf(time.Date(0, 0, 0, 15, 4, 5, 0, time.UTC)),
					time.Date(2025, 2, 18, 15, 4, 5, 0, time.FixedZone("PST", -8*3600))},
			}))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCompositeTypes exercises discovery and capture of the composite types
// ARRAY and STRUCT.
func TestCompositeTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, `(
        id              INTEGER PRIMARY KEY NOT ENFORCED,
        string_array    ARRAY<STRING>,
        int_array       ARRAY<INT64>,
        struct_val      STRUCT<
            name        STRING,
            age        INT64,
            scores     ARRAY<FLOAT64>
        >,
        array_struct    ARRAY<STRUCT<
            id         INT64,
            label      STRING
        >>
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		type person struct {
			Name   string
			Age    int64
			Scores []float64
		}
		type label struct {
			ID    int64
			Label string
		}

		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2, @p3, @p4)", tableName),
			[][]any{
				// Test basic array and struct values
				{1, []string{"a", "b", "c"},
					[]int64{1, 2, 3},
					&person{"Alice", 25, []float64{90.5, 85.0, 92.3}},
					[]label{{1, "first"}, {2, "second"}},
				},
				// Test empty arrays
				{2, []string{},
					[]int64{},
					&person{"Bob", 30, []float64{}},
					[]label{},
				},
				// Test NULL values
				{3, []string(nil), []int64(nil), (*person)(nil), []label(nil)},
			}))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestJSONType exercises discovery and capture of the JSON column type.
func TestJSONType(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, `(
        id          INTEGER PRIMARY KEY NOT ENFORCED,
        json_val    JSON
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1)", tableName),
			[][]any{
				// Test simple JSON object
				{1, bigquery.NullJSON{JSONVal: `{"name": "Alice", "age": 30}`, Valid: true}},
				// Test JSON array
				{2, bigquery.NullJSON{JSONVal: `[1, 2, 3, "four", true, null]`, Valid: true}},
				// Test nested JSON
				{3, bigquery.NullJSON{JSONVal: `{
				    "user": {"name": "Bob", "email": "bob@example.com"},
				    "orders": [
				        {"id": 1, "items": ["apple", "banana"]},
				        {"id": 2, "items": ["orange"]}
				    ]
				}`, Valid: true}},
				// Test SQL NULL and JSON null
				{4, bigquery.NullJSON{}},
				{5, bigquery.NullJSON{JSONVal: `null`, Valid: true}},
				// Test empty JSON object and array
				{6, bigquery.NullJSON{JSONVal: `{}`, Valid: true}},
				{7, bigquery.NullJSON{JSONVal: `[]`, Valid: true}},
				// Test JSON with special characters
				{8, bigquery.NullJSON{JSONVal: `{"message": "Hello, 世界!\nNew line\"Quotes\"\\Backslash"}`, Valid: true}},
			}))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFullRefresh exercises the scenario of a table without a configured cursor,
// which causes the capture to perform a full refresh every time it runs.
func TestFullRefresh(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(id INTEGER PRIMARY KEY NOT ENFORCED, data STRING)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	setShutdownAfterQuery(t, true)

	// Sorting is required for test stability because full-refresh captures are unordered.
	// The multiset of row-captures will be the same across all runs, so this works.
	cs.Validator = &st.SortedCaptureValidator{}

	require.NoError(t, parallelSetupQueries(ctx, t, control,
		fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1)", tableName), [][]any{
			{0, "Value for row 0"}, {1, "Value for row 1"},
			{2, "Value for row 2"}, {3, "Value for row 3"},
		}))
	cs.Capture(ctx, t, nil)
	t.Run("Capture1", func(t *testing.T) { cupaloy.SnapshotT(t, cs.Summary()); cs.Reset() })

	require.NoError(t, parallelSetupQueries(ctx, t, control,
		fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1)", tableName), [][]any{
			{4, "Value for row 4"}, {5, "Value for row 5"},
			{6, "Value for row 6"}, {7, "Value for row 7"},
		}))
	cs.Capture(ctx, t, nil)
	t.Run("Capture2", func(t *testing.T) { cupaloy.SnapshotT(t, cs.Summary()); cs.Reset() })
}

// TestCaptureWithUpdatedAtCursor exercises the use-case of a capture using a
// non-primary-key updated_at column (of type TIMESTAMP) as the cursor.
func TestCaptureWithUpdatedAtCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(id INTEGER PRIMARY KEY NOT ENFORCED, data STRING, updated_at TIMESTAMP)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial set of rows with sequential timestamps
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{0, "Value for row 0", baseTime.Add(0 * time.Minute)},
				{1, "Value for row 1", baseTime.Add(1 * time.Minute)},
				{2, "Value for row 2", baseTime.Add(2 * time.Minute)},
				{3, "Value for row 3", baseTime.Add(3 * time.Minute)},
				{4, "Value for row 4", baseTime.Add(4 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		// Second set of rows with later timestamps
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{5, "Value for row 5", baseTime.Add(10 * time.Minute)},
				{6, "Value for row 6", baseTime.Add(11 * time.Minute)},
				{7, "Value for row 7", baseTime.Add(12 * time.Minute)},
				{8, "Value for row 8", baseTime.Add(13 * time.Minute)},
				{9, "Value for row 9", baseTime.Add(14 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithDatetimeCursor exercises the use-case of a capture using a
// DATETIME updated_at column as the cursor.
func TestCaptureWithDatetimeCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(id INTEGER PRIMARY KEY NOT ENFORCED, data STRING, updated_at DATETIME)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// Initial set of rows with sequential timestamps
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{0, "Value for row 0", "2025-02-13 12:00:00.000000"},
				{1, "Value for row 1", "2025-02-13 12:01:00.000000"},
				{2, "Value for row 2", "2025-02-13 12:02:00.000000"},
				{3, "Value for row 3", "2025-02-13 12:03:00.000000"},
				{4, "Value for row 4", "2025-02-13 12:04:00.000000"},
			}))
		cs.Capture(ctx, t, nil)

		// Second set of rows with later timestamps
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{5, "Value for row 5", "2025-02-13 12:10:00.000000"},
				{6, "Value for row 6", "2025-02-13 12:11:00.000000"},
				{7, "Value for row 7", "2025-02-13 12:12:00.000000"},
				{8, "Value for row 8", "2025-02-13 12:13:00.000000"},
				{9, "Value for row 9", "2025-02-13 12:14:00.000000"},
			}))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithTwoColumnCursor exercises the use-case of a capture using a
// multiple-column compound cursor.
func TestCaptureWithTwoColumnCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT ENFORCED,
        col1 INTEGER,
        col2 INTEGER,
        data STRING
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "col1", "col2")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// First batch of rows with ascending values in both cursor columns
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2, @p3)", tableName),
			[][]any{
				{0, 1, 1, "Value for row 0"},
				{1, 1, 2, "Value for row 1"},
				{2, 1, 3, "Value for row 2"},
				{3, 2, 1, "Value for row 3"},
				{4, 2, 2, "Value for row 4"},
				{5, 2, 3, "Value for row 5"},
			}))
		cs.Capture(ctx, t, nil)

		// Second batch testing cursor behavior
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2, @p3)", tableName),
			[][]any{
				{6, 0, 9, "Value ignored because col1 is too small"},
				{7, 2, 0, "Value ignored because col2 is too small"},
				{8, 3, 1, "Value for row 8"},
				{9, 3, 2, "Value for row 9"},
				{10, 3, 3, "Value for row 10"},
				{11, 4, 1, "Value for row 11"},
				{12, 4, 2, "Value for row 12"},
				{13, 4, 3, "Value for row 13"},
			}))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithModifications exercises the use-case of a capture using an updated_at
// cursor where some rows are modified and deleted between captures.
func TestCaptureWithModifications(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(id INTEGER PRIMARY KEY NOT ENFORCED, data STRING, updated_at TIMESTAMP)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial set of rows with sequential timestamps
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{0, "Initial value for row 0", baseTime.Add(0 * time.Minute)},
				{1, "Initial value for row 1", baseTime.Add(1 * time.Minute)},
				{2, "Initial value for row 2", baseTime.Add(2 * time.Minute)},
				{3, "Initial value for row 3", baseTime.Add(3 * time.Minute)},
				{4, "Initial value for row 4", baseTime.Add(4 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		// Update and delete some rows, as well as inserting new ones
		require.NoError(t, executeSetupQuery(ctx, t, control,
			fmt.Sprintf("UPDATE %s SET data = @p0, updated_at = @p1 WHERE id = @p2", tableName),
			"Modified value for row 3", baseTime.Add(15*time.Minute), 3))
		require.NoError(t, executeSetupQuery(ctx, t, control,
			fmt.Sprintf("UPDATE %s SET data = @p0, updated_at = @p1 WHERE id = @p2", tableName),
			"Modified value for row 4", baseTime.Add(16*time.Minute), 4))
		require.NoError(t, executeSetupQuery(ctx, t, control,
			fmt.Sprintf("DELETE FROM %s WHERE id IN (1, 2)", tableName)))
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{5, "Value for row 5", baseTime.Add(20 * time.Minute)},
				{6, "Value for row 6", baseTime.Add(21 * time.Minute)},
				{7, "Value for row 7", baseTime.Add(22 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithEmptyPoll exercises the scenario where a polling interval finds no new rows.
func TestCaptureWithEmptyPoll(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(id INTEGER PRIMARY KEY NOT ENFORCED, data STRING, updated_at TIMESTAMP)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// First batch of rows
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{0, "Value for row 0", baseTime.Add(0 * time.Minute)},
				{1, "Value for row 1", baseTime.Add(1 * time.Minute)},
				{2, "Value for row 2", baseTime.Add(2 * time.Minute)},
				{3, "Value for row 3", baseTime.Add(3 * time.Minute)},
				{4, "Value for row 4", baseTime.Add(4 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		// No changes
		cs.Capture(ctx, t, nil)

		// Second batch of rows with later timestamps
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{5, "Value for row 5", baseTime.Add(10 * time.Minute)},
				{6, "Value for row 6", baseTime.Add(11 * time.Minute)},
				{7, "Value for row 7", baseTime.Add(12 * time.Minute)},
				{8, "Value for row 8", baseTime.Add(13 * time.Minute)},
				{9, "Value for row 9", baseTime.Add(14 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithNullCursor exercises the handling of NULL values in cursor columns.
func TestCaptureWithNullCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(id INTEGER PRIMARY KEY NOT ENFORCED, data STRING, sort_col INTEGER)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "sort_col")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	setShutdownAfterQuery(t, true)

	// Sorting is required for test stability because null-cursored rows are unordered.
	cs.Validator = &st.SortedCaptureValidator{}

	// First batch with mix of NULL and non-NULL cursor values
	require.NoError(t, parallelSetupQueries(ctx, t, control,
		fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
		[][]any{
			{0, "Value with NULL cursor", bigquery.NullInt64{}},
			{1, "Value with cursor 10", 10},
			{2, "Another NULL cursor", bigquery.NullInt64{}},
			{3, "Value with cursor 20", 20},
			{4, "Third NULL cursor", bigquery.NullInt64{}},
		}))
	cs.Capture(ctx, t, nil)
	t.Run("Capture1", func(t *testing.T) { cupaloy.SnapshotT(t, cs.Summary()); cs.Reset() })

	// Second batch testing NULL handling after initial cursor
	require.NoError(t, parallelSetupQueries(ctx, t, control,
		fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
		[][]any{
			{5, "Late NULL cursor", bigquery.NullInt64{}}, // Will not be captured
			{6, "Value with cursor 15", 15},               // Will not be captured (cursor 20 is already seen)
			{7, "Value with cursor 25", 25},
			{8, "Another late NULL", bigquery.NullInt64{}}, // Will not be captured
			{9, "Final value cursor 30", 30},
		}))
	cs.Capture(ctx, t, nil)
	t.Run("Capture2", func(t *testing.T) { cupaloy.SnapshotT(t, cs.Summary()); cs.Reset() })
}

// TestQueryTemplateOverride exercises a capture configured with an explicit query template
// in the resource spec rather than a blank template and specified table name/schema.
//
// This is the behavior of preexisting bindings which were created before the table/schema
// change in February 2025.
func TestQueryTemplateOverride(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(id INTEGER PRIMARY KEY NOT ENFORCED, data STRING, updated_at TIMESTAMP)")

	// Create a binding with a query template override instead of table/schema
	var res = Resource{
		Name:     "query_template_override",
		Template: fmt.Sprintf(`SELECT * FROM %[1]s {{if not .IsFirstQuery}} WHERE updated_at > @p0 {{end}} ORDER BY updated_at`, tableName),
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
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{0, "Value for row 0", baseTime.Add(0 * time.Minute)},
				{1, "Value for row 1", baseTime.Add(1 * time.Minute)},
				{2, "Value for row 2", baseTime.Add(2 * time.Minute)},
				{3, "Value for row 3", baseTime.Add(3 * time.Minute)},
				{4, "Value for row 4", baseTime.Add(4 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		// More rows with later timestamps
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{5, "Value for row 5", baseTime.Add(10 * time.Minute)},
				{6, "Value for row 6", baseTime.Add(11 * time.Minute)},
				{7, "Value for row 7", baseTime.Add(12 * time.Minute)},
				{8, "Value for row 8", baseTime.Add(13 * time.Minute)},
				{9, "Value for row 9", baseTime.Add(14 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessCapture exercises discovery and capture from a table without
// a defined primary key, but using a user-specified updated_at cursor.
func TestKeylessCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(data STRING, value INTEGER, updated_at TIMESTAMP)") // No PRIMARY KEY

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setCursorColumns(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial batch of data
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s (data, value, updated_at) VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{fmt.Sprintf("Initial row %d", 0), 0, baseTime.Add(0 * time.Minute)},
				{fmt.Sprintf("Initial row %d", 1), 1, baseTime.Add(1 * time.Minute)},
				{fmt.Sprintf("Initial row %d", 2), 2, baseTime.Add(2 * time.Minute)},
				{fmt.Sprintf("Initial row %d", 3), 3, baseTime.Add(3 * time.Minute)},
				{fmt.Sprintf("Initial row %d", 4), 4, baseTime.Add(4 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		// Add more rows with later timestamps
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s (data, value, updated_at) VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{fmt.Sprintf("Additional row %d", 5), 5, baseTime.Add(10 * time.Minute)},
				{fmt.Sprintf("Additional row %d", 6), 6, baseTime.Add(11 * time.Minute)},
				{fmt.Sprintf("Additional row %d", 7), 7, baseTime.Add(12 * time.Minute)},
				{fmt.Sprintf("Additional row %d", 8), 8, baseTime.Add(13 * time.Minute)},
				{fmt.Sprintf("Additional row %d", 9), 9, baseTime.Add(14 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		// Update some rows with new timestamps
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("UPDATE %s SET data = CONCAT(data, ' (updated)'), updated_at = @p0 WHERE value = @p1", tableName),
			[][]any{
				{baseTime.Add(15 * time.Minute), 3},
				{baseTime.Add(15 * time.Minute), 4},
				{baseTime.Add(15 * time.Minute), 5},
				{baseTime.Add(15 * time.Minute), 6},
				{baseTime.Add(15 * time.Minute), 7},
			}))
		cs.Capture(ctx, t, nil)

		// Delete and reinsert some rows
		require.NoError(t, executeSetupQuery(ctx, t, control,
			fmt.Sprintf("DELETE FROM %s WHERE value IN (4, 6)", tableName)))
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s (data, value, updated_at) VALUES (@p0, @p1, @p2)", tableName),
			[][]any{
				{"Reinserted row 4", 4, baseTime.Add(20 * time.Minute)},
				{"Reinserted row 6", 6, baseTime.Add(20 * time.Minute)},
			}))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessFullRefreshCapture exercises discovery and capture from a table
// without a defined primary key, and with the cursor left empty to test
// full-refresh behavior.
func TestKeylessFullRefreshCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(ctx, t, control, tableName, "(data STRING, value INTEGER)") // No PRIMARY KEY

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// Initial batch of data
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s (data, value) VALUES (@p0, @p1)", tableName),
			[][]any{
				{fmt.Sprintf("Initial row %d", 0), 0},
				{fmt.Sprintf("Initial row %d", 1), 1},
				{fmt.Sprintf("Initial row %d", 2), 2},
				{fmt.Sprintf("Initial row %d", 3), 3},
				{fmt.Sprintf("Initial row %d", 4), 4},
			}))
		cs.Capture(ctx, t, nil)

		// Add more rows - these should appear in the next full refresh
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s (data, value) VALUES (@p0, @p1)", tableName),
			[][]any{
				{fmt.Sprintf("Additional row %d", 5), 5},
				{fmt.Sprintf("Additional row %d", 6), 6},
				{fmt.Sprintf("Additional row %d", 7), 7},
				{fmt.Sprintf("Additional row %d", 8), 8},
				{fmt.Sprintf("Additional row %d", 9), 9},
			}))
		cs.Capture(ctx, t, nil)

		// Modify some existing rows - changes should appear in next full refresh
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("UPDATE %s SET data = CONCAT(data, ' (updated)') WHERE value = @p0", tableName),
			[][]any{{3}, {4}, {5}, {6}, {7}}))
		cs.Capture(ctx, t, nil)

		// Delete some rows and add new ones - changes should appear in next full refresh
		require.NoError(t, executeSetupQuery(ctx, t, control,
			fmt.Sprintf("DELETE FROM %s WHERE value IN (4, 6)", tableName)))
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s (data, value) VALUES (@p0, @p1)", tableName),
			[][]any{{"New row A", 20}, {"New row B", 21}}))
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
			var ctx, control = context.Background(), testBigQueryClient(t)
			var tableName, uniqueID = testTableName(t, uniqueTableID(t))
			createTestTable(ctx, t, control, tableName, "(data STRING, value INTEGER)") // No PRIMARY KEY

			// Create capture spec with specific feature flag
			var cs = testCaptureSpec(t)
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = tc.flag

			// Discover the table and verify discovery snapshot
			cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))

			// Empty cursor forces full refresh behavior
			setCursorColumns(t, cs.Bindings[0])

			t.Run("Discovery", func(t *testing.T) {
				cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings))
			})

			t.Run("Capture", func(t *testing.T) {
				setShutdownAfterQuery(t, true)

				// Initial data batch
				require.NoError(t, parallelSetupQueries(ctx, t, control,
					fmt.Sprintf("INSERT INTO %s (data, value) VALUES (@p0, @p1)", tableName),
					[][]any{
						{fmt.Sprintf("Initial row %d", 0), 0},
						{fmt.Sprintf("Initial row %d", 1), 1},
						{fmt.Sprintf("Initial row %d", 2), 2},
						{fmt.Sprintf("Initial row %d", 3), 3},
						{fmt.Sprintf("Initial row %d", 4), 4},
					}))
				cs.Capture(ctx, t, nil)

				// Add more rows
				require.NoError(t, parallelSetupQueries(ctx, t, control,
					fmt.Sprintf("INSERT INTO %s (data, value) VALUES (@p0, @p1)", tableName),
					[][]any{
						{fmt.Sprintf("Additional row %d", 5), 5},
						{fmt.Sprintf("Additional row %d", 6), 6},
						{fmt.Sprintf("Additional row %d", 7), 7},
						{fmt.Sprintf("Additional row %d", 8), 8},
						{fmt.Sprintf("Additional row %d", 9), 9},
					}))
				cs.Capture(ctx, t, nil)

				// Modify some existing rows
				require.NoError(t, parallelSetupQueries(ctx, t, control,
					fmt.Sprintf("UPDATE %s SET data = CONCAT(data, ' (updated)') WHERE value = @p0", tableName),
					[][]any{{3}, {4}, {5}, {6}, {7}}))
				cs.Capture(ctx, t, nil)

				// Delete and add new rows
				require.NoError(t, executeSetupQuery(ctx, t, control,
					fmt.Sprintf("DELETE FROM %s WHERE value IN (4, 6)", tableName)))
				require.NoError(t, parallelSetupQueries(ctx, t, control,
					fmt.Sprintf("INSERT INTO %s (data, value) VALUES (@p0, @p1)", tableName),
					[][]any{{"New row A", 20}}))
				cs.Capture(ctx, t, nil)

				cupaloy.SnapshotT(t, cs.Summary())
			})
		})
	}
}

// TestCaptureFromView exercises discovery and capture from a view with an updated_at cursor.
func TestCaptureFromView(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testBigQueryClient(t)
	var baseTableName, tableID = testTableName(t, uniqueTableID(t))
	var viewName, viewID = testTableName(t, uniqueTableID(t, "view"))

	// Create base table and view
	createTestTable(ctx, t, control, baseTableName, `(
		id INTEGER NOT NULL,
		name STRING,
		visible BOOL,
		updated_at TIMESTAMP
	)`)
	require.NoError(t, executeSetupQuery(ctx, t, control, fmt.Sprintf(`
		CREATE VIEW %s AS
		SELECT id, name, updated_at
		FROM %s
		WHERE visible = true`, viewName, baseTableName)))
	t.Cleanup(func() {
		_ = executeSetupQuery(ctx, t, control, fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName))
	})

	// By default views should not be discovered.
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(tableID), regexp.MustCompile(viewID))
	t.Run("DiscoveryWithoutViews", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	// Enable view discovery and re-discover bindings, then set a cursor for capturing the view.
	cs.EndpointSpec.(*Config).Advanced.DiscoverViews = true
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(tableID), regexp.MustCompile(viewID))
	t.Run("DiscoveryWithViews", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	// Update both the base table and the view incrementally using the updated_at column.
	setCursorColumns(t, cs.Bindings[0], "updated_at")
	setCursorColumns(t, cs.Bindings[1], "updated_at")

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// First batch: Insert rows into base table, some visible in view
		var firstBatch [][]any
		for i := 0; i < 10; i++ {
			firstBatch = append(firstBatch, []any{
				i,
				fmt.Sprintf("Row %d", i),
				i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i) * time.Minute),
			})
		}
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s (id, name, visible, updated_at) VALUES (@p0, @p1, @p2, @p3)", baseTableName),
			firstBatch))
		cs.Capture(ctx, t, nil)

		// Second batch: More rows with later timestamps
		var secondBatch [][]any
		for i := 10; i < 20; i++ {
			secondBatch = append(secondBatch, []any{
				i,
				fmt.Sprintf("Row %d", i),
				i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i) * time.Minute),
			})
		}
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("INSERT INTO %s (id, name, visible, updated_at) VALUES (@p0, @p1, @p2, @p3)", baseTableName),
			secondBatch))
		cs.Capture(ctx, t, nil)

		// Update some rows to change their visibility
		updateTime := baseTime.Add(20 * time.Minute)
		require.NoError(t, parallelSetupQueries(ctx, t, control,
			fmt.Sprintf("UPDATE %s SET visible = NOT visible, updated_at = @p0 WHERE id IN (@p1, @p2, @p3)", baseTableName),
			[][]any{{updateTime, 2, 3, 4}}))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}
