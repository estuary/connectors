package main

import (
	"bytes"
	"context"
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

	"cloud.google.com/go/bigquery"
	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
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
		"pivotal-base-360421",
		"The project ID to use for tests",
	)
	testDataset = flag.String(
		"test_dataset",
		"testdata",
		"The dataset (schema) to create test tables in",
	)
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
	response, err := bigqueryDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestQueryTemplate(t *testing.T) {
	res, err := bigqueryDriver.GenerateResource("foobar", "testdata", "foobar", "BASE TABLE")
	require.NoError(t, err)

	tmpl, err := template.New("query").Parse(res.Template)
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
	var client = testBigQueryClient(ctx, t)
	var uniqueID = "826935"
	var tableName = fmt.Sprintf("testdata.basic_capture_%s", uniqueID)

	createTestTable(ctx, t, client, tableName, "(id INTEGER PRIMARY KEY NOT ENFORCED, data STRING)")
	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	setCursorColumns(t, cs.Bindings[0], "id")

	t.Run("Capture", func(t *testing.T) {
		// Spawn a worker thread which will insert 10 rows of data as distinct inserts.
		// BigQuery insert latency is highly variable on test time-scales, so we signal
		// when the insertions are all done so the capturing code knows when to stop.
		var insertsDone atomic.Bool
		go func() {
			for i := 0; i < 10; i++ {
				executeSetupQuery(ctx, t, client, fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1)", tableName), i, fmt.Sprintf("Value for row %d", i))
				log.WithField("i", i).Debug("inserted row")
			}
			time.Sleep(5 * time.Second)
			insertsDone.Store(true)
		}()

		// Run the capture over and over for 5 seconds each time until all inserts have finished.
		for !insertsDone.Load() {
			var captureCtx, cancelCapture = context.WithCancel(ctx)
			time.AfterFunc(5*time.Second, func() {
				cancelCapture()
			})
			cs.Capture(captureCtx, t, nil)
		}
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestDatetimeCursor(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var client = testBigQueryClient(ctx, t)
	var uniqueID = "132448"
	var tableName = fmt.Sprintf("testdata.datetime_cursor_%s", uniqueID)

	createTestTable(ctx, t, client, tableName, "(id DATETIME, data STRING)")
	for _, x := range []string{"2023-08-10T07:54:54.123", "2024-10-23T03:22:31.456", "2024-10-23T03:23:00.789"} {
		executeSetupQuery(ctx, t, client, fmt.Sprintf("INSERT INTO %s VALUES (@p0, @p1)", tableName), x, fmt.Sprintf("Value for row %q", x))
	}

	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	setShutdownAfterQuery(t, true)
	setCursorColumns(t, cs.Bindings[0], "id")
	setQueryLimit(t, cs.Bindings[0], 1)

	t.Run("Capture", func(t *testing.T) {
		var deadline = time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			cs.Capture(ctx, t, nil)
		}
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func testBigQueryClient(ctx context.Context, t testing.TB) *bigquery.Client {
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
	client, err := bigquery.NewClient(ctx, *projectID, clientOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
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
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"polled":"<TIMESTAMP>"`] = regexp.MustCompile(`"polled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"LastPolled":"<TIMESTAMP>"`] = regexp.MustCompile(`"LastPolled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)

	return &st.CaptureSpec{
		Driver:       bigqueryDriver,
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

func executeSetupQuery(ctx context.Context, t testing.TB, client *bigquery.Client, query string, args ...interface{}) {
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
	require.NoError(t, err)
	status, err := job.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())
}

func createTestTable(ctx context.Context, t testing.TB, client *bigquery.Client, tableName, tableDef string) {
	t.Helper()
	executeSetupQuery(ctx, t, client, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() { executeSetupQuery(ctx, t, client, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	executeSetupQuery(ctx, t, client, fmt.Sprintf("CREATE TABLE %s%s", tableName, tableDef))
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

func setShutdownAfterQuery(t testing.TB, setting bool) {
	var oldSetting = TestShutdownAfterQuery
	TestShutdownAfterQuery = setting
	t.Cleanup(func() { TestShutdownAfterQuery = oldSetting })
}
