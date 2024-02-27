package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-mysql-org/go-mysql/client"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbAddress     = flag.String("db_address", "localhost:3306", "The database server address to use for tests")
	dbControlUser = flag.String("db_control_user", "root", "The user for test setup/control operations")
	dbControlPass = flag.String("db_control_pass", "secret1234", "The password the the test setup/control user")
	dbCaptureUser = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass = flag.String("db_capture_pass", "secret1234", "The password for the capture user")
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
	response, err := mysqlDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestQueryTemplate(t *testing.T) {
	res, err := mysqlDriver.GenerateResource("test_foobar", "test", "foobar", "BASE TABLE")
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

func TestQueryPlaceholderExpansion(t *testing.T) {
	var querySource = `SELECT * FROM "test"."foobar" WHERE (ka > @flow_cursor_value[0]) OR (ka = @flow_cursor_value[0] AND kb > @flow_cursor_value[1]) OR (ka = @flow_cursor_value[0] AND kb = @flow_cursor_value[1] AND kc > @flow_cursor_value[2]) OR (x > ?) OR (y > ?);`
	var argvals = []any{1, "two", 3.0, "xval", "yval"}
	var query, args, err = expandQueryPlaceholders(querySource, argvals)
	require.NoError(t, err)
	var buf = new(strings.Builder)
	fmt.Fprintf(buf, "Query: %s\n\n---\n", query)
	for i, arg := range args {
		fmt.Fprintf(buf, "Argument %d: %#v\n", i, arg)
	}
	cupaloy.SnapshotT(t, buf.String())
}

func TestBasicCapture(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var uniqueID = "826935"
	var tableName = fmt.Sprintf("test.basic_capture_%s", uniqueID)

	// Connect to the test database and create the test table
	var conn, err = client.Connect(*dbAddress, *dbControlUser, *dbControlPass, "mysql")
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	conn.Execute(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() { conn.Execute(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	conn.Execute(fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, data TEXT)", tableName))

	// Discover the table and then set a cursor
	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))
	var res Resource
	require.NoError(t, json.Unmarshal(cs.Bindings[0].ResourceConfigJson, &res))
	res.Cursor = []string{"id"}
	cs.Bindings[0].ResourceConfigJson = marshal(t, res)

	// Spawn a worker thread which will insert 250 rows of data over the course of 25 seconds.
	go func() {
		for i := 0; i < 250; i++ {
			time.Sleep(100 * time.Millisecond)
			conn.Execute(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName), i, fmt.Sprintf("Value for row %d", i))
			log.WithField("i", i).Debug("inserted row")
		}
	}()

	// Perform six captures each running for 5 seconds, then verify that
	// the resulting data is correct.
	for i := 0; i < 6; i++ {
		var captureCtx, cancelCapture = context.WithCancel(ctx)
		time.AfterFunc(5*time.Second, cancelCapture)
		cs.Capture(captureCtx, t, nil)
	}
	cupaloy.SnapshotT(t, cs.Summary())
}

func marshal(t testing.TB, v any) []byte {
	var bs, err = json.Marshal(v)
	require.NoError(t, err)
	return bs
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
		Advanced: advancedConfig{
			PollSchedule: "200ms",
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|-[0-9]+:[0-9]+)"`)
	sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)

	return &st.CaptureSpec{
		Driver:       mysqlDriver,
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
