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
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbAddress     = flag.String("db_address", "localhost:5432", "The database server address to use for tests")
	dbName        = flag.String("db_name", "postgres", "Use the named database for tests")
	dbControlUser = flag.String("db_control_user", "postgres", "The user for test setup/control operations")
	dbControlPass = flag.String("db_control_pass", "postgres", "The password the the test setup/control user")
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
	response, err := postgresDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
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
	t.Run("Discovery", func(t *testing.T) {
		var summary = new(strings.Builder)
		for idx, binding := range cs.Bindings {
			fmt.Fprintf(summary, "Binding %d:\n", idx)
			bs, err := json.MarshalIndent(binding, "  ", "  ")
			require.NoError(t, err)
			io.Copy(summary, bytes.NewReader(bs))
			fmt.Fprintf(summary, "\n")
		}
		if len(cs.Bindings) == 0 {
			fmt.Fprintf(summary, "(no output)")
		}
		cupaloy.SnapshotT(t, summary.String())
	})

	t.Run("Capture", func(t *testing.T) {
		// Spawn a worker thread which will insert 50 rows of data in parallel with the capture.
		var insertsDone atomic.Bool
		go func() {
			for i := 0; i < 250; i++ {
				time.Sleep(100 * time.Millisecond)
				executeControlQuery(ctx, t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName), i, fmt.Sprintf("Value for row %d", i))
				log.WithField("i", i).Debug("inserted row")
			}
			time.Sleep(1 * time.Second)
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
			PollSchedule: "200ms",
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	for k, v := range st.DefaultSanitizers {
		sanitizers[k] = v
	}
	sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)
	sanitizers[`"txid":999999`] = regexp.MustCompile(`"txid":[0-9]+`)
	sanitizers[`"CursorNames":["txid"],"CursorValues":[999999]`] = regexp.MustCompile(`"CursorNames":\["txid"\],"CursorValues":\[[0-9]+\]`)

	return &st.CaptureSpec{
		Driver:       postgresDriver,
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
