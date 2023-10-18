package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/jackc/pgx/v4/pgxpool"
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
	var uniqueID = "826935"
	var tableName = fmt.Sprintf("test.basic_capture_%s", uniqueID)

	// Connect to the test database and create the test table
	var controlURI = fmt.Sprintf(`postgres://%s:%s@%s/%s`, *dbControlUser, *dbControlPass, *dbAddress, *dbName)
	var conn, err = pgxpool.Connect(ctx, controlURI)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	_, err = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, data TEXT)", tableName))
	require.NoError(t, err)

	// Discover the table and accept the default configuration
	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))

	// Spawn a worker thread which will insert 250 rows of data over the course of 25 seconds.
	go func() {
		for i := 0; i < 250; i++ {
			time.Sleep(100 * time.Millisecond)
			_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName), i, fmt.Sprintf("Value for row %d", i))
			require.NoError(t, err)
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
			PollInterval: "200ms",
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	for k, v := range st.DefaultSanitizers {
		sanitizers[k] = v
	}
	sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)
	sanitizers[`"txid":999999`] = regexp.MustCompile(`"txid":[0-9]+`)

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
		})
	}
	return bindings
}
