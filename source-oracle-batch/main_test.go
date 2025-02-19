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
	"os/exec"
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
	response, err := oracleDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestBasicCapture(t *testing.T) {
	documentsPerCheckpoint = 50
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "826935"
	var tableName = fmt.Sprintf("c##flow_test_logminer.basic_capture_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, data VARCHAR(200)) ROWDEPENDENCIES", tableName))

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { snapshotBindings(t, cs.Bindings) })

	t.Run("Capture", func(t *testing.T) {
		// Spawn a worker thread which will insert 50 rows of data in parallel with the capture.
		var insertsDone atomic.Bool
		go func() {
			for i := 0; i < 250; i++ {
				time.Sleep(100 * time.Millisecond)
				executeControlQuery(ctx, t, control, fmt.Sprintf("INSERT INTO %s VALUES (:1, :2)", tableName), i, fmt.Sprintf("Value for row %d", i))
				log.WithField("i", i).Debug("inserted row")
			}
			time.Sleep(1 * time.Second)
			insertsDone.Store(true)
		}()

		var ids = make(map[string]bool)
		// Run the capture over and over for 5 seconds each time until all inserts have finished, then verify results.
		for !insertsDone.Load() {
			var captureCtx, cancelCapture = context.WithCancel(ctx)
			time.AfterFunc(5*time.Second, cancelCapture)
			cs.Capture(captureCtx, t, func(data json.RawMessage) {
				if !strings.Contains(string(data), "bindingStateV1") {
					var d doc
					err := json.Unmarshal(data, &d)
					require.NoError(t, err)
					ids[d.Id] = true
				}
			})
		}

		require.Equal(t, 250, len(ids))
		cupaloy.SnapshotT(t, cs.Summary())
	})

	documentsPerCheckpoint = 1000
}

type doc struct {
	Id string `json:"ID"`
}

func TestCaptureCheckpointSCN(t *testing.T) {
	documentsPerCheckpoint = 10
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "216995"
	var tableName = fmt.Sprintf("c##flow_test_logminer.check_scn_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, data VARCHAR(200))", tableName))

	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))

	t.Run("Capture", func(t *testing.T) {
		// Insert 600 documents in 20 transactions
		for i := 0; i < 20; i++ {
			time.Sleep(100 * time.Millisecond)
			// INSERT 30 documents as part of a single transaction so they share a ROWSCN
			executeControlQuery(ctx, t, control, fmt.Sprintf(`BEGIN
        FOR i IN (%d*30) .. (((%d+1)*30)-1)
        LOOP
          INSERT INTO %s VALUES (i, 'value for row ' || i);
        END LOOP;
      END;`, i, i, tableName))
			log.WithField("i", i).Debug("inserted batch of rows")
		}

		var captureCtx, cancelCapture = context.WithCancel(ctx)
		var ids = make(map[string]bool)

		// On first run, capture until checkpoint. We checkpoint early by having changed `documentsPerCheckpoint` so that we don't capture all documents
		// of the same ROWSCN in one run
		cs.Capture(captureCtx, t, func(data json.RawMessage) {
			if strings.Contains(string(data), "bindingStateV1") {
				log.Info("cancelling capture")
				cancelCapture()
			} else {
				var d doc
				err := json.Unmarshal(data, &d)
				require.NoError(t, err)
				ids[d.Id] = true
			}
		})
		time.Sleep(5 * time.Second)

		// Then run the capture again and see if we capture all documents
		captureCtx, cancelCapture = context.WithCancel(ctx)
		time.AfterFunc(5*time.Second, cancelCapture)
		cs.Capture(captureCtx, t, func(data json.RawMessage) {
			if !strings.Contains(string(data), "bindingStateV1") {
				var d doc
				err := json.Unmarshal(data, &d)
				require.NoError(t, err)
				ids[d.Id] = true
			}
		})

		require.Equal(t, 600, len(ids))
	})

	documentsPerCheckpoint = 1000
}

func TestBasicDatatypes(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "13111208"
	var tableName = fmt.Sprintf("c##flow_test_logminer.basic_datatypes_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, a_int INTEGER, a_bool NUMBER(1), a_date DATE, a_ts TIMESTAMP, a_tstz TIMESTAMP WITH TIME ZONE)", tableName))

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { snapshotBindings(t, cs.Bindings) })

	t.Run("Capture", func(t *testing.T) {
		executeControlQuery(ctx, t, control, fmt.Sprintf("INSERT INTO %s(id, a_int, a_bool, a_date, a_ts, a_tstz) VALUES (100, -12.34, 1, DATE '2024-02-26', TIMESTAMP '2024-02-26 12:34:56', TIMESTAMP '2024-02-26 12:34:56.00+00:00')", tableName))

		// Run the capture for 5 seconds, which should be plenty to pull down a few rows.
		var captureCtx, cancelCapture = context.WithCancel(ctx)
		time.AfterFunc(5*time.Second, cancelCapture)
		cs.Capture(captureCtx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestDecimals(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "75319739"
	var tableName = fmt.Sprintf("c##flow_test_logminer.decimals_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, x NUMERIC(6, 4), y NUMERIC(38, 16))", tableName))

	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))

	var insertQuery = fmt.Sprintf("INSERT INTO %s VALUES (:1,:2,:3)", tableName)
	executeControlQuery(ctx, t, control, insertQuery, 0, 12.3456, 1234567890)
	executeControlQuery(ctx, t, control, insertQuery, 1, 98.7654, "123456789101112.123456789101112")

	// Run the capture for 5 seconds, which should be plenty to pull down a few rows.
	var captureCtx, cancelCapture = context.WithCancel(ctx)
	time.AfterFunc(5*time.Second, cancelCapture)
	cs.Capture(captureCtx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

func TestFloatNaNs(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "10511"
	var tableName = fmt.Sprintf("c##flow_test_logminer.float_nans_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, a_double BINARY_DOUBLE, a_float BINARY_FLOAT)", tableName))

	cs.Bindings = discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID))

	t.Run("Discovery", func(t *testing.T) { snapshotBindings(t, cs.Bindings) })

	t.Run("Capture", func(t *testing.T) {
		executeControlQuery(ctx, t, control, fmt.Sprintf(`INSERT INTO %s VALUES (0, 2.0, 0f/0f)`, tableName))
		executeControlQuery(ctx, t, control, fmt.Sprintf(`INSERT INTO %s VALUES (1, 0f/0f, 3.0)`, tableName))
		executeControlQuery(ctx, t, control, fmt.Sprintf(`INSERT INTO %s VALUES (2, 1f/0f, -1f/0f)`, tableName))

		// Run the capture for 5 seconds, which should be plenty to pull down a few rows.
		var captureCtx, cancelCapture = context.WithCancel(ctx)
		time.AfterFunc(5*time.Second, cancelCapture)
		cs.Capture(captureCtx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestSchemaFilter(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "22492"
	var tableName = fmt.Sprintf("c##flow_test_logminer.schema_filtering_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, data VARCHAR(200))", tableName))

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
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"foo", "C##FLOW_TEST_LOGMINER"}
		snapshotBindings(t, discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID)))
	})
}

func TestKeyDiscovery(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	var control = testControlClient(ctx, t)
	var uniqueID = "329932"
	var tableName = fmt.Sprintf("c##flow_test_logminer.key_discovery_%s", uniqueID)

	executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName))
	t.Cleanup(func() { executeControlQuery(ctx, t, control, fmt.Sprintf("DROP TABLE %s", tableName)) })
	executeControlQuery(ctx, t, control, fmt.Sprintf("CREATE TABLE %s(k_smallint SMALLINT, k_int INTEGER, k_bool NUMBER(1), k_str VARCHAR(8), data VARCHAR(200), PRIMARY KEY (k_smallint, k_int, k_bool, k_str))", tableName))

	cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"C##FLOW_TEST_LOGMINER"}
	snapshotBindings(t, discoverStreams(ctx, t, cs, regexp.MustCompile(uniqueID)))
}

func testConfig(ctx context.Context, t testing.TB) *Config {
	t.Helper()

	var configFile = "config.pdb.yaml"
	var sops = exec.CommandContext(ctx, "sops", "--decrypt", "--output-type", "json", configFile)
	var configRaw, err = sops.Output()
	require.NoError(t, err)
	var jq = exec.CommandContext(ctx, "jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
	jq.Stdin = bytes.NewReader(configRaw)
	cleanedConfig, err := jq.Output()
	require.NoError(t, err)
	var config Config
	err = json.Unmarshal(cleanedConfig, &config)
	require.NoError(t, err)

	if err := config.Validate(); err != nil {
		t.Fatalf("error validating capture config: %v", err)
	}
	config.SetDefaults()

	return &config
}

func testControlClient(ctx context.Context, t testing.TB) *sql.DB {
	t.Helper()

	var config = testConfig(ctx, t)

	// Open control connection
	db, err := connectOracle(ctx, config)
	log.WithFields(log.Fields{
		"user": config.User,
		"addr": config.Address,
	}).Info("opening control connection")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	return db
}

func executeControlQuery(ctx context.Context, t testing.TB, client *sql.DB, query string, args ...interface{}) {
	t.Helper()
	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing setup query")
	var _, err = client.ExecContext(ctx, query, args...)
	if err != nil && strings.Contains(err.Error(), "ORA-00942") {
		// table or view does not exist error, ignore
		return
	}
	require.NoError(t, err)
}

func testCaptureSpec(t testing.TB) *st.CaptureSpec {
	TestShutdownAfterQuery = true

	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var ctx = context.Background()
	var endpointSpec = testConfig(ctx, t)
	endpointSpec.Advanced.PollSchedule = "200ms"

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"polled":"<TIMESTAMP>"`] = regexp.MustCompile(`"polled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"LastPolled":"<TIMESTAMP>"`] = regexp.MustCompile(`"LastPolled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)
	sanitizers[`"TXID":"999999"`] = regexp.MustCompile(`"TXID":"[0-9]+"`)
	sanitizers[`"CursorNames":["TXID"],"CursorValues":[999999]`] = regexp.MustCompile(`"CursorNames":\["TXID"\],"CursorValues":\["[0-9]+\"]`)

	return &st.CaptureSpec{
		Driver:       oracleDriver,
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
