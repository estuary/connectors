package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	firestore "cloud.google.com/go/firestore"
	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

var testCredentialsPath = flag.String(
	"creds_path",
	"~/.config/gcloud/application_default_credentials.json",
	"Path to the credentials JSON to use for test authentication",
)
var testProjectID = flag.String(
	"project_id",
	"estuary-sandbox",
	"The project ID to interact with during automated tests",
)
var testDatabaseName = flag.String(
	"database",
	"(default)",
	"The database to interact with during automated tests",
)

// Most capture tests run two capture phases in order to verify that
// resuming from a prior checkpoint works correctly. These are the
// sentinel values used to shut down those captures.
const (
	restartSentinel  = "5f91dae7-dc4d-48d9-bfec-2d3bfeec4164"
	shutdownSentinel = "a6d1c2e4-be25-4415-8f03-ab20abbcc5a6"
)

var DefaultSanitizers = make(map[string]*regexp.Regexp)

func TestMain(m *testing.M) {
	flag.Parse()
	if level, err := log.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		log.SetLevel(level)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	log.SetFormatter(&log.JSONFormatter{
		DataKey:  "data",
		FieldMap: log.FieldMap{log.FieldKeyTime: "@ts"},
	})
	DefaultSanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|-[0-9]+:[0-9]+)"`)
	DefaultSanitizers["project-id-123456"] = regexp.MustCompile(regexp.QuoteMeta(*testProjectID))
	os.Exit(m.Run())
}

func TestSpec(t *testing.T) {
	driver := driver{}
	response, err := driver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestSimpleCapture(t *testing.T) {
	var ctx = testContext(t, 10*time.Second)
	var capture = simpleCapture(t, "docs")
	var client = testFirestoreClient(ctx, t)
	t.Run("one", func(t *testing.T) {
		client.Upsert(ctx, t,
			"docs/1", `{"data": 1}`,
			"docs/2", `{"data": 2}`,
			"docs/3", `{"data": 3}`,
			"docs/4", `{"data": 4}`,
			"docs/5", `{"data": 5}`,
			"docs/6", `{"data": 6}`,
		)
		verifyCapture(ctx, t, capture)
	})
	t.Run("two", func(t *testing.T) {
		client.Upsert(ctx, t,
			"docs/7", `{"data": 7}`,
			"docs/8", `{"data": 8}`,
			"docs/9", `{"data": 9}`,
			"docs/10", `{"data": 10}`,
			"docs/11", `{"data": 11}`,
			"docs/12", `{"data": 12}`,
		)
		verifyCapture(ctx, t, capture)
	})
}

func TestDeletions(t *testing.T) {
	var ctx = testContext(t, 10*time.Second)
	var capture = simpleCapture(t, "docs")
	var client = testFirestoreClient(ctx, t)
	t.Run("one", func(t *testing.T) {
		client.Upsert(ctx, t,
			"docs/1", `{"data": 1}`,
			"docs/2", `{"data": 2}`,
			"docs/3", `{"data": 3}`,
			"docs/4", `{"data": 4}`,
		)
		time.Sleep(1 * time.Second)
		client.Delete(ctx, t, "docs/1", "docs/2")
		verifyCapture(ctx, t, capture)
	})
	t.Run("two", func(t *testing.T) {
		client.Delete(ctx, t, "docs/3", "docs/4")
		verifyCapture(ctx, t, capture)
	})
}

// This test exercises some behaviors around adding a new capture binding,
// especially in the tricky edge case where the new binding maps to the
// same collection group ID as a preexisting one.
func TestAddedBindingSameGroup(t *testing.T) {
	var ctx = testContext(t, 30*time.Second)
	var capture = simpleCapture(t, "users/*/docs")
	var client = testFirestoreClient(ctx, t)
	t.Run("one", func(t *testing.T) {
		client.Upsert(ctx, t,
			"users/1/docs/1", `{"data": 1}`,
			"users/1/docs/2", `{"data": 2}`,
			"groups/3/docs/7", `{"data": 7}`,
			"groups/3/docs/8", `{"data": 8}`,
		)
		verifyCapture(ctx, t, capture)
	})
	capture.Bindings = append(capture.Bindings, simpleBindings(t, "groups/*/docs")...)
	t.Run("two", func(t *testing.T) {
		client.Upsert(ctx, t,
			"users/1/docs/3", `{"data": 3}`,
			"users/1/docs/4", `{"data": 4}`,
			"groups/2/docs/5", `{"data": 5}`,
			"groups/2/docs/6", `{"data": 6}`,
		)
		verifyCapture(ctx, t, capture)
	})
}

func TestManySmallWrites(t *testing.T) {
	var ctx = testContext(t, 40*time.Second)
	var capture = simpleCapture(t, "users/*/docs")
	var client = testFirestoreClient(ctx, t)

	go func(ctx context.Context) {
		for user := 0; user < 10; user++ {
			for doc := 0; doc < 5; doc++ {
				var docName = fmt.Sprintf("users/%d/docs/%d", user, doc)
				var docData = fmt.Sprintf(`{"user": %d, "doc": %d}`, user, doc)
				client.Upsert(ctx, t, docName, docData)
				time.Sleep(100 * time.Millisecond)
			}
			if user == 4 {
				time.Sleep(1500 * time.Millisecond)
			}
		}
	}(ctx)

	t.Run("one", func(t *testing.T) { verifyCapture(ctx, t, capture) })
	t.Run("two", func(t *testing.T) { verifyCapture(ctx, t, capture) })
}

func TestMultipleWatches(t *testing.T) {
	var ctx = testContext(t, 40*time.Second)
	var capture = simpleCapture(t, "users/*/docs", "users/*/notes", "users/*/tasks")
	var client = testFirestoreClient(ctx, t)

	go func(ctx context.Context) {
		for user := 0; user < 10; user++ {
			for item := 0; item < 5; item++ {
				client.Upsert(ctx, t, fmt.Sprintf(`users/%d/docs/%d`, user, item), `{"data": "placeholder"}`)
				client.Upsert(ctx, t, fmt.Sprintf(`users/%d/notes/%d`, user, item), `{"data": "placeholder"}`)
				client.Upsert(ctx, t, fmt.Sprintf(`users/%d/tasks/%d`, user, item), `{"data": "placeholder"}`)
			}
			if user == 4 {
				time.Sleep(1500 * time.Millisecond)
			}
		}
	}(ctx)

	t.Run("one", func(t *testing.T) { verifyCapture(ctx, t, capture) })
	t.Run("two", func(t *testing.T) { verifyCapture(ctx, t, capture) })
}

func TestBindingDeletion(t *testing.T) {
	var ctx = testContext(t, 20*time.Second)
	var client = testFirestoreClient(ctx, t)
	for idx := 0; idx < 20; idx++ {
		client.Upsert(ctx, t, fmt.Sprintf("docs/%d", idx), `{"data": "placeholder"}`)
	}
	time.Sleep(1 * time.Second)

	var capture = simpleCapture(t, "docs")
	t.Run("one", func(t *testing.T) { verifyCapture(ctx, t, capture) })
	capture.Bindings = simpleBindings(t, "other")
	t.Run("two", func(t *testing.T) { verifyCapture(ctx, t, capture) })
	capture.Bindings = simpleBindings(t, "docs")
	t.Run("three", func(t *testing.T) { verifyCapture(ctx, t, capture) })
}

func TestDiscovery(t *testing.T) {
	var ctx = testContext(t, 300*time.Second)
	var client = testFirestoreClient(ctx, t)
	client.Upsert(ctx, t, "users/1", `{"name": "Will"}`)
	client.Upsert(ctx, t, "users/2", `{"name": "Alice"}`)
	client.Upsert(ctx, t, "users/1/docs/1", `{"foo": "bar", "asdf": 123}`)
	client.Upsert(ctx, t, "users/1/docs/2", `{"foo": "bar", "asdf": 456}`)
	client.Upsert(ctx, t, "users/2/docs/3", `{"foo": "baz", "asdf": 789}`)
	client.Upsert(ctx, t, "users/2/docs/4", `{"foo": "baz", "asdf": 1000}`)
	time.Sleep(1 * time.Second)
	simpleCapture(t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta("flow_source_tests")))
}

func testContext(t testing.TB, duration time.Duration) context.Context {
	t.Helper()
	if testing.Short() && duration > 10*time.Second {
		t.Skip("skipping long test")
	}
	var ctx, cancel = context.WithTimeout(context.Background(), duration)
	t.Cleanup(cancel)
	return ctx
}

func simpleCapture(t testing.TB, names ...string) *st.CaptureSpec {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	// Load credentials from disk and construct an endpoint spec
	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credentialsJSON, err := ioutil.ReadFile(credentialsPath)
	require.NoError(t, err)

	var endpointSpec = &config{
		CredentialsJSON: string(credentialsJSON),
		DatabasePath:    fmt.Sprintf("projects/%s/databases/%s", *testProjectID, *testDatabaseName),
	}

	return &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: endpointSpec,
		Bindings:     simpleBindings(t, names...),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   DefaultSanitizers,
	}
}

func simpleBindings(t testing.TB, names ...string) []*flow.CaptureSpec_Binding {
	var bindings []*flow.CaptureSpec_Binding
	for _, name := range names {
		var path = "flow_source_tests/*/" + name
		bindings = append(bindings, &flow.CaptureSpec_Binding{
			Collection:         flow.CollectionSpec{Name: flow.Collection(path)},
			ResourceConfigJson: json.RawMessage(fmt.Sprintf(`{"path": %q, "backfillMode": "async"}`, path)),
			ResourcePath:       []string{path},
			StateKey:           url.QueryEscape(path),
		})
	}
	return bindings
}

// verifyCapture performs a capture using the provided st.CaptureSpec and shuts it down after
// a suitable time has elapsed without any documents or state checkpoints being emitted. It
// then performs snapshot verification on the results.
func verifyCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) {
	t.Helper()
	var captureCtx, cancelCapture = context.WithCancel(ctx)
	const shutdownDelay = 1000 * time.Millisecond
	var shutdownWatchdog *time.Timer
	cs.Capture(captureCtx, t, func(data json.RawMessage) {
		if shutdownWatchdog == nil {
			shutdownWatchdog = time.AfterFunc(shutdownDelay, func() {
				log.WithField("delay", shutdownDelay.String()).Debug("capture shutdown watchdog expired")
				cancelCapture()
			})
		}
		shutdownWatchdog.Reset(shutdownDelay)
	})
	cupaloy.SnapshotT(t, cs.Summary())
	cs.Reset()
}

type firestoreClient struct {
	inner  *firestore.Client
	prefix string
}

func testFirestoreClient(ctx context.Context, t testing.TB) *firestoreClient {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credentialsJSON, err := ioutil.ReadFile(credentialsPath)
	require.NoError(t, err)

	client, err := firestore.NewClient(ctx, *testProjectID, option.WithCredentialsJSON(credentialsJSON))
	require.NoError(t, err)

	var prefix = fmt.Sprintf("flow_source_tests/%s", strings.ReplaceAll(t.Name(), "/", "_"))
	var tfc = &firestoreClient{
		inner:  client,
		prefix: prefix + "/",
	}
	t.Cleanup(func() {
		tfc.deleteCollectionRecursive(context.Background(), t, tfc.inner.Collection("flow_source_tests"))
		tfc.inner.Close()
	})
	tfc.deleteCollectionRecursive(ctx, t, tfc.inner.Collection("flow_source_tests"))

	_, err = client.Batch().Set(client.Doc(prefix), map[string]interface{}{"testName": t.Name()}).Commit(ctx)
	require.NoError(t, err)

	return tfc
}

func (c *firestoreClient) DeleteCollection(ctx context.Context, t testing.TB, collection string) {
	t.Helper()
	c.deleteCollectionRecursive(ctx, t, c.inner.Collection(c.prefix+collection))
}

func (c *firestoreClient) deleteCollectionRecursive(ctx context.Context, t testing.TB, ref *firestore.CollectionRef) {
	t.Helper()
	docs, err := ref.DocumentRefs(ctx).GetAll()
	require.NoError(t, err)
	for _, doc := range docs {
		log.WithField("path", doc.Path).Trace("deleting doc")
		subcolls, err := doc.Collections(ctx).GetAll()
		require.NoError(t, err)
		for _, subcoll := range subcolls {
			c.deleteCollectionRecursive(ctx, t, subcoll)
		}
		_, err = doc.Delete(ctx)
		require.NoError(t, err)
	}
}

func (c *firestoreClient) Upsert(ctx context.Context, t testing.TB, docKVs ...string) {
	t.Helper()

	// Interpret the variadic part of the arguments as an alternating sequence
	// of document names and document values.
	var names []string
	var values []string
	for idx, str := range docKVs {
		if idx%2 == 0 {
			names = append(names, str)
		} else {
			values = append(values, str)
		}
	}
	log.WithField("count", len(names)).Trace("upserting test documents")

	var wb = c.inner.Batch()
	for idx := range values {
		var fields = make(map[string]interface{})
		var err = json.Unmarshal(json.RawMessage(values[idx]), &fields)
		require.NoError(t, err)
		wb = wb.Set(c.inner.Doc(c.prefix+names[idx]), fields, firestore.MergeAll)
	}
	var _, err = wb.Commit(ctx)
	require.NoError(t, err)
}

func (c *firestoreClient) Delete(ctx context.Context, t testing.TB, names ...string) {
	t.Helper()
	log.WithField("count", len(names)).Debug("deleting test documents")

	var wb = c.inner.Batch()
	for _, docName := range names {
		wb = wb.Delete(c.inner.Doc(c.prefix + docName))
	}
	var _, err = wb.Commit(ctx)
	require.NoError(t, err)
}
