package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	firestore "cloud.google.com/go/firestore"
	"github.com/bradleyjkemp/cupaloy"
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
	"helpful-kingdom-273219",
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
	SnapshotSanitizers["project-id-123456"] = regexp.MustCompile(regexp.QuoteMeta(*testProjectID))
	os.Exit(m.Run())
}

func TestSpec(t *testing.T) {
	driver := driver{}
	response, err := driver.Spec(context.Background(), &pc.SpecRequest{})
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
		time.Sleep(100 * time.Millisecond)
		client.Upsert(ctx, t, "docs/ZZZ", fmt.Sprintf(`{"sentinel": %q}`, restartSentinel))
		capture.Verify(ctx, t, restartSentinel)
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
		time.Sleep(100 * time.Millisecond)
		client.Upsert(ctx, t, "docs/ZZZ", fmt.Sprintf(`{"sentinel": %q}`, shutdownSentinel))
		capture.Verify(ctx, t, shutdownSentinel)
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
		time.Sleep(100 * time.Millisecond)
		client.Upsert(ctx, t, "docs/ZZZ", fmt.Sprintf(`{"sentinel": %q}`, restartSentinel))
		capture.Verify(ctx, t, restartSentinel)
	})
	t.Run("two", func(t *testing.T) {
		client.Delete(ctx, t, "docs/3", "docs/4")
		time.Sleep(100 * time.Millisecond)
		client.Upsert(ctx, t, "docs/ZZZ", fmt.Sprintf(`{"sentinel": %q}`, shutdownSentinel))
		capture.Verify(ctx, t, shutdownSentinel)
	})
}

// The mapping from bindings to listen streams is many-to-one, with a
// single listen stream serving all bindings with some final path element.
// So for instance 'foo/*/bar' and 'asdf/*/fdsa/*/bar' are both fetched
// by a listen stream for 'bar'.
//
// But when a new binding is added this means that we need to resume
// from the earliest read time associated with any binding that maps
// to a particular stream, and that in turn means that we'll receive
// redundant (previously-captured) documents from the other bindings
// and need to filter those out.
//
// This test verifies that the filtering process works correctly.
func TestNewCollectionReadTime(t *testing.T) {
	var ctx = testContext(t, 10*time.Second)
	var capture = simpleCapture(t, "users/*/docs")
	var client = testFirestoreClient(ctx, t)
	t.Run("one", func(t *testing.T) {
		client.Upsert(ctx, t,
			"users/1/docs/1", `{"data": 1}`,
			"users/1/docs/2", `{"data": 2}`,
		)
		time.Sleep(100 * time.Millisecond)
		client.Upsert(ctx, t, "users/1/docs/ZZZ", fmt.Sprintf(`{"sentinel": %q}`, restartSentinel))
		capture.Verify(ctx, t, restartSentinel)
	})
	capture.Bindings = append(capture.Bindings, simpleBindings(t, "groups/*/docs")...)
	t.Run("two", func(t *testing.T) {
		client.Upsert(ctx, t,
			"users/1/docs/3", `{"data": 3}`,
			"users/1/docs/4", `{"data": 4}`,
			"groups/2/docs/5", `{"data": 5}`,
			"groups/2/docs/6", `{"data": 6}`,
		)
		time.Sleep(100 * time.Millisecond)
		client.Upsert(ctx, t, "groups/2/docs/ZZZ", fmt.Sprintf(`{"sentinel": %q}`, shutdownSentinel))
		capture.Verify(ctx, t, shutdownSentinel)
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
				client.Upsert(ctx, t,
					"users/999/docs/999", fmt.Sprintf(`{"sentinel": %q}`, restartSentinel),
				)
			}
			time.Sleep(1 * time.Second)
		}
		client.Upsert(ctx, t,
			"users/999/docs/999", fmt.Sprintf(`{"sentinel": %q}`, shutdownSentinel),
		)
	}(ctx)

	t.Run("one", func(t *testing.T) { capture.Verify(ctx, t, restartSentinel) })
	t.Run("two", func(t *testing.T) { capture.Verify(ctx, t, shutdownSentinel) })
}

func TestMultipleWatches(t *testing.T) {
	var ctx = testContext(t, 40*time.Second)
	var capture = simpleCapture(t, "users/*/docs", "users/*/notes", "users/*/tasks", "users/*/sentinels")
	var client = testFirestoreClient(ctx, t)

	go func(ctx context.Context) {
		for user := 0; user < 10; user++ {
			for item := 0; item < 5; item++ {
				client.Upsert(ctx, t, fmt.Sprintf(`users/%d/docs/%d`, user, item), `{"data": "placeholder"}`)
				client.Upsert(ctx, t, fmt.Sprintf(`users/%d/notes/%d`, user, item), `{"data": "placeholder"}`)
				client.Upsert(ctx, t, fmt.Sprintf(`users/%d/tasks/%d`, user, item), `{"data": "placeholder"}`)
			}
			time.Sleep(100 * time.Millisecond)
			if user == 4 {
				time.Sleep(900 * time.Millisecond)
				client.Upsert(ctx, t,
					"users/999/sentinels/999", fmt.Sprintf(`{"sentinel": %q}`, restartSentinel),
				)
			}
		}
		time.Sleep(900 * time.Millisecond)
		client.Upsert(ctx, t,
			"users/999/sentinels/999", fmt.Sprintf(`{"sentinel": %q}`, shutdownSentinel),
		)
	}(ctx)

	t.Run("one", func(t *testing.T) { capture.Verify(ctx, t, restartSentinel) })
	t.Run("two", func(t *testing.T) { capture.Verify(ctx, t, shutdownSentinel) })
}

func TestBindingDeletion(t *testing.T) {
	var ctx = testContext(t, 20*time.Second)
	var client = testFirestoreClient(ctx, t)
	for idx := 0; idx < 20; idx++ {
		client.Upsert(ctx, t, fmt.Sprintf("docs/%d", idx), `{"data": "placeholder"}`)
	}
	time.Sleep(1 * time.Second)
	client.Upsert(ctx, t, "docs/999", fmt.Sprintf(`{"sentinel": %q}`, shutdownSentinel))
	client.Upsert(ctx, t, "other/999", fmt.Sprintf(`{"sentinel": %q}`, shutdownSentinel))

	var capture = simpleCapture(t, "docs")
	t.Run("one", func(t *testing.T) { capture.Verify(ctx, t, shutdownSentinel) })
	capture.Bindings = simpleBindings(t, "other")
	t.Run("two", func(t *testing.T) { capture.Verify(ctx, t, shutdownSentinel) })
	capture.Bindings = simpleBindings(t, "docs")
	t.Run("three", func(t *testing.T) { capture.Verify(ctx, t, shutdownSentinel) })
}

func TestDiscovery(t *testing.T) {
	var ctx = testContext(t, 300*time.Second)
	var client = testFirestoreClient(ctx, t)
	client.Upsert(ctx, t, "users/1", `{"name": "Will"}`)
	client.Upsert(ctx, t, "users/1/docs/2", `{"foo": "bar", "asdf": 123}`)
	time.Sleep(1 * time.Second)
	simpleCapture(t).Discover(ctx, t, regexp.MustCompile(regexp.QuoteMeta("flow_source_tests")))
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

func simpleCapture(t testing.TB, names ...string) *testCaptureSpec {
	t.Helper()
	if os.Getenv("RUN_CAPTURES") != "yes" {
		t.Skipf("skipping %q capture: ${RUN_CAPTURES} != \"yes\"", t.Name())
	}

	// Load credentials from disk and construct an endpoint spec
	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credentialsJSON, err := ioutil.ReadFile(credentialsPath)
	require.NoError(t, err)

	var endpointSpec = &config{
		CredentialsJSON: string(credentialsJSON),
		DatabasePath:    fmt.Sprintf("projects/%s/databases/%s", *testProjectID, *testDatabaseName),
	}

	return &testCaptureSpec{
		Driver:       new(driver),
		EndpointSpec: endpointSpec,
		Bindings:     simpleBindings(t, names...),
	}
}

func simpleBindings(t testing.TB, names ...string) []*flow.CaptureSpec_Binding {
	var bindings []*flow.CaptureSpec_Binding
	for _, name := range names {
		var path = "flow_source_tests/*/" + name
		bindings = append(bindings, &flow.CaptureSpec_Binding{
			ResourceSpecJson: json.RawMessage(fmt.Sprintf(`{"path": %q}`, path)),
			ResourcePath:     []string{path},
		})
	}
	return bindings
}

type firestoreClient struct {
	inner  *firestore.Client
	prefix string
}

func testFirestoreClient(ctx context.Context, t testing.TB) *firestoreClient {
	t.Helper()
	if os.Getenv("RUN_CAPTURES") != "yes" {
		t.Skipf("skipping %q capture: ${RUN_CAPTURES} != \"yes\"", t.Name())
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
