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

func TestMain(m *testing.M) {
	flag.Parse()
	log.SetLevel(log.DebugLevel)
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

func sentinel(t *testing.T) string {
	return fmt.Sprintf("%q", fmt.Sprintf("FLOW_TEST_SENTINEL(%s)", t.Name()))
}

func TestSimpleCapture(t *testing.T) {
	var ctx = testContext(t, 60*time.Second)
	var capture = simpleCapture(t, "docs")
	var client = testFirestoreClient(ctx, t)
	t.Run("one", func(t *testing.T) {
		client.Upsert(ctx, t, map[string]string{
			"docs/1":   `{"data": 1}`,
			"docs/2":   `{"data": 2}`,
			"docs/3":   `{"data": 3}`,
			"docs/4":   `{"data": 4}`,
			"docs/5":   `{"data": 5}`,
			"docs/6":   `{"data": 6}`,
			"docs/ZZZ": fmt.Sprintf(`{"data": %s}`, sentinel(t)),
		})
		capture.Verify(ctx, t, sentinel(t))
	})
	t.Run("two", func(t *testing.T) {
		client.Upsert(ctx, t, map[string]string{
			"docs/7":   `{"data": 7}`,
			"docs/8":   `{"data": 8}`,
			"docs/9":   `{"data": 9}`,
			"docs/10":  `{"data": 10}`,
			"docs/11":  `{"data": 11}`,
			"docs/12":  `{"data": 12}`,
			"docs/ZZZ": fmt.Sprintf(`{"data": %s}`, sentinel(t)),
		})
		capture.Verify(ctx, t, sentinel(t))
	})
}

func TestDeletions(t *testing.T) {
	var ctx = testContext(t, 60*time.Second)
	var capture = simpleCapture(t, "docs")
	var client = testFirestoreClient(ctx, t)
	t.Run("one", func(t *testing.T) {
		client.Upsert(ctx, t, map[string]string{
			"docs/1": `{"data": 1}`,
			"docs/2": `{"data": 2}`,
			"docs/3": `{"data": 3}`,
			"docs/4": `{"data": 4}`,
		})
		client.Delete(ctx, t, "docs/1", "docs/2")
		client.Upsert(ctx, t, map[string]string{
			"docs/ZZZ": fmt.Sprintf(`{"data": %s}`, sentinel(t)),
		})
		capture.Verify(ctx, t, sentinel(t))
	})
	t.Run("two", func(t *testing.T) {
		client.Delete(ctx, t, "docs/3", "docs/4")
		client.Upsert(ctx, t, map[string]string{
			"docs/ZZZ": fmt.Sprintf(`{"data": %s}`, sentinel(t)),
		})
		capture.Verify(ctx, t, sentinel(t))
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
	var ctx = testContext(t, 60*time.Second)
	var capture = simpleCapture(t, "users/*/docs")
	var client = testFirestoreClient(ctx, t)
	t.Run("one", func(t *testing.T) {
		client.Upsert(ctx, t, map[string]string{
			"users/1/docs/1":   `{"data": 1}`,
			"users/1/docs/2":   `{"data": 2}`,
			"users/1/docs/ZZZ": fmt.Sprintf(`{"data": %s}`, sentinel(t)),
		})
		capture.Verify(ctx, t, sentinel(t))
	})
	capture.Bindings = append(capture.Bindings, simpleBindings(t, "groups/*/docs")...)
	t.Run("two", func(t *testing.T) {
		client.Upsert(ctx, t, map[string]string{
			"users/1/docs/3":    `{"data": 3}`,
			"users/1/docs/4":    `{"data": 4}`,
			"groups/2/docs/5":   `{"data": 5}`,
			"groups/2/docs/6":   `{"data": 6}`,
			"groups/2/docs/ZZZ": fmt.Sprintf(`{"data": %s}`, sentinel(t)),
		})
		capture.Verify(ctx, t, sentinel(t))
	})
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

	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credentialsJSON, err := ioutil.ReadFile(credentialsPath)
	require.NoError(t, err)

	client, err := firestore.NewClient(ctx, *testProjectID, option.WithCredentialsJSON(credentialsJSON))
	require.NoError(t, err)

	var tfc = &firestoreClient{
		inner:  client,
		prefix: fmt.Sprintf("flow_source_tests/%s/", strings.ReplaceAll(t.Name(), "/", "_")),
	}
	t.Cleanup(func() {
		tfc.deleteCollectionRecursive(context.Background(), t, tfc.inner.Collection("flow_source_tests"))
		tfc.inner.Close()
	})
	tfc.deleteCollectionRecursive(ctx, t, tfc.inner.Collection("flow_source_tests"))

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

func (c *firestoreClient) Upsert(ctx context.Context, t testing.TB, docs map[string]string) {
	t.Helper()
	log.WithField("count", len(docs)).Debug("upserting test documents")

	var wb = c.inner.Batch()
	for docName, docData := range docs {
		var fields = make(map[string]interface{})
		var err = json.Unmarshal(json.RawMessage(docData), &fields)
		require.NoError(t, err)
		wb = wb.Set(c.inner.Doc(c.prefix+docName), fields, firestore.MergeAll)
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
