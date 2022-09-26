package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
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
	"$HOME/.config/gcloud/application_default_credentials.json",
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
	return fmt.Sprintf("FLOW_TEST_SENTINEL('%s')", t.Name())
}

func TestSimpleCapture(t *testing.T) {
	var ctx = testContext(t, 60*time.Second)
	var capture = simpleCapture(t, "test/*/docs")
	var prefix = fmt.Sprintf("test/TestRun%04X", rand.Intn(65536))
	var checkpoint json.RawMessage
	t.Cleanup(func() { deleteCollection(context.Background(), t, "test") })
	deleteCollection(ctx, t, "test")

	t.Run("one", func(t *testing.T) {
		upsertDocuments(ctx, t, map[string]string{
			prefix + "/docs/1":   `{"data": 1}`,
			prefix + "/docs/2":   `{"data": 2}`,
			prefix + "/docs/3":   `{"data": 3}`,
			prefix + "/docs/4":   `{"data": 4}`,
			prefix + "/docs/5":   `{"data": 5}`,
			prefix + "/docs/6":   `{"data": 6}`,
			prefix + "/docs/ZZZ": fmt.Sprintf(`{"data": %q}`, sentinel(t)),
		})
		checkpoint = capture.Verify(ctx, t, checkpoint, sentinel(t))
	})
	t.Run("two", func(t *testing.T) {
		upsertDocuments(ctx, t, map[string]string{
			prefix + "/docs/7":   `{"data": 7}`,
			prefix + "/docs/8":   `{"data": 8}`,
			prefix + "/docs/9":   `{"data": 9}`,
			prefix + "/docs/10":  `{"data": 10}`,
			prefix + "/docs/11":  `{"data": 11}`,
			prefix + "/docs/12":  `{"data": 12}`,
			prefix + "/docs/ZZZ": fmt.Sprintf(`{"data": %q}`, sentinel(t)),
		})
		checkpoint = capture.Verify(ctx, t, checkpoint, sentinel(t))
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
	var capture = simpleCapture(t, "test/*/As/*/docs")
	var prefix = fmt.Sprintf("test/TestRun%04X", rand.Intn(65536))
	var checkpoint json.RawMessage
	t.Cleanup(func() { deleteCollection(context.Background(), t, "test") })
	deleteCollection(ctx, t, "test")

	t.Run("one", func(t *testing.T) {
		upsertDocuments(ctx, t, map[string]string{
			prefix + "/As/1/docs/1":   `{"data": 1}`,
			prefix + "/As/1/docs/2":   `{"data": 2}`,
			prefix + "/As/1/docs/ZZZ": fmt.Sprintf(`{"data": %q}`, sentinel(t)),
		})
		checkpoint = capture.Verify(ctx, t, checkpoint, sentinel(t))
	})

	capture.Bindings = append(capture.Bindings, &flow.CaptureSpec_Binding{
		ResourceSpecJson: json.RawMessage(fmt.Sprintf(`{"path": %q}`, "test/*/Bs/*/docs")),
		ResourcePath:     []string{"test/*/Bs/*/docs"},
	})

	t.Run("two", func(t *testing.T) {
		upsertDocuments(ctx, t, map[string]string{
			prefix + "/As/1/docs/3":   `{"data": 3}`,
			prefix + "/As/1/docs/4":   `{"data": 4}`,
			prefix + "/Bs/2/docs/5":   `{"data": 5}`,
			prefix + "/Bs/2/docs/6":   `{"data": 6}`,
			prefix + "/Bs/2/docs/ZZZ": fmt.Sprintf(`{"data": %q}`, sentinel(t)),
		})
		checkpoint = capture.Verify(ctx, t, checkpoint, sentinel(t))
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
	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "$HOME", os.Getenv("HOME"))
	credentialsJSON, err := ioutil.ReadFile(credentialsPath)
	require.NoError(t, err)

	var endpointSpec = &config{
		CredentialsJSON: string(credentialsJSON),
		DatabasePath:    fmt.Sprintf("projects/%s/databases/%s", *testProjectID, *testDatabaseName),
	}

	// Make a usable list of bindings out of the provided names
	var bindings []*flow.CaptureSpec_Binding
	for _, name := range names {
		bindings = append(bindings, &flow.CaptureSpec_Binding{
			ResourceSpecJson: json.RawMessage(fmt.Sprintf(`{"path": %q}`, name)),
			ResourcePath:     []string{name},
		})
	}

	return &testCaptureSpec{
		Driver:       new(driver),
		EndpointSpec: endpointSpec,
		Bindings:     bindings,
	}
}

func deleteCollection(ctx context.Context, t testing.TB, collection string) {
	t.Helper()
	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "$HOME", os.Getenv("HOME"))
	credentialsJSON, err := ioutil.ReadFile(credentialsPath)
	require.NoError(t, err)

	client, err := firestore.NewClient(ctx, *testProjectID, option.WithCredentialsJSON(credentialsJSON))
	require.NoError(t, err)
	defer client.Close()

	deleteCollectionRecursive(ctx, t, client.Collection(collection))
}

func deleteCollectionRecursive(ctx context.Context, t testing.TB, ref *firestore.CollectionRef) {
	t.Helper()
	docs, err := ref.DocumentRefs(ctx).GetAll()
	require.NoError(t, err)
	for _, doc := range docs {
		log.WithField("path", doc.Path).Trace("deleting doc")
		subcolls, err := doc.Collections(ctx).GetAll()
		require.NoError(t, err)
		for _, subcoll := range subcolls {
			deleteCollectionRecursive(ctx, t, subcoll)
		}
		_, err = doc.Delete(ctx)
		require.NoError(t, err)
	}
}

func upsertDocuments(ctx context.Context, t testing.TB, docs map[string]string) {
	t.Helper()
	log.WithField("count", len(docs)).Debug("upserting test documents")

	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "$HOME", os.Getenv("HOME"))
	credentialsJSON, err := ioutil.ReadFile(credentialsPath)
	require.NoError(t, err)

	client, err := firestore.NewClient(ctx, *testProjectID, option.WithCredentialsJSON(credentialsJSON))
	require.NoError(t, err)
	defer client.Close()

	var wb = client.Batch()
	for docName, docData := range docs {
		var fields = make(map[string]interface{})
		var err = json.Unmarshal(json.RawMessage(docData), &fields)
		require.NoError(t, err)
		wb = wb.Set(client.Doc(docName), fields, firestore.MergeAll)
	}
	_, err = wb.Commit(ctx)
	require.NoError(t, err)
}
