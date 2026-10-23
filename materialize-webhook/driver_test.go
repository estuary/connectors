package connector

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	bptest "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

// testServerAddr is where testdata/config.local.yaml sends webhooks. The
// connector runs as a local command of `flowctl`, so it reaches this
// process's server on localhost.
const testServerAddr = "localhost:18321"

func TestSpec(t *testing.T) {
	response, err := driver{}.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires flowctl")
	}
	startTestServer(t)

	makeResourceFn := func(s string, delta bool) resource {
		return resource{RelativePath: s}
	}

	t.Run("materialize", func(t *testing.T) {
		bptest.RunMaterializationTest(t, newMaterializerUnderTest, "testdata/materialize.flow.yaml", makeResourceFn, nil,
			bptest.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("apply", func(t *testing.T) {
		bptest.RunApplyTest(t, &driver{}, newMaterializerUnderTest, "testdata/apply.flow.yaml", makeResourceFn)
	})
}

// received holds the elements of every webhook request the test server
// received, by request URL.
var received = struct {
	sync.Mutex
	elements map[string][]json.RawMessage
}{elements: make(map[string][]json.RawMessage)}

func startTestServer(t *testing.T) {
	t.Helper()

	listener, err := net.Listen("tcp", testServerAddr)
	require.NoError(t, err)

	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		var elements []json.RawMessage
		if err == nil {
			err = json.Unmarshal(body, &elements)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		url := "http://" + r.Host + r.URL.Path
		received.Lock()
		received.elements[url] = append(received.elements[url], elements...)
		received.Unlock()
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
}

// materializationUnderTest reads back what the test server received for a
// binding's URL.
type materializationUnderTest struct {
	*materialization
}

func newMaterializerUnderTest(ctx context.Context, name string, cfg config, flags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	base, err := NewMaterializer(ctx, name, cfg, flags)
	if err != nil {
		return nil, err
	}
	return &materializationUnderTest{base.(*materialization)}, nil
}

// PopulateInfoSchema reports each binding's resource, with no fields, since
// the harness describes every resource it snapshots.
func (m *materializationUnderTest) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	for _, p := range resourcePaths {
		is.PushResource(p...)
	}
	return nil
}

func (m *materializationUnderTest) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	received.Lock()
	var payloads []string
	for _, element := range received.elements[path[0]] {
		payloads = append(payloads, string(element))
	}
	received.Unlock()

	if len(payloads) == 0 {
		return nil, nil, nil
	}
	slices.Sort(payloads)

	var rows [][]any
	for _, payload := range payloads {
		rows = append(rows, []any{payload})
	}
	return []string{"payload"}, rows, nil
}
