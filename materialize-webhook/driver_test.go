package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	bptest "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

// testServerAddr is where testdata/config.local.yaml sends webhooks. The
// connector runs as a local command of `flowctl`, so it reaches this
// process's server on localhost.
const testServerAddr = "localhost:18321"

func TestSpec(t *testing.T) {
	var response, err = driver{}.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestValidateConstraints(t *testing.T) {
	var collection = pf.CollectionSpec{
		Name: "acmeCo/things",
		Key:  []string{"/id"},
		Projections: []pf.Projection{
			{Field: "flow_document", Ptr: ""},
			{Field: "flow_published_at", Ptr: "/_meta/uuid"},
			{Field: "id", Ptr: "/id", IsPrimaryKey: true},
			{Field: "name", Ptr: "/name"},
			{Field: "nested/value", Ptr: "/nested/value"},
		},
	}

	var validate = func(flags, createdAt string) (map[string]pm.Response_Validated_Constraint_Type, error) {
		var validated, err = driver{}.Validate(context.Background(), &pm.Request_Validate{
			ConfigJson: json.RawMessage(fmt.Sprintf(`{"address":"https://example.com/hooks/","advanced":{"feature_flags":%q}}`, flags)),
			Bindings: []*pm.Request_Validate_Binding{{
				ResourceConfigJson: json.RawMessage(`{"relativePath":"things"}`),
				Collection:         collection,
			}},
			LastMaterialization: &pf.MaterializationSpec{CreatedAt: createdAt},
		})
		if err != nil {
			return nil, err
		}
		var out = make(map[string]pm.Response_Validated_Constraint_Type)
		for _, c := range validated.Bindings[0].ProjectionConstraints {
			out[c.Field] = c.Constraint.Type
		}
		return out, nil
	}

	var documentConstraints = map[string]pm.Response_Validated_Constraint_Type{
		"flow_document":     pm.Response_Validated_Constraint_LOCATION_REQUIRED,
		"flow_published_at": pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
		"id":                pm.Response_Validated_Constraint_LOCATION_REQUIRED,
		"name":              pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
		"nested/value":      pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
	}
	var fieldsConstraints = map[string]pm.Response_Validated_Constraint_Type{
		"flow_document":     pm.Response_Validated_Constraint_FIELD_OPTIONAL,
		"flow_published_at": pm.Response_Validated_Constraint_FIELD_OPTIONAL,
		"id":                pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
		"name":              pm.Response_Validated_Constraint_FIELD_OPTIONAL,
		"nested/value":      pm.Response_Validated_Constraint_FIELD_OPTIONAL,
	}

	for _, tt := range []struct {
		name      string
		flags     string
		createdAt string
		want      map[string]pm.Response_Validated_Constraint_Type
	}{
		{"task predating the cutoff", "", "2020-01-01", documentConstraints},
		{"task postdating the cutoff", "", "2099-01-01", fieldsConstraints},
		{"task predating the cutoff opting in", "field_selection", "2020-01-01", fieldsConstraints},
		{"task postdating the cutoff opting out", "no_field_selection", "2099-01-01", documentConstraints},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var got, err = validate(tt.flags, tt.createdAt)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStorePayloads(t *testing.T) {
	var mu sync.Mutex
	var requests = make(map[string][]string)
	var server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		requests[r.URL.Path] = append(requests[r.URL.Path], string(body))
		mu.Unlock()
	}))
	defer server.Close()

	var open = func(fieldSelection bool, bindings ...pf.MaterializationSpec_Binding) m.Transactor {
		var ctx = context.Background()
		var cfg = config{Address: pf.Endpoint(server.URL + "/")}
		var materializer, err = NewMaterializer(ctx, "acmeCo/webhook", cfg, map[string]bool{"field_selection": fieldSelection})
		require.NoError(t, err)

		var mapped []boilerplate.MappedBinding[config, resource, mappedType]
		for _, b := range bindings {
			var res resource
			require.NoError(t, json.Unmarshal(b.ResourceConfigJson, &res))
			mapped = append(mapped, boilerplate.MappedBinding[config, resource, mappedType]{MaterializationSpec_Binding: b, Config: res})
		}
		transactor, err := materializer.NewTransactor(ctx, pm.Request_Open{}, boilerplate.InfoSchema{}, mapped, nil)
		require.NoError(t, err)
		return transactor
	}
	var documents = open(false, pf.MaterializationSpec_Binding{
		ResourceConfigJson: json.RawMessage(`{"relativePath":"document"}`),
		ResourcePath:       []string{"document"},
		FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Document: "flow_document"},
	})
	var fields = open(true,
		pf.MaterializationSpec_Binding{
			ResourceConfigJson: json.RawMessage(`{"relativePath":"fields"}`),
			ResourcePath:       []string{"fields"},
			FieldSelection: pf.FieldSelection{
				Keys:   []string{"id"},
				Values: []string{"flow_published_at", "name", "nested"},
			},
		},
		pf.MaterializationSpec_Binding{
			ResourceConfigJson: json.RawMessage(`{"relativePath":"fields-and-document"}`),
			ResourcePath:       []string{"fields-and-document"},
			FieldSelection: pf.FieldSelection{
				Keys:     []string{"id"},
				Values:   []string{"name"},
				Document: "flow_document",
			},
		},
	)

	var doc = `{"_meta":{"uuid":"9d5b7e2a-0000-1000-8000-000000000000"},"id":1,"name":"say \"hi\"","nested":{"a":[1,2]}}`
	var store = func(binding int, key, values tuple.Tuple) *pm.Request {
		return &pm.Request{Store: &pm.Request_Store{
			Binding:      uint32(binding),
			KeyPacked:    key.Pack(),
			ValuesPacked: values.Pack(),
			DocJson:      json.RawMessage(doc),
		}}
	}
	var storeAll = func(transactor m.Transactor, requests ...*pm.Request) {
		var stream = &fakeStream{requests: append(requests, &pm.Request{StartCommit: &pm.Request_StartCommit{}})}
		var it = m.NewStoreIterator(context.Background(), stream, &pm.Request{Flush: &pm.Request_Flush{}})
		var _, err = transactor.Store(it)
		require.NoError(t, err)
	}
	storeAll(documents, store(0, tuple.Tuple{int64(1)}, tuple.Tuple{}))
	storeAll(fields,
		store(0, tuple.Tuple{int64(1)}, tuple.Tuple{"2026-10-23T00:00:00Z", `say "hi"`, []byte(`{"a":[1,2]}`)}),
		store(0, tuple.Tuple{int64(2)}, tuple.Tuple{nil, "plain", []byte(`null`)}),
		store(1, tuple.Tuple{int64(1)}, tuple.Tuple{1.5}),
	)

	require.Equal(t, map[string][]string{
		"/document": {`[` + doc + `]`},
		"/fields": {
			`[{"id":1,"flow_published_at":"2026-10-23T00:00:00Z","name":"say \"hi\"","nested":{"a":[1,2]}},` +
				`{"id":2,"flow_published_at":null,"name":"plain","nested":null}]`,
		},
		"/fields-and-document": {`[{"id":1,"name":1.5,"flow_document":` + doc + `}]`},
	}, requests)
}

type fakeStream struct {
	requests []*pm.Request
}

func (s *fakeStream) Send(*pm.Response) error { return nil }

func (s *fakeStream) RecvMsg(r *pm.Request) error {
	if len(s.requests) == 0 {
		return io.EOF
	}
	*r, s.requests = *s.requests[0], s.requests[1:]
	return nil
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires flowctl")
	}
	startTestServer(t)

	var makeResourceFn = func(s string, delta bool) resource {
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

	var listener, err = net.Listen("tcp", testServerAddr)
	require.NoError(t, err)

	var server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body, err = io.ReadAll(r.Body)
		var elements []json.RawMessage
		if err == nil {
			err = json.Unmarshal(body, &elements)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var url = "http://" + r.Host + r.URL.Path
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
	var base, err = NewMaterializer(ctx, name, cfg, flags)
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
