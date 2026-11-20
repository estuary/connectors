package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	pc "go.gazette.dev/core/consumer/protocol"
)

type cosmosRequest struct {
	method, path, partitionKey, sessionToken string
	body                                     []byte
}

type cosmosContainer struct {
	properties azcosmos.ContainerProperties
	items      map[string]json.RawMessage
	writes     int
}

var fixturePartitionPath = regexp.MustCompile(`/([^"/][^/]*|"([^"]*)")`)

// cosmosFixture runs the real SDK pipeline against an in-memory transport.
// It accepts only named resource reads and point item operations, and checks
// that the request partition value agrees with the stored document. Like the
// routing gateway, it rejects non-ASCII partition headers and point operations
// on ids that hold `%`, control characters or trailing whitespace.
type cosmosFixture struct {
	mu          sync.Mutex
	database    bool
	consistency azcosmos.ConsistencyLevel
	accountID   string
	containers  map[string]*cosmosContainer
	created     map[string]int
	requests    []cosmosRequest
	failures    map[string]int
}

func newCosmosFixture() (*cosmosFixture, driver) {
	f := &cosmosFixture{consistency: azcosmos.ConsistencyLevelSession, accountID: "test-account", containers: make(map[string]*cosmosContainer), created: make(map[string]int), failures: make(map[string]int)}
	return f, driver{clientOptions: &azcosmos.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: f,
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}}}
}

func (f *cosmosFixture) Do(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
	}
	r := cosmosRequest{method: req.Method, path: strings.Trim(req.URL.Path, "/"), partitionKey: req.Header.Get("x-ms-documentdb-partitionkey"), sessionToken: req.Header.Get("x-ms-session-token"), body: body}
	f.requests = append(f.requests, r)
	respond := func(status int, body any) (*http.Response, error) {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: status,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(data)),
			Request:    req,
		}, nil
	}
	if status := f.failures[r.method+" "+r.path]; status != 0 {
		return respond(status, map[string]string{"code": http.StatusText(status), "message": "test failure"})
	}
	for _, c := range []byte(r.partitionKey) {
		if c >= 0x80 {
			return nil, fmt.Errorf("partition header is not ASCII: %q", r.partitionKey)
		}
	}
	if req.Header.Get("x-ms-offer-throughput") != "" || req.Header.Get("x-ms-cosmos-offer-autopilot-settings") != "" {
		return nil, fmt.Errorf("unexpected throughput setting")
	}
	if r.path == "" && r.method == http.MethodGet {
		return respond(http.StatusOK, map[string]any{
			"id": f.accountID, "_rid": f.accountID,
			"readableLocations":     []map[string]string{{"name": "Test", "databaseAccountEndpoint": "http://gateway.cosmos.test:8081/"}},
			"writableLocations":     []map[string]string{{"name": "Test", "databaseAccountEndpoint": "http://gateway.cosmos.test:8081/"}},
			"userConsistencyPolicy": map[string]any{"defaultConsistencyLevel": f.consistency},
		})
	}
	if req.URL.Host != "gateway.cosmos.test:8081" {
		return nil, fmt.Errorf("request did not use the advertised gateway: %s", req.URL.Host)
	}
	if r.path == "dbs" && r.method == http.MethodPost {
		var props azcosmos.DatabaseProperties
		if err := json.Unmarshal(body, &props); err != nil {
			return nil, err
		}
		if props.ID != "test-database" {
			return nil, fmt.Errorf("unexpected database %q", props.ID)
		} else if f.database {
			return respond(http.StatusConflict, map[string]string{"code": "Conflict"})
		}
		f.database = true
		return respond(http.StatusCreated, props)
	}
	if r.path == "dbs/test-database" && r.method == http.MethodGet && f.database {
		return respond(http.StatusOK, azcosmos.DatabaseProperties{ID: "test-database"})
	}
	if r.path == "dbs/test-database/colls" && r.method == http.MethodPost && f.database {
		var props azcosmos.ContainerProperties
		if err := json.Unmarshal(body, &props); err != nil {
			return nil, err
		}
		if f.containers[props.ID] != nil {
			return respond(http.StatusConflict, map[string]string{"code": "Conflict"})
		}
		f.created[props.ID]++
		props.ResourceID = fmt.Sprintf("%s-%d", props.ID, f.created[props.ID])
		f.containers[props.ID] = &cosmosContainer{properties: props, items: make(map[string]json.RawMessage)}
		return respond(http.StatusCreated, props)
	}
	parts := strings.Split(r.path, "/")
	if len(parts) >= 4 && parts[0] == "dbs" && parts[1] == "test-database" && parts[2] == "colls" && f.database {
		container := f.containers[parts[3]]
		if container == nil {
			return respond(http.StatusNotFound, map[string]string{"code": "NotFound"})
		}
		if len(parts) == 4 {
			switch r.method {
			case http.MethodGet:
				return respond(http.StatusOK, container.properties)
			case http.MethodDelete:
				delete(f.containers, parts[3])
				return respond(http.StatusNoContent, map[string]any{})
			}
		}
		if len(parts) == 5 && parts[4] == "docs" && r.method == http.MethodPost {
			if req.Header.Get("x-ms-documentdb-is-upsert") != "true" {
				return nil, fmt.Errorf("expected an upsert")
			}
			var doc map[string]json.RawMessage
			var pk []json.RawMessage
			if err := json.Unmarshal(body, &doc); err != nil {
				return nil, err
			} else if err := json.Unmarshal([]byte(r.partitionKey), &pk); err != nil {
				return nil, err
			}
			path := container.properties.PartitionKeyDefinition.Paths[0]
			value := json.RawMessage(body)
			for _, part := range fixturePartitionPath.FindAllStringSubmatch(path, -1) {
				field := part[1]
				if strings.HasPrefix(field, `"`) {
					field = part[2]
				}
				var object map[string]json.RawMessage
				if err := json.Unmarshal(value, &object); err != nil {
					return nil, err
				}
				value = object[field]
			}
			if len(pk) != 1 || !sameJSON(pk[0], value) {
				return nil, fmt.Errorf("partition header %s does not match document path %s: %s", r.partitionKey, path, value)
			}
			var id string
			if err := json.Unmarshal(doc["id"], &id); err != nil {
				return nil, err
			}
			key := r.partitionKey + ":" + id
			status := http.StatusCreated
			if container.items[key] != nil {
				status = http.StatusOK
			}
			container.items[key] = bytes.Clone(body)
			container.writes++
			resp, err := respond(status, json.RawMessage(body))
			resp.Header.Set("x-ms-session-token", fmt.Sprintf("0:%d", container.writes))
			return resp, err
		}
		if len(parts) == 6 && parts[4] == "docs" {
			if strings.ContainsAny(parts[5], "%\r\n\t") || strings.TrimRightFunc(parts[5], unicode.IsSpace) != parts[5] {
				return respond(http.StatusUnauthorized, map[string]string{"code": "Unauthorized"})
			}
			key := r.partitionKey + ":" + parts[5]
			item := container.items[key]
			if item == nil {
				return respond(http.StatusNotFound, map[string]string{"code": "NotFound"})
			}
			switch r.method {
			case http.MethodGet:
				var doc map[string]json.RawMessage
				if err := json.Unmarshal(item, &doc); err != nil {
					return nil, err
				}
				for _, name := range systemFields {
					doc[name] = json.RawMessage(`"test-system-value"`)
				}
				return respond(http.StatusOK, doc)
			case http.MethodDelete:
				delete(container.items, key)
				return respond(http.StatusNoContent, map[string]any{})
			}
		}
	}
	if r.path == "dbs/test-database" && !f.database {
		return respond(http.StatusNotFound, map[string]string{"code": "NotFound"})
	}
	return nil, fmt.Errorf("unexpected Cosmos request: %s %s", r.method, r.path)
}

// sameJSON compares JSON values regardless of string escaping, keeping
// numbers exact.
func sameJSON(a, b []byte) bool {
	decode := func(raw []byte) (v any) {
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		if d.Decode(&v) != nil {
			return struct{}{}
		}
		return v
	}
	return reflect.DeepEqual(decode(a), decode(b))
}

func testMaterialization(t *testing.T, cfg config, delta bool) *pf.MaterializationSpec {
	t.Helper()
	raw, err := os.ReadFile("../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(raw))
	spec.Name = "test/cosmos-materialization"
	spec.ConfigJson, err = json.Marshal(cfg)
	require.NoError(t, err)
	template := spec.Bindings[0].Collection
	spec.Bindings = nil
	for _, tt := range []struct {
		name, pk string
		keys     []string
		types    []string
	}{
		{"orders", "", []string{"id"}, []string{"integer"}},
		{"events", "/seq", []string{"region", "seq"}, []string{"string", "integer"}},
	} {
		collection := template
		collection.Name = pf.Collection("test/" + tt.name)
		collection.Key = nil
		collection.PartitionFields = nil
		collection.Projections = []pf.Projection{
			{Field: "flow_document", Ptr: "", Inference: pf.Inference{Types: []string{"object"}, Exists: pf.Inference_MUST}},
			{Field: "canary", Ptr: "/canary", Inference: pf.Inference{Types: []string{"string"}}},
		}
		collection.WriteSchemaJson = json.RawMessage(`{"type":"object"}`)
		collection.ReadSchemaJson = nil
		for i, key := range tt.keys {
			collection.Key = append(collection.Key, "/"+key)
			collection.Projections = append(collection.Projections, pf.Projection{
				Field: key, Ptr: "/" + key, IsPrimaryKey: true,
				Inference: pf.Inference{Types: []string{tt.types[i]}, Exists: pf.Inference_MUST},
			})
		}
		slices.SortFunc(collection.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
		resourceJSON, err := json.Marshal(resource{Container: tt.name, PartitionKey: tt.pk, DeltaUpdates: delta})
		require.NoError(t, err)
		spec.Bindings = append(spec.Bindings, &pf.MaterializationSpec_Binding{
			Collection: collection, ResourceConfigJson: resourceJSON,
			ResourcePath: []string{cfg.Database, tt.name}, DeltaUpdates: delta,
			FieldSelection: pf.FieldSelection{Keys: tt.keys, Document: "flow_document"},
			StateKey:       tt.name,
		})
	}
	require.NoError(t, spec.Validate())
	return &spec
}

func validateRequest(spec *pf.MaterializationSpec) *pm.Request_Validate {
	req := &pm.Request_Validate{Name: spec.Name, ConfigJson: spec.ConfigJson, ConnectorType: spec.ConnectorType}
	for _, b := range spec.Bindings {
		req.Bindings = append(req.Bindings, &pm.Request_Validate_Binding{ResourceConfigJson: b.ResourceConfigJson, Collection: b.Collection, Backfill: b.Backfill})
	}
	return req
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()
	f, d := newCosmosFixture()
	cfg := testConfig()
	cfg.Endpoint = "http://account.cosmos.test:8081/"
	spec := testMaterialization(t, cfg, false)
	validated, err := d.Validate(ctx, validateRequest(spec))
	require.NoError(t, err)
	require.False(t, f.database, "Validate must not create resources")
	require.Len(t, validated.Bindings, 2)
	for i, b := range validated.Bindings {
		require.Equal(t, spec.Bindings[i].ResourcePath, b.ResourcePath)
		for _, p := range b.ProjectionConstraints {
			if p.Field == "canary" {
				require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, p.Constraint.Type)
			} else {
				require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, p.Constraint.Type)
			}
		}
	}
	applied, err := d.Apply(ctx, &pm.Request_Apply{Materialization: spec, Version: "v1"})
	require.NoError(t, err)
	require.Contains(t, applied.ActionDescription, "create database")
	require.Len(t, f.containers, 2)
	require.Equal(t, []string{"/id"}, f.containers["orders"].properties.PartitionKeyDefinition.Paths)
	require.Equal(t, []string{"/seq"}, f.containers["events"].properties.PartitionKeyDefinition.Paths)
	require.Nil(t, f.containers["orders"].properties.IndexingPolicy)
	require.Nil(t, f.containers["orders"].properties.DefaultTimeToLive)

	_, err = d.Validate(ctx, validateRequest(spec))
	require.ErrorContains(t, err, "already exists for new binding")
	cfg.Advanced.FeatureFlags = "allow_existing_tables_for_new_bindings"
	spec = testMaterialization(t, cfg, false)
	_, err = d.Validate(ctx, validateRequest(spec))
	require.NoError(t, err)
	applySpec(t, d, spec)

	f.containers["events"].properties.PartitionKeyDefinition.Paths = []string{"/region"}
	_, err = d.Validate(ctx, validateRequest(spec))
	require.ErrorContains(t, err, `expected a single partition key at "/seq"`)
	_, err = d.Apply(ctx, &pm.Request_Apply{Materialization: spec, Version: "v1"})
	require.ErrorContains(t, err, `expected a single partition key at "/seq"`)
	f.containers["events"].properties.PartitionKeyDefinition.Paths = []string{"/seq"}
	f.containers["events"].properties.PartitionKeyDefinition.Kind = azcosmos.PartitionKeyKindMultiHash
	_, err = d.Validate(ctx, validateRequest(spec))
	require.ErrorContains(t, err, "MultiHash")
	f.containers["events"].properties.PartitionKeyDefinition.Kind = azcosmos.PartitionKeyKindHash

	bad := validateRequest(spec)
	bad.Bindings[1].ResourceConfigJson = json.RawMessage(`{"container":"events","partition_key":"/canary"}`)
	_, err = d.Validate(ctx, bad)
	require.ErrorContains(t, err, "must be one of the collection key pointers")

	f.containers["orders"].items[`["old"]:old`] = json.RawMessage(`{"id":"old"}`)
	backfill := proto.Clone(spec).(*pf.MaterializationSpec)
	backfill.Bindings[0].Backfill++
	_, err = d.Apply(ctx, &pm.Request_Apply{Materialization: backfill, LastMaterialization: spec, Version: "v2"})
	require.NoError(t, err)
	require.Empty(t, f.containers["orders"].items)
	require.True(t, slices.ContainsFunc(f.requests, func(r cosmosRequest) bool {
		return r.method == http.MethodDelete && r.path == "dbs/test-database/colls/orders"
	}))

	f.failures["GET dbs/test-database"] = http.StatusUnauthorized
	_, err = d.Validate(ctx, validateRequest(spec))
	require.ErrorContains(t, err, "reading database")
	require.NotContains(t, err.Error(), cfg.Key)
}

func TestPartitionPathComparison(t *testing.T) {
	for _, tt := range []struct {
		ptr, path string
		matches   bool
	}{
		{"/region", `/"region"`, true},
		{"/a~0b", "/a~b", true},
		{"/outer/a~1b", `/"outer"/"a/b"`, true},
		{"/a~01b", `/"a~1b"`, true},
		{"/a~0b", "/a~0b", false},
		{"/a~1b", "/a/b", false},
		{"/a~1b", `/"a/b"extra`, false},
		{"/a~1b", `/"a/b`, false},
		{"/region", "/region/", false},
	} {
		t.Run(tt.ptr+":"+tt.path, func(t *testing.T) {
			err := checkPartitionKey("test-container", tt.ptr, azcosmos.PartitionKeyDefinition{
				Paths: []string{tt.path}, Kind: azcosmos.PartitionKeyKindHash,
			})
			if tt.matches {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "expected a single partition key")
				require.ErrorContains(t, err, "backfill the binding to recreate the container, or delete or rename the existing container if this materialization did not create it")
			}
		})
	}
}

type transactionStream struct {
	mu          sync.Mutex
	requests    []pm.Request
	responses   []pm.Response
	beforeStore func(*pm.Request_Store, []pm.Response) error
}

func (s *transactionStream) Send(resp *pm.Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, *proto.Clone(resp).(*pm.Response))
	return nil
}

func (s *transactionStream) RecvMsg(req *pm.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) == 0 {
		return io.EOF
	}
	*req, s.requests = s.requests[0], s.requests[1:]
	if req.Store != nil && s.beforeStore != nil {
		return s.beforeStore(req.Store, s.responses)
	}
	return nil
}

func storeRequestFor(binding uint32, key tuple.Tuple, doc string) pm.Request {
	return pm.Request{Store: &pm.Request_Store{Binding: binding, KeyPacked: key.Pack(), DocJson: json.RawMessage(doc)}}
}

func loadRequestFor(binding uint32, key tuple.Tuple) pm.Request {
	return pm.Request{Load: &pm.Request_Load{Binding: binding, KeyPacked: key.Pack()}}
}

func ack() pm.Request   { return pm.Request{Acknowledge: &pm.Request_Acknowledge{}} }
func flush() pm.Request { return pm.Request{Flush: &pm.Request_Flush{}} }
func startCommit() pm.Request {
	return pm.Request{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}}
}

func applySpec(t *testing.T, d driver, spec *pf.MaterializationSpec) {
	t.Helper()
	_, err := d.Apply(context.Background(), &pm.Request_Apply{Materialization: spec, Version: "v1"})
	require.NoError(t, err)
}

func transactionOpen(spec *pf.MaterializationSpec, state json.RawMessage) *pm.Request_Open {
	return &pm.Request_Open{
		Materialization: spec, Version: "v1", StateJson: state,
		Range: &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
	}
}

func runTransactions(t *testing.T, d driver, spec *pf.MaterializationSpec, stream *transactionStream) {
	t.Helper()
	require.NoError(t, m.RunTransactions(context.Background(), d, stream, transactionOpen(spec, nil), log.DebugLevel))
}

func loadedDocs(responses []pm.Response) []string {
	var docs []string
	for _, resp := range responses {
		if resp.Loaded != nil {
			docs = append(docs, string(resp.Loaded.DocJson))
		}
	}
	return docs
}

func lastStartedCommit(responses []pm.Response) *pm.Response_StartedCommit {
	var started *pm.Response_StartedCommit
	for _, resp := range responses {
		if resp.StartedCommit != nil {
			started = resp.StartedCommit
		}
	}
	return started
}

// mergeLoaded emulates the collection's merge reduction of the Store that marks
// order 1 "paid", using the connector's Loaded response for that binding.
func mergeLoaded(binding uint32) func(*pm.Request_Store, []pm.Response) error {
	return func(store *pm.Request_Store, responses []pm.Response) error {
		if !bytes.Contains(store.DocJson, []byte(`"paid"`)) {
			return nil
		}
		for _, resp := range responses {
			var doc, update map[string]json.RawMessage
			if resp.Loaded == nil || resp.Loaded.Binding != binding {
				continue
			} else if err := json.Unmarshal(resp.Loaded.DocJson, &doc); err != nil {
				return err
			} else if string(doc["id"]) != "1" {
				continue
			} else if err := json.Unmarshal(store.DocJson, &update); err != nil {
				return err
			}
			maps.Copy(doc, update)
			var err error
			store.DocJson, err = json.Marshal(doc)
			store.Exists = true
			return err
		}
		return fmt.Errorf("order 1 was not loaded")
	}
}

func TestFirstLoad(t *testing.T) {
	f, d := newCosmosFixture()
	cfg := testConfig()
	cfg.Endpoint = "http://account.cosmos.test:8081/"
	cfg.HardDelete = true
	cfg.Advanced.FeatureFlags = "allow_existing_tables_for_new_bindings"
	spec := testMaterialization(t, cfg, false)
	applySpec(t, d, spec)
	stream := &transactionStream{requests: []pm.Request{
		ack(),
		loadRequestFor(0, tuple.Tuple{int64(1)}), loadRequestFor(0, tuple.Tuple{int64(2)}),
		loadRequestFor(1, tuple.Tuple{"us", int64(7)}), loadRequestFor(1, tuple.Tuple{"eu/west", int64(8)}),
		flush(),
		storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1,"status":"new","canary":"o1","note":"keep"}`),
		storeRequestFor(0, tuple.Tuple{int64(2)}, `{"id":2,"status":"new","canary":"o2"}`),
		storeRequestFor(1, tuple.Tuple{"us", int64(7)}, `{"region":"us","seq":7,"canary":"e7"}`),
		storeRequestFor(1, tuple.Tuple{"eu/west", int64(8)}, `{"region":"eu/west","seq":8,"canary":"e8"}`),
		startCommit(), ack(),
		loadRequestFor(0, tuple.Tuple{int64(1)}), loadRequestFor(0, tuple.Tuple{int64(3)}),
		flush(),
		storeRequestFor(0, tuple.Tuple{int64(3)}, `{"id":3,"status":"new","canary":"o3"}`),
		storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1,"status":"paid"}`),
		startCommit(), ack(),
	}}
	stream.beforeStore = mergeLoaded(0)
	runTransactions(t, d, spec, stream)
	require.Len(t, f.containers["orders"].items, 3)
	require.Len(t, f.containers["events"].items, 2)
	for _, tt := range []struct{ container, key, want string }{
		{"orders", `["1"]:1`, `{"id":"1","_flow_id":1,"status":"paid","canary":"o1","note":"keep"}`},
		{"orders", `["2"]:2`, `{"id":"2","_flow_id":2,"status":"new","canary":"o2"}`},
		{"orders", `["3"]:3`, `{"id":"3","_flow_id":3,"status":"new","canary":"o3"}`},
		{"events", `[7]:us:7`, `{"id":"us:7","region":"us","seq":7,"canary":"e7"}`},
		{"events", `[8]:eu~2Fwest:8`, `{"id":"eu~2Fwest:8","region":"eu/west","seq":8,"canary":"e8"}`},
	} {
		require.JSONEq(t, tt.want, string(f.containers[tt.container].items[tt.key]))
	}
	loaded := loadedDocs(stream.responses)
	require.Len(t, loaded, 1)
	require.JSONEq(t, `{"id":1,"status":"new","canary":"o1","note":"keep"}`, loaded[0])
	for _, resp := range stream.responses {
		if resp.Opened != nil {
			require.True(t, resp.Opened.DisableLoadOptimization)
			require.Nil(t, resp.Opened.RuntimeCheckpoint)
		}
	}
	require.True(t, slices.ContainsFunc(f.requests, func(r cosmosRequest) bool {
		return r.method == http.MethodGet && strings.HasSuffix(r.path, "/docs/1") && r.sessionToken == "0:2"
	}))

	// A fresh preview must read the existing items with no connector checkpoint.
	stream = &transactionStream{requests: []pm.Request{
		ack(),
		loadRequestFor(0, tuple.Tuple{int64(1)}), loadRequestFor(1, tuple.Tuple{"eu/west", int64(8)}),
		flush(), startCommit(), ack(),
	}}
	runTransactions(t, d, spec, stream)
	require.Len(t, loadedDocs(stream.responses), 2)
}

func TestStoreModes(t *testing.T) {
	for _, delta := range []bool{false, true} {
		for _, hardDelete := range []bool{false, true} {
			t.Run(fmt.Sprintf("delta=%t/hardDelete=%t", delta, hardDelete), func(t *testing.T) {
				f, d := newCosmosFixture()
				cfg := testConfig()
				cfg.HardDelete = hardDelete
				spec := testMaterialization(t, cfg, delta)
				applySpec(t, d, spec)
				stream := &transactionStream{requests: []pm.Request{ack()}}
				if delta {
					stream.requests = append(stream.requests, loadRequestFor(0, tuple.Tuple{int64(1)}))
				}
				stream.requests = append(stream.requests, flush())
				for i := int64(1); i <= 3; i++ {
					stream.requests = append(stream.requests, storeRequestFor(0, tuple.Tuple{i}, fmt.Sprintf(`{"id":%d,"old":true}`, i)))
				}
				stream.requests = append(stream.requests,
					storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1,"latest":true}`),
					storeRequestFor(0, tuple.Tuple{int64(2)}, `{"id":2,"_meta":{"op":"\u0064"}}`),
				)
				deleted := storeRequestFor(0, tuple.Tuple{int64(3)}, `{"id":3}`)
				deleted.Store.Delete = true // Exists remains false: this key has never been loaded.
				if hardDelete {
					deleted.Store.DocJson = json.RawMessage(`null`)
				}
				stream.requests = append(stream.requests, deleted)
				deleted = storeRequestFor(0, tuple.Tuple{int64(4)}, `{"id":4}`)
				deleted.Store.Delete = true // Deleting a missing item is also successful.
				stream.requests = append(stream.requests, deleted,
					startCommit(), ack(),
				)
				runTransactions(t, d, spec, stream)
				require.JSONEq(t, `{"id":"1","_flow_id":1,"latest":true}`, string(f.containers["orders"].items[`["1"]:1`]))
				if hardDelete {
					require.Len(t, f.containers["orders"].items, 1)
				} else {
					require.Len(t, f.containers["orders"].items, 4)
				}
				if delta {
					require.False(t, slices.ContainsFunc(f.requests, func(r cosmosRequest) bool { return r.method == http.MethodGet && strings.Contains(r.path, "/docs/") }))
				}
			})
		}
	}
}

func TestLargeIntegerStore(t *testing.T) {
	f, d := newCosmosFixture()
	spec := testMaterialization(t, testConfig(), false)
	applySpec(t, d, spec)
	const id int64 = 9007199254740993
	stream := &transactionStream{requests: []pm.Request{
		ack(), flush(),
		storeRequestFor(0, tuple.Tuple{id}, `{"id":9007199254740993,"number":18446744073709551615}`),
		startCommit(), ack(), loadRequestFor(0, tuple.Tuple{id}),
		flush(), startCommit(),
		ack(),
	}}
	runTransactions(t, d, spec, stream)
	stored := f.containers["orders"].items[`["9007199254740993"]:`+strconv.FormatInt(id, 10)]
	require.Contains(t, string(stored), `"_flow_id":9007199254740993`)
	require.Contains(t, string(stored), `"number":18446744073709551615`)
	require.Equal(t, []string{`{"id":9007199254740993,"number":18446744073709551615}`}, loadedDocs(stream.responses))
}

func TestPartitionValues(t *testing.T) {
	for _, value := range []any{int64(9007199254740993), uint64(math.MaxUint64), true, false, "日本%:/\\?#", 1.25} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			f, d := newCosmosFixture()
			spec := testMaterialization(t, testConfig(), false)
			applySpec(t, d, spec)
			key := tuple.Tuple{"test", value}
			doc, err := json.Marshal(map[string]any{"region": "test", "seq": value})
			require.NoError(t, err)
			stream := &transactionStream{requests: []pm.Request{
				ack(), flush(),
				storeRequestFor(1, key, string(doc)),
				startCommit(), ack(), loadRequestFor(1, key),
				flush(), startCommit(),
				ack(),
			}}
			runTransactions(t, d, spec, stream)
			require.Len(t, f.containers["events"].items, 1)
			require.Equal(t, []string{string(doc)}, loadedDocs(stream.responses))
		})
	}
}

func TestTransactionErrors(t *testing.T) {
	for _, tt := range []struct {
		name, operation, want string
		requests              []pm.Request
	}{
		{"read error", "GET dbs/test-database/colls/orders/docs/1", "reading item", []pm.Request{
			loadRequestFor(0, tuple.Tuple{int64(1)}), flush(),
		}},
		{"write error", "POST dbs/test-database/colls/orders/docs", "storing item", []pm.Request{
			flush(), storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1}`),
		}},
		{"invalid key", "", "encoding key", []pm.Request{
			flush(), storeRequestFor(0, tuple.Tuple{math.NaN()}, `{"id":1}`),
		}},
		{"invalid document", "", "document must be a JSON object", []pm.Request{
			flush(), storeRequestFor(0, tuple.Tuple{int64(1)}, `null`),
		}},
		{"incomplete store stream", "", "reading Store: EOF", []pm.Request{
			flush(),
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, d := newCosmosFixture()
			spec := testMaterialization(t, testConfig(), false)
			applySpec(t, d, spec)
			f.failures[tt.operation] = http.StatusBadRequest
			stream := &transactionStream{requests: append([]pm.Request{ack()}, tt.requests...)}
			if tt.name != "incomplete store stream" {
				stream.requests = append(stream.requests, startCommit())
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := m.RunTransactions(ctx, d, stream, transactionOpen(spec, nil), log.DebugLevel)
			require.ErrorContains(t, err, tt.want)
			require.Nil(t, lastStartedCommit(stream.responses))
		})
	}
}

func TestLoadBatches(t *testing.T) {
	f, d := newCosmosFixture()
	spec := testMaterialization(t, testConfig(), false)
	applySpec(t, d, spec)
	stream := &transactionStream{requests: []pm.Request{
		ack(), flush(),
	}}
	const count = 2*loadBatchSize + 1
	for i := int64(1); i <= count; i++ {
		stream.requests = append(stream.requests, storeRequestFor(0, tuple.Tuple{i}, fmt.Sprintf(`{"id":%d}`, i)))
	}
	stream.requests = append(stream.requests, startCommit(), ack())
	for i := int64(0); i <= count; i++ {
		stream.requests = append(stream.requests, loadRequestFor(0, tuple.Tuple{i}))
	}
	stream.requests = append(stream.requests, flush(), startCommit(), ack())
	runTransactions(t, d, spec, stream)
	require.Len(t, f.containers["orders"].items, count)
	seen := make(map[string]bool)
	for _, resp := range stream.responses {
		if resp.Loaded != nil {
			doc := string(resp.Loaded.DocJson)
			require.False(t, seen[doc], "duplicate Loaded response: %s", doc)
			seen[doc] = true
		}
	}
	require.Len(t, seen, count)
	for i := 1; i <= count; i++ {
		require.True(t, seen[fmt.Sprintf(`{"id":%d}`, i)])
	}
}
