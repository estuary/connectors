package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestSpec(t *testing.T) {
	t.Parallel()

	response, err := Driver().Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)
	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))

	// The boilerplate sets the protocol version before sending the response.
	response.Protocol = 3032023
	require.NoError(t, response.Validate())

	var endpoint, resourceSchema struct {
		Properties map[string]map[string]any `json:"properties"`
		Required   []string                  `json:"required"`
	}
	require.NoError(t, json.Unmarshal(response.ConfigSchemaJson, &endpoint))
	require.ElementsMatch(t, []string{"url", "credentials", "org", "bucket"}, endpoint.Required)
	require.Len(t, endpoint.Properties, 6)
	credentials := endpoint.Properties["credentials"]
	require.Equal(t, "Authentication", credentials["title"])
	require.Equal(t, map[string]any{"propertyName": "credentials_title"}, credentials["discriminator"])
	apiToken := credentials["oneOf"].([]any)[0].(map[string]any)["properties"].(map[string]any)
	require.Equal(t, true, apiToken["access_token"].(map[string]any)["secret"])
	discriminator := apiToken["credentials_title"].(map[string]any)
	require.Equal(t, "Authentication Method", discriminator["title"])
	require.Equal(t, "API Token", discriminator["const"])
	require.Equal(t, "API Token", discriminator["default"])
	advanced := endpoint.Properties["advanced"]["properties"].(map[string]any)
	require.Equal(t, true, endpoint.Properties["advanced"]["advanced"])
	require.Equal(t, true, advanced["ssl_ca_certificate"].(map[string]any)["multiline"])
	require.Equal(t, false, advanced["ssl_skip_verify"].(map[string]any)["default"])
	for _, field := range []string{"url", "org", "bucket"} {
		require.Equal(t, "string", endpoint.Properties[field]["type"])
	}
	require.Equal(t, "boolean", endpoint.Properties["hardDelete"]["type"])
	require.Equal(t, false, endpoint.Properties["hardDelete"]["default"])

	require.NoError(t, json.Unmarshal(response.ResourceConfigSchemaJson, &resourceSchema))
	require.Equal(t, []string{"measurement"}, resourceSchema.Required)
	require.Len(t, resourceSchema.Properties, 3)
	require.Equal(t, true, resourceSchema.Properties["measurement"]["x-collection-name"])
	require.Equal(t, "flow_published_at", resourceSchema.Properties["timestampField"]["default"])
	require.Equal(t, "array", resourceSchema.Properties["tagFields"]["type"])
	require.Equal(t, map[string]any{"type": "string"}, resourceSchema.Properties["tagFields"]["items"])
}

func TestEndpointConfig(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		value any
		err   string
	}{
		{"local URL", "url", "http://host:8086", ""},
		{"cloud URL", "url", "https://us-east-1-1.aws.cloud2.influxdata.com", ""},
		{"base path", "url", "https://influxdb.invalid/proxy/", ""},
		{"missing URL", "url", "", "missing 'url'"},
		{"missing token", "credentials", map[string]any{"credentials_title": "API Token"}, "missing 'credentials.access_token'"},
		{"missing credentials", "credentials", nil, "missing 'credentials.access_token'"},
		{"unknown authentication", "credentials", map[string]any{"credentials_title": "Password", "access_token": "made-up-token"}, "must be 'API Token'"},
		{"default authentication", "credentials", map[string]any{"access_token": "made-up-token"}, ""},
		{"legacy token", "token", "made-up-token", "unknown field"},
		{"invalid CA", "advanced", map[string]any{"ssl_ca_certificate": "not a certificate"}, "PEM-encoded CA certificate"},
		{"conflicting TLS", "advanced", map[string]any{"ssl_ca_certificate": "certificate", "ssl_skip_verify": true}, "not both"},
		{"skip verification", "advanced", map[string]any{"ssl_skip_verify": true}, ""},
		{"missing org", "org", "", "missing 'org'"},
		{"missing bucket", "bucket", " ", "missing 'bucket'"},
		{"relative URL", "url", "influxdb.invalid:8086", "absolute HTTP or HTTPS"},
		{"unsupported scheme", "url", "ftp://influxdb.invalid", "absolute HTTP or HTTPS"},
		{"missing host", "url", "http:///", "absolute HTTP or HTTPS"},
		{"malformed URL", "url", "http://host:%", "absolute HTTP or HTTPS"},
		{"URL credentials", "url", "http://user:made-up-password@host", "must not include credentials"},
		{"URL query", "url", "http://host?bucket=sensors", "must not include credentials"},
		{"URL fragment", "url", "http://host/#fragment", "must not include credentials"},
		{"hard delete enabled", "hardDelete", true, ""},
		{"hard delete disabled", "hardDelete", false, ""},
		{"hard delete string", "hardDelete", "true", "cannot unmarshal string"},
		{"hard delete number", "hardDelete", 1, "cannot unmarshal number"},
		{"unknown field", "extra", "value", "unknown field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]any{"url": "http://influxdb.invalid:8086", "credentials": map[string]any{"credentials_title": "API Token", "access_token": "made-up-token"}, "org": "estuary", "bucket": "sensors"}
			cfg[tc.field] = tc.value
			raw, err := json.Marshal(cfg)
			require.NoError(t, err)
			decoded, err := resolveEndpointConfig(raw)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.field == "hardDelete" && tc.value == true, decoded.HardDelete)
			}
		})
	}
}

func TestResourceConfig(t *testing.T) {
	res, err := resolveResourceConfig(json.RawMessage(`{"measurement":"readings"}`))
	require.NoError(t, err)
	require.Equal(t, "flow_published_at", res.TimestampField)
	require.Nil(t, res.TagFields)

	res, err = resolveResourceConfig(json.RawMessage(`{"measurement":"readings","timestampField":"ts","tagFields":[]}`))
	require.NoError(t, err)
	require.Equal(t, "ts", res.TimestampField)
	require.NotNil(t, res.TagFields)
	require.Empty(t, res.TagFields)

	for _, tc := range []struct{ raw, err string }{
		{`{}`, "missing 'measurement'"},
		{`{"measurement":" "}`, "missing 'measurement'"},
		{`{"measurement":"files\\"}`, "ending in a backslash"},
		{`{"measurement":"line\nbreak"}`, "does not support newlines"},
		{`{"measurement":"#readings"}`, "must not start with '#'"},
		{`{"measurement":"readings","timestampField":""}`, "missing 'timestampField'"},
		{`{"measurement":"readings","tagFields":[""]}`, "empty field name"},
		{`{"measurement":"readings","tagFields":["sensor","sensor"]}`, "duplicate tag field"},
		{`{"measurement":"readings","tagFields":["flow_published_at"]}`, "must not also be a tag field"},
		{`{"measurement":"readings","tagFields":"sensor"}`, "cannot unmarshal"},
		{`{"measurement":"readings","extra":true}`, "unknown field"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			_, err := resolveResourceConfig(json.RawMessage(tc.raw))
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resource string
		required []string
		optional []string
	}{
		{"key timestamp", `{"measurement":"readings","timestampField":"ts"}`, []string{"sensor", "ts"}, []string{"flow_document", "flow_published_at"}},
		{"default timestamp", `{"measurement":"readings"}`, []string{"sensor", "ts", "flow_published_at"}, []string{"flow_document"}},
		{"explicit tags", `{"measurement":"readings","timestampField":"ts","tagFields":["ok","count"]}`, []string{"ok", "count", "ts"}, []string{"flow_document", "flow_published_at"}},
		{"empty tags", `{"measurement":"readings","timestampField":"ts","tagFields":[]}`, []string{"ts"}, []string{"flow_document", "flow_published_at"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, calls := testDriver(t, http.StatusOK, `{"buckets":[{"name":"sensors & lab"}]}`)
			response, err := d.Validate(context.Background(), &pm.Request_Validate{
				ConfigJson: testEndpoint,
				Bindings: []*pm.Request_Validate_Binding{{
					ResourceConfigJson: json.RawMessage(tc.resource),
					Collection:         testCollection(),
				}},
			})
			require.NoError(t, err)
			require.Equal(t, 1, *calls)
			require.Len(t, response.Bindings, 1)
			binding := response.Bindings[0]
			require.True(t, binding.DeltaUpdates)
			require.False(t, binding.CaseInsensitiveFields)
			require.Equal(t, []string{"readings"}, binding.ResourcePath)

			constraints := make(map[string]pm.Response_Validated_Constraint_Type)
			for _, p := range binding.ProjectionConstraints {
				require.NotEmpty(t, p.Constraint.Reason)
				constraints[p.Field] = p.Constraint.Type
			}
			// The timestamp and tags are read by name, so their exact fields are required.
			for _, field := range tc.required {
				require.Equal(t, pm.Response_Validated_Constraint_FIELD_REQUIRED, constraints[field], field)
				delete(constraints, field)
			}
			for _, field := range tc.optional {
				require.Equal(t, pm.Response_Validated_Constraint_FIELD_OPTIONAL, constraints[field], field)
				delete(constraints, field)
			}
			// meta has no nested fields in its schema, so its JSON value is required.
			require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, constraints["meta"])
			delete(constraints, "meta")
			for field, constraint := range constraints {
				require.Equal(t, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, constraint, field)
			}
		})
	}
}

func TestValidateObjectFields(t *testing.T) {
	for _, tc := range []struct {
		name       string
		types      []string
		constraint pm.Response_Validated_Constraint_Type
	}{
		// meta has no child projections, so an object there is required as JSON.
		{"object", []string{"object"}, pm.Response_Validated_Constraint_LOCATION_REQUIRED},
		{"nullable object", []string{"null", "object"}, pm.Response_Validated_Constraint_LOCATION_REQUIRED},
		{"array", []string{"array"}, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED},
		{"nullable array", []string{"array", "null"}, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED},
		{"object or string", []string{"object", "string"}, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collection := testCollection()
			collection.GetProjection("meta").Inference.Types = tc.types
			collection.GetProjection("flow_document").Inference.Types = []string{"null", "object"}
			constraints, err := validateConstraints(t, &pm.Request_Validate{
				ConfigJson:        testEndpoint,
				LinkedCollections: []pf.CollectionSpec{collection},
				Bindings: []*pm.Request_Validate_Binding{{
					ResourceConfigJson: json.RawMessage(`{"measurement":"readings","timestampField":"ts"}`),
				}},
			})
			require.NoError(t, err)
			require.Equal(t, tc.constraint, constraints["meta"])
			require.Equal(t, pm.Response_Validated_Constraint_FIELD_OPTIONAL, constraints["flow_document"])
			require.Equal(t, pm.Response_Validated_Constraint_FIELD_OPTIONAL, constraints["flow_published_at"])
		})
	}
}

func TestValidateInvalidFields(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resource string
		change   func(*pf.CollectionSpec)
		err      string
	}{
		{"missing timestamp", `{"measurement":"readings","timestampField":"missing"}`, nil, `timestamp field "missing" is not a projection`},
		{"numeric timestamp", `{"measurement":"readings","timestampField":"count"}`, nil, "must be a date-time string"},
		{"unformatted timestamp", `{"measurement":"readings","timestampField":"sensor"}`, nil, "must be a date-time string"},
		{"nullable timestamp", `{"measurement":"readings","timestampField":"ts"}`, func(c *pf.CollectionSpec) {
			c.GetProjection("ts").Inference.Types = []string{"null", "string"}
		}, "must be a date-time string"},
		{"optional timestamp", `{"measurement":"readings","timestampField":"ts"}`, func(c *pf.CollectionSpec) {
			c.GetProjection("ts").Inference.Exists = pf.Inference_MAY
		}, "must always be present"},
		{"missing tag", `{"measurement":"readings","tagFields":["missing"]}`, nil, `tag field "missing" is not a projection`},
		{"object tag", `{"measurement":"readings","tagFields":["meta"]}`, nil, "must be scalar"},
		{"array tag", `{"measurement":"readings","tagFields":["samples"]}`, nil, "must be scalar"},
		{"time tag", `{"measurement":"readings","tagFields":["time"]}`, addTimeProjection(false), `tag field "time" is reserved`},
		{"default time tag", `{"measurement":"readings"}`, addTimeProjection(true), `tag field "time" is reserved`},
		{"untagged time key", `{"measurement":"readings","tagFields":["sensor"]}`, addTimeProjection(true), `key field "time" is reserved`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collection := testCollection()
			if tc.change != nil {
				tc.change(&collection)
			}
			_, err := validateBinding(t, testEndpoint, tc.resource, collection)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestValidateBucketErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		err    string
	}{
		{"unauthorized", http.StatusUnauthorized, `{"message":"unauthorized access"}`, "authentication failed (HTTP 401): check your API token: unauthorized access"},
		{"forbidden", http.StatusForbidden, `{"message":"read permission required"}`, "HTTP 403: read permission required"},
		{"missing bucket", http.StatusOK, `{"buckets":[]}`, `bucket "sensors & lab" was not found in organization "estuary + test"`},
		{"missing bucket 404", http.StatusNotFound, `{"code":"not found","message":"bucket \"sensors & lab\" not found"}`, `HTTP 404: bucket "sensors & lab" not found; the bucket must already exist in organization "estuary + test"`},
		{"wrong bucket", http.StatusOK, `{"buckets":[{"name":"other"}]}`, "bucket must already exist"},
		{"invalid response", http.StatusOK, `invalid json`, "decoding bucket response"},
		{"not found", http.StatusNotFound, "API endpoint not found", "HTTP 404: API endpoint not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := testDriver(t, tc.status, tc.body)
			_, err := d.Validate(context.Background(), &pm.Request_Validate{ConfigJson: testEndpoint})
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestValidateConfigBeforeRequest(t *testing.T) {
	d, calls := testDriver(t, http.StatusOK, `{"buckets":[]}`)
	_, err := d.Validate(context.Background(), &pm.Request_Validate{ConfigJson: json.RawMessage(`{}`)})
	require.ErrorContains(t, err, "missing 'url'")
	require.Zero(t, *calls)
}

func TestValidateOperationProjection(t *testing.T) {
	for _, field := range []string{"_meta/op", "change_operation"} {
		t.Run(field, func(t *testing.T) {
			collection := testCollection()
			collection.Projections = append([]pf.Projection{{
				Field: field, Ptr: "/_meta/op", Inference: pf.Inference{Types: []string{"null", "string"}},
			}}, collection.Projections...)
			for _, tc := range []struct {
				name       string
				endpoint   json.RawMessage
				constraint pm.Response_Validated_Constraint_Type
			}{
				{"hard deletes", testHardDeleteEndpoint, pm.Response_Validated_Constraint_LOCATION_REQUIRED},
				{"soft deletes", testEndpoint, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED},
			} {
				t.Run(tc.name, func(t *testing.T) {
					constraints, err := validateBinding(t, tc.endpoint, `{"measurement":"readings","timestampField":"ts"}`, collection)
					require.NoError(t, err)
					require.Equal(t, tc.constraint, constraints[field])
					require.Equal(t, pm.Response_Validated_Constraint_FIELD_OPTIONAL, constraints["flow_document"])
				})
			}
		})
	}
}

func TestValidateHardDeleteDocument(t *testing.T) {
	for _, field := range []string{"flow_document", "source_document"} {
		t.Run(field, func(t *testing.T) {
			collection := testCollection()
			collection.GetProjection("flow_document").Field = field
			addProjections(&collection)
			d, _ := testDriver(t, http.StatusOK, `{"buckets":[{"name":"sensors & lab"}]}`)
			response, err := d.Validate(context.Background(), &pm.Request_Validate{
				ConfigJson: testHardDeleteEndpoint,
				Bindings: []*pm.Request_Validate_Binding{{
					Collection: collection, ResourceConfigJson: json.RawMessage(`{"measurement":"readings","timestampField":"ts"}`),
				}},
			})
			require.NoError(t, err)
			constraints := make(map[string]*pm.Response_Validated_Constraint)
			for _, p := range response.Bindings[0].ProjectionConstraints {
				constraints[p.Field] = p.Constraint
			}
			require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, constraints[field].Type)
			require.Contains(t, constraints[field].Reason, "deleted documents")
		})
	}
}

func TestValidateTransportError(t *testing.T) {
	d := driver{httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("test connection failed")
	})}}
	_, err := d.Validate(context.Background(), &pm.Request_Validate{ConfigJson: testEndpoint})
	require.ErrorContains(t, err, "test connection failed")
}

func TestApplyAndOpen(t *testing.T) {
	d, calls := testDriver(t, http.StatusOK, `{"buckets":[]}`)
	spec := &pf.MaterializationSpec{
		Name:          "tests/influxdb",
		ConnectorType: pf.MaterializationSpec_IMAGE,
		ConfigJson:    testEndpoint,
		Bindings: []*pf.MaterializationSpec_Binding{{
			ResourceConfigJson: json.RawMessage(`{"measurement":"readings","timestampField":"ts"}`),
			ResourcePath:       []string{"readings"},
			Collection:         testCollection(),
			FieldSelection:     pf.FieldSelection{Keys: []string{"sensor", "ts"}, Values: []string{"temp"}},
			DeltaUpdates:       true,
		}},
		ShardTemplate:       &pc.ShardSpec{Id: "tests/influxdb/shard", MaxTxnDuration: time.Second},
		RecoveryLogTemplate: testJournal("recovery/tests/influxdb/log"),
	}
	applied, err := d.Apply(context.Background(), &pm.Request_Apply{Materialization: spec, Version: "v1"})
	require.NoError(t, err)
	require.Empty(t, applied.ActionDescription)
	require.Zero(t, *calls)

	tx, opened, _, err := d.NewTransactor(context.Background(), pm.Request_Open{Materialization: spec}, nil)
	require.NoError(t, err)
	defer tx.Destroy()
	require.NoError(t, opened.Validate())
	tr := tx.(*transactor)
	require.Len(t, tr.bindings, 1)
	require.Equal(t, []string{"sensor"}, tr.bindings[0].resource.TagFields)
	require.Equal(t, "ts", tr.bindings[0].resource.TimestampField)
	require.Zero(t, *calls)
}

func TestOpenLinkedCollections(t *testing.T) {
	open := testOpen()
	spec := open.Materialization
	for i, b := range spec.Bindings {
		spec.LinkedCollections = append(spec.LinkedCollections, b.Collection)
		b.CollectionIndex = uint32(i)
		b.Collection = pf.CollectionSpec{}
	}
	require.NoError(t, spec.Validate())

	d := driver{httpClient: &http.Client{}}
	tx, _, _, err := d.NewTransactor(context.Background(), *open, nil)
	require.NoError(t, err)
	defer tx.Destroy()
	tr := tx.(*transactor)
	require.Len(t, tr.bindings, 2)
	require.Equal(t, []string{"sensor"}, tr.bindings[0].resource.TagFields)
	require.True(t, tr.bindings[0].timestampInKey)
	require.Equal(t, map[string]string{"temp": "number", "count": "integer"}, tr.bindings[0].numericFields)
	require.Equal(t, []string{"id"}, tr.bindings[1].resource.TagFields)

	spec.Bindings[1].CollectionIndex = 5
	_, _, _, err = d.NewTransactor(context.Background(), *open, nil)
	require.ErrorContains(t, err, "binding 1: cannot resolve collection")
}

var testEndpoint = json.RawMessage(`{"url":"http://influxdb.invalid:8086/proxy/","credentials":{"credentials_title":"API Token","access_token":"made-up-token"},"org":"estuary + test","bucket":"sensors & lab"}`)
var testHardDeleteEndpoint = json.RawMessage(`{"url":"http://influxdb.invalid:8086/proxy/","credentials":{"credentials_title":"API Token","access_token":"made-up-token"},"org":"estuary + test","bucket":"sensors & lab","hardDelete":true}`)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func testDriver(t *testing.T, status int, body string) (driver, *int) {
	t.Helper()
	calls := new(int)
	return driver{httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		*calls++
		require.Equal(t, http.MethodGet, req.Method)
		require.Equal(t, "/proxy/api/v2/buckets", req.URL.Path)
		require.Equal(t, "estuary + test", req.URL.Query().Get("org"))
		require.Equal(t, "sensors & lab", req.URL.Query().Get("name"))
		require.Len(t, req.URL.Query(), 2)
		require.Equal(t, "Token made-up-token", req.Header.Get("Authorization"))
		require.Equal(t, "application/json", req.Header.Get("Accept"))
		return &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})}}, calls
}

// validateConstraints runs Validate against an existing bucket and returns each field's constraint type.
func validateConstraints(t *testing.T, req *pm.Request_Validate) (map[string]pm.Response_Validated_Constraint_Type, error) {
	t.Helper()
	d, _ := testDriver(t, http.StatusOK, `{"buckets":[{"name":"sensors & lab"}]}`)
	response, err := d.Validate(context.Background(), req)
	if err != nil {
		return nil, err
	}
	constraints := make(map[string]pm.Response_Validated_Constraint_Type)
	for _, p := range response.Bindings[0].ProjectionConstraints {
		constraints[p.Field] = p.Constraint.Type
	}
	return constraints, nil
}

func validateBinding(t *testing.T, endpoint json.RawMessage, resource string, collection pf.CollectionSpec) (map[string]pm.Response_Validated_Constraint_Type, error) {
	t.Helper()
	return validateConstraints(t, &pm.Request_Validate{
		ConfigJson: endpoint,
		Bindings:   []*pm.Request_Validate_Binding{{ResourceConfigJson: json.RawMessage(resource), Collection: collection}},
	})
}

// addProjections appends projections to a collection and keeps them sorted by field.
func addProjections(c *pf.CollectionSpec, projections ...pf.Projection) {
	c.Projections = append(c.Projections, projections...)
	sort.Slice(c.Projections, func(i, j int) bool { return c.Projections[i].Field < c.Projections[j].Field })
}

func testCollection() pf.CollectionSpec {
	return pf.CollectionSpec{
		Name:              "tests/readings",
		Key:               []string{"/sensor", "/ts"},
		UuidPtr:           "/_meta/uuid",
		PartitionTemplate: testJournal("tests/readings/pivot=00"),
		Projections: []pf.Projection{
			{Field: "count", Ptr: "/count", Inference: pf.Inference{Types: []string{"integer"}}},
			{Field: "flow_document", Ptr: "", Inference: pf.Inference{Types: []string{"object"}}},
			{Field: "flow_published_at", Ptr: "/_meta/uuid", Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}, Exists: pf.Inference_MUST}},
			{Field: "meta", Ptr: "/meta", Inference: pf.Inference{Types: []string{"object"}}},
			{Field: "note", Ptr: "/note", Inference: pf.Inference{Types: []string{"string"}}},
			{Field: "ok", Ptr: "/ok", Inference: pf.Inference{Types: []string{"boolean"}}},
			{Field: "samples", Ptr: "/samples", Inference: pf.Inference{Types: []string{"array"}}},
			{Field: "sensor", Ptr: "/sensor", IsPrimaryKey: true, Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}},
			{Field: "temp", Ptr: "/temp", Inference: pf.Inference{Types: []string{"number"}}},
			{Field: "ts", Ptr: "/ts", IsPrimaryKey: true, Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}, Exists: pf.Inference_MUST}},
		},
	}
}

func testJournal(name string) *pb.JournalSpec {
	return &pb.JournalSpec{
		Name:        pb.Journal(name),
		Replication: 3,
		Fragment: pb.JournalSpec_Fragment{
			Length:           512 * 1024 * 1024,
			CompressionCodec: pb.CompressionCodec_GZIP,
			RefreshInterval:  30 * time.Second,
		},
	}
}

func TestValidateHardDeleteScope(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint json.RawMessage
		resource string
		err      string
	}{
		{"key tags", testHardDeleteEndpoint, `{"measurement":"readings"}`, ""},
		{"key tags and key timestamp", testHardDeleteEndpoint, `{"measurement":"readings","timestampField":"ts"}`, ""},
		{"key timestamp only", testHardDeleteEndpoint, `{"measurement":"readings","timestampField":"ts","tagFields":[]}`, `add "sensor" to 'tagFields'`},
		{"no tags", testHardDeleteEndpoint, `{"measurement":"readings","tagFields":[]}`, `add "sensor", "ts" to 'tagFields'`},
		{"non-key tags", testHardDeleteEndpoint, `{"measurement":"readings","tagFields":["ok"]}`, `add "sensor", "ts" to 'tagFields'`},
		// A delete of one key would remove every other key sharing the tagged part.
		{"partial key tags", testHardDeleteEndpoint, `{"measurement":"readings","tagFields":["sensor"]}`, `add "ts" to 'tagFields'`},
		{"soft deletes", testEndpoint, `{"measurement":"readings","tagFields":[]}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateBinding(t, tc.endpoint, tc.resource, testCollection())
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.err)
			}
		})
	}
}

func TestValidateNestedObjectFields(t *testing.T) {
	collection := testCollection()
	addProjections(&collection,
		pf.Projection{Field: "meta/build", Ptr: "/meta/build", Inference: pf.Inference{Types: []string{"object"}}},
		pf.Projection{Field: "meta/build/version", Ptr: "/meta/build/version", Inference: pf.Inference{Types: []string{"string"}}},
		pf.Projection{Field: "build", Ptr: "/meta/build", Explicit: true, Inference: pf.Inference{Types: []string{"null", "object"}}},
		pf.Projection{Field: "meta/extra", Ptr: "/meta/extra", Inference: pf.Inference{Types: []string{"null", "object"}}},
	)
	constraints, err := validateBinding(t, testEndpoint, `{"measurement":"readings","timestampField":"ts"}`, collection)
	require.NoError(t, err)
	// Top-level and user-defined objects with child projections are recommended,
	// never required, so Flow may select their scalar leaves and users may
	// exclude them. Nested objects with child projections are already part of
	// their parent's JSON value.
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, constraints["meta"])
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_OPTIONAL, constraints["meta/build"])
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, constraints["build"])
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, constraints["meta/build/version"])
	// A nested object without child projections has no leaves for Flow to select,
	// so its JSON value is required even though its parent has children.
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, constraints["meta/extra"])
}

func TestValidateChildlessObjectFields(t *testing.T) {
	collection := testCollection()
	addProjections(&collection,
		pf.Projection{Field: "data", Ptr: "/data", Inference: pf.Inference{Types: []string{"object"}}},
		pf.Projection{Field: "data/level", Ptr: "/data/level", Inference: pf.Inference{Types: []string{"integer"}}},
		pf.Projection{Field: "data/extra", Ptr: "/data/extra", Inference: pf.Inference{Types: []string{"object"}}},
		pf.Projection{Field: "extra", Ptr: "/data/extra", Explicit: true, Inference: pf.Inference{Types: []string{"object"}}},
		// A sibling whose pointer shares a prefix is not a child of /data.
		pf.Projection{Field: "database", Ptr: "/database", Inference: pf.Inference{Types: []string{"object"}}},
	)
	constraints, err := validateBinding(t, testEndpoint, `{"measurement":"readings","timestampField":"ts"}`, collection)
	require.NoError(t, err)
	// Childless objects, like meta: {type: object}, are required as JSON because
	// Flow's default field selection would not select them.
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, constraints["meta"])
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, constraints["database"])
	// data has children, so Flow selects data/level instead of data itself.
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, constraints["data"])
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, constraints["data/level"])
	// data/extra has no children, so it would otherwise never be selected.
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, constraints["data/extra"])
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, constraints["extra"])
}

// addTimeProjection adds a date-time projection named "time", optionally replacing /ts in the key.
func addTimeProjection(key bool) func(*pf.CollectionSpec) {
	return func(c *pf.CollectionSpec) {
		addProjections(c, pf.Projection{
			Field: "time", Ptr: "/time", IsPrimaryKey: key,
			Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}, Exists: pf.Inference_MUST},
		},
		)
		if key {
			c.Key = []string{"/sensor", "/time"}
			c.GetProjection("ts").IsPrimaryKey = false
		}
	}
}

func TestValidateTimeField(t *testing.T) {
	for _, tc := range []struct {
		name       string
		resource   string
		key        bool
		constraint pm.Response_Validated_Constraint_Type
	}{
		{"value", `{"measurement":"readings","timestampField":"ts"}`, false, pm.Response_Validated_Constraint_FIELD_FORBIDDEN},
		{"timestamp", `{"measurement":"readings","timestampField":"time"}`, false, pm.Response_Validated_Constraint_FIELD_REQUIRED},
		{"key timestamp", `{"measurement":"readings","timestampField":"time"}`, true, pm.Response_Validated_Constraint_FIELD_REQUIRED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collection := testCollection()
			addTimeProjection(tc.key)(&collection)
			constraints, err := validateBinding(t, testEndpoint, tc.resource, collection)
			require.NoError(t, err)
			require.Equal(t, tc.constraint, constraints["time"])
		})
	}
}

func TestValidateTimeKeyAtTimestampLocation(t *testing.T) {
	collection := testCollection()
	addTimeProjection(true)(&collection)
	addProjections(&collection, pf.Projection{
		Field: "event_time", Ptr: "/time",
		Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}, Exists: pf.Inference_MUST},
	},
	)

	validate := func(resource string, groupBy ...string) (map[string]pm.Response_Validated_Constraint_Type, error) {
		return validateConstraints(t, &pm.Request_Validate{
			ConfigJson: testEndpoint,
			Bindings: []*pm.Request_Validate_Binding{{
				ResourceConfigJson: json.RawMessage(resource),
				Collection:         collection,
				GroupBy:            groupBy,
			}},
		})
	}

	// The default group-by uses the key field "time", which is reserved.
	for _, groupBy := range [][]string{nil, {"sensor", "time"}} {
		_, err := validate(`{"measurement":"readings","timestampField":"event_time","tagFields":["sensor"]}`, groupBy...)
		require.ErrorContains(t, err, `group-by field "time" is reserved by InfluxDB; set 'timestampField' to "time", or set the binding's fields.groupBy`)
	}

	// The key field "time" projects the same location as the timestamp field,
	// and the group-by uses the timestamp field instead.
	constraints, err := validate(`{"measurement":"readings","timestampField":"event_time","tagFields":["sensor"]}`, "sensor", "event_time")
	require.NoError(t, err)
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_REQUIRED, constraints["event_time"])
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, constraints["time"])

	// A timestamp at another location still leaves the "time" key field reserved.
	_, err = validate(`{"measurement":"readings","timestampField":"flow_published_at","tagFields":["sensor"]}`)
	require.ErrorContains(t, err, `key field "time" is reserved`)
}

func TestOpenRejectsTimeField(t *testing.T) {
	open := testOpen()
	binding := open.Materialization.Bindings[0]
	addTimeProjection(false)(&binding.Collection)
	binding.FieldSelection.Values = append(binding.FieldSelection.Values, "time")
	_, _, _, err := Driver().NewTransactor(context.Background(), *open, nil)
	require.ErrorContains(t, err, `selected field "time" is reserved`)
}

func TestOpenRequiresNamedFields(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resource string
		values   []string
		err      string
	}{
		{"tag", `{"measurement":"orders","tagFields":["id","status"]}`, []string{"amount", "flow_published_at", "order_status"}, `field "status" must be selected`},
		{"timestamp", `{"measurement":"orders"}`, []string{"amount", "published", "status"}, `field "flow_published_at" must be selected`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			open := testOpen()
			binding := open.Materialization.Bindings[1]
			// A second projection of the same location can satisfy a location
			// requirement while the named field is left out.
			status := *binding.Collection.GetProjection("status")
			status.Field = "order_status"
			published := *binding.Collection.GetProjection("flow_published_at")
			published.Field = "published"
			addProjections(&binding.Collection, status, published)
			binding.ResourceConfigJson = json.RawMessage(tc.resource)
			binding.FieldSelection.Values = tc.values
			_, _, _, err := Driver().NewTransactor(context.Background(), *open, nil)
			require.ErrorContains(t, err, tc.err)
		})
	}
}
