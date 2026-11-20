package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

const defaultTimestampField = "flow_published_at"

// InfluxDB reserves the key "time". It rejects points with a "time" tag and
// drops a "time" field without an error.
const reservedTimeKey = "time"

type config struct {
	URL        string `json:"url" jsonschema:"title=InfluxDB URL,description=URL of your InfluxDB 2.x instance.,minLength=1" jsonschema_extras:"order=1"`
	Token      string `json:"token" jsonschema:"title=API Token,description=API token with read and write access to your bucket.,minLength=1" jsonschema_extras:"secret=true,order=2"`
	Org        string `json:"org" jsonschema:"title=Organization,description=Name of your InfluxDB organization.,minLength=1" jsonschema_extras:"order=3"`
	Bucket     string `json:"bucket" jsonschema:"title=Bucket,description=Name of the existing bucket to write points to.,minLength=1" jsonschema_extras:"order=4"`
	HardDelete bool   `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=Delete points when documents are deleted in your source. Otherwise deletion documents are written as points.,default=false" jsonschema_extras:"order=5"`
}

func (c config) Validate() error {
	for _, required := range []struct{ name, value string }{
		{"url", c.URL}, {"token", c.Token}, {"org", c.Org}, {"bucket", c.Bucket},
	} {
		if strings.TrimSpace(required.value) == "" {
			return fmt.Errorf("missing '%s'", required.name)
		}
	}

	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return fmt.Errorf("'url' must be an absolute HTTP or HTTPS URL")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("'url' must not include credentials, a query, or a fragment")
	}
	return nil
}

type resource struct {
	Measurement    string   `json:"measurement" jsonschema:"title=Measurement,description=Name of the measurement to write points to.,minLength=1" jsonschema_extras:"x-collection-name=true,order=1"`
	TimestampField string   `json:"timestampField,omitempty" jsonschema:"title=Timestamp Field,description=Projection field containing each point's time as an always-present date-time string.,default=flow_published_at,minLength=1" jsonschema_extras:"order=2"`
	TagFields      []string `json:"tagFields,omitempty" jsonschema:"title=Tag Fields,description=Projection fields to write as tags. Defaults to the collection key fields other than the timestamp field.,uniqueItems=true" jsonschema_extras:"order=3"`
}

func (r resource) Validate() error {
	if strings.TrimSpace(r.Measurement) == "" {
		return fmt.Errorf("missing 'measurement'")
	}
	if err := checkLineElement(r.Measurement); err != nil {
		return fmt.Errorf("invalid 'measurement': %w", err)
	}
	// Line protocol treats a line starting with '#' as a comment and has no escape for it.
	if strings.HasPrefix(r.Measurement, "#") {
		return fmt.Errorf("invalid 'measurement': must not start with '#' (line protocol treats it as a comment)")
	}
	if r.TimestampField == "" {
		return fmt.Errorf("missing 'timestampField'")
	}
	seen := make(map[string]bool)
	for _, field := range r.TagFields {
		if field == "" {
			return fmt.Errorf("'tagFields' must not contain an empty field name")
		}
		if field == r.TimestampField {
			return fmt.Errorf("timestamp field %q must not also be a tag field", field)
		}
		if seen[field] {
			return fmt.Errorf("duplicate tag field %q", field)
		}
		seen[field] = true
	}
	return nil
}

func (r *resource) resolveFields(collection *pf.CollectionSpec) error {
	timestamp := collection.GetProjection(r.TimestampField)
	if timestamp == nil {
		return fmt.Errorf("timestamp field %q is not a projection of collection %q", r.TimestampField, collection.Name)
	}
	if !slices.Equal(timestamp.Inference.Types, []string{"string"}) || timestamp.Inference.String_ == nil || timestamp.Inference.String_.Format != "date-time" {
		return fmt.Errorf("timestamp field %q must be a date-time string", r.TimestampField)
	}
	if timestamp.Inference.Exists != pf.Inference_MUST {
		return fmt.Errorf("timestamp field %q must always be present", r.TimestampField)
	}

	// An explicitly empty tag list is different from an omitted list.
	if r.TagFields == nil {
		r.TagFields = []string{}
		for _, ptr := range collection.Key {
			if ptr == timestamp.Ptr {
				continue
			}
			field := keyField(collection, ptr)
			if field == "" {
				return fmt.Errorf("collection key location %q has no projection", ptr)
			}
			r.TagFields = append(r.TagFields, field)
		}
	}
	for _, field := range r.TagFields {
		p := collection.GetProjection(field)
		if p == nil {
			return fmt.Errorf("tag field %q is not a projection of collection %q", field, collection.Name)
		}
		if slices.Contains(p.Inference.Types, "object") || slices.Contains(p.Inference.Types, "array") {
			return fmt.Errorf("tag field %q must be scalar, not an object or array", field)
		}
		if field == reservedTimeKey {
			return fmt.Errorf("tag field %q is reserved by InfluxDB; set 'timestampField' to %q to use it as the point's time", field, field)
		}
	}
	return nil
}

// checkDeleteScope requires every collection key location, other than a key
// timestamp, to be a tag. InfluxDB delete predicates only filter on tags and
// time, so an untagged key location would widen a delete to other documents.
func (r resource) checkDeleteScope(collection *pf.CollectionSpec) error {
	timestampPtr := collection.GetProjection(r.TimestampField).Ptr
	var missing []string
	for _, ptr := range collection.Key {
		if ptr == timestampPtr || slices.ContainsFunc(r.TagFields, func(field string) bool {
			p := collection.GetProjection(field)
			return p != nil && p.Ptr == ptr
		}) {
			continue
		}
		name := ptr
		if field := keyField(collection, ptr); field != "" {
			name = field
		}
		missing = append(missing, fmt.Sprintf("%q", name))
	}
	if len(missing) != 0 {
		return fmt.Errorf("hard deletes require every collection key field, other than a key timestamp, to be a tag field; add %s to 'tagFields'", strings.Join(missing, ", "))
	}
	return nil
}

// keyTags returns the tag fields that project a location of the collection key.
func (r resource) keyTags(collection *pf.CollectionSpec) map[string]bool {
	tags := make(map[string]bool)
	for _, field := range r.TagFields {
		if p := collection.GetProjection(field); p != nil && slices.Contains(collection.Key, p.Ptr) {
			tags[field] = true
		}
	}
	return tags
}

func Driver() driver {
	return driver{}
}

type driver struct {
	httpClient *http.Client
}

var _ boilerplate.Connector = &driver{}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	endpointSchema, err := schemagen.GenerateSchema("Materialize InfluxDB Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := schemagen.GenerateSchema("InfluxDB Measurement", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-influxdb",
	}, nil
}

// hasChildProjections reports whether any projection is nested under ptr.
func hasChildProjections(collection *pf.CollectionSpec, ptr string) bool {
	return slices.ContainsFunc(collection.Projections, func(p pf.Projection) bool {
		return strings.HasPrefix(p.Ptr, ptr+"/")
	})
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	cfg, err := resolveEndpointConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}
	client, err := cfg.client(d.httpClient)
	if err != nil {
		return nil, err
	}
	defer client.httpClient.CloseIdleConnections()
	if err := client.checkBucket(ctx); err != nil {
		return nil, err
	}

	var out []*pm.Response_Validated_Binding
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		collection := req.BindingCollection(b)
		if collection == nil {
			return nil, fmt.Errorf("binding has no collection")
		}
		if err := res.resolveFields(collection); err != nil {
			return nil, fmt.Errorf("measurement %q: %w", res.Measurement, err)
		}
		if cfg.HardDelete {
			if err := res.checkDeleteScope(collection); err != nil {
				return nil, fmt.Errorf("measurement %q: %w", res.Measurement, err)
			}
		}
		// A key field named "time" is fine when it projects the timestamp's location,
		// as long as the group-by uses another field for that location.
		timestampPtr := collection.GetProjection(res.TimestampField).Ptr
		for _, p := range collection.Projections {
			if p.Field == reservedTimeKey && p.IsPrimaryKey && p.Field != res.TimestampField && p.Ptr != timestampPtr {
				return nil, fmt.Errorf("measurement %q: key field %q is reserved by InfluxDB; set 'timestampField' to %q", res.Measurement, p.Field, p.Field)
			}
		}
		if res.TimestampField != reservedTimeKey && slices.Contains(groupByFields(b.GroupBy, collection), reservedTimeKey) {
			return nil, fmt.Errorf("measurement %q: group-by field %q is reserved by InfluxDB; set 'timestampField' to %q, or set the binding's fields.groupBy to use another projection of its location (such as %q)", res.Measurement, reservedTimeKey, reservedTimeKey, res.TimestampField)
		}
		hasOperation := slices.ContainsFunc(collection.Projections, func(p pf.Projection) bool { return p.Ptr == "/_meta/op" })

		var constraints []*pm.Response_Validated_ProjectionConstraint
		for _, p := range collection.Projections {
			constraint := new(pm.Response_Validated_Constraint)
			switch {
			// Points read the timestamp and tags by field name, so require those exact fields.
			case p.Field == res.TimestampField:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_REQUIRED
				constraint.Reason = "The timestamp field must be materialized"
			case slices.Contains(res.TagFields, p.Field):
				constraint.Type = pm.Response_Validated_Constraint_FIELD_REQUIRED
				constraint.Reason = "Tag fields must be materialized"
			case p.Field == reservedTimeKey:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
				constraint.Reason = "InfluxDB reserves the field name \"time\"; set it as the timestamp field to use it"
			case cfg.HardDelete && p.Ptr == "/_meta/op":
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "The operation must be selected to identify deleted documents"
			case cfg.HardDelete && !hasOperation && p.IsRootDocumentProjection():
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "The full document must be selected to identify deleted documents when there is no operation projection"
			case p.IsRootDocumentProjection() || p.Field == "flow_published_at":
				constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
				constraint.Reason = "The full document and publication time may be materialized"
			case slices.Contains(p.Inference.Types, "object") && !slices.ContainsFunc(p.Inference.Types, func(t string) bool { return t != "object" && t != "null" }):
				// Flow's default selection picks an object's scalar leaves and never a
				// recommended object, so an object without child projections is required
				// as JSON or its values would be dropped. Nested objects are already
				// part of their parent's JSON value.
				if !hasChildProjections(collection, p.Ptr) {
					constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
					constraint.Reason = "Objects without nested fields in their schema are materialized as JSON values"
				} else if p.Explicit || strings.Count(p.Ptr, "/") == 1 {
					constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
					constraint.Reason = "Object fields may be materialized as JSON values"
				} else {
					constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
					constraint.Reason = "Nested object fields are already included in their parent object's JSON value"
				}
			case slices.ContainsFunc(p.Inference.Types, func(t string) bool { return t != "null" }):
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
				constraint.Reason = "This field should be materialized as a point field"
			default:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
				constraint.Reason = "Null or missing fields are omitted from points"
			}
			constraints = append(constraints, &pm.Response_Validated_ProjectionConstraint{
				Field:      p.Field,
				Constraint: constraint,
			})
		}
		out = append(out, &pm.Response_Validated_Binding{
			CaseInsensitiveFields: false,
			ProjectionConstraints: constraints,
			DeltaUpdates:          true,
			ResourcePath:          []string{res.Measurement},
		})
	}
	return &pm.Response_Validated{Bindings: out}, nil
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	if _, err := resolveEndpointConfig(req.Materialization.ConfigJson); err != nil {
		return nil, err
	}
	for _, b := range req.Materialization.Bindings {
		if _, err := resolveResourceConfig(b.ResourceConfigJson); err != nil {
			return nil, err
		}
	}
	return &pm.Response_Applied{}, nil
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	if err := boilerplate.RequireSingleShard(open); err != nil {
		return nil, nil, nil, err
	}
	cfg, err := resolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, nil, err
	}
	var bindings []*measurementBinding
	for i, b := range open.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, nil, nil, err
		}
		collection := open.Materialization.BindingCollection(b)
		if collection == nil {
			return nil, nil, nil, fmt.Errorf("binding %d: cannot resolve collection", i)
		}
		if err := res.resolveFields(collection); err != nil {
			return nil, nil, nil, fmt.Errorf("measurement %q: %w", res.Measurement, err)
		}
		if cfg.HardDelete {
			if err := res.checkDeleteScope(collection); err != nil {
				return nil, nil, nil, fmt.Errorf("measurement %q: %w", res.Measurement, err)
			}
		}
		selectedFields := b.FieldSelection.AllFields()
		for _, field := range append([]string{res.TimestampField}, res.TagFields...) {
			if !slices.Contains(selectedFields, field) {
				return nil, nil, nil, fmt.Errorf("measurement %q: field %q must be selected", res.Measurement, field)
			}
		}
		binding := &measurementBinding{
			path:           b.ResourcePath,
			resource:       res,
			fields:         b.FieldSelection,
			numericFields:  make(map[string]string),
			timestampInKey: slices.Contains(collection.Key, collection.GetProjection(res.TimestampField).Ptr),
			keyTags:        res.keyTags(collection),
		}
		for _, field := range selectedFields {
			p := collection.GetProjection(field)
			if p == nil {
				return nil, nil, nil, fmt.Errorf("selected field %q has no projection", field)
			}
			if field == reservedTimeKey && field != res.TimestampField {
				return nil, nil, nil, fmt.Errorf("measurement %q: selected field %q is reserved by InfluxDB; set 'timestampField' to %q or exclude it", res.Measurement, field, field)
			}
			if p.Ptr == "/_meta/op" {
				binding.operationField = field
			}
			// Tuple numbers reflect the JSON value, which may be integral even
			// when the projection's type is number. Keep the destination type stable.
			if slices.Contains(p.Inference.Types, "number") {
				binding.numericFields[field] = "number"
			} else if slices.Contains(p.Inference.Types, "integer") {
				binding.numericFields[field] = "integer"
			}
		}
		bindings = append(bindings, binding)
	}
	client, err := cfg.client(d.httpClient)
	if err != nil {
		return nil, nil, nil, err
	}
	return &transactor{client: client, bindings: bindings, be: be}, &pm.Response_Opened{}, nil, nil
}

// groupByFields returns the binding's group-by fields, defaulting to the
// canonical projections of the collection key.
func groupByFields(groupBy []string, collection *pf.CollectionSpec) []string {
	if len(groupBy) != 0 {
		return groupBy
	}
	var fields []string
	for _, ptr := range collection.Key {
		if field := keyField(collection, ptr); field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

// keyField returns the canonical projection of a collection key location, or "".
func keyField(collection *pf.CollectionSpec, ptr string) string {
	for _, p := range collection.Projections {
		if p.Ptr == ptr && p.IsPrimaryKey {
			return p.Field
		}
	}
	return ""
}

func resolveEndpointConfig(specJson json.RawMessage) (config, error) {
	var cfg config
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing InfluxDB endpoint config: %w", err)
	}
	return cfg, nil
}

func resolveResourceConfig(specJson json.RawMessage) (resource, error) {
	res := resource{TimestampField: defaultTimestampField}
	if err := pf.UnmarshalStrict(specJson, &res); err != nil {
		return res, fmt.Errorf("parsing InfluxDB resource config: %w", err)
	}
	return res, nil
}
