package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type driver struct{}

var _ boilerplate.Connector = &driver{}

// NewDriver builds the connector, and is its entry point: an importing
// caller is handed the assembled connector rather than its pieces.
func NewDriver() boilerplate.Connector { return driver{} }

type customHeader struct {
	Name  string `json:"name,omitempty" jsonschema:"title=Name"`
	Value string `json:"value,omitempty" jsonschema:"title=Value"`
}

func (h customHeader) Validate() error {
	if h.Name == "" {
		return fmt.Errorf("header name must not be empty")
	} else if h.Value == "" {
		return fmt.Errorf("value for header %v must not be empty", h.Name)
	}
	return nil
}

type headers struct {
	CustomHeaders []customHeader `json:"customHeaders,omitempty" jsonschema:"title=Custom Headers"`
}

var featureFlagDefaults = map[string]common.FlagDefault{
	// Sends an object of each document's selected fields instead of the full
	// document, and makes every field selectable.
	"field_selection": common.FlagEnabledForTasksCreatedAfter("2026-10-30"),
}

type config struct {
	Address  pf.Endpoint    `json:"address" jsonschema:"title=Address,description=Base address URL. Must end in a trailing '/'."`
	Headers  headers        `json:"headers,omitempty"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	if err := c.Address.Validate(); err != nil {
		return fmt.Errorf("address: %w", err)
	} else if !strings.HasSuffix(string(c.Address), "/") {
		return fmt.Errorf("address must end in a trailing '/'")
	}

	for i := range c.Headers.CustomHeaders {
		var header = c.Headers.CustomHeaders[i]
		if err := header.Validate(); err != nil {
			return fmt.Errorf("header: %w", err)
		}
	}
	return nil
}

func (c config) DefaultNamespace() string { return "" }

func (c config) FeatureFlags() (string, map[string]common.FlagDefault) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

type resource struct {
	RelativePath string `json:"relativePath" jsonschema:"title=Relative Path,description=Path which is joined with the base Address to build a complete URL" jsonschema_extras:"x-collection-name=true"`

	// address is the endpoint's base address, set by WithDefaults so that
	// Parameters can resolve the binding's full URL.
	address pf.Endpoint
}

func (r resource) Validate() error {
	if _, err := url.Parse(r.RelativePath); err != nil {
		return fmt.Errorf("relativePath: %w", err)
	}
	return nil
}

func (r resource) URL() *url.URL {
	var u, err = url.Parse(r.RelativePath)
	if err != nil {
		panic(err)
	}
	return u
}

func (r resource) WithDefaults(cfg config) resource {
	r.address = cfg.Address
	return r
}

// Parameters returns the binding's resolved webhook URL as its resource path.
// Webhooks only support delta updates.
func (r resource) Parameters() ([]string, bool, error) {
	return []string{r.address.URL().ResolveReference(r.URL()).String()}, true, nil
}

// fieldConfig and mappedType are required by the Materializer but carry
// nothing, since webhooks have no destination schema.
type fieldConfig struct{}

func (fieldConfig) Validate() error    { return nil }
func (fieldConfig) CastToString() bool { return false }

type mappedType struct{}

func (mappedType) String() string                            { return "json" }
func (mappedType) Compatible(boilerplate.ExistingField) bool { return true }
func (mappedType) CanMigrate(boilerplate.ExistingField) bool { return false }

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Webhook", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Webhook URL", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-webhook",
	}, nil
}

// Validate validates the Webhook configuration. Without the field_selection
// flag, it constrains projections to the document root (only).
func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	var flags, err = boilerplate.ResolveFlags(cfg, req.LastMaterialization)
	if err != nil {
		return nil, err
	}

	var out []*pm.Response_Validated_Binding
	for _, binding := range req.Bindings {

		// Verify that the resource parses, and joins into an absolute URL.
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var resolved = cfg.Address.URL().ResolveReference(res.URL())
		if !resolved.IsAbs() {
			return nil, fmt.Errorf("resolved webhook address %s is not absolute", resolved)
		}

		var constraints []*pm.Response_Validated_ProjectionConstraint
		for _, projection := range binding.Collection.Projections {
			var constraint = projectionConstraint(projection, flags["field_selection"])
			constraints = append(constraints, &pm.Response_Validated_ProjectionConstraint{
				Field:      projection.Field,
				Constraint: &constraint,
			})
		}

		out = append(out, &pm.Response_Validated_Binding{
			CaseInsensitiveFields: false,
			ProjectionConstraints: constraints,
			// Only delta updates are supported by webhooks.
			DeltaUpdates: true,
			ResourcePath: []string{resolved.String()},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func projectionConstraint(projection pf.Projection, fieldSelection bool) pm.Response_Validated_Constraint {
	var constraint pm.Response_Validated_Constraint
	switch {
	case fieldSelection && projection.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "The root document may be included as a field"
	case fieldSelection && projection.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "Document keys should usually be included"
	case fieldSelection:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "This field may be included"
	case projection.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case projection.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Document keys must be included"
	default:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Webhooks only materialize the full document"
	}
	return constraint
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, NewMaterializer)
}

func (driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, NewMaterializer, boilerplate.WithConfigUpdates([]string{"advanced", "feature_flags"}))
}

type materialization struct {
	cfg          config
	featureFlags map[string]bool
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &materialization{}

func NewMaterializer(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	return &materialization{cfg: cfg, featureFlags: featureFlags}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		NoCreateNamespaces:  true,
		NoTruncateResources: true,
	}
}

// PopulateInfoSchema adds nothing: a webhook has no destination state to
// introspect, so every binding is applied as a new resource, which is a no-op.
func (d *materialization) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	return nil
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	return &cerrors.PrereqErr{}
}

func (d *materialization) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
	return projectionConstraint(p, d.featureFlags["field_selection"])
}

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (mappedType, boilerplate.ElementConverter) {
	return mappedType{}, nil
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	return "", nil
}

func (d *materialization) CreateResource(ctx context.Context, b boilerplate.MappedBinding[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}

func (d *materialization) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	// Test cleanup invokes the returned fn without nil-checking it.
	return "", func(context.Context) error { return nil }, nil
}

func (d *materialization) UpdateResource(
	ctx context.Context,
	path []string,
	existing boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[config, resource, mappedType],
) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}

func (d *materialization) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}

func (d *materialization) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (d *materialization) NewTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, mappedType],
	be *m.BindingEvents,
) (m.Transactor, error) {
	var bindings []binding
	for _, b := range mappedBindings {
		var bound = binding{
			// Join paths of each binding with the base URL.
			address: d.cfg.Address.URL().ResolveReference(b.Config.URL()),
			path:    b.ResourcePath,
		}
		if d.featureFlags["field_selection"] {
			bound.sendFields = true
			var fs = b.FieldSelection
			for _, field := range slices.Concat(fs.Keys, fs.Values) {
				bound.fieldNames = append(bound.fieldNames, jsonFieldName(field))
			}
			if fs.Document != "" {
				bound.documentName = jsonFieldName(fs.Document)
			}
		}
		bindings = append(bindings, bound)
	}

	return &transactor{
		bindings:      bindings,
		customHeaders: d.cfg.Headers.CustomHeaders,
		be:            be,
	}, nil
}

func (d *materialization) ListTestTasks(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (d *materialization) CleanupTestTask(ctx context.Context, name string) error {
	return nil
}

// SnapshotTestResource is only used by integration tests, which wrap the
// materialization with one that reads back the requests a test server received.
func (d *materialization) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	return nil, nil, fmt.Errorf("webhooks can't be read back")
}

func (d *materialization) Close(ctx context.Context) {}

// jsonFieldName returns the field name as a JSON object key, with its colon.
func jsonFieldName(field string) []byte {
	var name, err = json.Marshal(field)
	if err != nil {
		panic(err) // Marshalling a string can't fail.
	}
	return append(name, ':')
}

type binding struct {
	address    *url.URL
	path       []string
	sendFields bool
	// fieldNames are the encoded names of the selected keys and values, and
	// documentName that of the selected document, if any.
	fieldNames   [][]byte
	documentName []byte
}

// appendFields appends a JSON object of the stored document's selected
// fields to buf.
func (b *binding) appendFields(buf *bytes.Buffer, it *m.StoreIterator) error {
	buf.WriteByte('{')
	for i, v := range slices.Concat(it.Key, it.Values) {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.Write(b.fieldNames[i])
		// Objects, arrays, and fields of several types are packed as JSON.
		if raw, ok := v.([]byte); ok {
			buf.Write(raw)
		} else if encoded, err := json.Marshal(v); err != nil {
			return fmt.Errorf("encoding value %v: %w", v, err)
		} else {
			buf.Write(encoded)
		}
	}
	if b.documentName != nil {
		if len(b.fieldNames) != 0 {
			buf.WriteByte(',')
		}
		buf.Write(b.documentName)
		buf.Write(it.RawJSON)
	}
	buf.WriteByte('}')
	return nil
}

type transactor struct {
	bindings      []binding
	customHeaders []customHeader
	be            *m.BindingEvents
}

func (t *transactor) UnmarshalState(state json.RawMessage) error { return nil }
func (t *transactor) Flush(context.Context, map[int]time.Time) error {
	return nil
}

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}

// Load should not be called and panics.
func (d *transactor) Load(it *m.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("Load should never be called for webhook.Driver")
	}
	return nil
}

func (d *transactor) sendWebhook(ctx context.Context, address string, body bytes.Buffer) error {
	for attempt := 0; true; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff(attempt)):
			// Fallthrough
		}

		var request, err = http.NewRequest("POST", address, bytes.NewReader((body.Bytes())))
		if err != nil {
			return fmt.Errorf("http.NewRequest(%s): %w", address, err)
		}

		request.Header.Add("Content-Type", "application/json")

		for i := range d.customHeaders {
			var header = d.customHeaders[i]
			request.Header.Add(header.Name, header.Value)
		}

		response, err := http.DefaultClient.Do(request)
		if err == nil {
			err = response.Body.Close()
		}
		if err == nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
			err = fmt.Errorf("unexpected webhook response code %d from %s", response.StatusCode, address)
		}

		if err == nil {
			break
		} else if attempt == 10 {
			return fmt.Errorf("webhook failed after many attempts: %w", err)
		}
	}

	return nil
}

// Store streams StoreIterator documents directly into the webhook body.
func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	const requestSizeCutoff = 1024 * 1024 * 1 // 1 MiB
	var body bytes.Buffer
	var ctx = it.Context()

	// sent counts the documents of each binding whose request succeeded, for
	// the transaction health report; inBody is those in the pending request.
	var sent = make([]int64, len(d.bindings))
	var inBody int64

	var previousBinding = -1
	for it.Next(false) {
		if previousBinding == -1 {
			previousBinding = it.Binding
		}

		// Send a webhook if the binding switches or the body is over the cutoff
		if it.Binding != previousBinding || body.Len() >= requestSizeCutoff {
			body.WriteString("]")
			if err := d.sendWebhook(ctx, d.bindings[previousBinding].address.String(), body); err != nil {
				return nil, fmt.Errorf("draining previous binding: %w", err)
			}
			sent[previousBinding] += inBody
			inBody = 0

			body.Reset()
			previousBinding = it.Binding
		}

		if body.Len() == 0 {
			body.WriteString("[")
		} else {
			body.WriteString(",")
		}

		if b := &d.bindings[it.Binding]; b.sendFields {
			if err := b.appendFields(&body, it); err != nil {
				return nil, err
			}
		} else if _, err := body.Write(it.RawJSON); err != nil {
			return nil, err
		}
		inBody++
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	if body.Len() > 0 {
		body.WriteString("]")
		if err := d.sendWebhook(ctx, d.bindings[previousBinding].address.String(), body); err != nil {
			return nil, fmt.Errorf("draining previous binding: %w", err)
		}
		sent[previousBinding] += inBody
		body.Reset()
	}

	for i, n := range sent {
		if n > 0 {
			d.be.ReportRowStats(it.Round, d.bindings[i].path, m.TotalRowStats(n))
		}
	}
	return nil, nil
}

// Destroy is a no-op.
func (d *transactor) Destroy() {}

func (d *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

func backoff(attempt int) time.Duration {
	switch attempt {
	case 0:
		return 0
	case 1:
		return time.Millisecond * 100
	case 2, 3, 4, 5, 6, 7, 8, 9, 10:
		return time.Second * time.Duration(attempt-1)
	default:
		return 10 * time.Second
	}
}
