package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	m "github.com/estuary/connectors/go/protocols/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"golang.org/x/sync/errgroup"
)

// driver implements the pm.DriverServer interface.
type driver struct{}

type StreamEncoder interface {
	Encode(row []any) error
	Written() int
	Close() error
}

type CustomHeader struct {
	Name  string `json:"name,omitempty" jsonschema:"title=Name"`
	Value string `json:"value,omitempty" jsonschema:"title=Value"`
}

func (h CustomHeader) Validate() error {
	if h.Name == "" {
		return fmt.Errorf("header name must not be empty")
	} else if h.Value == "" {
		return fmt.Errorf("value for header %v must not be empty", h.Name)
	}
	return nil
}

type headers struct {
	CustomHeaders []CustomHeader `json:"customHeaders,omitempty" jsonschema:"title=Custom Headers"`
}

type config struct {
	Address pf.Endpoint `json:"address" jsonschema:"title=Address,description=Base address URL. Must end in a trailing '/'."`
	Headers headers     `json:"headers,omitempty"`
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	if err := c.Address.Validate(); err != nil {
		return fmt.Errorf("address: %w", err)
	} else if !strings.HasSuffix(string(c.Address), "/") {
		return fmt.Errorf("address must end in a trailing '/'")
	}

	for i := range c.Headers.CustomHeaders {
		header := c.Headers.CustomHeaders[i]
		if err := header.Validate(); err != nil {
			return fmt.Errorf("header: %w", err)
		}
	}
	return nil
}

type resource struct {
	RelativePath string `json:"relativePath,omitempty" jsonschema:"title=Relative Path,description=Path which is joined with the base Address to build a complete URL" jsonschema_extras:"x-collection-name=true"`
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

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("Webhook", &config{}).MarshalJSON()
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

// Validate validates the Webhook configuration and constrains projections
// to the document root (only).
func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
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

		var constraints = make(map[string]*pm.Response_Validated_Constraint)
		for _, projection := range binding.Collection.Projections {
			var constraint = new(pm.Response_Validated_Constraint)
			switch {
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
			constraints[projection.Field] = constraint
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints: constraints,
			// Only delta updates are supported by webhooks.
			DeltaUpdates: true,
			ResourcePath: []string{resolved.String()},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

// Apply is a no-op.
func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return &pm.Response_Applied{}, nil
}

func (driver) NewTransactor(ctx context.Context, open pm.Request_Open, _ *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	var cfg config
	if err := pf.UnmarshalStrict(open.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var addresses []*url.URL

	for _, binding := range open.Materialization.Bindings {
		// Join paths of each binding with the base URL.
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, nil, nil, fmt.Errorf("parsing resource config: %w", err)
		}
		addresses = append(addresses, cfg.Address.URL().ResolveReference(res.URL()))
	}

	var transactor = &transactor{
		addresses:     addresses,
		customHeaders: cfg.Headers.CustomHeaders,
	}
	return transactor, &pm.Response_Opened{}, nil, nil
}

type transactor struct {
	addresses     []*url.URL
	customHeaders []CustomHeader
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

// Load should not be called and panics.
func (d *transactor) Load(it *m.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("Load should never be called for webhook.Driver")
	}
	return nil
}

// Store streams StoreIterator documents directly into the webhook body.
func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var pipeWriter *io.PipeWriter
	group := errgroup.Group{}

	startWebhook := func(address string) {
		pipeReader, w := io.Pipe()

		group.Go(func() error {
			request, err := http.NewRequest("POST", address, pipeReader)
			if err != nil {
				return fmt.Errorf("http.NewRequest(%s): %w", address, err)
			}

			request.Header.Add("Content-Type", "application/json")

			for i := range d.customHeaders {
				header := d.customHeaders[i]
				request.Header.Add(header.Name, header.Value)
			}

			response, err := http.DefaultClient.Do(request)

			if err == nil {
				err = response.Body.Close()
			}

			if err == nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
				return fmt.Errorf("unexpected webhook response code %d from %s",
					response.StatusCode, address)
			}

			return nil
		})

		pipeWriter = w
	}

	finishWebhook := func() error {
		if pipeWriter == nil {
			return nil
		}

		pipeWriter.Write([]byte("]"))
		pipeWriter.Close()

		if err := group.Wait(); err != nil {
			return fmt.Errorf("group.Wait(): %w", err)
		}

		pipeWriter = nil
		return nil
	}

	const requestSizeCutoff = 1024 * 1024 // 100 MiB
	byteCount := 0
	var previousAddress string

	for it.Next() {
		address := d.addresses[it.Binding].String()

		// The webhook for the previous binding must finish before the next binding's webhook starts.
		if previousAddress != "" && address != previousAddress {
			if err := finishWebhook(); err != nil {
				return nil, fmt.Errorf("finishWebhooks: %w", err)
			}
		}

		previousAddress = address

		if pipeWriter == nil {
			startWebhook(address)
			pipeWriter.Write([]byte("["))
		} else {
			pipeWriter.Write([]byte(","))
		}

		pipeWriter.Write(it.RawJSON)
		byteCount += len(it.RawJSON)

		if byteCount >= requestSizeCutoff {
			if err := finishWebhook(); err != nil {
				return nil, fmt.Errorf("finishWebhook: %w", err)
			}

			byteCount = 0
		}
	}

	if err := finishWebhook(); err != nil {
		return nil, fmt.Errorf("finishWebhook: %w", err)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("store iterator error: %w", err)
	}

	return nil, nil
}

// Destroy is a no-op.
func (d *transactor) Destroy() {}

func main() { boilerplate.RunMain(new(driver)) }
