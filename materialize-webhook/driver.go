package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// driver implements the pm.DriverServer interface.
type driver struct{}

type config struct {
	Address pf.Endpoint `json:"address" jsonschema:"title=Address,description=Base address URL. Must end in a trailing '/'."`
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	if err := c.Address.Validate(); err != nil {
		return fmt.Errorf("address: %w", err)
	} else if !strings.HasSuffix(string(c.Address), "/") {
		return fmt.Errorf("address must end in a trailing '/'")
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

func (driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var cfg config
	if err := pf.UnmarshalStrict(open.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var addresses []*url.URL

	for _, binding := range open.Materialization.Bindings {
		// Join paths of each binding with the base URL.
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, nil, fmt.Errorf("parsing resource config: %w", err)
		}
		addresses = append(addresses, cfg.Address.URL().ResolveReference(res.URL()))
	}

	var transactor = &transactor{
		addresses: addresses,
	}
	return transactor, &pm.Response_Opened{}, nil
}

type transactor struct {
	addresses []*url.URL
}

// Load should not be called and panics.
func (d *transactor) Load(it *pm.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("Load should never be called for webhook.Driver")
	}
	return nil
}

// Store invokes the Webhook URL, with a body containing StoreIterator documents.
func (d *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	var bodies = make([]bytes.Buffer, len(d.addresses))
	var ctx = it.Context()

	// TODO(johnny): perform incremental POSTs rather than queuing a single call.
	for it.Next() {
		var b = &bodies[it.Binding]

		if b.Len() != 0 {
			b.WriteString(",\n")
		} else {
			b.WriteString("[\n")
		}
		if _, err := b.Write(it.RawJSON); err != nil {
			return nil, err
		}
	}

	for i := range bodies {
		bodies[i].WriteString("\n]")
	}

	for i, address := range d.addresses {
		var address = address.String()
		var body = &bodies[i]

		for attempt := 0; true; attempt++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff(attempt)):
				// Fallthrough.
			}

			request, err := http.NewRequest("POST", address, bytes.NewReader(body.Bytes()))
			if err != nil {
				return nil, fmt.Errorf("http.NewRequest(%s): %w", address, err)
			}
			request.Header.Add("Content-Type", "application/json")

			response, err := http.DefaultClient.Do(request)
			if err == nil {
				err = response.Body.Close()
			}
			if err == nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
				err = fmt.Errorf("unexpected webhook response code %d from %s",
					response.StatusCode, address)
			}

			if err == nil {
				body.Reset() // Reset for next use.
				break
			} else if attempt == 10 {
				return nil, fmt.Errorf("webhook failed after many attempts: %w", err)
			}

			log.WithFields(log.Fields{
				"err":     err,
				"attempt": attempt,
				"address": address,
			}).Error("failed to invoke Webhook (will retry)")
		}
	}
	return nil, nil
}

// Destroy is a no-op.
func (d *transactor) Destroy() {}

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

func main() { boilerplate.RunMain(new(driver)) }
