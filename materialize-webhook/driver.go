package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

// driver implements the pm.DriverServer interface.
type driver struct{}

type config struct {
	Address pf.Endpoint `json:"address"`
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	return c.Address.Validate()
}

type resource struct {
	// Path which is joined with the base Address to build a complete URL.
	RelativePath string `json:"relativePath,omitempty"`
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

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	endpointSchema, err := jsonschema.Reflect(new(config)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := jsonschema.Reflect(new(resource)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://docs.estuary.dev",
	}, nil
}

// Validate validates the Webhook configuration and constrains projections
// to the document root (only).
func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var out []*pm.ValidateResponse_Binding
	for _, binding := range req.Bindings {

		// Verify that the resource parses, and joins into an absolute URL.
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var resolved = cfg.Address.URL().ResolveReference(res.URL())
		if !resolved.IsAbs() {
			return nil, fmt.Errorf("resolved webhook address %s is not absolute", resolved)
		}

		var constraints = make(map[string]*pm.Constraint)
		for _, projection := range binding.Collection.Projections {
			var constraint = new(pm.Constraint)
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document must be materialized"
			default:
				constraint.Type = pm.Constraint_FIELD_FORBIDDEN
				constraint.Reason = "Webhooks only materialize the full document"
			}
			constraints[projection.Field] = constraint
		}

		out = append(out, &pm.ValidateResponse_Binding{
			Constraints: constraints,
			// Only delta updates are supported by webhooks.
			DeltaUpdates: true,
			ResourcePath: []string{resolved.String()},
		})
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

// ApplyUpsert is a no-op.
func (driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return &pm.ApplyResponse{}, nil
}

// ApplyDelete is a no-op.
func (driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return &pm.ApplyResponse{}, nil
}

// Transactions implements the DriverServer interface.
func (driver) Transactions(stream pm.Driver_TransactionsServer) error {
	return pm.RunTransactions(stream, func(ctx context.Context, open pm.TransactionRequest_Open) (pm.Transactor, *pm.TransactionResponse_Opened, error) {
		var cfg config
		if err := pf.UnmarshalStrict(open.Materialization.EndpointSpecJson, &cfg); err != nil {
			return nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
		}

		var addresses []*url.URL

		for _, binding := range open.Materialization.Bindings {
			// Join paths of each binding with the base URL.
			var res resource
			if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
				return nil, nil, fmt.Errorf("parsing resource config: %w", err)
			}
			addresses = append(addresses, cfg.Address.URL().ResolveReference(res.URL()))
		}

		var transactor = &transactor{
			addresses: addresses,
		}
		return transactor, &pm.TransactionResponse_Opened{}, nil
	})
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
