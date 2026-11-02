// materialize-null receives a materialization's Load and Store requests and
// discards them, so a benchmark run through it measures the runtime and the
// connector protocol rather than any destination.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type config struct{}

func (config) Validate() error { return nil }

type resource struct {
	Table string `json:"table"`
	Delta bool   `json:"delta_updates,omitempty"`
}

type driver struct{}

func (driver) Spec(context.Context, *pm.Request_Spec) (*pm.Response_Spec, error) {
	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(`{"type":"object"}`),
		ResourceConfigSchemaJson: json.RawMessage(`{"type":"object","properties":{"table":{"type":"string","x-collection-name":true},"delta_updates":{"type":"boolean"}},"required":["table"]}`),
		DocumentationUrl:         "https://github.com/estuary/connectors",
	}, nil
}

// Validate selects the same fields a SQL materialization recommends: keys,
// the root document and every top-level field.
func (driver) Validate(_ context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var out []*pm.Response_Validated_Binding
	for _, b := range req.Bindings {
		var res resource
		if err := json.Unmarshal(b.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var constraints []*pm.Response_Validated_ProjectionConstraint
		for _, p := range b.Collection.Projections {
			var c = &pm.Response_Validated_Constraint{Type: pm.Response_Validated_Constraint_FIELD_OPTIONAL}
			switch {
			case p.IsPrimaryKey || p.IsRootDocumentProjection():
				c.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
			case len(p.Ptr) > 1 && !containsSlash(p.Ptr[1:]):
				c.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
			}
			c.Reason = "null"
			constraints = append(constraints, &pm.Response_Validated_ProjectionConstraint{Field: p.Field, Constraint: c})
		}
		out = append(out, &pm.Response_Validated_Binding{
			ProjectionConstraints: constraints,
			DeltaUpdates:          res.Delta,
			ResourcePath:          []string{res.Table},
		})
	}
	return &pm.Response_Validated{Bindings: out}, nil
}

func containsSlash(s string) bool {
	for _, r := range s {
		if r == '/' {
			return true
		}
	}
	return false
}

func (driver) Apply(context.Context, *pm.Request_Apply) (*pm.Response_Applied, error) {
	return &pm.Response_Applied{}, nil
}

func (driver) NewTransactor(_ context.Context, open pm.Request_Open, _ *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return transactor{}, &pm.Response_Opened{}, &m.MaterializeOptions{ExtendedLogging: true}, nil
}

type transactor struct{}

func (transactor) UnmarshalState(json.RawMessage) error           { return nil }
func (transactor) Flush(context.Context, map[int]time.Time) error { return nil }
func (transactor) Destroy()                                       {}
func (transactor) Acknowledge(context.Context, []json.RawMessage, []string) (*pf.ConnectorState, error) {
	return nil, nil
}
func (transactor) RecoverCheckpoint(context.Context, pf.MaterializationSpec, pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

// Load finds nothing, as against an empty table.
func (transactor) Load(it *m.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
	}
	return it.Err()
}

func (transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	for it.Next(false) {
	}
	return nil, it.Err()
}

func main() { boilerplate.RunMain(new(driver)) }
