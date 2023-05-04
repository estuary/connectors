package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-firebolt/firebolt"
	"github.com/estuary/connectors/materialize-firebolt/schemalate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// driver implements the DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Firebolt Connection", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Firebolt Table", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-firebolt",
	}, nil
}

func ValidateBindings(cfg config, materialization string, bindings []*pm.Request_Validate_Binding) ([]map[string]*pm.Response_Validated_Constraint, error) {
	existing, err := LoadSpec(cfg, materialization)
	if err != nil {
		return nil, fmt.Errorf("loading materialization spec: %w", err)
	}

	var out []map[string]*pm.Response_Validated_Constraint
	for _, proposed := range bindings {
		var res resource
		if err := pf.UnmarshalStrict(proposed.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		// Make sure the specified resource is valid to build
		var constraints map[string]*pm.Response_Validated_Constraint

		if existingBinding, ok := existing[res.Table]; ok {
			constraints, err = schemalate.ValidateExistingProjection(existingBinding, proposed)
		} else {
			// A new binding that didn't exist in previous materialization spec
			constraints, err = schemalate.ValidateNewProjection(proposed)
		}

		if err != nil {
			return nil, fmt.Errorf("validating binding and generating constraints: %w", err)
		} else {
			out = append(out, constraints)
		}
	}

	return out, nil
}

// Validate retrieves existing materialization spec and validates the new bindings either against
// the old materialization spec, or validate it as new
func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var constraints, err = ValidateBindings(cfg, req.Name.String(), req.Bindings)
	if err != nil {
		return nil, err
	}

	var out []*pm.Response_Validated_Binding
	for i, proposed := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(proposed.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints[i],
			DeltaUpdates: true,
			ResourcePath: []string{res.Table},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

// Apply creates main and external table and persist materialization spec on S3
func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var fb, err = firebolt.New(firebolt.Config{
		EngineURL: cfg.EngineURL,
		Database:  cfg.Database,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})

	if err != nil {
		return nil, fmt.Errorf("creating firebolt client: %w", err)
	}

	var tables []string

	// Validate the new MaterializationSpec against its own constraints as a sanity check
	// Map the materialization bindings to ValidateRequest_Binding to generate constraints again
	var mappedBindings = []*pm.Request_Validate_Binding{}
	for _, proposed := range req.Materialization.Bindings {
		mappedBinding := pm.Request_Validate_Binding{
			ResourceConfigJson: proposed.ResourceConfigJson,
			Collection:         proposed.Collection,
		}
		mappedBindings = append(mappedBindings, &mappedBinding)
	}

	constraints, err := ValidateBindings(cfg, req.Materialization.Name.String(), mappedBindings)
	if err != nil {
		return nil, err
	}

	// Send the materialization bindings along with the constraints for validation
	for i, proposed := range req.Materialization.Bindings {
		err := schemalate.ValidateBindingAgainstConstraints(proposed, constraints[i])
		if err != nil {
			return nil, fmt.Errorf("validating binding %v against constraints %v: %w", proposed, constraints, err)
		}
	}

	queries, err := schemalate.GetQueriesBundle(req.Materialization)
	if err != nil {
		return nil, fmt.Errorf("building queries bundle: %w", err)
	}

	for i, bundle := range queries.Bindings {
		if !req.DryRun {
			_, err := fb.Query(bundle.CreateExternalTable)
			if err != nil {
				return nil, fmt.Errorf("running external table creation query: %w", err)
			}

			_, err = fb.Query(bundle.CreateTable)
			if err != nil {
				return nil, fmt.Errorf("running table creation query: %w", err)
			}
		}

		tables = append(tables, string(req.Materialization.Bindings[i].ResourceConfigJson))
	}

	if req.DryRun {
		return &pm.Response_Applied{ActionDescription: fmt.Sprint("to create tables: ", strings.Join(tables, ","))}, nil
	}

	err = WriteSpec(cfg, req.Materialization, req.Version)
	if err != nil {
		return nil, fmt.Errorf("writing materialization spec to s3: %w", err)
	}
	return &pm.Response_Applied{ActionDescription: fmt.Sprint("created tables: ", strings.Join(tables, ","))}, nil
}

func SliceContains(expected string, actual []string) bool {
	for _, ty := range actual {
		if ty == expected {
			return true
		}
	}
	return false
}

func main() { boilerplate.RunMain(new(driver)) }
