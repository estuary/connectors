package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/alecthomas/jsonschema"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-firebolt/firebolt"
	"github.com/estuary/connectors/materialize-firebolt/schemalate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// driver implements the DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	endpointSchema, err := jsonschema.Reflect(&config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := jsonschema.Reflect(&resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://docs.estuary.dev/reference/Connectors/materialization-connectors/Firebolt/",
	}, nil
}

// Retrieve existing materialization spec and validate the new bindings either against
// the old materialization spec, or validate it as new
func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	haveExisting, existing, err := LoadSpec(cfg, req.Materialization.String())
	if err != nil {
		return nil, fmt.Errorf("loading materialization spec: %w", err)
	}

	var out []*pm.ValidateResponse_Binding
	for _, proposed := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(proposed.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		// Make sure the specified resource is valid to build
		var constraints map[string]*pm.Constraint

		if haveExisting {
			if existingBinding, ok := existing[res.Table]; ok {
				constraints, err = schemalate.ValidateExistingProjection(existingBinding, proposed)
			} else {
				// A new binding that didn't exist in previous materialization spec
				constraints, err = schemalate.ValidateNewProjection(proposed)
			}
		} else {
			constraints, err = schemalate.ValidateNewProjection(proposed)
		}

		if err != nil {
			return nil, fmt.Errorf("validating binding and generating constraints: %w", err)
		} else {
			out = append(out, &pm.ValidateResponse_Binding{
				Constraints:  constraints,
				DeltaUpdates: true,
				ResourcePath: []string{res.Table},
			})
		}
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

// Create main and external table and persist materialization spec on S3
func (d driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	fb, err := firebolt.New(firebolt.Config{
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
	haveExisting, existing, err := LoadSpec(cfg, req.Materialization.Materialization.String())
	if err != nil {
		return nil, fmt.Errorf("loading materialization spec: %w", err)
	}

	for _, proposed := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(proposed.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		// Make sure the specified resource is valid to build
		var constraints map[string]*pm.Constraint
		mappedBinding := pm.ValidateRequest_Binding{
			ResourceSpecJson: proposed.ResourceSpecJson,
			Collection:       proposed.Collection,
		}

		if haveExisting {
			if existingBinding, ok := existing[res.Table]; ok {
				constraints, err = schemalate.ValidateExistingProjection(existingBinding, &mappedBinding)
			} else {
				// A new binding that didn't exist in previous materialization spec
				constraints, err = schemalate.ValidateNewProjection(&mappedBinding)
			}
		} else {
			constraints, err = schemalate.ValidateNewProjection(&mappedBinding)
		}

		if err != nil {
			return nil, fmt.Errorf("validating binding and generating constraints: %w", err)
		}
		err := schemalate.ValidateBindingAgainstConstraints(proposed, constraints)
		if err != nil {
			return nil, fmt.Errorf("validating binding %v against constraints %v: %w", proposed, constraints, err)
		}
	}

	// TODO: send materialization spec to GetQueriesBundle
	// so we can check whether creating a table is necessary or not
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

		tables = append(tables, string(req.Materialization.Bindings[i].ResourceSpecJson))
	}

	if req.DryRun {
		return &pm.ApplyResponse{ActionDescription: fmt.Sprint("to create tables: ", strings.Join(tables, ","))}, nil
	}

	err = WriteSpec(cfg, req.Materialization)
	if err != nil {
		return nil, fmt.Errorf("writing materialization spec to s3: %w", err)
	}
	return &pm.ApplyResponse{ActionDescription: fmt.Sprint("created tables: ", strings.Join(tables, ","))}, nil
}

// Delete main and external tables
func (driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	fb, err := firebolt.New(firebolt.Config{
		EngineURL: cfg.EngineURL,
		Database:  cfg.Database,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})

	if err != nil {
		return nil, fmt.Errorf("creating firebolt client: %w", err)
	}

	var tables []string
	queries, err := schemalate.GetQueriesBundle(req.Materialization)
	if err != nil {
		return nil, fmt.Errorf("building firebolt search schema: %w", err)
	}

	for i, bundle := range queries.Bindings {
		if !req.DryRun {
			_, err := fb.Query(bundle.DropTable)
			if err != nil {
				return nil, fmt.Errorf("running table drop query: %w", err)
			}

			_, err = fb.Query(bundle.DropExternalTable)
			if err != nil {
				return nil, fmt.Errorf("running external table drop query: %w", err)
			}
		}

		tables = append(tables, string(req.Materialization.Bindings[i].ResourceSpecJson))
	}

	if req.DryRun {
		return &pm.ApplyResponse{ActionDescription: fmt.Sprint("to delete tables: ", strings.Join(tables, ","))}, nil
	}

	err = CleanSpec(cfg, req.Materialization.Materialization.String())
	if err != nil {
		return nil, fmt.Errorf("cleaning up materialization spec: %w", err)
	}
	return &pm.ApplyResponse{ActionDescription: fmt.Sprint("deleted tables: ", strings.Join(tables, ","))}, nil
}

func main() { boilerplate.RunMain(new(driver)) }
