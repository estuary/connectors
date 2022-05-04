package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/alecthomas/jsonschema"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"google.golang.org/api/googleapi"
)

type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	var endpointSchema, err = jsonschema.Reflect(&config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := jsonschema.Reflect(&bindingResource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://go.estuary.dev/TODO",
	}, nil
}

func (d driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	cfg, err := NewConfig(req.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	specStorage, err := NewMaterializationStorage(ctx, cfg)
	if err != nil {
		return nil, err
	}

	existing, err := specStorage.LoadBindings(ctx, req.Materialization.String())
	if err != nil {
		return nil, err
	}

	out := &pm.ValidateResponse{
		Bindings: []*pm.ValidateResponse_Binding{},
	}

	for _, proposed := range req.Bindings {
		var res bindingResource
		var constraints map[string]*pm.Constraint

		if err := pf.UnmarshalStrict(proposed.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		if existingBinding, ok := existing[res.Table]; ok {
			constraints, err = ConstraintsForExistingBinding(existingBinding, &proposed.Collection, res.Delta)

			if err != nil {
				return nil, fmt.Errorf("validating binding and generating constraints: %w", err)
			}

		} else {
			constraints = ConstraintsForNewBinding(&proposed.Collection, res.Delta)
		}

		out.Bindings = append(out.Bindings, &pm.ValidateResponse_Binding{
			Constraints:  constraints,
			DeltaUpdates: res.Delta,
			ResourcePath: []string{res.Table},
		})
	}

	return out, err
}

func (driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := NewConfig(req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	specStorage, err := NewMaterializationStorage(ctx, cfg)
	if err != nil {
		return nil, err
	}

	existingBindings, err := specStorage.LoadBindings(ctx, req.Materialization.Materialization.String())
	if err != nil {
		return nil, err
	}

	bigqueryClient, err := cfg.BigQueryClient(ctx)
	if err != nil {
		return nil, err
	}

	for _, binding := range req.Materialization.Bindings {
		var br bindingResource
		var err error
		var table *Table

		if err = pf.UnmarshalStrict(binding.ResourceSpecJson, &br); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		if table, err = NewTable(binding, &br); err != nil {
			return nil, fmt.Errorf("creating the schema for table (%s): %w", br.Table, err)
		}

		if err = table.Validate(existingBindings[br.Table]); err != nil {
			return nil, err
		}

		// Everything below this line will run the Upsert, so we need to skip at this point
		// if the request is in DryRun.
		if req.DryRun {
			continue
		}

		err = bigqueryClient.Dataset(cfg.Dataset).Table(table.Name()).Create(ctx, &bigquery.TableMetadata{
			Schema: table.Schema,
		})

		// Check first if the error is because the table already exists.
		// this is the same as CREATE TABLE IF NOT EXISTS, this is a recoverable error.
		if err != nil {
			if err, ok := err.(*googleapi.Error); ok && err.Code == 409 { // 409 ALREADY EXISTS
				log.Printf("the table already exists, recoverable error.")
			} else {
				return nil, fmt.Errorf("creating bigquery table (%s): %w", br.Table, err)
			}
		}
	}

	response := &pm.ApplyResponse{}

	if req.DryRun {
		return response, err
	}

	if err = specStorage.Write(ctx, req.Materialization, req.Version); err != nil {
		return nil, err
	}

	return response, nil
}

func (driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := NewConfig(req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	specStorage, err := NewMaterializationStorage(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Everything below this condition cannot run in DryRun mode.
	if req.DryRun {
		return &pm.ApplyResponse{}, nil
	}

	specStorage.Delete(ctx, req.Materialization)

	bigqueryClient, err := cfg.BigQueryClient(ctx)
	if err != nil {
		return nil, err
	}

	for _, binding := range req.Materialization.Bindings {
		var br bindingResource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &br); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		table := bigqueryClient.Dataset(cfg.Dataset).Table(br.Table)
		err := table.Delete(ctx)
		if err != nil {
			return nil, fmt.Errorf("deleting bigquery table (%s): %w", binding.String(), err)
		}
	}

	return &pm.ApplyResponse{}, nil
}

func (d driver) Transactions(stream pm.Driver_TransactionsServer) error {
	var cfg *config
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	}

	if cfg, err = NewConfig(open.Open.Materialization.EndpointSpecJson); err != nil {
		return err
	}

	return RunTransactor(context.Background(), cfg, stream, open.Open)
}

func main() { boilerplate.RunMain(new(driver)) }
