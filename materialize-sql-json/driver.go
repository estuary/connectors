package sql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	"github.com/estuary/connectors/go/boilerplate"
	"github.com/estuary/connectors/go/protocol"
)

// Transactor implements the Load and Store methods of the boilerplate.Driver interface.
type Transactor interface {
	Load(context.Context, <-chan protocol.LoadRequest, func(protocol.LoadResponse)) error
	Store(context.Context, <-chan protocol.StoreRequest) (boilerplate.StartCommitFn, error)
}

type Driver struct {
	// URL at which documentation for the driver may be found.
	DocumentationURL string
	// Instance of the type into which endpoint specifications are parsed.
	EndpointSpecType interface{}
	// Instance of the type into which resource specifications are parsed.
	ResourceSpecType Resource
	// NewEndpoint returns an *Endpoint which will be used to handle interactions with the database.
	NewEndpoint func(_ context.Context, endpointConfig json.RawMessage) (*Endpoint, error)
	// Transactor to delegate Load and Store to.
	transactor Transactor
}

// StoredSpec is a record of the materialization bindings that will be stored in the sql
// endpoint of the materialization. It is used to validate proposed changes to the bindings of the
// materialization.
// type StoredSpec struct {
// 	Materialization string                  `json:"materialization"`
// 	Bindings        []protocol.ApplyBinding `json:"bindings"`
// }

var _ boilerplate.Driver = &Driver{}

func (d *Driver) Spec(ctx context.Context, req protocol.SpecRequest) (*protocol.SpecResponse, error) {
	var endpoint, resource []byte
	var err error

	if endpoint, err = schemagen.GenerateSchema("SQL Connection", d.EndpointSpecType).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	} else if resource, err = schemagen.GenerateSchema("SQL Table", d.ResourceSpecType).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &protocol.SpecResponse{
		DocumentationUrl:     d.DocumentationURL,
		ConfigSchema:         json.RawMessage(endpoint),
		ResourceConfigSchema: json.RawMessage(resource),
	}, nil
}

func (d *Driver) Validate(ctx context.Context, req protocol.ValidateRequest) (*protocol.ValidateResponse, error) {
	var (
		err            error
		endpoint       *Endpoint
		loadedBindings []protocol.ApplyBinding
		resp           = new(protocol.ValidateResponse)
	)

	if endpoint, err = d.NewEndpoint(ctx, req.Config); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if loadedBindings, _, err = loadBindings(ctx, endpoint, req.Name); err != nil {
		return nil, fmt.Errorf("loading current applied materialization spec: %w", err)
	}

	// Produce constraints for each request binding, in turn.
	for _, bindingSpec := range req.Bindings {
		var constraints, _, resource, err = resolveResourceToExistingBinding(
			endpoint,
			bindingSpec.ResourceConfig,
			bindingSpec.Collection,
			loadedBindings,
		)
		if err != nil {
			return nil, err
		}

		resp.Bindings = append(resp.Bindings,
			protocol.ValidatedBinding{
				ResourcePath: resource.Path(),
				Constraints:  constraints,
				DeltaUpdates: resource.DeltaUpdates(),
			},
		)
	}
	return resp, nil
}

func (d *Driver) Apply(ctx context.Context, req protocol.ApplyRequest) (*protocol.ApplyResponse, error) {
	// TODO(whb): For now we are only handling the case of deleting the entire materialization,
	// which will be received as an ApplyRequest with no bindings. Future work could enable handling
	// of deletion for individual bindings that are removed from a spec while the entire spec is not
	// being deleted.
	if len(req.Bindings) == 0 {
		return d.applyDelete(ctx, req)
	}

	var (
		endpoint         *Endpoint
		loadedBindings   []protocol.ApplyBinding
		loadedVersion    string
		reqBindingsBytes []byte
		err              error
	)

	if endpoint, err = d.NewEndpoint(ctx, req.Config); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if loadedBindings, loadedVersion, err = loadBindings(ctx, endpoint, req.Name); err != nil {
		return nil, fmt.Errorf("loading prior applied materialization spec: %w", err)
	} else if reqBindingsBytes, err = json.Marshal(req.Bindings); err != nil {
		panic(err) // Cannot fail to marshal.
	}

	if loadedVersion == req.Version {
		// A re-application of the current version is a no-op.
		// TODO: Note that in the response action description?
		return &protocol.ApplyResponse{}, nil
	}

	// The Materialization specifications meta table is always required.
	// The Checkpoints table is optional; include it if set by the Endpoint.
	var tableShapes = []TableShape{endpoint.MetaSpecs}
	if endpoint.MetaCheckpoints != nil {
		tableShapes = append(tableShapes, *endpoint.MetaCheckpoints)
	}

	var statements []string

	for bindingIndex, bindingSpec := range req.Bindings {
		var collection = bindingSpec.Collection.Name
		var constraints, loadedBinding, resource, err = resolveResourceToExistingBinding(
			endpoint,
			bindingSpec.ResourceConfig,
			bindingSpec.Collection,
			loadedBindings,
		)

		var tableShape = BuildTableShape(req.Name, req.Bindings, bindingIndex, resource)

		if err != nil {
			return nil, err
		} else if err = ValidateSelectedFields(constraints, bindingSpec); err != nil {
			// The applied binding is not a valid solution for its own constraints.
			return nil, fmt.Errorf("binding for %s: %w", collection, err)
		} else if loadedBinding != nil && !bindingSpec.FieldSelection.Equal(&loadedBinding.FieldSelection) {
			// We don't handle any form of schema migrations, so we require that the list of
			// fields in the request is identical to the current fields.
			return nil, fmt.Errorf(
				"the set of binding %s fields in the request differs from the existing fields,"+
					"which is disallowed because this driver does not perform schema migrations. "+
					"Request fields: %s , Existing fields: %s",
				collection,
				bindingSpec.FieldSelection.AllFields(),
				loadedBinding.FieldSelection.AllFields(),
			)
		} else if loadedBinding != nil {
			// If the table has already been created, make sure all non-null fields
			// are marked as such
			for _, field := range bindingSpec.FieldSelection.Values {
				var p = bindingSpec.Collection.GetProjection(field)
				var previousProjection = loadedBinding.Collection.GetProjection(field)

				var previousNullable = previousProjection.Inference.Exists != protocol.MustExist || SliceContains("null", p.Inference.Types)
				var newNullable = p.Inference.Exists != protocol.MustExist || SliceContains("null", p.Inference.Types)

				if !previousNullable && newNullable {
					var table, err = ResolveTable(tableShape, endpoint.Dialect)
					if err != nil {
						return nil, err
					}
					var input = AlterColumnNullableInput{
						Table:      table,
						Identifier: field,
					}
					if statement, err := RenderAlterColumnNullableTemplate(input, endpoint.AlterColumnNullableTemplate); err != nil {
						return nil, err
					} else {
						statements = append(statements, statement)
					}
				}
			}

			continue
		}

		tableShapes = append(tableShapes, tableShape)
	}

	for _, shape := range tableShapes {
		var table, err = ResolveTable(shape, endpoint.Dialect)
		if err != nil {
			return nil, err
		} else if statement, err := RenderTableTemplate(table, endpoint.CreateTableTemplate); err != nil {
			return nil, err
		} else {
			statements = append(statements, statement)
		}

		if shape.AdditionalSql != "" {
			statements = append(statements, shape.AdditionalSql)
		}
	}

	// Insert or update the materialization specification.
	var args = []interface{}{
		endpoint.Identifier(endpoint.MetaSpecs.Path...),
		endpoint.Literal(req.Version),
		endpoint.Literal(base64.StdEncoding.EncodeToString(reqBindingsBytes)),
		endpoint.Literal(req.Name),
	}
	if loadedBindings == nil {
		statements = append(statements, fmt.Sprintf(
			"INSERT INTO %[1]s (version, spec, materialization) VALUES (%[2]s, %[3]s, %[4]s);", args...))
	} else {
		statements = append(statements, fmt.Sprintf(
			"UPDATE %[1]s SET version = %[2]s, spec = %[3]s WHERE materialization = %[4]s;", args...))
	}

	if req.DryRun {
		// No-op.
	} else if err = endpoint.Client.ExecStatements(ctx, statements); err != nil {
		return nil, fmt.Errorf("applying schema updates: %w", err)
	}

	// Build and return a description of what happened (or would have happened).
	return &protocol.ApplyResponse{ActionDescription: strings.Join(statements, "\n")}, nil
}

func (d *Driver) applyDelete(ctx context.Context, req protocol.ApplyRequest) (*protocol.ApplyResponse, error) {
	var (
		endpoint       *Endpoint
		loadedBindings []protocol.ApplyBinding
		loadedVersion  string
		err            error
	)

	if endpoint, err = d.NewEndpoint(ctx, req.Config); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if loadedBindings, loadedVersion, err = loadBindings(ctx, endpoint, req.Name); err != nil {
		return nil, fmt.Errorf("loading prior applied materialization spec: %w", err)
	} else if loadedVersion == "" {
		return &protocol.ApplyResponse{
			ActionDescription: "Could not find materialization in the database. " +
				"Doing nothing, on the assumption that the table was manually dropped.",
		}, nil
	} else if loadedVersion != req.Version {
		return nil, fmt.Errorf(
			"applied and current materializations are different versions (applied: %s vs current: %s)",
			loadedVersion, req.Version)
	}

	// Delete the materialization from the specs metadata table,
	// and also the checkpoints table if it exists.
	var statements = []string{
		fmt.Sprintf("DELETE FROM %s WHERE materialization = %s AND version = %s;",
			endpoint.Identifier(endpoint.MetaSpecs.Path...),
			endpoint.Literal(req.Name),
			endpoint.Literal(req.Version)),
	}
	if endpoint.MetaCheckpoints != nil {
		statements = append(statements,
			fmt.Sprintf("DELETE FROM %s WHERE materialization = %s;",
				endpoint.Identifier(endpoint.MetaCheckpoints.Path...),
				endpoint.Literal(req.Name)),
		)
	}

	// Drop all bound tables.
	for _, binding := range loadedBindings {
		statements = append(statements, fmt.Sprintf(
			"DROP TABLE IF EXISTS %s;",
			endpoint.Identifier(binding.ResourcePath...)))
	}

	if req.DryRun {
		// No-op.
	} else if err = endpoint.Client.ExecStatements(ctx, statements); err != nil {
		return nil, fmt.Errorf("removing dropped tables: %w", err)
	}

	// Build and return a description of what happened (or would have happened).
	return &protocol.ApplyResponse{ActionDescription: strings.Join(statements, "\n")}, nil
}

func (d *Driver) Open(ctx context.Context, req protocol.OpenRequest) (*protocol.OpenResponse, error) {
	var loadedVersion string

	var endpoint, err = d.NewEndpoint(ctx, req.Config)
	if err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if _, loadedVersion, err = loadBindings(ctx, endpoint, req.Name); err != nil {
		return nil, fmt.Errorf("loading prior applied materialization spec: %w", err)
	} else if loadedVersion == "" {
		return nil, fmt.Errorf("materialization has not been applied")
	} else if loadedVersion != req.Version {
		return nil, fmt.Errorf(
			"applied and current materializations are different versions (applied: %s vs current: %s)",
			loadedVersion, req.Version)
	}

	var tables []Table
	for index, binding := range req.Bindings {
		var resource = endpoint.NewResource(endpoint)

		if err := protocol.UnmarshalStrict(binding.ResourceConfig, resource); err != nil {
			return nil, fmt.Errorf("resource binding for collection %q: %w", binding.Collection.Name, err)
		}
		var shape = BuildTableShape(req.Name, req.Bindings, index, resource)

		if table, err := ResolveTable(shape, endpoint.Dialect); err != nil {
			return nil, err
		} else {
			tables = append(tables, table)
		}
	}

	var fence = Fence{
		TablePath:       nil, // Set later iff endpoint.MetaCheckpoints != nil.
		Materialization: req.Name,
		KeyBegin:        uint32(req.KeyBegin),
		KeyEnd:          uint32(req.KeyEnd),
		Fence:           0,
		Checkpoint:      "", // Set later iff endpoint.MetaCheckpoints != nil.
	}

	if endpoint.MetaCheckpoints != nil {
		// We must install a fence to prevent another (zombie) instances of this
		// materialization from committing further transactions.
		var metaCheckpoints, err = ResolveTable(*endpoint.MetaCheckpoints, endpoint.Dialect)
		if err != nil {
			return nil, fmt.Errorf("resolving checkpoints table: %w", err)
		}

		// Initialize a checkpoint such that the materialization starts from scratch,
		// regardless of the recovery log checkpoint.
		fence.TablePath = endpoint.MetaCheckpoints.Path
		fence.Checkpoint = protocol.ExplicitZeroCheckpoint

		// The returned fence here will contain the actual checkpoint that should be used in the
		// open response.
		fence, err = endpoint.Client.InstallFence(ctx, metaCheckpoints, fence)
		if err != nil {
			return nil, fmt.Errorf("installing checkpoints fence: %w", err)
		}
	}

	d.transactor, err = endpoint.NewTransactor(ctx, endpoint, fence, tables)
	if err != nil {
		return nil, fmt.Errorf("building transactor: %w", err)
	}

	return &protocol.OpenResponse{
		RuntimeCheckpoint: fence.Checkpoint,
	}, nil
}

func (d *Driver) Load(ctx context.Context, loads <-chan protocol.LoadRequest, loaded func(protocol.LoadResponse)) error {
	return d.transactor.Load(ctx, loads, loaded)
}

func (d *Driver) Store(ctx context.Context, stores <-chan protocol.StoreRequest) (boilerplate.StartCommitFn, error) {
	return d.transactor.Store(ctx, stores)
}
