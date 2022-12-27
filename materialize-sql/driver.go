package sql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/sirupsen/logrus"
)

// Driver implements the pm.DriverServer interface.
type Driver struct {
	// URL at which documentation for the driver may be found.
	DocumentationURL string
	// Instance of the type into which endpoint specifications are parsed.
	EndpointSpecType interface{}
	// Instance of the type into which resource specifications are parsed.
	ResourceSpecType Resource
	// NewEndpoint returns an *Endpoint which will be used to handle interactions with the database.
	NewEndpoint func(_ context.Context, endpointConfig json.RawMessage) (*Endpoint, error)
}

var _ pm.DriverServer = &Driver{}

// Spec implements the DriverServer interface.
func (d *Driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	var endpoint, resource []byte

	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = schemagen.GenerateSchema("SQL Connection", d.EndpointSpecType).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	} else if resource, err = schemagen.GenerateSchema("SQL Table", d.ResourceSpecType).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpoint),
		ResourceSpecSchemaJson: json.RawMessage(resource),
		DocumentationUrl:       d.DocumentationURL,
	}, nil
}

// Validate implements the DriverServer interface.
func (d *Driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var (
		err        error
		endpoint   *Endpoint
		loadedSpec *pf.MaterializationSpec
		resp       = new(pm.ValidateResponse)
	)

	if err = req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = d.NewEndpoint(ctx, req.EndpointSpecJson); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if loadedSpec, _, err = loadSpec(ctx, endpoint, req.Materialization); err != nil {
		return nil, fmt.Errorf("loading current applied materialization spec: %w", err)
	}

	// Produce constraints for each request binding, in turn.
	for _, bindingSpec := range req.Bindings {
		var constraints, _, resource, err = resolveResourceToExistingBinding(
			endpoint,
			bindingSpec.ResourceSpecJson,
			&bindingSpec.Collection,
			loadedSpec,
		)
		if err != nil {
			return nil, err
		}

		resp.Bindings = append(resp.Bindings,
			&pm.ValidateResponse_Binding{
				Constraints:  constraints,
				DeltaUpdates: resource.DeltaUpdates(),
				ResourcePath: resource.Path(),
			})
	}
	return resp, nil
}

// ApplyUpsert implements the DriverServer interface.
func (d *Driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var (
		endpoint      *Endpoint
		loadedSpec    *pf.MaterializationSpec
		loadedVersion string
		specBytes     []byte
		err           error
	)

	if err = req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = d.NewEndpoint(ctx, req.Materialization.EndpointSpecJson); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if loadedSpec, loadedVersion, err = loadSpec(ctx, endpoint, req.Materialization.Materialization); err != nil {
		return nil, fmt.Errorf("loading prior applied materialization spec: %w", err)
	} else if specBytes, err = req.Materialization.Marshal(); err != nil {
		panic(err) // Cannot fail to marshal.
	}

	if loadedVersion == req.Version {
		// A re-application of the current version is a no-op.
		return new(pm.ApplyResponse), nil
	}

	// The Materialization specifications meta table is always required.
	// The Checkpoints table is optional; include it if set by the Endpoint.
	var tableShapes = []TableShape{endpoint.MetaSpecs}
	if endpoint.MetaCheckpoints != nil {
		tableShapes = append(tableShapes, *endpoint.MetaCheckpoints)
	}

	for bindingIndex, bindingSpec := range req.Materialization.Bindings {
		var collection = bindingSpec.Collection.Collection
		var constraints, loadedBinding, resource, err = resolveResourceToExistingBinding(
			endpoint,
			bindingSpec.ResourceSpecJson,
			&bindingSpec.Collection,
			loadedSpec,
		)
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
				bindingSpec.FieldSelection.String(),
				loadedBinding.FieldSelection.String(),
			)
		} else if loadedBinding != nil {
			continue // Table was already created. Nothing to do.
		}

		tableShapes = append(tableShapes,
			BuildTableShape(req.Materialization, bindingIndex, resource))
	}

	var statements []string

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
		endpoint.Literal(base64.StdEncoding.EncodeToString(specBytes)),
		endpoint.Literal(req.Materialization.Materialization.String()),
	}
	if loadedSpec == nil {
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
	return &pm.ApplyResponse{ActionDescription: strings.Join(statements, "\n")}, nil
}

// ApplyDelete implements the DriverServer interface.
func (d *Driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var (
		endpoint      *Endpoint
		loadedVersion string
		err           error
	)

	if err = req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = d.NewEndpoint(ctx, req.Materialization.EndpointSpecJson); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if _, loadedVersion, err = loadSpec(ctx, endpoint, req.Materialization.Materialization); err != nil {
		return nil, fmt.Errorf("loading prior applied materialization spec: %w", err)
	} else if loadedVersion == "" {
		return &pm.ApplyResponse{
			ActionDescription: "Could not find materialization in the database. " +
				"Doing nothing, on the assumption that the table was manually dropped.",
		}, nil
	} else if loadedVersion != req.Version {
		return nil, fmt.Errorf(
			"applied and current materializations are different versions (applied: %s vs current: %s)",
			loadedVersion, req.Version)
	}

	var materialization = req.Materialization.Materialization.String()

	// Delete the materialization from the specs metadata table,
	// and also the checkpoints table if it exists.
	var statements = []string{
		fmt.Sprintf("DELETE FROM %s WHERE materialization = %s AND version = %s;",
			endpoint.Identifier(endpoint.MetaSpecs.Path...),
			endpoint.Literal(materialization),
			endpoint.Literal(req.Version)),
	}
	if endpoint.MetaCheckpoints != nil {
		statements = append(statements,
			fmt.Sprintf("DELETE FROM %s WHERE materialization = %s;",
				endpoint.Identifier(endpoint.MetaCheckpoints.Path...),
				endpoint.Literal(materialization)),
		)
	}

	// Drop all bound tables.
	for _, bindingSpec := range req.Materialization.Bindings {
		statements = append(statements, fmt.Sprintf(
			"DROP TABLE IF EXISTS %s;",
			endpoint.Identifier(bindingSpec.ResourcePath...)))
	}

	if req.DryRun {
		// No-op.
	} else if err = endpoint.Client.ExecStatements(ctx, statements); err != nil {
		return nil, fmt.Errorf("removing dropped tables: %w", err)
	}

	// Build and return a description of what happened (or would have happened).
	return &pm.ApplyResponse{ActionDescription: strings.Join(statements, "\n")}, nil
}

// Transactions implements the DriverServer interface.
func (d *Driver) Transactions(stream pm.Driver_TransactionsServer) error {
	var (
		endpoint      *Endpoint
		open          *pm.TransactionRequest
		loadedVersion string
		err           error
	)

	if open, err = stream.Recv(); err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	} else if endpoint, err = d.NewEndpoint(stream.Context(), open.Open.Materialization.EndpointSpecJson); err != nil {
		return fmt.Errorf("building endpoint: %w", err)
	} else if _, loadedVersion, err = loadSpec(stream.Context(), endpoint,
		open.Open.Materialization.Materialization); err != nil {
		return fmt.Errorf("loading prior applied materialization spec: %w", err)
	} else if loadedVersion == "" {
		return fmt.Errorf("materialization has not been applied")
	} else if loadedVersion != open.Open.Version {
		return fmt.Errorf(
			"applied and current materializations are different versions (applied: %s vs current: %s)",
			loadedVersion, open.Open.Version)
	}

	var tables []Table
	for index, spec := range open.Open.Materialization.Bindings {
		var resource = endpoint.NewResource(endpoint)

		if err := pf.UnmarshalStrict(spec.ResourceSpecJson, resource); err != nil {
			return fmt.Errorf("resource binding for collection %q: %w", spec.Collection.Collection, err)
		}
		var shape = BuildTableShape(open.Open.Materialization, index, resource)

		if table, err := ResolveTable(shape, endpoint.Dialect); err != nil {
			return err
		} else {
			tables = append(tables, table)
		}
	}

	var fence = Fence{
		TablePath:       nil, // Set later iff endpoint.MetaCheckpoints != nil.
		Materialization: open.Open.Materialization.Materialization,
		KeyBegin:        open.Open.KeyBegin,
		KeyEnd:          open.Open.KeyEnd,
		Fence:           0,
		Checkpoint:      nil, // Set later iff endpoint.MetaCheckpoints != nil.
	}

	if endpoint.MetaCheckpoints != nil {
		// We must install a fence to prevent another (zombie) instances of this
		// materialization from committing further transactions.
		var metaCheckpoints, err = ResolveTable(*endpoint.MetaCheckpoints, endpoint.Dialect)
		if err != nil {
			return fmt.Errorf("resolving checkpoints table: %w", err)
		}

		// Initialize a checkpoint such that the materialization starts from scratch,
		// regardless of the recovery log checkpoint.
		fence.TablePath = endpoint.MetaCheckpoints.Path
		fence.Checkpoint = pm.ExplicitZeroCheckpoint

		fence, err = endpoint.Client.InstallFence(stream.Context(), metaCheckpoints, fence)
		if err != nil {
			return fmt.Errorf("installing checkpoints fence: %w", err)
		}
	}

	transactor, err := endpoint.NewTransactor(stream.Context(), endpoint, fence, tables)
	if err != nil {
		return fmt.Errorf("building transactor: %w", err)
	}

	// Respond to the runtime that we've opened the transactions stream.
	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: fence.Checkpoint}}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	return pm.RunTransactions(stream, transactor, logrus.WithFields(logrus.Fields{
		"materialization": open.Open.Materialization.Materialization,
		"keyBegin":        open.Open.KeyBegin,
		"keyEnd":          open.Open.KeyEnd,
		"fence":           fence.Fence,
	}))
}
