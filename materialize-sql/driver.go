package sql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
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

// docsUrlFromEnv looks for an environment variable set as DOCS_URL to use for the spec response
// documentation URL. It uses that instead of the default documentation URL from the connector if
// found.
func docsUrlFromEnv(providedURL string) string {
	fromEnv := os.Getenv("DOCS_URL")
	if fromEnv != "" {
		return fromEnv
	}

	return providedURL
}

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
		DocumentationUrl:       docsUrlFromEnv(d.DocumentationURL),
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
	} else if prereqErrs := endpoint.CheckPrerequisites(ctx, req.EndpointSpecJson); prereqErrs.Len() != 0 {
		return nil, prereqErrs
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

	// The Materialization specifications meta table is almost always requied.
	// It is only not required if the database is ephemeral (i.e. sqlite).
	// The Checkpoints table is optional; include it if set by the Endpoint.
	var tableShapes = []TableShape{}
	if endpoint.MetaSpecs != nil {
		tableShapes = append(tableShapes, *endpoint.MetaSpecs)
	}
	if endpoint.MetaCheckpoints != nil {
		tableShapes = append(tableShapes, *endpoint.MetaCheckpoints)
	}

	var statements []string
	var newCollections []string

	for bindingIndex, bindingSpec := range req.Materialization.Bindings {
		var collection = bindingSpec.Collection.Collection
		newCollections = append(newCollections, string(collection))

		var constraints, loadedBinding, resource, err = resolveResourceToExistingBinding(
			endpoint,
			bindingSpec.ResourceSpecJson,
			&bindingSpec.Collection,
			loadedSpec,
		)

		var tableShape = BuildTableShape(req.Materialization, bindingIndex, resource)

		if err != nil {
			return nil, err
		} else if err = ValidateSelectedFields(constraints, bindingSpec); err != nil {
			// The applied binding is not a valid solution for its own constraints.
			return nil, fmt.Errorf("binding for %s: %w", collection, err)
		} else if loadedBinding != nil && !bindingSpec.FieldSelection.Equal(&loadedBinding.FieldSelection) && !isBindingMigratable(loadedBinding, bindingSpec) {
			// if the binding is not migratable, we require that the list of
			// fields in the request is identical to the current fields.
			return nil, fmt.Errorf(
				"the set of binding %s fields in the request differs from the existing fields,"+
					"which is disallowed because this driver only supports automatic migration for adding new nullable fields and removing non-key fields. "+
					"Request fields: %s , Existing fields: %s",
				collection,
				bindingSpec.FieldSelection.String(),
				loadedBinding.FieldSelection.String(),
			)
		} else if loadedBinding != nil {
			for _, field := range bindingSpec.FieldSelection.Values {
				var p = bindingSpec.Collection.GetProjection(field)
				var previousProjection = loadedBinding.Collection.GetProjection(field)

				// Make sure new columns are added to the table
				if previousProjection == nil {
					var table, err = ResolveTable(tableShape, endpoint.Dialect)
					if err != nil {
						return nil, err
					}
					var input = AlterInput{
						Table:      table,
						Identifier: endpoint.Identifier(field),
					}
					if statement, err := RenderAlterTemplate(input, endpoint.AlterTableAddColumnTemplate); err != nil {
						return nil, err
					} else {
						statements = append(statements, statement)
					}
				} else {
					var previousNullable = previousProjection.Inference.Exists != pf.Inference_MUST || SliceContains("null", p.Inference.Types)
					var newNullable = p.Inference.Exists != pf.Inference_MUST || SliceContains("null", p.Inference.Types)

					// If the table has already been created, make sure all non-null fields
					// are marked as such
					if !previousNullable && newNullable {
						var table, err = ResolveTable(tableShape, endpoint.Dialect)
						if err != nil {
							return nil, err
						}
						var input = AlterInput{
							Table:      table,
							Identifier: endpoint.Identifier(field),
						}
						if statement, err := RenderAlterTemplate(input, endpoint.AlterColumnNullableTemplate); err != nil {
							return nil, err
						} else {
							statements = append(statements, statement)
						}
					}
				}
			}

			var allNewFields = bindingSpec.FieldSelection.AllFields()

			// Mark columns that have been removed from field selection as nullable
			for _, field := range loadedBinding.FieldSelection.AllFields() {
				// If the projection is part of the new field selection, just skip
				if SliceContains(field, allNewFields) {
					continue
				}

				var table, err = ResolveTable(tableShape, endpoint.Dialect)
				if err != nil {
					return nil, err
				}
				var input = AlterInput{
					Table:      table,
					Identifier: endpoint.Identifier(field),
				}
				if statement, err := RenderAlterTemplate(input, endpoint.AlterColumnNullableTemplate); err != nil {
					return nil, err
				} else {
					statements = append(statements, statement)
				}
			}

			continue
		}

		tableShapes = append(tableShapes, tableShape)
	}

	if loadedSpec != nil {
		// If a binding from loaded spec is missing from new spec, we drop the table
		for _, bindingSpec := range loadedSpec.Bindings {
			var collection = string(bindingSpec.Collection.Collection)

			if !SliceContains(collection, newCollections) {
				statements = append(statements, fmt.Sprintf(
					"DROP TABLE IF EXISTS %s;",
					endpoint.Identifier(bindingSpec.ResourcePath...)))
			}
		}
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

	if endpoint.MetaSpecs != nil {
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

	var statements = []string{}
	// Delete the materialization from the specs metadata table,
	// and also the checkpoints table if it exists.
	if endpoint.MetaSpecs != nil {
		statements = append(statements,
			fmt.Sprintf("DELETE FROM %s WHERE materialization = %s AND version = %s;",
				endpoint.Identifier(endpoint.MetaSpecs.Path...),
				endpoint.Literal(materialization),
				endpoint.Literal(req.Version)),
		)
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
	return pm.RunTransactions(stream, d.newTransactor)
}

func (d *Driver) newTransactor(ctx context.Context, open pm.TransactionRequest_Open) (pm.Transactor, *pm.TransactionResponse_Opened, error) {
	var loadedVersion string

	var endpoint, err = d.NewEndpoint(ctx, open.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, nil, fmt.Errorf("building endpoint: %w", err)
	}

	if endpoint.MetaSpecs != nil {
		if _, loadedVersion, err = loadSpec(ctx, endpoint, open.Materialization.Materialization); err != nil {
			return nil, nil, fmt.Errorf("loading prior applied materialization spec: %w", err)
		} else if loadedVersion == "" {
			return nil, nil, fmt.Errorf("materialization has not been applied")
		} else if loadedVersion != open.Version {
			return nil, nil, fmt.Errorf(
				"applied and current materializations are different versions (applied: %s vs current: %s)",
				loadedVersion, open.Version)
		}
	}

	var tables []Table
	for index, spec := range open.Materialization.Bindings {
		var resource = endpoint.NewResource(endpoint)

		if err := pf.UnmarshalStrict(spec.ResourceSpecJson, resource); err != nil {
			return nil, nil, fmt.Errorf("resource binding for collection %q: %w", spec.Collection.Collection, err)
		}
		var shape = BuildTableShape(open.Materialization, index, resource)

		if table, err := ResolveTable(shape, endpoint.Dialect); err != nil {
			return nil, nil, err
		} else {
			tables = append(tables, table)
		}
	}

	var fence = Fence{
		TablePath:       nil, // Set later iff endpoint.MetaCheckpoints != nil.
		Materialization: open.Materialization.Materialization,
		KeyBegin:        open.KeyBegin,
		KeyEnd:          open.KeyEnd,
		Fence:           0,
		Checkpoint:      nil, // Set later iff endpoint.MetaCheckpoints != nil.
	}

	if endpoint.MetaCheckpoints != nil {
		// We must install a fence to prevent another (zombie) instances of this
		// materialization from committing further transactions.
		var metaCheckpoints, err = ResolveTable(*endpoint.MetaCheckpoints, endpoint.Dialect)
		if err != nil {
			return nil, nil, fmt.Errorf("resolving checkpoints table: %w", err)
		}

		// Initialize a checkpoint such that the materialization starts from scratch,
		// regardless of the recovery log checkpoint.
		fence.TablePath = endpoint.MetaCheckpoints.Path
		fence.Checkpoint = pm.ExplicitZeroCheckpoint

		fence, err = endpoint.Client.InstallFence(ctx, metaCheckpoints, fence)
		if err != nil {
			return nil, nil, fmt.Errorf("installing checkpoints fence: %w", err)
		}
	}

	transactor, err := endpoint.NewTransactor(ctx, endpoint, fence, tables)
	if err != nil {
		return nil, nil, fmt.Errorf("building transactor: %w", err)
	}

	return transactor, &pm.TransactionResponse_Opened{RuntimeCheckpoint: fence.Checkpoint}, nil
}

func isBindingMigratable(existingBinding *pf.MaterializationSpec_Binding, newBinding *pf.MaterializationSpec_Binding) bool {
	var existingFields = existingBinding.FieldSelection
	var allExistingFields = existingFields.AllFields()
	var allExistingKeys = existingFields.Keys
	var newFields = newBinding.FieldSelection
	var allNewFields = newFields.AllFields()
	var allNewKeys = newFields.Keys

	for _, field := range allExistingKeys {
		// All keys in the existing binding must also be present in the new binding
		if !SliceContains(field, allNewKeys) {
			return false
		}

		if string(existingFields.FieldConfigJson[field]) != string(newFields.FieldConfigJson[field]) {
			return false
		}
	}

	for _, field := range allNewFields {
		// Make sure new fields are nullable
		if !SliceContains(field, allExistingFields) {
			var p = newBinding.Collection.GetProjection(field)
			var isNullable = p.Inference.Exists != pf.Inference_MUST || SliceContains("null", p.Inference.Types)
			if !isNullable {
				return false
			}
		}
	}

	return true
}
