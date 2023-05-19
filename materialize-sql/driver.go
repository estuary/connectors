package sql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"go.gazette.dev/core/consumer/protocol"
)

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

var _ boilerplate.Connector = &Driver{}

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
func (d *Driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	var endpoint, resource []byte

	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = schemagen.GenerateSchema("SQL Connection", d.EndpointSpecType).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	} else if resource, err = schemagen.GenerateSchema("SQL Table", d.ResourceSpecType).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpoint),
		ResourceConfigSchemaJson: json.RawMessage(resource),
		DocumentationUrl:         docsUrlFromEnv(d.DocumentationURL),
	}, nil
}

// Validate implements the DriverServer interface.
func (d *Driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var (
		err        error
		endpoint   *Endpoint
		loadedSpec *pf.MaterializationSpec
		resp       = new(pm.Response_Validated)
	)

	if err = req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = d.NewEndpoint(ctx, req.ConfigJson); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if prereqErrs := endpoint.CheckPrerequisites(ctx, req.ConfigJson); prereqErrs.Len() != 0 {
		return nil, cerrors.NewUserError(nil, prereqErrs.Error())
	} else if loadedSpec, _, err = loadSpec(ctx, endpoint, req.Name); err != nil {
		return nil, fmt.Errorf("loading current applied materialization spec: %w", err)
	}

	// Produce constraints for each request binding, in turn.
	for _, bindingSpec := range req.Bindings {
		var constraints, _, resource, err = resolveResourceToExistingBinding(
			endpoint,
			bindingSpec.ResourceConfigJson,
			&bindingSpec.Collection,
			loadedSpec,
		)
		if err != nil {
			return nil, err
		}

		resp.Bindings = append(resp.Bindings,
			&pm.Response_Validated_Binding{
				Constraints:  constraints,
				DeltaUpdates: resource.DeltaUpdates(),
				ResourcePath: resource.Path(),
			})
	}
	return resp, nil
}

// ApplyUpsert implements the DriverServer interface.
func (d *Driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	var (
		endpoint   *Endpoint
		loadedSpec *pf.MaterializationSpec
		specBytes  []byte
		err        error
	)

	if len(req.Materialization.Bindings) == 0 {
		// Empty bindings means we are deleting the materialization, in this case we
		// don't have anything to do, so return early
		return &pm.Response_Applied{ActionDescription: ""}, nil
	}


	if err = req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = d.NewEndpoint(ctx, req.Materialization.ConfigJson); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if loadedSpec, _, err = loadSpec(ctx, endpoint, req.Materialization.Name); err != nil {
		return nil, fmt.Errorf("loading prior applied materialization spec: %w", err)
	} else if specBytes, err = req.Materialization.Marshal(); err != nil {
		panic(err) // Cannot fail to marshal.
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

	for bindingIndex, bindingSpec := range req.Materialization.Bindings {
		var collection = bindingSpec.Collection.Name

		var constraints, loadedBinding, resource, err = resolveResourceToExistingBinding(
			endpoint,
			bindingSpec.ResourceConfigJson,
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
				if !SliceContains(field, loadedBinding.FieldSelection.Values) {
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
			endpoint.Literal(req.Materialization.Name.String()),
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
	return &pm.Response_Applied{ActionDescription: strings.Join(statements, "\n")}, nil
}

func (d *Driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var loadedVersion string

	var endpoint, err = d.NewEndpoint(ctx, open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, fmt.Errorf("building endpoint: %w", err)
	}

	if endpoint.MetaSpecs != nil {
		if _, loadedVersion, err = loadSpec(ctx, endpoint, open.Materialization.Name); err != nil {
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

		if err := pf.UnmarshalStrict(spec.ResourceConfigJson, resource); err != nil {
			return nil, nil, fmt.Errorf("resource binding for collection %q: %w", spec.Collection.Name, err)
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
		Materialization: open.Materialization.Name,
		KeyBegin:        open.Range.KeyBegin,
		KeyEnd:          open.Range.KeyEnd,
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

	var cp *protocol.Checkpoint
	if len(fence.Checkpoint) > 0 {
		cp = new(protocol.Checkpoint)
		if err := cp.Unmarshal(fence.Checkpoint); err != nil {
			return nil, nil, fmt.Errorf("unmarshalling fence.Checkpoint, %w", err)
		}
	}

	return transactor, &pm.Response_Opened{RuntimeCheckpoint: cp}, nil
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

		if string(existingFields.FieldConfigJsonMap[field]) != string(newFields.FieldConfigJsonMap[field]) {
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
