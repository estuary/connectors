package sql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-boilerplate/validate"
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
	NewEndpoint func(_ context.Context, endpointConfig json.RawMessage, tenant string) (*Endpoint, error)
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
	} else if endpoint, err = d.NewEndpoint(ctx, req.ConfigJson, mustGetTenantNameFromTaskName(req.Name.String())); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if prereqErrs := endpoint.CheckPrerequisites(ctx, endpoint); prereqErrs.Len() != 0 {
		return nil, cerrors.NewUserError(nil, prereqErrs.Error())
	} else if loadedSpec, _, err = loadSpec(ctx, endpoint, req.Name); err != nil {
		return nil, fmt.Errorf("loading current applied materialization spec: %w", err)
	}

	validator := validate.NewValidator(constrainter{dialect: endpoint.Dialect})

	// Produce constraints for each request binding, in turn.
	for _, bindingSpec := range req.Bindings {
		res := endpoint.NewResource(endpoint)
		if err = pf.UnmarshalStrict(bindingSpec.ResourceConfigJson, res); err != nil {
			return nil, fmt.Errorf("unmarshalling resource binding for collection %q: %w", bindingSpec.Collection.Name.String(), err)
		}

		constraints, err := validator.ValidateBinding(
			res.Path(),
			res.DeltaUpdates(),
			bindingSpec.Collection,
			bindingSpec.FieldConfigJsonMap,
			loadedSpec,
		)
		if err != nil {
			return nil, err
		}

		resp.Bindings = append(resp.Bindings,
			&pm.Response_Validated_Binding{
				Constraints:  constraints,
				DeltaUpdates: res.DeltaUpdates(),
				ResourcePath: res.Path(),
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

	if err = req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = d.NewEndpoint(ctx, req.Materialization.ConfigJson, mustGetTenantNameFromTaskName(req.Materialization.String())); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if loadedSpec, _, err = loadSpec(ctx, endpoint, req.Materialization.Name); err != nil {
		return nil, fmt.Errorf("loading prior applied materialization spec: %w", err)
	} else if specBytes, err = req.Materialization.Marshal(); err != nil {
		panic(err) // Cannot fail to marshal.
	}

	// The Materialization specifications meta table is almost always required. It is only not
	// required if the database is ephemeral (i.e. sqlite). The Checkpoints table is optional;
	// include it if set by the Endpoint.
	var tableShapes = []TableShape{}
	if endpoint.MetaSpecs != nil {
		tableShapes = append(tableShapes, *endpoint.MetaSpecs)
	}
	if endpoint.MetaCheckpoints != nil {
		tableShapes = append(tableShapes, *endpoint.MetaCheckpoints)
	}

	var executedColumnModifications []string

	validator := validate.NewValidator(constrainter{dialect: endpoint.Dialect})

	for bindingIndex, bindingSpec := range req.Materialization.Bindings {
		resource := endpoint.NewResource(endpoint)
		if err = pf.UnmarshalStrict(bindingSpec.ResourceConfigJson, resource); err != nil {
			return nil, fmt.Errorf("unmarshalling resource binding for collection %q: %w", bindingSpec.Collection.Name.String(), err)
		}

		var collection = bindingSpec.Collection.Name
		var tableShape = BuildTableShape(req.Materialization, bindingIndex, resource)

		loadedBinding, err := validate.FindExistingBinding(resource.Path(), bindingSpec.Collection.Name, loadedSpec)
		if err != nil {
			return nil, err
		}

		if err != nil {
			return nil, err
		} else if err = validator.ValidateSelectedFields(bindingSpec, loadedSpec); err != nil {
			// The applied binding is not a valid solution for its own constraints.
			return nil, fmt.Errorf("binding for %s: %w", collection, err)
		} else if loadedBinding != nil {
			newTable, err := ResolveTable(tableShape, endpoint.Dialect)
			if err != nil {
				return nil, err
			}

			for _, newCol := range newTable.Columns() {
				// Add any new columns.
				if !slices.Contains(loadedBinding.FieldSelection.AllFields(), newCol.Field) {
					mapped, err := endpoint.MapTypeNullable(&newCol.Projection)
					if err != nil {
						return nil, err
					}

					addAction, err := endpoint.Client.AddColumnToTable(ctx, req.DryRun, newTable.Identifier, newCol.Identifier, mapped.DDL)
					if err != nil {
						return nil, fmt.Errorf("adding column '%s %s' to table '%s': %w", newCol.Identifier, mapped.DDL, newTable.Identifier, err)
					}

					if addAction != "" {
						executedColumnModifications = append(executedColumnModifications, addAction)
					}
				} else {
					// The new column is part of the existing table.
					previousProjection := loadedBinding.Collection.GetProjection(newCol.Field)
					previousNullable := previousProjection.Inference.Exists != pf.Inference_MUST || slices.Contains(previousProjection.Inference.Types, "null")

					// The column was previously created as not nullable, but the new specification
					// indicates that it now nullable. Drop the "NOT NULL" constraint on the table.
					if !previousNullable && !newCol.MustExist {
						alterAction, err := endpoint.Client.DropNotNullForColumn(ctx, req.DryRun, newTable, *newCol)
						if err != nil {
							return nil, fmt.Errorf("dropping NOT NULL constraint for column '%s' in table '%s' because it is no longer a required property: %w", newTable.Identifier, newCol.Identifier, err)
						}

						if alterAction != "" {
							executedColumnModifications = append(executedColumnModifications, alterAction)
						}
					}
				}
			}

			var allNewFields = bindingSpec.FieldSelection.AllFields()

			// Mark columns that have been removed from field selection as nullable
			for _, field := range loadedBinding.FieldSelection.AllFields() {
				// If the projection is part of the new field selection, just skip
				if slices.Contains(allNewFields, field) {
					continue
				}

				var col = Column{
					Identifier: endpoint.Identifier(field),
					Projection: Projection{Projection: *loadedBinding.Collection.GetProjection(field)},
				}
				alterAction, err := endpoint.Client.DropNotNullForColumn(ctx, req.DryRun, newTable, col)
				if err != nil {
					return nil, fmt.Errorf("dropping NOT NULL constraint for removed field for column '%s' in table '%s': %w", newTable.Identifier, field, err)
				}

				if alterAction != "" {
					executedColumnModifications = append(executedColumnModifications, alterAction)
				}
			}

			// For existing tables, do not add the rendered CreateTableTemplate, since the tables
			// already exist and do not need to be re-created.
			continue
		}

		tableShapes = append(tableShapes, tableShape)
	}

	// Build a list of table creation statements, followed by the spec update statement.
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
	return &pm.Response_Applied{
		ActionDescription: strings.Join(append(statements, executedColumnModifications...), "\n"),
	}, nil
}

func (d *Driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var loadedVersion string

	var endpoint, err = d.NewEndpoint(ctx, open.Materialization.ConfigJson, mustGetTenantNameFromTaskName(open.Materialization.String()))
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

func mustGetTenantNameFromTaskName(taskName string) string {
	return strings.Split(taskName, "/")[0]
}
