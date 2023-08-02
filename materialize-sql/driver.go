package sql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/go/pkg/slices"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
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

	if err = req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = d.NewEndpoint(ctx, req.Materialization.ConfigJson, mustGetTenantNameFromTaskName(req.Materialization.String())); err != nil {
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
	var executedColumnModifications []string

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
		} else if loadedBinding != nil {
			newTable, err := ResolveTable(tableShape, endpoint.Dialect)
			if err != nil {
				return nil, err
			}

			for _, newCol := range newTable.Columns() {
				// Add any new columns.
				if !slices.Contains(loadedBinding.FieldSelection.AllFields(), newCol.Field) {
					// New columns are always created as nullable.
					nullableProjection := newCol.Projection
					nullableProjection.Inference.Exists = pf.Inference_MAY

					// Get the dialect-specific DDL for this field, in its nullable form.
					mapped, err := endpoint.MapType(&nullableProjection)
					if err != nil {
						return nil, err
					}

					addAction, err := endpoint.Client.AddColumnToTable(ctx, req.DryRun, newTable.Identifier, newCol.Identifier, mapped.DDL)
					if err != nil {
						return nil, fmt.Errorf("adding column '%s %s' to table '%s': %w", newTable.Identifier, mapped.DDL, newCol.Identifier, err)
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
						alterAction, err := endpoint.Client.DropNotNullForColumn(ctx, req.DryRun, newTable.Identifier, newCol.Identifier)
						if err != nil {
							return nil, fmt.Errorf("dropping NOT NULL constraint for no longer required field for column '%s' in table '%s': %w", newTable.Identifier, newCol.Identifier, err)
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

				alterAction, err := endpoint.Client.DropNotNullForColumn(ctx, req.DryRun, newTable.Identifier, endpoint.Identifier(field))
				if err != nil {
					return nil, fmt.Errorf("dropping NOT NULL constraint for removed field for column '%s' in table '%s': %w", newTable.Identifier, endpoint.Identifier(field), err)
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

// storeThreshold is a somewhat crude indication that a transaction was likely part of a backfill
// vs. a smaller incremental streaming transaction. If a materialization is configured with a commit
// delay, it should only apply that delay to transactions that occur after it has fully backfilled
// from the collection. The idea is that if a transaction stored a lot of documents it's probably
// part of a backfill.
var storeThreshold = 5_000_000

// CommitWithDelay wraps a commitFn in an async operation that may add additional delay prior to
// returning. When used as the returned OpFuture from a materialization utilizing async commits,
// this can spread out transaction processing and result in fewer, larger transactions which may be
// desirable to reduce warehouse compute costs or comply with rate limits. The delay is bypassed if
// the actual commit operation takes longer than the configured delay, or if they number of stored
// documents is large (see storeThreshold above).
//
// Delaying the return from this function delays acknowledgement back to the runtime that the commit
// has finished. The commit will still apply to the endpoint, but holding back the runtime
// acknowledgement will delay the start of the next transaction while allowing the runtime to
// continue combining over documents for the next transaction.
//
// It is always possible for a connector to restart between committing to the endpoint and sending
// the runtime acknowledgement of that commit. The chance of this happening is greater when
// intentionally adding a delay between these events. When the endpoint is authoritative and
// persists checkpoints transactionally with updating the state of the view (as is typical with a
// SQL materialization), what will happen in this case is that when the connector restarts it will
// read the previously persisted checkpoint and acknowledge it to the runtime then. In this way the
// materialization will resume from where it left off with respect to the endpoint state.
func CommitWithDelay(ctx context.Context, skipDelay bool, delay time.Duration, stored int, commitFn func(context.Context) error) pf.OpFuture {
	return pf.RunAsyncOperation(func() error {
		started := time.Now()

		if err := commitFn(ctx); err != nil {
			return err
		}

		if skipDelay {
			log.Debug("will not delay commit acknowledge since skipDelay was true")
			return nil
		}

		remainingDelay := delay - time.Since(started)

		logEntry := log.WithFields(log.Fields{
			"stored":          stored,
			"storedThreshold": storeThreshold,
			"remainingDelay":  remainingDelay.String(),
			"configuredDelay": delay.String(),
		})

		if stored > storeThreshold || remainingDelay <= 0 {
			logEntry.Debug("will acknowledge commit without further delay")
			return nil
		}

		logEntry.Debug("delaying before acknowledging commit")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(remainingDelay):
			return nil
		}
	})
}
