package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/protocols/materialize"
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
	// StartTunnel starts up an SSH tunnel if one is configured prior to running
	// any operations that require connectivity to the database.
	StartTunnel func(ctx context.Context, conf any) error
	// NewEndpoint returns an *Endpoint which will be used to handle interactions with the database.
	NewEndpoint func(_ context.Context, endpointConfig json.RawMessage, tenant string) (*Endpoint, error)
	// PreReqs performs verification checks that the provided configuration can
	// be used to interact with the endpoint to the degree required by the
	// connector, to as much of an extent as possible. The returned PrereqErr
	// can include multiple separate errors if it possible to determine that
	// there is more than one issue that needs corrected.
	PreReqs func(ctx context.Context, conf any, tenant string) *cerrors.PrereqErr
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

func (d *Driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var (
		err      error
		endpoint *Endpoint
		client   Client
		resp     = new(pm.Response_Validated)
		conf     = d.EndpointSpecType
	)

	if err = req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err := json.Unmarshal(req.ConfigJson, conf); err != nil {
		return nil, fmt.Errorf("parsing endpoint configuration: %w", err)
	} else if err := d.StartTunnel(ctx, conf); err != nil {
		return nil, fmt.Errorf("starting network tunnel: %w", err)
	} else if prereqErrs := d.PreReqs(ctx, conf, mustGetTenantNameFromTaskName(req.Name.String())); prereqErrs.Len() != 0 {
		return nil, cerrors.NewUserError(nil, prereqErrs.Error())
	} else if endpoint, err = d.NewEndpoint(ctx, req.ConfigJson, mustGetTenantNameFromTaskName(req.Name.String())); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if client, err = endpoint.NewClient(ctx, endpoint); err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	defer client.Close()

	resources := make([]Resource, 0, len(req.Bindings))
	resourcePaths := make([][]string, 0, len(req.Bindings))
	for _, b := range req.Bindings {
		res := endpoint.NewResource(endpoint)
		if err = pf.UnmarshalStrict(b.ResourceConfigJson, res); err != nil {
			return nil, fmt.Errorf("unmarshalling resource binding for collection %q: %w", b.Collection.Name.String(), err)
		}
		resources = append(resources, res)
		resourcePaths = append(resourcePaths, res.Path())
	}
	if endpoint.MetaCheckpoints != nil {
		resourcePaths = append(resourcePaths, endpoint.MetaCheckpoints.Path)
	}

	is, err := client.InfoSchema(ctx, resourcePaths)
	if err != nil {
		return nil, err
	}
	validator := boilerplate.NewValidator(
		constrainter{dialect: endpoint.Dialect, featureFlags: endpoint.FeatureFlags},
		is,
		endpoint.Dialect.MaxColumnCharLength,
		endpoint.Dialect.CaseInsensitiveColumns,
		endpoint.FeatureFlags,
	)

	if p := is.AmbiguousResourcePaths(resourcePaths); len(p) > 0 {
		// This is mostly a sanity-check since it is very unlikely to happen, given that Flow
		// collection names don't allow for collections that are identical other than
		// capitalization. It's still technically possible though if a user configures a different
		// destination table name than the default, or the materialize connector does something
		// weird with transforming table names (ex: materialize-bigquery).
		return nil, fmt.Errorf("cannot materialize ambigous resource paths: [%s]", p)
	}

	// Produce constraints for each request binding, in turn.
	for idx, bindingSpec := range req.Bindings {
		res := resources[idx]

		constraints, err := validator.ValidateBinding(
			res.Path(),
			res.DeltaUpdates(),
			bindingSpec.Backfill,
			bindingSpec.Collection,
			bindingSpec.FieldConfigJsonMap,
			req.LastMaterialization,
		)
		if err != nil {
			return nil, err
		}

		resp.Bindings = append(resp.Bindings,
			&pm.Response_Validated_Binding{
				Constraints:  constraints,
				DeltaUpdates: res.DeltaUpdates(),
				ResourcePath: res.Path(),
				SerPolicy:    endpoint.SerPolicy,
			})
	}

	return resp, nil
}

func (d *Driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	var (
		endpoint *Endpoint
		client   Client
		err      error
		conf     = d.EndpointSpecType
	)

	if err = req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err := json.Unmarshal(req.Materialization.ConfigJson, conf); err != nil {
		return nil, fmt.Errorf("parsing endpoint configuration: %w", err)
	} else if err := d.StartTunnel(ctx, conf); err != nil {
		return nil, fmt.Errorf("starting network tunnel: %w", err)
	} else if endpoint, err = d.NewEndpoint(ctx, req.Materialization.ConfigJson, mustGetTenantNameFromTaskName(req.Materialization.Name.String())); err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	} else if client, err = endpoint.NewClient(ctx, endpoint); err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	defer client.Close()

	resourcePaths := make([][]string, 0, len(req.Materialization.Bindings))
	for _, b := range req.Materialization.Bindings {
		resourcePaths = append(resourcePaths, b.ResourcePath)
	}
	if endpoint.MetaCheckpoints != nil {
		resourcePaths = append(resourcePaths, endpoint.MetaCheckpoints.Path)
	}

	is, err := client.InfoSchema(ctx, resourcePaths)
	if err != nil {
		return nil, err
	}

	if sm, ok := client.(SchemaManager); ok {
		// Create any schemas that don't already exist, if the endpoint supports schemas.
		existingSchemas, err := sm.ListSchemas(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting schema list from client: %w", err)
		}

		requiredSchemas := make(map[string]struct{})
		for _, p := range resourcePaths {
			requiredSchemas[endpoint.Dialect.TableLocator(p).TableSchema] = struct{}{}
		}

		for r := range requiredSchemas {
			if !slices.Contains(existingSchemas, r) {
				if err := sm.CreateSchema(ctx, r); err != nil {
					return nil, fmt.Errorf("client creating schema '%s': %w", r, err)
				}
				log.WithField("schema", r).Info("created schema")
			}
		}
	}

	if endpoint.MetaCheckpoints != nil && is.GetResource(endpoint.MetaCheckpoints.Path) == nil {
		// Create the checkpoints table if it doesn't already exist.
		if resolved, err := ResolveTable(*endpoint.MetaCheckpoints, endpoint.Dialect); err != nil {
			return nil, err
		} else if createStatement, err := RenderTableTemplate(resolved, endpoint.CreateTableTemplate); err != nil {
			return nil, err
		} else if err := client.CreateTable(ctx, TableCreate{
			Table:              resolved,
			TableCreateSql:     createStatement,
			ResourceConfigJson: nil, // not applicable for meta tables
		}); err != nil {
			return nil, fmt.Errorf("creating checkpoints table: %w", err)
		} else {
			log.WithField("table", resolved.Identifier).Info("created checkpoints table")
		}
	}

	return boilerplate.ApplyChanges(ctx, req, newSqlApplier(client, is, endpoint, constrainter{dialect: endpoint.Dialect, featureFlags: endpoint.FeatureFlags}), is, endpoint.ConcurrentApply)
}

func (d *Driver) NewTransactor(ctx context.Context, open pm.Request_Open, be *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	var conf = d.EndpointSpecType

	if err := json.Unmarshal(open.Materialization.ConfigJson, conf); err != nil {
		return nil, nil, nil, fmt.Errorf("parsing endpoint configuration: %w", err)
	} else if err := d.StartTunnel(ctx, conf); err != nil {
		return nil, nil, nil, fmt.Errorf("starting network tunnel: %w", err)
	}

	endpoint, err := d.NewEndpoint(ctx, open.Materialization.ConfigJson, mustGetTenantNameFromTaskName(open.Materialization.Name.String()))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building endpoint: %w", err)
	}

	client, err := endpoint.NewClient(ctx, endpoint)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating client: %w", err)
	}
	defer client.Close()

	var resourcePaths [][]string
	var tables []Table
	for index, spec := range open.Materialization.Bindings {
		resourcePaths = append(resourcePaths, spec.ResourcePath)
		var resource = endpoint.NewResource(endpoint)

		if err := pf.UnmarshalStrict(spec.ResourceConfigJson, resource); err != nil {
			return nil, nil, nil, fmt.Errorf("resource binding for collection %q: %w", spec.Collection.Name, err)
		}
		var shape = BuildTableShape(open.Materialization, index, resource)

		if table, err := ResolveTable(shape, endpoint.Dialect); err != nil {
			return nil, nil, nil, err
		} else {
			table.StateKey = spec.StateKey
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
		resourcePaths = append(resourcePaths, endpoint.MetaCheckpoints.Path)

		// We must install a fence to prevent another (zombie) instances of this
		// materialization from committing further transactions.
		var metaCheckpoints, err = ResolveTable(*endpoint.MetaCheckpoints, endpoint.Dialect)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("resolving checkpoints table: %w", err)
		}

		// Initialize a checkpoint such that the materialization starts from scratch,
		// regardless of the recovery log checkpoint.
		fence.TablePath = endpoint.MetaCheckpoints.Path
		fence.Checkpoint = pm.ExplicitZeroCheckpoint

		fence, err = client.InstallFence(ctx, metaCheckpoints, fence)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("installing checkpoints fence: %w", err)
		}
	}

	is, err := client.InfoSchema(ctx, resourcePaths)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("getting info schema: %w", err)
	}

	transactor, options, err := endpoint.NewTransactor(ctx, endpoint, fence, tables, open, is, be)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building transactor: %w", err)
	}

	var cp *protocol.Checkpoint
	if len(fence.Checkpoint) > 0 {
		cp = new(protocol.Checkpoint)
		if err := cp.Unmarshal(fence.Checkpoint); err != nil {
			return nil, nil, nil, fmt.Errorf("unmarshalling fence.Checkpoint, %w", err)
		}
	}

	return transactor, &pm.Response_Opened{RuntimeCheckpoint: cp}, options, nil
}

func mustGetTenantNameFromTaskName(taskName string) string {
	return strings.Split(taskName, "/")[0]
}
