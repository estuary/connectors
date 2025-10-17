package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type Driver[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]] struct {
	// URL at which documentation for the driver may be found.
	DocumentationURL string
	// StartTunnel starts up an SSH tunnel if one is configured prior to running
	// any operations that require connectivity to the database.
	StartTunnel func(ctx context.Context, cfg EC) error
	// NewEndpoint returns an *Endpoint which will be used to handle interactions with the database.
	NewEndpoint func(_ context.Context, cfg EC, featureFlags map[string]bool) (*Endpoint[EC], error)
	// PreReqs performs verification checks that the provided configuration can
	// be used to interact with the endpoint to the degree required by the
	// connector, to as much of an extent as possible. The returned PrereqErr
	// can include multiple separate errors if it possible to determine that
	// there is more than one issue that needs corrected.
	PreReqs func(ctx context.Context, cfg EC) *cerrors.PrereqErr
	// OAuth2 is an optional OAuth2 specification for the connector.
	OAuth2 *pf.OAuth2
}

var _ boilerplate.Connector = &Driver[boilerplate.EndpointConfiger, Resource]{}

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

func (d *Driver[EC, RC]) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	var endpoint, resource []byte

	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if endpoint, err = schemagen.GenerateSchema("SQL Connection", new(EC)).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	} else if resource, err = schemagen.GenerateSchema("SQL Table", new(RC)).MarshalJSON(); err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpoint),
		ResourceConfigSchemaJson: json.RawMessage(resource),
		DocumentationUrl:         docsUrlFromEnv(d.DocumentationURL),
		Oauth2:                   d.OAuth2,
	}, nil
}

func (d *Driver[EC, RC]) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, d.newMaterialization)
}

func (d *Driver[EC, RC]) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, d.newMaterialization)
}

func (d *Driver[EC, RC]) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, d.newMaterialization)
}

type sqlMaterialization[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]] struct {
	materializationName string
	featureFlags        map[string]bool
	driver              *Driver[EC, RC]
	endpoint            *Endpoint[EC]
	client              Client
}

func (d *Driver[EC, RC]) newMaterialization(ctx context.Context, materializationName string, cfg EC, featureFlags map[string]bool) (boilerplate.Materializer[EC, FieldConfig, RC, MappedType], error) {
	if err := d.StartTunnel(ctx, cfg); err != nil {
		return nil, fmt.Errorf("starting network tunnel: %w", err)
	}

	endpoint, err := d.NewEndpoint(ctx, cfg, featureFlags)
	if err != nil {
		return nil, fmt.Errorf("creating endpoint: %w", err)
	}

	client, err := endpoint.NewClient(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &sqlMaterialization[EC, RC]{
		materializationName: materializationName,
		featureFlags:        featureFlags,
		driver:              d,
		endpoint:            endpoint,
		client:              client,
	}, nil
}

var _ boilerplate.Materializer[
	boilerplate.EndpointConfiger,
	FieldConfig,
	Resource,
	MappedType,
] = &sqlMaterialization[boilerplate.EndpointConfiger, Resource]{}

func (s *sqlMaterialization[EC, RC]) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	return s.driver.PreReqs(ctx, s.endpoint.Config)
}

func (s *sqlMaterialization[EC, RC]) Config() boilerplate.MaterializeCfg {
	_, doCreateSchemas := s.client.(SchemaManager)

	return boilerplate.MaterializeCfg{
		Locate:                   ToLocatePathFn(s.endpoint.Dialect.TableLocator),
		TranslateNamespace:       s.endpoint.SchemaLocator,
		TranslateField:           s.endpoint.ColumnLocator,
		MaxFieldLength:           s.endpoint.Dialect.MaxColumnCharLength,
		CaseInsensitiveFields:    s.endpoint.Dialect.CaseInsensitiveColumns,
		CaseInsensitiveResources: s.endpoint.Dialect.CaseInsensitiveResources,
		ConcurrentApply:          s.endpoint.ConcurrentApply,
		NoCreateNamespaces:       !doCreateSchemas,
		SerPolicy:                s.endpoint.SerPolicy,
		MaterializeOptions:       s.endpoint.Options,
	}
}

func (s *sqlMaterialization[EC, RC]) CreateNamespace(ctx context.Context, ns string) (string, error) {
	desc, err := s.client.(SchemaManager).CreateSchema(ctx, ns)
	if err != nil {
		return "", fmt.Errorf("creating schema: %w", err)
	}

	return desc, nil
}

func (s *sqlMaterialization[EC, RC]) CreateResource(ctx context.Context, binding boilerplate.MappedBinding[EC, RC, MappedType]) (string, boilerplate.ActionApplyFn, error) {
	table, err := getTable(s.endpoint, s.materializationName, binding)
	if err != nil {
		return "", nil, err
	}

	createStatement, err := RenderTableTemplate(table, s.endpoint.CreateTableTemplate)
	if err != nil {
		return "", nil, err
	}

	return createStatement, func(ctx context.Context) error {
		if err := s.client.CreateTable(ctx, TableCreate{
			Table:          table,
			TableCreateSql: createStatement,
			Resource:       binding.Config,
		}); err != nil {
			log.WithFields(log.Fields{
				"table":          table.Identifier,
				"tableCreateSql": createStatement,
			}).Error("table creation failed")
			return fmt.Errorf("failed to create table %q: %w", table.Identifier, err)
		}

		return nil
	}, nil
}

func (s *sqlMaterialization[EC, RC]) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return s.client.DeleteTable(ctx, path)
}

func (s *sqlMaterialization[EC, RC]) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return s.client.TruncateTable(ctx, path)
}

func (s *sqlMaterialization[EC, RC]) MapType(p boilerplate.Projection, fc FieldConfig) (MappedType, boilerplate.ElementConverter) {
	pp := buildProjection(&p.Projection)

	m := s.endpoint.Dialect.MapType(&pp, fc)
	m.MigratableTypes = s.endpoint.Dialect.MigratableTypes

	return m, boilerplate.ElementConverter(m.Converter)
}

func (s *sqlMaterialization[EC, RC]) NewConstraint(p pf.Projection, deltaUpdates bool, fc FieldConfig) pm.Response_Validated_Constraint {
	_, isNumeric := m.AsFormattedNumeric(&p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "All Locations that are part of the collections key are recommended"
	case p.IsRootDocumentProjection() && s.endpoint.NoFlowDocument:
		// When flow_document is disabled, root document projection becomes optional
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Root document projection is optional when flow_document is disabled"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case len(p.Inference.Types) == 0:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"
	case !deltaUpdates && s.endpoint.NoFlowDocument && strings.Count(p.Ptr, "/") == 1 && p.Inference.Exists == pf.Inference_MUST:
		// When flow_document is disabled, all root-level properties become LOCATION_REQUIRED
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Required root-level properties must be present when flow_document is disabled"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "object"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Object fields may be materialized"
	default:
		// Any other case is one where the field is an array or has multiple types.
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field is able to be materialized"
	}

	return constraint
}

func (s *sqlMaterialization[EC, RC]) PopulateInfoSchema(ctx context.Context, paths [][]string, is *boilerplate.InfoSchema) error {
	if s.endpoint.MetaCheckpoints != nil {
		paths = append(paths, s.endpoint.MetaCheckpoints.Path)
	}

	if schemaLister, ok := s.client.(SchemaManager); ok {
		schemas, err := schemaLister.ListSchemas(ctx)
		if err != nil {
			return fmt.Errorf("listing schemas: %w", err)
		}
		for _, schema := range schemas {
			is.PushNamespace(schema)
		}
	}

	return s.client.PopulateInfoSchema(ctx, is, paths)
}

func (s *sqlMaterialization[EC, RC]) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	// Create the checkpoints table if it doesn't already exist & this endpoint
	// needs a checkpoints table.
	var createStatement string
	if s.endpoint.MetaCheckpoints != nil && is.GetResource(s.endpoint.MetaCheckpoints.Path) == nil {
		if resolved, err := ResolveTable(*s.endpoint.MetaCheckpoints, s.endpoint.Dialect); err != nil {
			return "", err
		} else if createStatement, err = RenderTableTemplate(resolved, s.endpoint.CreateTableTemplate); err != nil {
			return "", err
		} else if err := s.client.CreateTable(ctx, TableCreate{
			Table:          resolved,
			TableCreateSql: createStatement,
		}); err != nil {
			return "", fmt.Errorf("creating checkpoints table: %w", err)
		}
	}

	return createStatement, nil
}

func (s *sqlMaterialization[EC, RC]) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existingResource boilerplate.ExistingResource,
	bindingUpdate boilerplate.BindingUpdate[EC, RC, MappedType],
) (string, boilerplate.ActionApplyFn, error) {
	table, err := getTable(s.endpoint, s.materializationName, bindingUpdate.Binding)
	if err != nil {
		return "", nil, err
	}

	getColumn := func(field string) (Column, error) {
		for _, c := range table.Columns() {
			if field == c.Field {
				return *c, nil
			}
		}
		return Column{}, fmt.Errorf("could not find column for field %q in table %s", field, table.Identifier)
	}

	alter := TableAlter{
		Table:        table,
		DropNotNulls: bindingUpdate.NewlyNullableFields,
	}

	for _, newProjection := range bindingUpdate.NewProjections {
		col, err := getColumn(newProjection.Field)
		if err != nil {
			return "", nil, err
		}

		if existingResource.GetField(col.Field+ColumnMigrationTemporarySuffix) != nil {
			// At this stage we don't have the target MappedType anymore, but
			// it's okay because if we don't have the original column anymore
			// (hence the new projection), it means we have already created the
			// new column and set its value.
			alter.ColumnTypeChanges = append(alter.ColumnTypeChanges, ColumnTypeMigration{
				Column:               col,
				ProgressColumnExists: true,
				OriginalColumnExists: false,
			})
			continue
		}
		alter.AddColumns = append(alter.AddColumns, col)
	}

	for _, migrate := range bindingUpdate.FieldsToMigrate {
		col, err := getColumn(migrate.To.Field)
		if err != nil {
			return "", nil, err
		}

		migrationSpec := s.endpoint.Dialect.MigratableTypes.FindMigrationSpec(migrate.From.Type, migrate.To.Mapped.NullableDDL)
		var m = ColumnTypeMigration{
			Column:               col,
			MigrationSpec:        *migrationSpec,
			OriginalColumnExists: true,
			ProgressColumnExists: existingResource.GetField(col.Field+ColumnMigrationTemporarySuffix) != nil,
		}
		alter.ColumnTypeChanges = append(alter.ColumnTypeChanges, m)
	}

	// If there is nothing to do, skip
	if len(alter.AddColumns) == 0 && len(alter.DropNotNulls) == 0 && len(alter.ColumnTypeChanges) == 0 {
		return "", nil, nil
	}

	return s.client.AlterTable(ctx, alter)
}

func (s *sqlMaterialization[EC, RC]) NewMaterializerTransactor(
	ctx context.Context,
	open pm.Request_Open,
	is boilerplate.InfoSchema,
	bindings []boilerplate.MappedBinding[EC, RC, MappedType],
	be *m.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	tables := make([]Table, 0, len(bindings))
	for _, binding := range bindings {
		table, err := getTable(s.endpoint, s.materializationName, binding)
		if err != nil {
			return nil, fmt.Errorf("getting table for binding %d: %w", binding.Index, err)
		}
		table.StateKey = binding.StateKey
		tables = append(tables, table)
	}

	var fence = Fence{
		TablePath:       nil, // Set later iff endpoint.MetaCheckpoints != nil.
		Materialization: open.Materialization.Name,
		KeyBegin:        open.Range.KeyBegin,
		KeyEnd:          open.Range.KeyEnd,
		Fence:           0,
		Checkpoint:      nil, // Set later iff endpoint.MetaCheckpoints != nil.
	}

	if checkpointsShape := s.endpoint.MetaCheckpoints; checkpointsShape != nil {
		// We must install a fence to prevent another (zombie) instances of this
		// materialization from committing further transactions.
		var metaCheckpoints, err = ResolveTable(*checkpointsShape, s.endpoint.Dialect)
		if err != nil {
			return nil, fmt.Errorf("resolving checkpoints table: %w", err)
		}

		// Initialize a checkpoint such that the materialization starts from scratch,
		// regardless of the recovery log checkpoint.
		fence.TablePath = checkpointsShape.Path
		fence.Checkpoint = pm.ExplicitZeroCheckpoint

		fence, err = s.client.InstallFence(ctx, metaCheckpoints, fence)
		if err != nil {
			return nil, fmt.Errorf("installing checkpoints fence: %w", err)
		}
	}

	transactor, err := s.endpoint.NewTransactor(ctx, s.featureFlags, s.endpoint, fence, tables, open, &is, be)
	if err != nil {
		return nil, fmt.Errorf("building transactor: %w", err)
	}

	return &checkpointRecoverer{
		Transactor: transactor,
		cp:         fence.Checkpoint,
	}, nil
}

var _ boilerplate.MaterializerTransactor = &checkpointRecoverer{}

// TODO(whb): This wrapper is a temporary implementation of the new Materializer
// transactor interface, which separates the recovering of a checkpoint with the
// initialization of the Transactor. This should be revisited when we implement
// the pattern of materializations always doing an acknowledgement before Apply.
type checkpointRecoverer struct {
	m.Transactor
	cp []byte
}

func (c *checkpointRecoverer) RecoverCheckpoint(context.Context, pf.MaterializationSpec, pf.RangeSpec) (boilerplate.RuntimeCheckpoint, error) {
	return c.cp, nil
}

func (s *sqlMaterialization[EC, RC]) Close(ctx context.Context) {
	s.client.Close()
}

func getTable[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]](endpoint *Endpoint[EC], materializationName string, binding boilerplate.MappedBinding[EC, RC, MappedType]) (Table, error) {
	path, delta, err := binding.Config.Parameters()
	if err != nil {
		return Table{}, fmt.Errorf("getting parameters for binding %d: %w", binding.Index, err)
	}
	tableShape := BuildTableShape(materializationName, &binding.MaterializationSpec_Binding, binding.Index, path, delta)
	return ResolveTable(tableShape, endpoint.Dialect)
}
