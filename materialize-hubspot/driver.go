package hubspot

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
)

var (
	flowPropertyGroupName = "flow"

	// flowPropertyGroup is the property group created to hold properties
	// created by the materialization.
	//
	// Property groups are per CRMObject and all properties must be assigned to
	// a group, so this group is created for each object that is materialized.
	flowPropertyGroup = &PropertyGroup{
		Name:         flowPropertyGroupName,
		Label:        "Estuary properties",
		DisplayOrder: DisplayOrderLast,
	}
)

type Driver struct{}

var _ boilerplate.Connector = (*Driver)(nil)

func (*Driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("HubSpot", &Config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("HubSpot Objects", &Resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-hubspot",
		Oauth2:                   OAuth2Spec(),
	}, nil
}

// Similar to materialize.RunValidate, but with a different Validator and Schema type.
func (d *Driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var cfg *Config
	if err := boilerplate.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, err
	}

	parsedFlags := boilerplate.ParseFlags(cfg)
	materializer, err := newMaterialization(ctx, req.Name.String(), cfg, parsedFlags)
	if err != nil {
		return nil, err
	}
	defer materializer.Close(ctx)

	prereqErrs := materializer.CheckPrerequisites(ctx)
	if prereqErrs != nil && prereqErrs.Len() != 0 {
		return nil, cerrors.NewUserError(nil, prereqErrs.Error())
	}

	resources := make([]Resource, 0, len(req.Bindings))
	for _, b := range req.Bindings {
		var resource Resource
		if err := boilerplate.UnmarshalStrict(b.ResourceConfigJson, &resource); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		resources = append(resources, resource)
	}

	schema, err := materializer.LoadSchema(ctx, resources)
	if err != nil {
		return nil, fmt.Errorf("unable to load schema: %w", err)
	}

	validator := NewValidator(schema)
	var out []*pm.Response_Validated_Binding
	for idx, b := range req.Bindings {
		resource := resources[idx]

		path, err := resource.Path()
		if err != nil {
			return nil, err
		}

		if constraints, err := validator.ValidateBinding(resource, b, req.LastMaterialization); err != nil {
			return nil, fmt.Errorf("validating binding: %w", err)
		} else {
			out = append(out, &pm.Response_Validated_Binding{
				CaseInsensitiveFields: false,
				Constraints:           constraints,
				DeltaUpdates:          true,
				ResourcePath:          path,
				SerPolicy:             nil,
			})
		}
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (*Driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var cfg *Config
	if err := boilerplate.UnmarshalStrict(req.Materialization.ConfigJson, &cfg); err != nil {
		return nil, err
	}

	parsedFlags := boilerplate.ParseFlags(cfg)
	materializer, err := newMaterialization(ctx, req.Materialization.Name.String(), cfg, parsedFlags)
	if err != nil {
		return nil, err
	}
	defer materializer.Close(ctx)

	resources := make([]Resource, 0, len(req.Materialization.Bindings))
	for _, b := range req.Materialization.Bindings {
		var resource Resource
		if err := boilerplate.UnmarshalStrict(b.ResourceConfigJson, &resource); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		resources = append(resources, resource)
	}

	schema, err := materializer.LoadSchema(ctx, resources)
	if err != nil {
		return nil, fmt.Errorf("unable to load schema: %w", err)
	}

	mapped := make([]*mappedBinding, 0, len(req.Materialization.Bindings))
	for idx, b := range req.Materialization.Bindings {
		mappedBinding, err := buildMappedBinding(resources[idx], b)
		if err != nil {
			return nil, err
		}

		mapped = append(mapped, mappedBinding)
	}

	actions := []string{}
	for _, b := range mapped {
		for _, field := range b.fields {
			if _, ok := schema.GetProperty(b.object, field.Property.Name); !ok {
				if !schema.HasPropertyGroup(b.object, flowPropertyGroupName) {
					actions = append(actions, fmt.Sprintf("Create Property Group %q", flowPropertyGroup.Name))
					if err := materializer.CreatePropertyGroup(ctx, b.object, flowPropertyGroup); err != nil {
						return nil, err
					}

					schema.AddPropertyGroup(b.object, flowPropertyGroup)
				}

				actions = append(actions, fmt.Sprintf("Create Property %q", field.Property.Name))

				err := materializer.CreateProperty(ctx, b.object, field.Property)
				if err != nil {
					return nil, fmt.Errorf("unable to create property for field %q: %w", field.Property.Name, err)
				}
			}
		}
	}

	return &pm.Response_Applied{
		ActionDescription: strings.Join(actions, "\n"),
	}, nil
}

// mappedBinding is a binding free from the actual schema with which it may or
// may not be compatible.
type mappedBinding struct {
	object   CRMObject
	fields   []*MappedField
	docField *MappedField
}

func (m *mappedBinding) Key() *MappedField {
	return m.fields[0]
}

type materialization struct {
	client *Client
	config *Config
}

func (m *materialization) LoadSchema(ctx context.Context, resources []Resource) (*Schema, error) {
	return LoadSchema(ctx, m.client, resources)
}

func (m *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	now := time.Now()
	_, err := m.client.AccessToken(ctx, now)
	if err != nil {
		errs.Err(err)
	}

	return errs
}

func (m *materialization) CreatePropertyGroup(ctx context.Context, object CRMObject, group *PropertyGroup) error {
	return m.client.CreatePropertyGroup(ctx, object, group)
}

func (m *materialization) CreateProperty(ctx context.Context, object CRMObject, property *Property) error {
	return m.client.CreateProperty(ctx, object, property)
}

func (m *materialization) NewTransactor(
	ctx context.Context,
	req pm.Request_Open,
	schema *Schema,
	mappedBindings []*mappedBinding,
	be *m.BindingEvents,
) (m.Transactor, error) {
	var bindings []*binding
	for _, mapped := range mappedBindings {
		idProperty, ok := schema.GetProperty(mapped.object, mapped.Key().Property.Name)
		if !ok {
			return nil, fmt.Errorf("property not found: %q", mapped.Key().Property.Name)
		}

		properties := schema.CRM.Objects[mapped.object].Properties

		log.WithFields(log.Fields{
			"object":          mapped.object,
			"id_property":     idProperty.Name,
			"unique_property": idProperty.HasUniqueValue,
		}).Info("add binding")

		bindings = append(bindings, &binding{
			object:     mapped.object,
			properties: properties,
			idProperty: idProperty,
			fields:     mapped.fields,
			docField:   mapped.docField,
		})
	}

	return &transactor{
		client:   m.client,
		bindings: bindings,
	}, nil
}

func (m *materialization) Close(context.Context) {
	m.client.Close()
}

func newMaterialization(
	ctx context.Context,
	materializationName string,
	config *Config,
	featureFlags map[string]bool,
) (*materialization, error) {
	client, err := NewClient(config.Credentials, config.Limiter(), config.SearchLimiter())
	if err != nil {
		return nil, err
	}

	return &materialization{
		client: client,
		config: config,
	}, nil
}

func (*Driver) NewTransactor(
	ctx context.Context,
	req pm.Request_Open,
	be *m.BindingEvents,
) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	if err := req.Validate(); err != nil {
		return nil, nil, nil, fmt.Errorf("validating request: %w", err)
	}

	var cfg *Config
	if err := boilerplate.UnmarshalStrict(req.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, nil, err
	}

	featureFlags := boilerplate.ParseFlags(cfg)
	materializer, err := newMaterialization(ctx, req.Materialization.Name.String(), cfg, featureFlags)
	if err != nil {
		return nil, nil, nil, err
	}

	resources := make([]Resource, 0, len(req.Materialization.Bindings))
	for _, b := range req.Materialization.Bindings {
		var resource Resource
		if err := boilerplate.UnmarshalStrict(b.ResourceConfigJson, &resource); err != nil {
			return nil, nil, nil, fmt.Errorf("parsing resource config: %w", err)
		}

		resources = append(resources, resource)
	}

	schema, err := materializer.LoadSchema(ctx, resources)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to load schema: %w", err)
	}

	mapped := make([]*mappedBinding, 0, len(req.Materialization.Bindings))
	for idx, b := range req.Materialization.Bindings {
		mappedBinding, err := buildMappedBinding(resources[idx], b)
		if err != nil {
			return nil, nil, nil, err
		}

		mapped = append(mapped, mappedBinding)
	}

	transactor, err := materializer.NewTransactor(ctx, req, schema, mapped, be)
	if err != nil {
		return nil, nil, nil, err
	}

	checkpoint, err := transactor.RecoverCheckpoint(ctx, *req.Materialization, *req.Range)
	if err != nil {
		return nil, nil, nil, err
	}

	var cp *protocol.Checkpoint
	if len(checkpoint) > 0 {
		cp = new(protocol.Checkpoint)
		if err := cp.Unmarshal(checkpoint); err != nil {
			return nil, nil, nil, fmt.Errorf("unmarshalling checkpoint: %w", err)
		}
	}

	return transactor, &pm.Response_Opened{
		RuntimeCheckpoint:       cp,
		DisableLoadOptimization: true,
	}, nil, nil
}

// The MappedBinding shouldn't require the info schema, because the values
// don't necessarily exist so we don't depend on them being any particular way.
//
// MaterializationSpec_Binding has a field selection.
//
// A Request_Validate_Binding is similar but doesn't have the field selection
// and some other pieces of information.
func buildMappedBinding(resource Resource, b *pf.MaterializationSpec_Binding) (*mappedBinding, error) {
	object, err := resource.CRMObject()
	if err != nil {
		return nil, err
	}

	projections := make(map[string]pf.Projection, len(b.Collection.Projections))
	for _, proj := range b.Collection.Projections {
		projections[proj.Field] = proj
	}

	fields := append(b.FieldSelection.Keys, b.FieldSelection.Values...)

	mappedBinding := &mappedBinding{
		object: object,
		fields: make([]*MappedField, 0, len(fields)),
	}

	mapField := func(field string) (*MappedField, error) {
		var fieldConfig FieldConfig
		if raw := b.FieldSelection.FieldConfigJsonMap[field]; raw != nil {
			if err := boilerplate.UnmarshalStrict(raw, &fieldConfig); err != nil {
				return nil, fmt.Errorf("parsing field config json: %w", err)
			}
		}
		projection := projections[field]
		return NewMappedField(projection, fieldConfig)
	}

	for _, field := range fields {
		mapped, err := mapField(field)
		if err != nil {
			return nil, err
		}
		mappedBinding.fields = append(mappedBinding.fields, mapped)
	}
	if doc := b.FieldSelection.Document; doc != "" {
		mapped, err := mapField(b.FieldSelection.Document)
		if err != nil {
			return nil, err
		}
		mappedBinding.docField = mapped
	}

	return mappedBinding, nil
}
