package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/protocols/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/pingcap/log"
)

var _ boilerplate.Connector = &driver{}
var _ boilerplate.Materializer[config, fieldConfig, resource, mappedProperty] = &driver{}

type driver struct {
	cfg    config
	client *client
}

func newDriver(ctx context.Context, cfg config) (boilerplate.Materializer[config, fieldConfig, resource, mappedProperty], error) {
	client, err := cfg.toClient(true)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &driver{
		cfg:    cfg,
		client: client,
	}, nil
}

func (d *driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("configSchema", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("resourceConfigSchema", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-elasticsearch", endpointSchema, resourceSchema)
}

func (d *driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newDriver)
}

func (d *driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	_, res, err := boilerplate.RunApply(ctx, req, newDriver)
	return res, err
}

func (d *driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newDriver)
}

func (d *driver) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		ConcurrentApply: true,
		Translate:       translateField,
	}
}

func (d *driver) PopulateInfoSchema(ctx context.Context, resourcePaths [][]string, is *boilerplate.InfoSchema) error {
	res, err := d.client.es.Indices.Get(
		[]string{"*"},
		d.client.es.Indices.Get.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("getting index metadata: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("getting index metadata error response [%s] %s", res.Status(), res.String())
	}

	type indexMetaResponse struct {
		Mappings struct {
			Properties map[string]mappedProperty `json:"properties"`
		} `json:"mappings"`
	}

	var indexMeta map[string]indexMetaResponse
	if err := json.NewDecoder(res.Body).Decode(&indexMeta); err != nil {
		return fmt.Errorf("decoding index metadata response: %w", err)
	}

	for index, meta := range indexMeta {
		pr := is.PushResource(index)
		for field, prop := range meta.Mappings.Properties {
			pr.PushField(boilerplate.ExistingField{
				Name:     field,
				Nullable: true,
				Type:     string(prop.Type),
			})
		}
	}

	return nil
}

func (d *driver) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr { return nil }

func (d *driver) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
	_, isNumeric := boilerplate.AsFormattedNumeric(&p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case len(p.Inference.Types) == 0:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"
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

func (d *driver) MapType(p pf.Projection, fc fieldConfig) mappedProperty {
	return propForProjection(p, p.Inference.Types, fc)
}

func (d *driver) Compatible(existing boilerplate.ExistingField, proposed mappedProperty) bool {
	return strings.EqualFold(existing.Type, string(proposed.Type))
}

func (d *driver) DescriptionForType(prop mappedProperty) string {
	return string(prop.Type)
}

func (d *driver) CreateResource(ctx context.Context, res boilerplate.MappedBinding[mappedProperty, resource]) (string, boilerplate.ActionApplyFn, error) {
	props := make(map[string]mappedProperty)
	for _, p := range res.SelectedProjections() {
		props[translateField(p.Field)] = p.Mapped
	}

	index := res.ResourcePath[0]
	return fmt.Sprintf("create index %q", index), func(ctx context.Context) error {
		return d.client.createIndex(ctx, index, res.Config.Shards, d.cfg.Advanced.Replicas, props)
	}, nil
}

func (d *driver) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	index := resourcePath[0]
	return fmt.Sprintf("delete index %q", index), func(ctx context.Context) error {
		return d.client.deleteIndex(ctx, index)
	}, nil
}

func (d *driver) UpdateResource(
	ctx context.Context,
	update boilerplate.MaterializerBindingUpdate[mappedProperty],
	existing boilerplate.ExistingResource,
	res boilerplate.MappedBinding[mappedProperty, resource],
) (string, boilerplate.ActionApplyFn, error) {
	var actions []string
	for _, p := range update.NewProjections {
		actions = append(actions, fmt.Sprintf(
			"add mapping %q to index %q with type %q",
			translateField(p.Field),
			res.Config.Index,
			p.Mapped.Type,
		))
	}

	return strings.Join(actions, "\n"), func(ctx context.Context) error {
		for _, p := range update.NewProjections {
			if err := d.client.addMappingToIndex(ctx, res.Config.Index, translateField(p.Field), p.Mapped); err != nil {
				return err
			}
		}

		return nil
	}, nil
}

func (d *driver) NewMaterializerTransactor(
	ctx context.Context,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[mappedProperty, resource],
	be *boilerplate.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	isServerless, err := d.client.isServerless(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting serverless status: %w", err)
	}
	if isServerless {
		log.Info("connected to a serverless elasticsearch cluster")
	}

	indexToBinding := make(map[string]int)
	var bindings []binding
	for idx, b := range mappedBindings {
		fields := append(b.FieldSelection.Keys, b.FieldSelection.Values...)
		floatFields := make([]bool, len(fields))
		wrapFields := make([]bool, len(fields))
		for idx, p := range b.SelectedProjections() {
			if p.IsRootDocumentProjection() {
				continue
			}

			fields[idx] = translateField(p.Field)
			if p.Mapped.Type == elasticTypeDouble {
				floatFields[idx] = true
			} else if mustWrapAndFlatten(p.Projection) {
				wrapFields[idx] = true
			}
		}

		indexToBinding[b.ResourcePath[0]] = idx
		bindings = append(bindings, binding{
			index:        b.ResourcePath[0],
			deltaUpdates: b.DeltaUpdates,
			fields:       fields,
			floatFields:  floatFields,
			wrapFields:   wrapFields,
			docField:     translateField(b.FieldSelection.Document),
		})
	}

	var transactor = &transactor{
		hardDelete:     d.cfg.HardDelete,
		client:         d.client,
		bindings:       bindings,
		isServerless:   isServerless,
		indexToBinding: indexToBinding,
	}
	return transactor, nil
}
