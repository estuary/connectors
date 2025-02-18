package boilerplate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/protocols/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer/protocol"
)

type MaterializeCfg struct {
	Locate                LocatePathFn
	Translate             TranslateFieldFn
	MaxFieldLength        int
	CaseInsensitiveFields bool
	ConcurrentApply       bool
	MaterializeOptions    MaterializeOptions
}

type MappedProjection[MT any] struct {
	pf.Projection
	Mapped MT
}

type MappedBinding[MT any, RC Resourcer] struct {
	pf.MaterializationSpec_Binding
	Config       RC
	Keys, Values []MappedProjection[MT]
	Document     *MappedProjection[MT]
}

func (mb *MappedBinding[MT, RC]) SelectedProjections() []MappedProjection[MT] {
	var out []MappedProjection[MT]
	out = append(out, mb.Keys...)
	out = append(out, mb.Values...)
	if mb.Document != nil {
		out = append(out, *mb.Document)
	}
	return out
}

type MaterializerBindingUpdate[MT any] struct {
	NewProjections      []MappedProjection[MT]
	NewlyNullableFields []ExistingField
	NewlyDeltaUpdates   bool
}

type RuntimeCheckpoint []byte

type MaterializerTransactor interface {
	Open(context.Context, pm.Request_Open) (RuntimeCheckpoint, error)
	m.Transactor
}

type Resourcer interface {
	pb.Validator
	Parameters() (path []string, deltaUpdates bool, err error)
}

type Materializer[
	EC, FC pb.Validator, // endpoint and field-level configurations
	RC Resourcer, // resource-level configuration
	MT any, // mapped type
] interface {
	Config() MaterializeCfg
	PopulateInfoSchema(context.Context, [][]string, *InfoSchema) error
	CheckPrerequisites(context.Context) *cerrors.PrereqErr
	NewConstraint(p pf.Projection, deltaUpdates bool, fieldConfig FC) pm.Response_Validated_Constraint
	MapType(pf.Projection, FC) MT
	Compatible(ExistingField, MT) bool
	DescriptionForType(MT) string
	CreateResource(context.Context, MappedBinding[MT, RC]) (string, ActionApplyFn, error)
	DeleteResource(context.Context, []string) (string, ActionApplyFn, error)
	UpdateResource(context.Context, MaterializerBindingUpdate[MT], ExistingResource, MappedBinding[MT, RC]) (string, ActionApplyFn, error)
	NewMaterializerTransactor(context.Context, InfoSchema, []MappedBinding[MT, RC], *BindingEvents) (MaterializerTransactor, error)
}

type NewMaterializerFn[EC, FC pb.Validator, RC Resourcer, MT any] func(context.Context, EC) (Materializer[EC, FC, RC, MT], error)

func RunSpec(ctx context.Context, req *pm.Request_Spec, docUrl string, endpointSchema, resourceSchema json.RawMessage) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         docUrl,
	}, nil
}

func RunValidate[EC, FC pb.Validator, RC Resourcer, MT any](
	ctx context.Context,
	req *pm.Request_Validate,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
) (*pm.Response_Validated, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var cfg EC
	if err := unmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, err
	}

	materializer, err := newMaterializer(ctx, cfg)
	if err != nil {
		return nil, err
	}

	mCfg := materializer.Config()

	prereqErrs := materializer.CheckPrerequisites(ctx)
	if prereqErrs != nil && prereqErrs.Len() != 0 {
		return nil, cerrors.NewUserError(nil, prereqErrs.Error())
	}

	paths := make([][]string, 0, len(req.Bindings))
	deltas := make([]bool, 0, len(req.Bindings))
	for _, b := range req.Bindings {
		var resCfg RC
		if err := unmarshalStrict(b.ResourceConfigJson, &resCfg); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		} else if path, delta, err := resCfg.Parameters(); err != nil {
			return nil, err
		} else {
			paths = append(paths, path)
			deltas = append(deltas, delta)
		}
	}

	is := initInfoSchema(mCfg)
	if err := materializer.PopulateInfoSchema(ctx, paths, is); err != nil {
		return nil, err
	}

	validator := NewValidator(&constrainterAdapter[EC, FC, RC, MT]{m: materializer}, is, mCfg.MaxFieldLength, mCfg.CaseInsensitiveFields)
	var out []*pm.Response_Validated_Binding
	for idx, b := range req.Bindings {
		path := paths[idx]
		delta := deltas[idx]
		if constraints, err := validator.ValidateBinding(path, delta, b.Backfill, b.Collection, b.FieldConfigJsonMap, req.LastMaterialization); err != nil {
			return nil, fmt.Errorf("validating binding: %w", err)
		} else {
			out = append(out, &pm.Response_Validated_Binding{
				Constraints:  constraints,
				DeltaUpdates: delta,
				ResourcePath: path,
			})
		}
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func RunApply[EC, FC pb.Validator, RC Resourcer, MT any](
	ctx context.Context,
	req *pm.Request_Apply,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
) (*InfoSchema, *pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, nil, fmt.Errorf("validating request: %w", err)
	}

	var cfg EC
	if err := unmarshalStrict(req.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, err
	}

	materializer, err := newMaterializer(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	mCfg := materializer.Config()

	// TODO(whb): Run acknowledge here before applying updates.

	paths := make([][]string, 0, len(req.Materialization.Bindings))
	for _, b := range req.Materialization.Bindings {
		paths = append(paths, b.ResourcePath)
	}

	is := initInfoSchema(mCfg)
	if err := materializer.PopulateInfoSchema(ctx, paths, is); err != nil {
		return nil, nil, err
	}

	computed, err := computeCommonUpdates(req.LastMaterialization, req.Materialization, is)
	if err != nil {
		return nil, nil, err
	}

	actionDescriptions := []string{}
	actions := []ActionApplyFn{}

	addAction := func(desc string, a ActionApplyFn) {
		if a != nil { // Convenience for handling endpoints that return `nil` for a no-op action.
			actionDescriptions = append(actionDescriptions, desc)
			actions = append(actions, a)
		}
	}

	for _, bindingIdx := range computed.newBindings {
		if mapped, err := buildMappedBinding[EC](materializer, *req.Materialization, bindingIdx); err != nil {
			return nil, nil, err
		} else if desc, action, err := materializer.CreateResource(ctx, *mapped); err != nil {
			return nil, nil, fmt.Errorf("getting CreateResource action: %w", err)
		} else {
			addAction(desc, action)
		}
	}

	for _, bindingIdx := range computed.backfillBindings {
		if deleteDesc, deleteAction, err := materializer.DeleteResource(ctx, req.Materialization.Bindings[bindingIdx].ResourcePath); err != nil {
			return nil, nil, fmt.Errorf("getting DeleteResource action to replace resource: %w", err)
		} else if mapped, err := buildMappedBinding[EC](materializer, *req.Materialization, bindingIdx); err != nil {
			return nil, nil, err
		} else if createDesc, createAction, err := materializer.CreateResource(ctx, *mapped); err != nil {
			return nil, nil, fmt.Errorf("getting CreateResource action to replace resource: %w", err)
		} else {
			addAction(deleteDesc+"\n"+createDesc, func(ctx context.Context) error {
				if err := deleteAction(ctx); err != nil {
					return err
				} else if err := createAction(ctx); err != nil {
					return err
				}
				return nil
			})
		}
	}

	for bindingIdx, commonUpdates := range computed.updatedBindings {
		update := MaterializerBindingUpdate[MT]{
			NewlyNullableFields: commonUpdates.NewlyNullableFields,
			NewlyDeltaUpdates:   commonUpdates.NewlyDeltaUpdates,
		}

		mapped, err := buildMappedBinding[EC, FC, RC, MT](materializer, *req.Materialization, bindingIdx)
		if err != nil {
			return nil, nil, err
		}
		ps := mapped.SelectedProjections()

		for _, p := range commonUpdates.NewProjections {
			i := slices.IndexFunc(ps, func(pp MappedProjection[MT]) bool {
				return pp.Field == p.Field
			})
			update.NewProjections = append(update.NewProjections, ps[i])
		}

		if desc, action, err := materializer.UpdateResource(ctx, update, *is.GetResource(mapped.ResourcePath), *mapped); err != nil {
			return nil, nil, err
		} else {
			addAction(desc, action)
		}
	}

	if err := runActions(ctx, actions, mCfg.ConcurrentApply); err != nil {
		return nil, nil, err
	}

	return is, &pm.Response_Applied{ActionDescription: strings.Join(actionDescriptions, "\n")}, nil
}

func RunNewTransactor[EC, FC pb.Validator, RC Resourcer, MT any](
	ctx context.Context,
	req pm.Request_Open,
	be *BindingEvents,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
) (m.Transactor, *pm.Response_Opened, *MaterializeOptions, error) {
	if err := req.Validate(); err != nil {
		return nil, nil, nil, fmt.Errorf("validating request: %w", err)
	}

	var epCfg EC
	if err := unmarshalStrict(req.Materialization.ConfigJson, &epCfg); err != nil {
		return nil, nil, nil, err
	}

	materializer, err := newMaterializer(ctx, epCfg)
	if err != nil {
		return nil, nil, nil, err
	}
	mCfg := materializer.Config()

	paths := make([][]string, 0, len(req.Materialization.Bindings))
	for _, b := range req.Materialization.Bindings {
		paths = append(paths, b.ResourcePath)
	}

	is := initInfoSchema(mCfg)
	if err := materializer.PopulateInfoSchema(ctx, paths, is); err != nil {
		return nil, nil, nil, err
	}

	mapped := make([]MappedBinding[MT, RC], 0, len(req.Materialization.Bindings))
	for bindingIdx, b := range req.Materialization.Bindings {
		var res RC
		if err := unmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, nil, nil, fmt.Errorf("parsing resource config: %w", err)
		} else if m, err := buildMappedBinding[EC, FC, RC, MT](materializer, *req.Materialization, bindingIdx); err != nil {
			return nil, nil, nil, err
		} else {
			mapped = append(mapped, *m)
		}
	}

	mt, err := materializer.NewMaterializerTransactor(ctx, *is, mapped, be)
	if err != nil {
		return nil, nil, nil, err
	}

	checkpoint, err := mt.Open(ctx, req)
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

	return mt, &pm.Response_Opened{RuntimeCheckpoint: cp}, &mCfg.MaterializeOptions, nil
}

func initInfoSchema(cfg MaterializeCfg) *InfoSchema {
	locatePath := func(rp []string) []string { return rp }
	translateField := func(f string) string { return f }
	if cfg.Locate != nil {
		locatePath = cfg.Locate
	}
	if cfg.Translate != nil {
		translateField = cfg.Translate
	}

	return NewInfoSchema(locatePath, translateField)
}

func buildMappedBinding[EC, FC pb.Validator, RC Resourcer, MT any](
	materializer Materializer[EC, FC, RC, MT],
	spec pf.MaterializationSpec,
	idx int,
) (*MappedBinding[MT, RC], error) {
	binding := *spec.Bindings[idx]

	var resCfg RC
	if err := unmarshalStrict(binding.ResourceConfigJson, &resCfg); err != nil {
		return nil, fmt.Errorf("parsing resource config: %w", err)
	}

	mapped := &MappedBinding[MT, RC]{
		MaterializationSpec_Binding: binding,
		Config:                      resCfg,
	}

	var do = func(dst *[]MappedProjection[MT], fields []string) error {
		for _, f := range fields {
			p := binding.Collection.GetProjection(f)

			var fieldCfg FC
			if raw := binding.FieldSelection.FieldConfigJsonMap[f]; raw != nil {
				if err := unmarshalStrict(raw, &fieldCfg); err != nil {
					return fmt.Errorf("unmarshalling field config json: %w", err)
				}
			}

			*dst = append(*dst, MappedProjection[MT]{
				Projection: *p,
				Mapped:     materializer.MapType(*p, fieldCfg),
			})
		}
		return nil
	}

	if err := do(&mapped.Keys, binding.FieldSelection.Keys); err != nil {
		return nil, err
	} else if err := do(&mapped.Values, binding.FieldSelection.Values); err != nil {
		return nil, err
	}

	if field := binding.FieldSelection.Document; field != "" {
		var doc []MappedProjection[MT]
		if err := do(&doc, []string{field}); err != nil {
			return nil, err
		}
		mapped.Document = &doc[0]
	}

	return mapped, nil
}

type constrainterAdapter[EC, FC pb.Validator, RC Resourcer, MT any] struct {
	m Materializer[EC, FC, RC, MT]
}

func (c *constrainterAdapter[EC, FC, RC, MT]) NewConstraints(p *pf.Projection, deltaUpdates bool, rawFieldConfig json.RawMessage) (*pm.Response_Validated_Constraint, error) {
	var fieldCfg FC
	if len(rawFieldConfig) > 0 {
		if err := unmarshalStrict(rawFieldConfig, &fieldCfg); err != nil {
			return nil, err
		}
	}

	cc := c.m.NewConstraint(*p, deltaUpdates, fieldCfg)
	return &cc, nil
}

func (c *constrainterAdapter[EC, FC, RC, MT]) Compatible(existing ExistingField, proposed *pf.Projection, rawFieldConfig json.RawMessage) (bool, error) {
	var fieldCfg FC
	if len(rawFieldConfig) > 0 {
		if err := unmarshalStrict(rawFieldConfig, &fieldCfg); err != nil {
			return false, err
		}
	}

	return c.m.Compatible(existing, c.m.MapType(*proposed, fieldCfg)), nil
}

func (c *constrainterAdapter[EC, FC, RC, MT]) DescriptionForType(p *pf.Projection, rawFieldConfig json.RawMessage) (string, error) {
	var fieldCfg FC
	if len(rawFieldConfig) > 0 {
		if err := unmarshalStrict(rawFieldConfig, &fieldCfg); err != nil {
			return "", err
		}
	}

	return c.m.DescriptionForType(c.m.MapType(*p, fieldCfg)), nil
}

func unmarshalStrict[T pb.Validator](raw []byte, into *T) error {
	var d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()

	if err := d.Decode(into); err != nil {
		return err
	}

	return (*into).Validate()
}
