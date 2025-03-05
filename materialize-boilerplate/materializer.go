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
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer/protocol"
)

// MaterializeCfg represents common configuration options for how a
// materialization operates.
type MaterializeCfg struct {
	// Locate takes a Flow resource path and outputs an equal-length string
	// slice containing translated location components that can be used to find
	// the resource in the InfoSchema. These translated components may represent
	// the name of a schema and table in a SQL endpoint, for example. It is
	// recommended that materializations do any sanitization of the resource
	// paths as part of the Validated response, so that the path of an Applied
	// resource does not need further sanitization and will match the value from
	// the InfoSchema. In this case Locate is not needed, and it mostly exists
	// as a compatibility for materializations that haven't historically
	// sanitized paths in Validate.
	Locate LocatePathFn

	// TranslateFieldFn takes a Flow field name and outputs a translated string
	// that can be used to find the field in the InfoSchema, with translations
	// applied in a similar way as LocatePathFn. It is relatively common for
	// some kind of translation to need to be done if the destination does not
	// support special characters vs. those supported by Flow field names, or if
	// the destination lowercases all column identifiers, for example.
	Translate TranslateFieldFn

	// MaxFieldLength is used to produce "forbidden" constraints on field names
	// that are too long. If maxFieldLength is 0, no constraints are enforced.
	// The length of a field name is in terms of characters, not bytes.
	MaxFieldLength int

	// CaseInsensitiveFields is used to indicate if fields that differ only in
	// capitalization will conflict in the materialized resource. For example,
	// "thisfield" and "thisField" may have their capitalization preserved from
	// the InfoSchema, but the materialized resource creation will still result
	// in an error due to conflicts if both are included. If enabled, fields
	// that differ only in capitalization will be constrained as optional, with
	// only 1 of them being allowed to be selected for materialization.
	CaseInsensitiveFields bool

	// ConcurrentApply of Apply actions, for system that may benefit from a
	// scatter/gather strategy for changing many resources in a single Apply
	// RPC.
	ConcurrentApply bool

	// NoCreateNamespaces indicates that this materialization does not support a
	// concept of a "namespace" (commonly a "schema" for SQL databases), and so
	// namespaces should not be listed or created if missing. If false (the
	// default) namespaces will be created if they don't exist. By convention,
	// the second to the last component of the resource path is assumed to be
	// the namespace.
	NoCreateNamespaces bool

	// MaterializeOptions is a proxy for the options from boilerplate.go.
	// Eventually this should be consolidated.
	MaterializeOptions MaterializeOptions
}

// ElementConverter maps from a TupleElement into a runtime type instance that's
// compatible with the materialization.
type ElementConverter func(tuple.TupleElement) (any, error)

// MappedProjection adds materialization-specific type mapping information to a
// basic Flow projection, as well as other useful metadata.
type MappedProjection[MT any] struct {
	pf.Projection
	Comment string
	Mapped  MT
}

// MappedBinding is all of the projections for the selected fields of the
// materialization, with materialization-specific type mapping and parsed
// resource configuration.
type MappedBinding[EC pb.Validator, RC Resourcer[RC, EC], MT any] struct {
	pf.MaterializationSpec_Binding
	Config       RC
	Keys, Values []MappedProjection[MT]
	Document     *MappedProjection[MT]

	converters []ElementConverter
}

// SelectedProjections returns all of the projections for the selected fields of
// the materialization in a single slice as a convenience method.
func (mb *MappedBinding[EC, RC, MT]) SelectedProjections() []MappedProjection[MT] {
	var out []MappedProjection[MT]
	out = append(out, mb.Keys...)
	out = append(out, mb.Values...)
	if mb.Document != nil {
		out = append(out, *mb.Document)
	}
	return out
}

func (mb *MappedBinding[EC, RC, MT]) ConvertKey(key tuple.Tuple) ([]any, error) {
	return mb.convertTuple(key, 0, make([]any, 0, len(mb.Keys)))
}

func (mb *MappedBinding[EC, RC, MT]) ConvertAll(key, values tuple.Tuple, doc json.RawMessage) ([]any, error) {
	var err error
	out := make([]any, 0, len(mb.Keys)+len(mb.Values)+1)

	if out, err = mb.convertTuple(key, 0, out); err != nil {
		return nil, err
	} else if out, err = mb.convertTuple(values, len(mb.Keys), out); err != nil {
		return nil, err
	} else if mb.Document != nil {
		if out, err = mb.convertTuple(tuple.Tuple{doc}, len(mb.Keys)+len(mb.Values), out); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (mb *MappedBinding[EC, RC, MT]) convertTuple(in tuple.Tuple, offset int, out []any) ([]any, error) {
	for idx, val := range in {
		converted := val
		if converter := mb.converters[idx+offset]; converter != nil {
			var err error
			if converted, err = converter(val); err != nil {
				return nil, fmt.Errorf("converting value for field %s of binding %s: %w", mb.Keys[idx].Field, mb.ResourcePath, err)
			}
		}
		out = append(out, converted)
	}

	return out, nil
}

// MaterializerBindingUpdate is a distilled representation of the typical kinds
// of changes a destination system will care about in response to a new binding
// or change to an existing binding.
type MaterializerBindingUpdate[MT any] struct {
	NewProjections      []MappedProjection[MT]
	NewlyNullableFields []ExistingField
	NewlyDeltaUpdates   bool
}

// RuntimeCheckpoint is the raw bytes of a persisted Flow checkpoint. In the
// `Opened` response, it will be marshalled into a protocol.Checkpoint.
type RuntimeCheckpoint []byte

// MaterializerTransactor adds the RecoverCheckpoint method to the traditional
// "Transactor" interface. Eventually we should consolidate these two
// interfaces.
type MaterializerTransactor interface {
	// RecoverCheckpoint specifically retrieves the last persisted checkpoint in
	// the destination system. Systems that do not use the "authoritative
	// endpoint" pattern to persist a checkpoint should return `nil` for
	// RuntimeCheckpoint.
	RecoverCheckpoint(context.Context, pf.MaterializationSpec, pf.RangeSpec) (RuntimeCheckpoint, error)
	m.Transactor
}

// Resourcer represents a parsed resource config.
type Resourcer[T any, EC pb.Validator] interface {
	pb.Validator

	// WithDefaults provides the parsed endpoint config so that any defaults
	// from the endpoint config can be set on the resource, for example a
	// default "schema" to use if the corresponding value is absent from the
	// resource config.
	WithDefaults(EC) T

	// Parameters provides the basic information to Validate a resource.
	Parameters() (path []string, deltaUpdates bool, err error)
}

// FieldConfiger represents a parsed field config.
type FieldConfiger interface {
	pb.Validator
	// CastToString is a common field configuration option that will cause a
	// field to be converted to a string when it is materialized.
	CastToString() bool
}

// Materializer is everything a fully-featured materialization needs to be
// capable of. This includes producing constraints for newly selected
// projections, validating changes to resource specs, converging the state of
// existing resources to resource specs, and managing the transactions
// lifecycle.
type Materializer[
	EC pb.Validator, // endpoint config
	FC FieldConfiger, // field config
	RC Resourcer[RC, EC], // resource config
	MT any, // mapped type
] interface {
	// Config should return a MaterializeCfg with non-defaults populated as
	// needed.
	Config() MaterializeCfg

	// PopulateInfoSchema adds existing resources and fields to the initialized
	// InfoSchema.
	PopulateInfoSchema(context.Context, [][]string, *InfoSchema) error

	// CheckPrerequisites generally performs user input validation in terms of
	// making sure the configured endpoint is reachable by pinging it, verifying
	// that a configured bucket can be written to / read from, etc.
	CheckPrerequisites(context.Context) *cerrors.PrereqErr

	// NewConstraint calculates the constraint for a new projection.
	NewConstraint(p pf.Projection, deltaUpdates bool, fieldConfig FC) pm.Response_Validated_Constraint

	// MapType maps a projection and its field configuration into a
	// materialization-specific type and ElementConverter to convert it to an
	// appropriate value for the destination. ElementConverter can be `nil` if
	// no conversion is needed.
	MapType(p Projection, fieldCfg FC) (MT, ElementConverter)

	// Compatible determines if an existing materialized field as reported in
	// the InfoSchema is compatible with a proposed mapped projection.
	Compatible(ExistingField, MT) bool

	// DescriptionForType produces a description for a mapped projection.
	DescriptionForType(MT) string

	// CreateNamespace creates a namespace in the destination system, for
	// example a "schema" in a SQL database.
	CreateNamespace(context.Context, string) (string, error)

	// CreateResource creates a new resource in the endpoint. It is called only
	// if the resource does not already exist, either because it is brand new or
	// because it was previously deleted as part of backfilling a binding.
	CreateResource(context.Context, MappedBinding[EC, RC, MT]) (string, ActionApplyFn, error)

	// DeleteResource deletes a resource from the endpoint. It is used for
	// replacing a materialized resource when the `backfill` counter is
	// incremented. It will only be called if the materialized resource exists
	// in the destination system, the resource exists in the prior spec, and the
	// backfill counter of the new spec is greater than the prior spec.
	DeleteResource(context.Context, []string) (string, ActionApplyFn, error)

	// UpdateResource updates an existing resource. The
	// MaterializerBindingUpdate contains specific information about what is
	// changing for the resource. It's called for every binding on every Apply,
	// even if there are no pre-computed updates. This is to allow
	// materializations to perform additional specific actions on binding
	// changes that are not covered by the general cases.
	UpdateResource(context.Context, []string, ExistingResource, MaterializerBindingUpdate[MT]) (string, ActionApplyFn, error)

	// NewMaterializerTransactor builds a new transactor for handling the
	// transactions lifecycle of the materialization.
	NewMaterializerTransactor(context.Context, pm.Request_Open, InfoSchema, []MappedBinding[EC, RC, MT], *BindingEvents) (MaterializerTransactor, error)
}

type NewMaterializerFn[EC pb.Validator, FC FieldConfiger, RC Resourcer[RC, EC], MT any] func(context.Context, EC) (Materializer[EC, FC, RC, MT], error)

// RunSpec produces a spec response from the typical inputs of documentation
// url, endpoint config schema, and resource config schema.
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

// RunValidate produces a Validated response for a Validate request.
func RunValidate[EC pb.Validator, FC FieldConfiger, RC Resourcer[RC, EC], MT any](
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
	deltaUpdates := make([]bool, 0, len(req.Bindings))
	for _, b := range req.Bindings {
		var resCfg RC
		if err := unmarshalStrict(b.ResourceConfigJson, &resCfg); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		res := resCfg.WithDefaults(cfg)
		if path, delta, err := res.Parameters(); err != nil {
			return nil, err
		} else {
			paths = append(paths, path)
			deltaUpdates = append(deltaUpdates, delta)
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
		delta := deltaUpdates[idx]
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

// RunApply produces an Applied response for an Apply request. In addition, the
// initialized Materializer and populated InfoSchema is returned so that
// materializations can do other things after the standard apply actions are
// completed, such as creating additional metadata tables for checkpoints.
func RunApply[T Materializer[EC, FC, RC, MT], EC pb.Validator, FC FieldConfiger, RC Resourcer[RC, EC], MT any](
	ctx context.Context,
	req *pm.Request_Apply,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
) (T, *InfoSchema, *pm.Response_Applied, error) {
	var materializer T
	if err := req.Validate(); err != nil {
		return materializer, nil, nil, fmt.Errorf("validating request: %w", err)
	}

	var endpointCfg EC
	if err := unmarshalStrict(req.Materialization.ConfigJson, &endpointCfg); err != nil {
		return materializer, nil, nil, err
	}

	var err error
	nm, err := newMaterializer(ctx, endpointCfg)
	if err != nil {
		return materializer, nil, nil, err
	}

	// TODO(whb): Some point soon we will have the last committed checkpoint
	// available here in the Apply message, and should start calling Unmarshal
	// State + Acknowledge somewhere around here to commit any previously staged
	// transaction before applying the next spec's updates.

	// TODO(whb): This type assertion is not very satisfying but I haven't
	// figured out a way to do it more cleanly.
	materializer = nm.(T)
	mCfg := materializer.Config()

	paths := make([][]string, 0, len(req.Materialization.Bindings))
	for _, b := range req.Materialization.Bindings {
		paths = append(paths, b.ResourcePath)
	}

	is := initInfoSchema(mCfg)
	if err := materializer.PopulateInfoSchema(ctx, paths, is); err != nil {
		return materializer, nil, nil, err
	}

	computed, err := computeCommonUpdates(req.LastMaterialization, req.Materialization, is)
	if err != nil {
		return materializer, nil, nil, err
	}

	actionDescriptions := []string{}
	actions := []ActionApplyFn{}

	if !mCfg.NoCreateNamespaces {
		// Create any required namespaces before other actions, which may
		// include resource creation. Otherwise resources creation may fail due
		// to namespaces not yet existing.
		requiredNamespaces := make(map[string]struct{})
		for _, b := range req.Materialization.Bindings {
			path := is.locatePath(b.ResourcePath)
			if len(path) < 2 {
				continue
			}
			requiredNamespaces[path[len(path)-2]] = struct{}{}
		}

		for ns := range requiredNamespaces {
			if slices.Contains(is.namespaces, ns) {
				continue
			} else if desc, err := materializer.CreateNamespace(ctx, ns); err != nil {
				return materializer, nil, nil, err
			} else {
				actionDescriptions = append(actionDescriptions, desc)
			}
		}
	}

	addAction := func(desc string, a ActionApplyFn) {
		if a != nil { // Convenience for handling endpoints that return `nil` for a no-op action.
			actionDescriptions = append(actionDescriptions, desc)
			actions = append(actions, a)
		}
	}

	for _, bindingIdx := range computed.newBindings {
		if mapped, err := buildMappedBinding(endpointCfg, materializer, *req.Materialization, bindingIdx); err != nil {
			return materializer, nil, nil, err
		} else if desc, action, err := materializer.CreateResource(ctx, *mapped); err != nil {
			return materializer, nil, nil, fmt.Errorf("getting CreateResource action: %w", err)
		} else {
			addAction(desc, action)
		}
	}

	for _, bindingIdx := range computed.backfillBindings {
		if deleteDesc, deleteAction, err := materializer.DeleteResource(ctx, req.Materialization.Bindings[bindingIdx].ResourcePath); err != nil {
			return materializer, nil, nil, fmt.Errorf("getting DeleteResource action to replace resource: %w", err)
		} else if mapped, err := buildMappedBinding(endpointCfg, materializer, *req.Materialization, bindingIdx); err != nil {
			return materializer, nil, nil, err
		} else if createDesc, createAction, err := materializer.CreateResource(ctx, *mapped); err != nil {
			return materializer, nil, nil, fmt.Errorf("getting CreateResource action to replace resource: %w", err)
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

		mapped, err := buildMappedBinding(endpointCfg, materializer, *req.Materialization, bindingIdx)
		if err != nil {
			return materializer, nil, nil, err
		}
		ps := mapped.SelectedProjections()

		for _, p := range commonUpdates.NewProjections {
			i := slices.IndexFunc(ps, func(pp MappedProjection[MT]) bool {
				return pp.Field == p.Field
			})
			update.NewProjections = append(update.NewProjections, ps[i])
		}

		if desc, action, err := materializer.UpdateResource(ctx, mapped.ResourcePath, *is.GetResource(mapped.ResourcePath), update); err != nil {
			return materializer, nil, nil, err
		} else {
			addAction(desc, action)
		}
	}

	if err := runActions(ctx, actions, actionDescriptions, mCfg.ConcurrentApply); err != nil {
		return materializer, nil, nil, err
	}

	return materializer, is, &pm.Response_Applied{ActionDescription: strings.Join(actionDescriptions, "\n")}, nil
}

// RunNewTransactor builds a transactor and Opened response from an Open
// request.
func RunNewTransactor[EC pb.Validator, FC FieldConfiger, RC Resourcer[RC, EC], MT any](
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

	mapped := make([]MappedBinding[EC, RC, MT], 0, len(req.Materialization.Bindings))
	for bindingIdx := range req.Materialization.Bindings {
		if m, err := buildMappedBinding(epCfg, materializer, *req.Materialization, bindingIdx); err != nil {
			return nil, nil, nil, err
		} else {
			mapped = append(mapped, *m)
		}
	}

	transactor, err := materializer.NewMaterializerTransactor(ctx, req, *is, mapped, be)
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

	return transactor, &pm.Response_Opened{RuntimeCheckpoint: cp}, &mCfg.MaterializeOptions, nil
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

func buildMappedBinding[EC pb.Validator, FC FieldConfiger, RC Resourcer[RC, EC], MT any](
	endpointCfg EC,
	materializer Materializer[EC, FC, RC, MT],
	spec pf.MaterializationSpec,
	idx int,
) (*MappedBinding[EC, RC, MT], error) {
	binding := *spec.Bindings[idx]

	var resCfg RC
	if err := unmarshalStrict(binding.ResourceConfigJson, &resCfg); err != nil {
		return nil, fmt.Errorf("parsing resource config: %w", err)
	}

	mapped := &MappedBinding[EC, RC, MT]{
		MaterializationSpec_Binding: binding,
		Config:                      resCfg.WithDefaults(endpointCfg),
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

			mt, converter := materializer.MapType(mapProjection(*p, fieldCfg), fieldCfg)
			*dst = append(*dst, MappedProjection[MT]{
				Projection: *p,
				Comment:    commentForProjection(*p),
				Mapped:     mt,
			})
			mapped.converters = append(mapped.converters, converter)
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

func commentForProjection(p pf.Projection) string {
	var out string

	var source = "auto-generated"
	if p.Explicit {
		source = "user-provided"
	}
	out = fmt.Sprintf("%s projection of JSON at: %s with inferred types: %s",
		source, p.Ptr, p.Inference.Types)

	if p.Inference.Description != "" {
		out = p.Inference.Description + "\n" + out
	}
	if p.Inference.Title != "" {
		out = p.Inference.Title + "\n" + out
	}

	return out
}

// constrainterAdapter is purely a shim between the new Materializer interface
// and the old Constrainter interface. Eventually these should be consolidated.
type constrainterAdapter[EC pb.Validator, FC FieldConfiger, RC Resourcer[RC, EC], MT any] struct {
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

func (c *constrainterAdapter[EC, FC, RC, MT]) Compatible(existing ExistingField, p *pf.Projection, rawFieldConfig json.RawMessage) (bool, error) {
	var fieldCfg FC
	if len(rawFieldConfig) > 0 {
		if err := unmarshalStrict(rawFieldConfig, &fieldCfg); err != nil {
			return false, err
		}
	}

	mt, _ := c.m.MapType(mapProjection(*p, fieldCfg), fieldCfg)
	return c.m.Compatible(existing, mt), nil
}

func (c *constrainterAdapter[EC, FC, RC, MT]) DescriptionForType(p *pf.Projection, rawFieldConfig json.RawMessage) (string, error) {
	var fieldCfg FC
	if len(rawFieldConfig) > 0 {
		if err := unmarshalStrict(rawFieldConfig, &fieldCfg); err != nil {
			return "", err
		}
	}

	mt, _ := c.m.MapType(mapProjection(*p, fieldCfg), fieldCfg)
	return c.m.DescriptionForType(mt), nil
}

func unmarshalStrict[T pb.Validator](raw []byte, into *T) error {
	var d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()

	if err := d.Decode(into); err != nil {
		return err
	}

	return (*into).Validate()
}
