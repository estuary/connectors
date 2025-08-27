package boilerplate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
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

	// TranslateNamespace takes a configured namespace and outputs a translated
	// string that can be used to find the namespace in the InfoSchema.
	TranslateNamespace TranslateNamespaceFn

	// TranslateField takes a Flow field name and outputs a translated string
	// that can be used to find the field in the InfoSchema, with translations
	// applied in a similar way as LocatePathFn. It is relatively common for
	// some kind of translation to need to be done if the destination does not
	// support special characters vs. those supported by Flow field names, or if
	// the destination lowercases all column identifiers, for example.
	TranslateField TranslateFieldFn

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

	// NoTruncateResources indicates that this materialization does not support
	// truncating materialized resources in-place, and any backfill must always
	// result in a resource deletion + re-creation.
	NoTruncateResources bool

	// Serialization policy to use for all bindings of this materialization.
	SerPolicy *pf.SerPolicy

	// MaterializeOptions is a proxy for the options from boilerplate.go.
	// Eventually this should be consolidated.
	MaterializeOptions m.MaterializeOptions
}

// ElementConverter maps from a TupleElement into a runtime type instance that's
// compatible with the materialization.
type ElementConverter func(tuple.TupleElement) (any, error)

// MappedProjection adds materialization-specific type mapping information to a
// basic Flow projection, as well as other useful metadata.
type MappedProjection[MT MappedTyper] struct {
	Projection
	Comment string
	Mapped  MT
}

// MappedBinding is all of the projections for the selected fields of the
// materialization, with materialization-specific type mapping and parsed
// resource configuration.
type MappedBinding[EC EndpointConfiger, RC Resourcer[RC, EC], MT MappedTyper] struct {
	pf.MaterializationSpec_Binding
	Index        int
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

// BindingUpdate is a distilled representation of the typical kinds
// of changes a destination system will care about in response to a new binding
// or change to an existing binding.
type BindingUpdate[EC EndpointConfiger, RC Resourcer[RC, EC], MT MappedTyper] struct {
	Binding             MappedBinding[EC, RC, MT]
	NewProjections      []MappedProjection[MT]
	NewlyNullableFields []ExistingField
	FieldsToMigrate     []MigrateField[MT]
	NewlyDeltaUpdates   bool
}

// MigrateField is an existing field that must be migrated to be compatible with
// an updated projection.
type MigrateField[MT MappedTyper] struct {
	From ExistingField
	To   MappedProjection[MT]
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

// EndpointConfiger represents a parsed endpoint config.
type EndpointConfiger interface {
	pb.Validator

	// DefaultNamespace is the namespace used for bindings if no explicit
	// namespace is configured for the binding. It may also contain metadata
	// tables, and needs to exist even if no binding is actually created in it.
	// This can return an empty string if namespaces do not apply to the
	// materialization.
	DefaultNamespace() string

	// FeatureFlags returns the raw string of comma-separated feature flags, and
	// the default feature flag values that should be applied.
	FeatureFlags() (raw string, defaults map[string]bool)
}

// Resourcer represents a parsed resource config.
type Resourcer[T any, EC EndpointConfiger] interface {
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
	EC EndpointConfiger,
	FC FieldConfiger,
	RC Resourcer[RC, EC],
	MT MappedTyper,
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

	// Setup performs extra materialization-specific actions when handling an
	// Apply RPC prior to any of the standard actions for resource creation or
	// alteration. For example, creating metadata tables that are not
	// represented by a binding. It may return a string to describe the actions
	// that were taken.
	//
	// Since Apply is always ran before Open, this effectively runs the Setup
	// actions before Open as well.
	Setup(context.Context, *InfoSchema) (string, error)

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
	UpdateResource(context.Context, []string, ExistingResource, BindingUpdate[EC, RC, MT]) (string, ActionApplyFn, error)

	// TruncateResource removes all existing data from an existing materialized
	// resource, while leaving the schema intact. This is called when a resource
	// is backfilled, but all existing materialized fields are still compatible
	// with the materialized collection.
	TruncateResource(context.Context, []string) (string, ActionApplyFn, error)

	// NewMaterializerTransactor builds a new transactor for handling the
	// transactions lifecycle of the materialization.
	NewMaterializerTransactor(context.Context, pm.Request_Open, InfoSchema, []MappedBinding[EC, RC, MT], *m.BindingEvents) (MaterializerTransactor, error)

	// Close performs any cleanup actions that should be done when gracefully
	// exiting.
	Close(context.Context)
}

type NewMaterializerFn[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper] func(context.Context, string, EC, map[string]bool) (Materializer[EC, FC, RC, MT], error)

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
func RunValidate[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
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

	parsedFlags := parseFlags(cfg)
	materializer, err := newMaterializer(ctx, req.Name.String(), cfg, parsedFlags)
	if err != nil {
		return nil, err
	}
	defer materializer.Close(ctx)

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

	validator := NewValidator(&constrainterAdapter[EC, FC, RC, MT]{m: materializer}, is, mCfg.MaxFieldLength, mCfg.CaseInsensitiveFields, parsedFlags)
	var out []*pm.Response_Validated_Binding
	for idx, b := range req.Bindings {
		path := paths[idx]
		delta := deltaUpdates[idx]
		if constraints, err := validator.ValidateBinding(path, delta, b.Backfill, b.Collection, b.FieldConfigJsonMap, req.LastMaterialization); err != nil {
			return nil, fmt.Errorf("validating binding: %w", err)
		} else {
			out = append(out, &pm.Response_Validated_Binding{
				CaseInsensitiveFields: materializer.Config().CaseInsensitiveFields,
				Constraints:           constraints,
				DeltaUpdates:          delta,
				ResourcePath:          path,
				SerPolicy:             mCfg.SerPolicy,
			})
		}
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

// RunApply produces an Applied response for an Apply request.
func RunApply[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	ctx context.Context,
	req *pm.Request_Apply,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var endpointCfg EC
	if err := unmarshalStrict(req.Materialization.ConfigJson, &endpointCfg); err != nil {
		return nil, err
	}

	parsedFlags := parseFlags(endpointCfg)
	materializer, err := newMaterializer(ctx, req.Materialization.Name.String(), endpointCfg, parsedFlags)
	if err != nil {
		return nil, err
	}
	defer materializer.Close(ctx)

	// TODO(whb): Some point soon we will have the last committed checkpoint
	// available here in the Apply message, and should start calling Unmarshal
	// State + Acknowledge somewhere around here to commit any previously staged
	// transaction before applying the next spec's updates.

	mCfg := materializer.Config()

	paths := make([][]string, 0, len(req.Materialization.Bindings))
	for _, b := range req.Materialization.Bindings {
		paths = append(paths, b.ResourcePath)
	}

	is := initInfoSchema(mCfg)
	if err := materializer.PopulateInfoSchema(ctx, paths, is); err != nil {
		return nil, err
	}

	var (
		// There are several categories of apply actions:
		// * Namespace creation comes first, since creation of resources may
		// require these namespaces to exist. Namespaces must actually be
		// created before the `Setup` method of the Materializer is called, for
		// example for cases like a materialization needing to create a metadata
		// table.
		// * A materialization-specific setup action may come next, e.g. the
		// previously mentioned metadata table creation.
		// * Any existing resources that require truncation have it done. This
		// isn't done concurrent with the other resource-specific actions to
		// prevent potential concurrency conflicts when operating on the same
		// resource.
		// * The more typical actions are last: Adding fields, dropping
		// nullability constraints, migration column types, etc.
		namespaceActionDesciptions   []string
		setupActionDescriptions      []string // there will only be one entry here, but a slice is used for convenience in the rest of this code
		truncationActionDescriptions []string
		truncationActions            []ActionApplyFn // may run concurrently
		resourceActionDescriptions   []string
		resourceActions              []ActionApplyFn // also may be run concurrently, but not concurrent with truncations
	)

	addResourceAction := func(desc string, a ActionApplyFn) {
		if a != nil { // Convenience for handling endpoints that return `nil` for a no-op action.
			resourceActionDescriptions = append(resourceActionDescriptions, desc)
			resourceActions = append(resourceActions, a)
		}
	}

	if !mCfg.NoCreateNamespaces {
		// Create any required namespaces before other actions, which may
		// include resource creation. Otherwise resources creation may fail due
		// to namespaces not yet existing.
		requiredNamespaces := make(map[string]struct{})
		if ns := endpointCfg.DefaultNamespace(); ns != "" {
			requiredNamespaces[ns] = struct{}{}
		}

		for _, b := range req.Materialization.Bindings {
			if p := b.ResourcePath; len(p) < 2 {
				continue
			} else {
				requiredNamespaces[(p[len(p)-2])] = struct{}{}
			}
		}

		for ns := range requiredNamespaces {
			if is.HasNamespace(ns) {
				continue
			} else if desc, err := materializer.CreateNamespace(ctx, ns); err != nil {
				return nil, err
			} else {
				namespaceActionDesciptions = append(namespaceActionDesciptions, desc)
			}
		}
	}

	if desc, err := materializer.Setup(ctx, is); err != nil {
		return nil, fmt.Errorf("running setup: %w", err)
	} else if desc != "" {
		setupActionDescriptions = append(setupActionDescriptions, desc)
	}

	common, err := computeCommonUpdates(req.LastMaterialization, req.Materialization, is)
	if err != nil {
		return nil, err
	}

	for _, bindingIdx := range common.newBindings {
		if mapped, err := buildMappedBinding(endpointCfg, materializer, *req.Materialization, bindingIdx); err != nil {
			return nil, err
		} else if desc, action, err := materializer.CreateResource(ctx, *mapped); err != nil {
			return nil, fmt.Errorf("getting CreateResource action: %w", err)
		} else {
			addResourceAction(desc, action)
		}
	}

	validator := NewValidator(&constrainterAdapter[EC, FC, RC, MT]{m: materializer}, is, mCfg.MaxFieldLength, mCfg.CaseInsensitiveFields, parsedFlags)
	for _, bindingIdx := range common.backfillBindings {
		mapped, err := buildMappedBinding(endpointCfg, materializer, *req.Materialization, bindingIdx)
		if err != nil {
			return nil, err
		}

		// If the existing resource is compatible with the proposed binding spec
		// without incrementing the backfill counter, it only needs to be
		// truncated rather than fully dropping and re-creating the table.
		var doTruncate bool
		thisBinding := req.Materialization.Bindings[bindingIdx]
		lastBinding := findLastBinding(thisBinding.ResourcePath, req.LastMaterialization)
		existing := is.GetResource(thisBinding.ResourcePath)
		if _, err := validator.ValidateBinding(
			thisBinding.ResourcePath,
			thisBinding.DeltaUpdates,
			lastBinding.Backfill, // validate against the last binding's backfill counter
			thisBinding.Collection,
			thisBinding.FieldSelection.FieldConfigJsonMap,
			req.LastMaterialization,
		); err == nil {
			// No general errors with the new binding spec, so check if any
			// selected fields are incompatible.
			doTruncate = !slices.ContainsFunc(mapped.SelectedProjections(), func(m MappedProjection[MT]) bool {
				if f := existing.GetField(m.Field); f != nil && !mustRecreateTypeChange(&m.Projection.Projection, m.Mapped, *f) {
					return true
				}
				return false
			})
		}

		if !mCfg.NoTruncateResources && doTruncate {
			if desc, action, err := materializer.TruncateResource(ctx, thisBinding.ResourcePath); err != nil {
				return nil, fmt.Errorf("getting TruncateResource action: %w", err)
			} else {
				truncationActionDescriptions = append(truncationActionDescriptions, desc)
				truncationActions = append(truncationActions, action)
			}

			// A resource may be truncated, but require other updates as well.
			upd, err := computeBindingUpdate(is, existing, lastBinding, thisBinding)
			if err != nil {
				return nil, err
			}
			common.updatedBindings[bindingIdx] = *upd
		} else {
			if deleteDesc, deleteAction, err := materializer.DeleteResource(ctx, thisBinding.ResourcePath); err != nil {
				return nil, fmt.Errorf("getting DeleteResource action to replace resource: %w", err)
			} else if createDesc, createAction, err := materializer.CreateResource(ctx, *mapped); err != nil {
				return nil, fmt.Errorf("getting CreateResource action to replace resource: %w", err)
			} else {
				addResourceAction(deleteDesc+"\n"+createDesc, func(ctx context.Context) error {
					if err := deleteAction(ctx); err != nil {
						return err
					} else if err := createAction(ctx); err != nil {
						return err
					}
					return nil
				})
			}
		}
	}

	for bindingIdx, commonUpdates := range common.updatedBindings {
		mb, err := buildMappedBinding(endpointCfg, materializer, *req.Materialization, bindingIdx)
		if err != nil {
			return nil, err
		}

		update := BindingUpdate[EC, RC, MT]{
			Binding:             *mb,
			NewlyNullableFields: commonUpdates.newlyNullableFields,
			NewlyDeltaUpdates:   commonUpdates.newlyDeltaUpdates,
		}

		ps := mb.SelectedProjections()
		for _, p := range commonUpdates.newProjections {
			i := slices.IndexFunc(ps, func(pp MappedProjection[MT]) bool {
				return pp.Field == p.Field
			})
			update.NewProjections = append(update.NewProjections, ps[i])
		}

		existingResource := is.GetResource(mb.ResourcePath)
		for _, p := range mb.Values {
			if existingField := existingResource.GetField(p.Field); existingField == nil {
				continue
			} else if !p.Mapped.Compatible(*existingField) && p.Mapped.CanMigrate(*existingField) {
				update.FieldsToMigrate = append(update.FieldsToMigrate, MigrateField[MT]{
					From: *existingField,
					To:   p,
				})
			} else if !p.Mapped.Compatible(*existingField) {
				// This is mostly a sanity check that some other process (user
				// modifications, perhaps) didn't change the type of a column in
				// a way that we can't deal with. There is also a scarce chance
				// that a specific sequence of events could occur during
				// existing field migrations if they use column renaming, where
				// the "old" column is dropped and before the "new" column can
				// be renamed (if these operations are not atomic) the source
				// field type is changed again in some way and a new column is
				// created with that source field type. This scenario would
				// involve one invoked Apply RPC racing with another, which is
				// not likely, but perhaps not totally impossible.
				log.WithFields(log.Fields{
					"resourcePath": mb.ResourcePath,
					"field":        p.Field,
					"existingType": existingField.Type,
					"mappedType":   p.Mapped.String(),
				}).Warn("existing field type mismatch without migration support")
			}
		}

		if desc, action, err := materializer.UpdateResource(ctx, mb.ResourcePath, *is.GetResource(mb.ResourcePath), update); err != nil {
			return nil, err
		} else {
			addResourceAction(desc, action)
		}
	}

	if err := runActions(ctx, truncationActions, truncationActionDescriptions, mCfg.ConcurrentApply); err != nil {
		return nil, err
	} else if err := runActions(ctx, resourceActions, resourceActionDescriptions, mCfg.ConcurrentApply); err != nil {
		return nil, err
	}

	allActions := append(namespaceActionDesciptions, setupActionDescriptions...)
	allActions = append(allActions, truncationActionDescriptions...)
	allActions = append(allActions, resourceActionDescriptions...)

	return &pm.Response_Applied{ActionDescription: strings.Join(allActions, "\n")}, nil
}

// RunNewTransactor builds a transactor and Opened response from an Open
// request.
func RunNewTransactor[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	ctx context.Context,
	req pm.Request_Open,
	be *m.BindingEvents,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	if err := req.Validate(); err != nil {
		return nil, nil, nil, fmt.Errorf("validating request: %w", err)
	}

	var epCfg EC
	if err := unmarshalStrict(req.Materialization.ConfigJson, &epCfg); err != nil {
		return nil, nil, nil, err
	}

	featureFlags := parseFlags(epCfg)
	materializer, err := newMaterializer(ctx, req.Materialization.Name.String(), epCfg, featureFlags)
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

	return transactor, &pm.Response_Opened{
		RuntimeCheckpoint:       cp,
		DisableLoadOptimization: featureFlags["allow_existing_tables_for_new_bindings"],
	}, &mCfg.MaterializeOptions, nil
}

func initInfoSchema(cfg MaterializeCfg) *InfoSchema {
	locatePath := func(rp []string) []string { return rp }
	translateNamespace := func(f string) string { return f }
	translateField := func(f string) string { return f }
	if cfg.Locate != nil {
		locatePath = cfg.Locate
	}
	if cfg.TranslateNamespace != nil {
		translateNamespace = cfg.TranslateNamespace
	}
	if cfg.TranslateField != nil {
		translateField = cfg.TranslateField
	}

	return NewInfoSchema(locatePath, translateNamespace, translateField, cfg.CaseInsensitiveFields)
}

func buildMappedBinding[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
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
		Index:                       idx,
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

			mp := mapProjection(*p, fieldCfg)
			mt, converter := materializer.MapType(mp, fieldCfg)
			*dst = append(*dst, MappedProjection[MT]{
				Projection: mp,
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

	if p.Ptr == "" {
		out = fmt.Sprintf("%s projection of the root document with inferred types: %s",
			source, p.Inference.Types)
	} else {
		out = fmt.Sprintf("%s projection of JSON at: %s with inferred types: %s",
			source, p.Ptr, p.Inference.Types)
	}

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
type constrainterAdapter[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper] struct {
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
	return mustRecreateTypeChange(p, mt, existing), nil
}

// mustRecreateTypeChange returns true if the proposed projection requires a
// drop & re-create of the target based on the existing materialized field.
// Generally this is true if there is a type change that can't be migrated.
func mustRecreateTypeChange[MT MappedTyper](p *pf.Projection, mt MT, existing ExistingField) bool {
	canMigrate := mt.CanMigrate(existing)
	if p.IsRootDocumentProjection() || p.IsPrimaryKey {
		// There are currently no known cases where migrating the root document
		// column's type would be useful. Somewhat similarly, it would
		// theoretically be possible for a few systems to migrate collection key
		// columns, but for the most part this is not practical.
		canMigrate = false
	}

	return mt.Compatible(existing) || canMigrate
}

func (c *constrainterAdapter[EC, FC, RC, MT]) DescriptionForType(p *pf.Projection, rawFieldConfig json.RawMessage) (string, error) {
	var fieldCfg FC
	if len(rawFieldConfig) > 0 {
		if err := unmarshalStrict(rawFieldConfig, &fieldCfg); err != nil {
			return "", err
		}
	}

	mt, _ := c.m.MapType(mapProjection(*p, fieldCfg), fieldCfg)
	return mt.String(), nil
}

func parseFlags(cfg EndpointConfiger) map[string]bool {
	rawFlags, defaultFlags := cfg.FeatureFlags()
	parsedFlags := common.ParseFeatureFlags(rawFlags, defaultFlags)
	if rawFlags != "" {
		log.WithField("flags", parsedFlags).Info("parsed feature flags")
	}

	return parsedFlags
}

func unmarshalStrict[T pb.Validator](raw []byte, into *T) error {
	var d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()

	if err := d.Decode(into); err != nil {
		return err
	}

	return (*into).Validate()
}
