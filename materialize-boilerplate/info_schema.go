package boilerplate

import (
	"fmt"
	"slices"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// EndpointField contains information about a materialized field in an endpoint. The name may not be
// the same as the Flow field name, depending upon how the Flow field name is transformed by either
// the connector or the endpoint itself.
type EndpointField struct {
	Name               string
	Nullable           bool
	Type               string
	CharacterMaxLength int
}

// LocatePathFn takes a Flow resource path and outputs an equal-length string slice containing
// translated location components that can be used to find the resource in the InfoSchema. These
// translations include the changes in capitalization, removal/replacement of special characters,
// etc. that a Flow field name undergoes as it is materialized as a "column" in the endpoint (or
// equivalent). These translated components may represent the name of a schema and table in a SQL
// endpoint, for example.
type LocatePathFn func([]string) []string

// TranslateFieldFn takes a Flow field name and outputs a translated string that can be used to find
// the field in the InfoSchema, with translations applied in a similar way as LocatePathFn.
type TranslateFieldFn func(string) string

// InfoSchema contains the information about materialized collections and fields that exist within
// the endpoint.
type InfoSchema struct {
	// resources is a mapping of string keys representing locations within the destination system to
	// EndpointFields. The string keys are as-reported by the endpoint, so they are necessarily
	// "post-translation". As a convention, if the resource has multiple components (like a schema &
	// table name), these components are joined with dots.
	resources      map[string][]EndpointField
	locatePath     LocatePathFn
	translateField TranslateFieldFn
}

// NewInfoSchema creates a new InfoSchema that will use the `locate` and `translateField` functions
// to look up EndpointFields for Flow resource paths and fields.
func NewInfoSchema(locate LocatePathFn, translateField TranslateFieldFn) *InfoSchema {
	return &InfoSchema{
		resources:      map[string][]EndpointField{},
		locatePath:     locate,
		translateField: translateField,
	}
}

// PushField adds a "field" to the InfoSchema. The location is as-reported by the endpoint.
func (i *InfoSchema) PushField(field EndpointField, location ...string) {
	rk := joinPath(location)

	if slices.ContainsFunc(i.resources[rk], func(f EndpointField) bool {
		return field.Name == f.Name
	}) {
		// This should never happen and would represent an application logic error, but sanity
		// checking it here just in case to mitigate what might otherwise be very difficult to debug
		// situations.
		panic(fmt.Sprintf(
			"logic error: PushField when resourceKey %q already contains field %q",
			rk, field.Name,
		))
	}

	i.resources[rk] = append(i.resources[rk], field)
}

// GetField returns the EndpointField for a Flow resource path and field name.
func (i *InfoSchema) GetField(resourcePath []string, fieldName string) (EndpointField, error) {
	rk := joinPath(i.locatePath(resourcePath))

	if !i.HasResource(resourcePath) {
		return EndpointField{}, fmt.Errorf("resourceKey %q not found", rk)
	}

	if !i.HasField(resourcePath, fieldName) {
		return EndpointField{}, fmt.Errorf("field %q does not exist in resourceKey %q", rk, fieldName)
	}

	for _, f := range i.resources[rk] {
		if f.Name == i.translateField(fieldName) {
			return f, nil
		}
	}

	panic("not reached")
}

// HasField reports whether a Flow field exists in the InfoSchema under the given Flow resource
// path.
func (i *InfoSchema) HasField(resourcePath []string, fieldName string) bool {
	rk := joinPath(i.locatePath(resourcePath))

	if !i.HasResource(resourcePath) {
		return false
	}

	for _, f := range i.resources[rk] {
		if f.Name == i.translateField(fieldName) {
			return true
		}
	}

	return false
}

// FieldsForResource returns all of the EndpointFields under the given Flow resource path.
func (i *InfoSchema) FieldsForResource(resourcePath []string) ([]EndpointField, error) {
	rk := joinPath(i.locatePath(resourcePath))

	if !i.HasResource(resourcePath) {
		return nil, fmt.Errorf("resourceKey %q (path %q) not found", rk, resourcePath)
	}

	return i.resources[rk], nil
}

// HasResource reports whether the Flow resource path exists in the InfoSchema.
func (i *InfoSchema) HasResource(resourcePath []string) bool {
	rk := joinPath(i.locatePath(resourcePath))
	_, ok := i.resources[rk]
	return ok
}

// ExtractProjection finds the projection in a Flow collection spec that must correspond to a field
// reported by the endpoint. Fields in the collection spec must have the same one-way translation
// applied to them that a field does when materializing it in order to do this lookup.
func (i *InfoSchema) ExtractProjection(efn string, collection pf.CollectionSpec) (pf.Projection, bool, error) {
	var found bool
	var out pf.Projection

	for idx := range collection.Projections {
		matches := i.translateField(collection.Projections[idx].Field) == efn
		if found && matches {
			// This should never happen since the standard constraints from `Validator` forbid it,
			// but I'm leaving it here as a sanity check just in case.
			return pf.Projection{}, false, fmt.Errorf("ambiguous endpoint field name when looking for projection %q", efn)
		} else if matches {
			found = true
			out = collection.Projections[idx]
		}
	}

	return out, found, nil
}

// InSelectedFields returns true if the provided endpoint field name (as reported by the endpoint)
// exists in the field selection for a materialization. It does a similar translation of fields from
// the field selection as ExtractProjection in order to do this matching.
func (i *InfoSchema) InSelectedFields(efn string, fs pf.FieldSelection) (bool, error) {
	var found bool

	for _, f := range fs.AllFields() {
		matches := i.translateField(f) == efn
		if found && matches {
			// This should never happen since the standard constraints from `Validator` forbid it,
			// but I'm leaving it here as a sanity check just in case.
			return false, fmt.Errorf("ambiguous endpoint field name when looking for selected field %q", efn)
		} else if matches {
			found = true
		}
	}

	return found, nil
}

// AmbiguousResourcePaths is a utility for determining if a resource path could be ambiguous when
// materialized to the destination system. This should only be possible if the destination system
// (or the connector) transforms resource paths in some way that could result in conflicts. Usually
// the connector should take this into consideration when producing the resource path with its
// `Validate` response, but we haven't always done a good job with that.
func (i *InfoSchema) AmbiguousResourcePaths(resourcePaths [][]string) [][]string {
	seenPaths := make(map[string][][]string)

	for _, rp := range resourcePaths {
		rk := joinPath(i.locatePath(rp))
		seenPaths[rk] = append(seenPaths[rk], rp)
	}

	var out [][]string
	for _, p := range seenPaths {
		if len(p) > 1 {
			out = append(out, p...)
		}
	}

	// Sort the output to make it a little more clear which paths are ambiguous if there are a lot
	// of them.
	slices.SortFunc(out, func(a, b []string) int {
		return strings.Compare(joinPath(i.locatePath(a)), joinPath(i.locatePath(b)))
	})

	return out
}

func joinPath(in []string) string {
	return strings.Join(in, ".")
}
