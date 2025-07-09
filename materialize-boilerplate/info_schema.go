package boilerplate

import (
	"fmt"
	"slices"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// ExistingField contains information about a materialized field in an endpoint. The name may not be
// the same as the Flow field name, depending upon how the Flow field name is transformed by either
// the connector or the endpoint itself.
type ExistingField struct {
	Name               string
	Nullable           bool
	Type               string
	CharacterMaxLength int
	HasDefault         bool
}

// Existing resource contains information about a materialized resource in an
// endpoint. The location is as-reported by the endpoint, and may not match the
// corresponding resource path directly.
type ExistingResource struct {
	fields                []ExistingField
	location              []string
	translateField        TranslateFieldFn
	caseInsensitiveFields bool

	// Meta allows for storing additional information about a resource
	// separately, so that it may be retrieved later with a type assertion. If
	// this ends up being something that's used extensively we could make this a
	// generic type parameter instead, but that tends to complicate things quite
	// a bit.
	Meta any
}

// PushField adds a field to the ExistingResource. The location is as-reported
// by the endpoint.
func (r *ExistingResource) PushField(field ExistingField) {
	if slices.ContainsFunc(r.fields, func(f ExistingField) bool {
		return field.Name == f.Name
	}) {
		// This should never happen and would represent an application logic
		// error, but sanity checking it here just in case to mitigate what
		// might otherwise be very difficult to debug situations.
		panic(fmt.Sprintf(
			"logic error: PushField when %s already contains field %q",
			r.location, field.Name,
		))
	}

	r.fields = append(r.fields, field)
}

// GetField returns the ExistingField for a given Flow field name, or nil if it
// is not found.
func (r *ExistingResource) GetField(name string) *ExistingField {
	for _, f := range r.fields {
		translated := r.translateField(name)
		if r.caseInsensitiveFields && strings.EqualFold(f.Name, translated) {
			return &f
		} else if f.Name == translated {
			return &f
		}
	}

	return nil
}

// AllFields returns all the fields reported in the existing resource. To get a
// specific existing field by Flow field name with correct consideration for
// translations / case folding, use GetField instead.
func (r *ExistingResource) AllFields() []ExistingField {
	return r.fields
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
	namespaces            []string
	resources             []*ExistingResource
	locatePath            LocatePathFn
	translateField        TranslateFieldFn
	caseInsensitiveFields bool
}

// NewInfoSchema creates a new InfoSchema that will use the `locate` and `translateField` functions
// to look up EndpointFields for Flow resource paths and fields.
func NewInfoSchema(locate LocatePathFn, translateField TranslateFieldFn, caseInsensitiveFields bool) *InfoSchema {
	return &InfoSchema{
		locatePath:            locate,
		translateField:        translateField,
		caseInsensitiveFields: caseInsensitiveFields,
	}
}

// PushNamespace is used to push a namespace to the InfoSchema for tracking and
// creation of necessary namespaces. Multiple calls with the same namespace are
// idempotent.
func (i *InfoSchema) PushNamespace(namespace string) {
	if slices.Contains(i.namespaces, namespace) {
		return
	}

	i.namespaces = append(i.namespaces, namespace)
}

// PushResource adds a resource with no fields to the InfoSchema. The returned
// resource can be used to add existing field records. Multiple calls with the
// same location will return the same reference, allowing resources to be
// pre-populated by a different query than listing the fields for them.
func (i *InfoSchema) PushResource(location ...string) *ExistingResource {
	for _, r := range i.resources {
		if slices.Equal(r.location, location) {
			return r
		}
	}

	res := &ExistingResource{
		location:              location,
		translateField:        i.translateField,
		caseInsensitiveFields: i.caseInsensitiveFields,
	}

	i.resources = append(i.resources, res)
	return res
}

// GetResource returns the ExistingResource for a given Flow resource path, or
// nil if it is not found.
func (i *InfoSchema) GetResource(resourcePath []string) *ExistingResource {
	for _, r := range i.resources {
		if slices.Equal(r.location, i.locatePath(resourcePath)) {
			return r
		}
	}

	return nil
}

// inSelectedFields returns true if the provided endpoint field name (as reported by the endpoint)
// exists in the field selection for a materialization.
func (i *InfoSchema) inSelectedFields(endpointFieldName string, fs pf.FieldSelection) (bool, error) {
	var found bool

	for _, f := range fs.AllFields() {
		var matches bool
		translated := i.translateField(f)
		if i.caseInsensitiveFields && strings.EqualFold(translated, endpointFieldName) {
			matches = true
		} else if translated == endpointFieldName {
			matches = true
		}

		if found && matches {
			// This should never happen since the standard constraints from `Validator` forbid it,
			// but I'm leaving it here as a sanity check just in case.
			return false, fmt.Errorf("ambiguous endpoint field name when looking for selected field %q", endpointFieldName)
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
		rk := strings.Join(i.locatePath(rp), ".")
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
		return slices.Compare(i.locatePath(a), i.locatePath(b))
	})

	return out
}
