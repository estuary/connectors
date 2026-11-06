package connector

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// maxFieldName is the longest field name Twenty takes (Twenty v2.52, probed Nov 2026).
const maxFieldName = 63

// reservedFieldNames are names Twenty refuses for a custom field, from RESERVED_METADATA_NAME_KEYWORDS
// in twenty-shared (v2.52), plus the system fields every object has.
var reservedFieldNames = strings.Fields(`
	approvedAccessDomain approvedAccessDomains appToken appTokens billingCustomer billingCustomers
	billingEntitlement billingEntitlements billingMeter billingMeters billingProduct billingProducts
	billingSubscription billingSubscriptions billingSubscriptionItem billingSubscriptionItems
	featureFlag featureFlags job jobs keyValuePair keyValuePairs pageLayout pageLayouts pageLayoutTab
	pageLayoutTabs pageLayoutWidget pageLayoutWidgets twoFactorMethod twoFactorMethods user users
	userWorkspace userWorkspaces workspace workspaces role roles userWorkspaceRole userWorkspaceRoles
	plan plans event events field fields link links currency currencies fullNames address addresses
	type types object objects index relation relations aggregate connect create disconnect search
	searches
	id createdAt updatedAt deletedAt createdBy updatedBy position searchVector`)

// newField is a field the connector creates in Twenty for a binding's create_fields.
type newField struct {
	name     string
	typ      string
	settings map[string]any
	unique   bool
}

// newFieldType is the Twenty field type the connector creates for a projection, or why it can't
// pick one. A field it creates must then pass mapKind like any other.
func newFieldType(p *pf.Projection) (newField, string) {
	types := slices.DeleteFunc(slices.Clone(p.Inference.Types), func(t string) bool { return t == "null" })
	slices.Sort(types)
	format := ""
	if p.Inference.String_ != nil {
		format = p.Inference.String_.Format
	}
	f := newField{name: p.Field}
	switch {
	case slices.Equal(types, []string{"string"}) && format == "date-time":
		f.typ = "DATE_TIME"
	case slices.Equal(types, []string{"string"}) && format == "date":
		f.typ = "DATE"
	case slices.Equal(types, []string{"string"}):
		f.typ = "TEXT"
	case slices.Equal(types, []string{"integer"}):
		// Twenty stores numbers as doubles; zero decimals only changes how they're shown.
		f.typ, f.settings = "NUMBER", map[string]any{"type": "number", "decimals": 0}
	case slices.Equal(types, []string{"number"}), slices.Equal(types, []string{"integer", "number"}):
		f.typ = "NUMBER"
	case slices.Equal(types, []string{"boolean"}):
		f.typ = "BOOLEAN"
	case slices.Equal(types, []string{"array"}):
		// Twenty's ARRAY holds strings and refuses null items, so only arrays that can hold
		// nothing else get one.
		if p.Inference.Array != nil && slices.Equal(p.Inference.Array.ItemTypes, []string{"string"}) {
			f.typ = "ARRAY"
		} else {
			f.typ = "RAW_JSON"
		}
	case slices.Equal(types, []string{"object"}):
		f.typ = "RAW_JSON"
	default:
		return newField{}, fmt.Sprintf("The connector can't pick a Twenty field type for %s values; create the field %q in Twenty", describe(types, format), p.Field)
	}
	return f, ""
}

// creates reports whether the binding creates the field, and the field.
func (r resource) creates(obj *objectMeta, p *pf.Projection) (newField, string, bool) {
	if !slices.Contains(r.CreateFields, p.Field) || slices.Contains(r.ownFields(), p.Field) {
		return newField{}, "", false
	}
	if _, ok := obj.field(p.Field); ok {
		return newField{}, "", false
	}
	f, why := newFieldType(p)
	return f, why, true
}

// ownNewField is the field the connector creates for one of the binding's own fields: its ID field
// as a unique Text field, and its delete fields.
func (r resource) ownNewField(name string) (newField, bool) {
	switch {
	case name == "":
		return newField{}, false
	case name == r.IDField:
		return newField{name: name, typ: "TEXT", unique: true}, true
	case name == r.DeletedField:
		return newField{name: name, typ: "BOOLEAN"}, true
	case name == r.DeletedAtField:
		return newField{name: name, typ: "DATE_TIME"}, true
	}
	return newField{}, false
}

// ownCreates returns the own fields that the binding lists in create_fields and its object lacks.
func (r resource) ownCreates(obj *objectMeta) []newField {
	var out []newField
	for _, name := range r.CreateFields {
		if f, ok := r.ownNewField(name); ok {
			if _, exists := obj.field(name); !exists {
				out = append(out, f)
			}
		}
	}
	return out
}

// withOwnFields returns a copy of objects with the own fields that the bindings will create, so
// that Validate checks the bindings, and relations to their ID fields, as they will be after
// Apply. Open checks Twenty itself. It also returns what it plans, by object and field name, as
// checkCreates's seen, so that a collection field created under the same name meets the same
// type check. collections are the bindings' collection names.
func withOwnFields(objects []objectMeta, resources []resource, collections []string) ([]objectMeta, map[[2]string][2]string, error) {
	out := make([]objectMeta, len(objects))
	for i, o := range objects {
		out[i] = o
		out[i].Fields = slices.Clone(o.Fields)
	}
	seen := make(map[[2]string][2]string)
	for i, res := range resources {
		// What Twenty lacks, so that a field another binding plans still meets the type check.
		real, err := findObject(objects, res.Object)
		if err != nil {
			continue // Validate says so for the binding
		}
		obj, _ := findObject(out, res.Object)
		for _, f := range res.ownCreates(real) {
			key := [2]string{obj.NamePlural, f.name}
			if prev, ok := seen[key]; ok {
				if prev[1] != f.typ {
					return nil, nil, fmt.Errorf("the collections %q and %q would create the Twenty field %s.%s as %s and as %s",
						prev[0], collections[i], obj.NamePlural, f.name, prev[1], f.typ)
				}
				continue
			}
			seen[key] = [2]string{collections[i], f.typ}
			obj.Fields = append(obj.Fields, fieldMeta{Name: f.name, Type: f.typ, IsUnique: f.unique, IsActive: true})
		}
	}
	return out, seen, nil
}

// createFields adds to Twenty the fields that bindings list in create_fields, have selected, and
// that their objects lack, and the own fields they list. It never changes or deletes a field. It
// returns what it created, as "object.field (TYPE)".
func createFields(ctx context.Context, c *client, objects []objectMeta, spec *pf.MaterializationSpec) ([]string, error) {
	type planned struct {
		obj        *objectMeta
		field      newField
		collection string
	}
	plan := make(map[[2]string]planned) // object and field name
	resources, err := specResources(spec)
	if err != nil {
		return nil, err
	}
	for i, b := range spec.Bindings {
		res := resources[i]
		if len(res.CreateFields) == 0 {
			continue
		}
		obj, err := findObject(objects, res.Object)
		if err != nil {
			return nil, userError(err)
		}
		collection := spec.BindingCollection(b)
		if collection == nil {
			return nil, fmt.Errorf("the binding for %q has no collection", res.Object)
		}
		for _, f := range res.ownCreates(obj) {
			key := [2]string{obj.NamePlural, f.name}
			if prev, ok := plan[key]; ok && prev.field.typ != f.typ {
				err := fmt.Errorf("the collections %q and %q would create the Twenty field %s.%s as %s and as %s",
					prev.collection, collection.Name, obj.NamePlural, f.name, prev.field.typ, f.typ)
				return nil, userError(err)
			}
			plan[key] = planned{obj, f, collection.Name.String()}
		}
		for _, name := range res.CreateFields {
			p := collection.GetProjection(name)
			if p == nil || !slices.Contains(b.FieldSelection.Values, name) {
				continue
			}
			f, why, ok := res.creates(obj, p)
			if !ok {
				continue
			} else if why != "" {
				err := fmt.Errorf("the collection %q: %s", collection.Name, why)
				return nil, userError(err)
			}
			key := [2]string{obj.NamePlural, name}
			if prev, ok := plan[key]; ok && prev.field.typ != f.typ {
				err := fmt.Errorf("the collections %q and %q would create the Twenty field %s.%s as %s and as %s",
					prev.collection, collection.Name, obj.NamePlural, name, prev.field.typ, f.typ)
				return nil, userError(err)
			} else if ok {
				if prev.field.settings == nil || f.settings == nil {
					// An integer and a number collection field: a plain NUMBER shows both.
					f.settings = nil
				}
				// Another binding's ID field, written by this one's projection too.
				f.unique = prev.field.unique
			}
			plan[key] = planned{obj, f, collection.Name.String()}
		}
	}

	var created []string
	byName := func(a, b [2]string) int { return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1])) }
	for _, key := range slices.SortedFunc(maps.Keys(plan), byName) {
		p := plan[key]
		what := fmt.Sprintf("%s.%s (%s)", key[0], key[1], p.field.typ)
		if err := c.createField(ctx, p.obj.ID, p.field); err != nil {
			// A create Twenty applied but whose answer was lost comes back from the retry as a
			// name already taken, so look again before failing.
			if !createdAs(ctx, c, key, p.field) {
				done := "none"
				if len(created) > 0 {
					done = strings.Join(created, ", ")
				}
				return created, fmt.Errorf("creating the Twenty field %s: %w (fields created before it: %s)", what, err, done)
			}
		}
		log.WithFields(log.Fields{"field": what, "collection": p.collection}).Info("created a Twenty field")
		created = append(created, what)
	}
	return created, nil
}

// checkCreates fails when bindings would create one field as different types, so Validate says so
// before Apply. seen maps an object and field name to the collection and type that create it.
func checkCreates(seen map[[2]string][2]string, collection *pf.CollectionSpec, res resource, obj *objectMeta) error {
	for _, name := range res.CreateFields {
		p := collection.GetProjection(name)
		if p == nil {
			continue
		}
		f, why, ok := res.creates(obj, p)
		if !ok || why != "" {
			continue
		}
		key := [2]string{obj.NamePlural, name}
		if prev, ok := seen[key]; ok && prev[1] != f.typ {
			return fmt.Errorf("the collections %q and %q would create the Twenty field %s.%s as %s and as %s",
				prev[0], collection.Name, obj.NamePlural, name, prev[1], f.typ)
		}
		seen[key] = [2]string{collection.Name.String(), f.typ}
	}
	return nil
}

// createdAs reports whether Twenty now has the field, active, of the type, and unique if it must be.
func createdAs(ctx context.Context, c *client, key [2]string, want newField) bool {
	objects, err := c.objects(ctx)
	if err != nil {
		return false
	}
	obj, err := findObject(objects, key[0])
	if err != nil {
		return false
	}
	f, ok := obj.field(key[1])
	return ok && f.IsActive && f.Type == want.typ && (f.IsUnique || !want.unique)
}

// appliedDescription is Apply's account of what it did.
func appliedDescription(created []string) string {
	if len(created) == 0 {
		return ""
	}
	return "created Twenty fields: " + strings.Join(created, ", ")
}
