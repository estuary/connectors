package connector

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// findObject returns the active Twenty object with the plural API name.
func findObject(objects []objectMeta, name string) (*objectMeta, error) {
	for i := range objects {
		if objects[i].NamePlural != name {
			continue
		}
		if !objects[i].IsActive {
			return nil, fmt.Errorf("the Twenty object %q is inactive", name)
		}
		return &objects[i], nil
	}
	return nil, fmt.Errorf("Twenty has no object %q; use an object's plural API name, like \"companies\"", name)
}

func (o *objectMeta) field(name string) (fieldMeta, bool) {
	for _, f := range o.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return fieldMeta{}, false
}

// checkOwnFields checks the fields the connector writes itself: the ID field, and the delete
// flags.
func checkOwnFields(res resource, obj *objectMeta) error {
	checks := []struct{ option, name, typ string }{{"id_field", res.IDField, "TEXT"}}
	if res.DeletedField != "" {
		checks = append(checks, struct{ option, name, typ string }{"deleted_field", res.DeletedField, "BOOLEAN"})
	}
	if res.DeletedAtField != "" {
		checks = append(checks, struct{ option, name, typ string }{"deleted_at_field", res.DeletedAtField, "DATE_TIME"})
	}
	for _, c := range checks {
		f, ok := obj.field(c.name)
		if !ok {
			return fmt.Errorf("%q: the Twenty object %q has no field %q", c.option, obj.NamePlural, c.name)
		}
		if f.Type != c.typ {
			return fmt.Errorf("%q: %s.%s is a %s field, and must be a %s field", c.option, obj.NamePlural, c.name, f.Type, c.typ)
		}
		if ok, why := writable(f); !ok {
			return fmt.Errorf("%q: %s", c.option, why)
		}
		if c.option == "id_field" && !f.IsUnique {
			return fmt.Errorf("%q: %s.%s must be unique, so that each key has one record", c.option, obj.NamePlural, c.name)
		}
	}
	return nil
}

// link is a relation field that a binding writes. The record's join column gets the Twenty id
// of the record whose ID field holds the key in the collection field source.
type link struct {
	field      string    // the relation field, like "company"
	joinColumn string    // where the id goes, like "companyId"
	target     string    // the object it points at, like "companies"
	idField    string    // the field there that holds the keys, like "externalId"
	source     string    // the collection field that holds the key
	match      string    // how keys find their records: matchText, matchID, matchMember or matchUnique
	idMeta     fieldMeta // the field idField names, for matchUnique
}

const (
	matchText   = ""       // a unique text field, by exact value
	matchID     = "id"     // the record's Twenty id
	matchMember = "member" // a workspace member's email or name, matched in memory
	matchUnique = "unique" // the primary value of a unique links or emails field, like a domain
)

// targetValue is the value a link's key is looked up by: for a domain or an email, the form
// Twenty stores, as the connector writes it. It's false for a key that holds no domain.
func (l link) targetValue(key string) (string, bool) {
	switch {
	case l.match != matchUnique:
		return key, true
	case l.idMeta.Type == "EMAILS":
		return normalizeEmail(key), true
	case l.idMeta.Settings.Type == "domain":
		return domainHost(key)
	}
	return key, true
}

// metadataError is a failed request to Twenty's metadata API, rather than a binding that doesn't
// match Twenty.
type metadataError struct{ err error }

func (e *metadataError) Error() string { return e.err.Error() }
func (e *metadataError) Unwrap() error { return e.err }

// checkRelations checks a binding's relations against Twenty's metadata and returns them as
// links. Each must be an active relation field of the object that points at one record, and the
// object it points at must have the unique text field the relation names.
func checkRelations(ctx context.Context, c *client, objects []objectMeta, res resource, obj *objectMeta) ([]link, error) {
	var out []link
	morphs := make(map[string]string) // a morph relation's id -> the field of it already linked
	for _, rel := range res.Relations {
		f, ok := obj.field(rel.Field)
		var why string
		switch {
		case !ok:
			why = fmt.Sprintf("the Twenty object %q has no field %q", obj.NamePlural, rel.Field)
		case f.Type == "MORPH_RELATION" && f.Settings.JoinColumnName == "":
			why = fmt.Sprintf("%s.%s can point at more than one object. Link through its field for one object instead, like %q", obj.NamePlural, f.Name, "targetCompany")
		case f.Type == "MORPH_RELATION" && morphs[f.MorphID] != "":
			why = fmt.Sprintf("%s.%s and %s.%s are two targets of one relation, so a record can link through only one of them", obj.NamePlural, morphs[f.MorphID], obj.NamePlural, f.Name)
		case f.Type != "RELATION" && f.Type != "MORPH_RELATION":
			why = fmt.Sprintf("%s.%s is a %s field, not a relation", obj.NamePlural, f.Name, f.Type)
		case f.Settings.RelationType != "MANY_TO_ONE":
			why = fmt.Sprintf("%s.%s lists the records that point at this one. Link them from their own object's binding instead", obj.NamePlural, f.Name)
		case f.Settings.JoinColumnName == "":
			why = fmt.Sprintf("Twenty's metadata gives %s.%s no join column", obj.NamePlural, f.Name)
		case !f.IsActive:
			why = fmt.Sprintf("the Twenty field %q is inactive", f.Name)
		case f.IsSystem || (f.Writability != "" && f.Writability != "OPEN"):
			why = fmt.Sprintf("Twenty manages the field %q", f.Name)
		}
		if why != "" {
			return nil, fmt.Errorf("%q: %s", "relations", why)
		}

		var name string
		var err error
		if f.Type == "MORPH_RELATION" {
			morphs[f.MorphID] = f.Name
			name, err = c.morphTarget(ctx, f)
		} else {
			name, err = c.relationTarget(ctx, f.ID)
		}
		if err != nil {
			return nil, &metadataError{fmt.Errorf("reading which object %s.%s points at: %w", obj.NamePlural, f.Name, err)}
		} else if name == "" {
			return nil, fmt.Errorf("%q: %s.%s is a field of a relation that can point at more than one object, and the connector can't tell which one it is for: it links through fields Twenty named target plus the object, like %q", "relations", obj.NamePlural, f.Name, "targetCompany")
		}
		target, err := findObject(objects, name)
		if err != nil {
			return nil, fmt.Errorf("%q: %s.%s points at %q: %w", "relations", obj.NamePlural, f.Name, name, err)
		}
		match := matchText
		switch {
		case rel.IDField == "id":
			match = matchID
		case name == "workspaceMembers" && (rel.IDField == "userEmail" || rel.IDField == "name"):
			match = matchMember
		}
		idf, ok := target.field(rel.IDField)
		if ok && idf.IsUnique && (idf.Type == "LINKS" || idf.Type == "EMAILS") {
			match = matchUnique
		}
		switch {
		case !ok:
			why = fmt.Sprintf("%s.%s points at %s, which has no field %q", obj.NamePlural, f.Name, name, rel.IDField)
		case !idf.IsActive:
			why = fmt.Sprintf("the Twenty field %s.%s is inactive", name, idf.Name)
		case match != matchText:
			// Not a unique text field, so not looked up by a filter.
		case idf.Type != "TEXT":
			why = fmt.Sprintf("%s.%s is a %s field, and must be a unique Text, Links or Emails field, or id", name, idf.Name, idf.Type)
		case !idf.IsUnique:
			why = fmt.Sprintf("%s.%s must be unique, so that each key names one record", name, idf.Name)
		}
		if why != "" {
			return nil, fmt.Errorf("%q: %s", "relations", why)
		}
		l := link{field: f.Name, joinColumn: f.Settings.JoinColumnName, target: name, idField: idf.Name, source: rel.source(), match: match}
		if match == matchUnique {
			l.idMeta = idf
		}
		out = append(out, l)
	}
	return out, nil
}

// linkSources groups a binding's links by the collection field that holds their keys, and checks
// that each such field is in the collection and holds strings or integers.
func linkSources(links []link, collection *pf.CollectionSpec) (map[string][]link, error) {
	sources := make(map[string][]link)
	for _, l := range links {
		sources[l.source] = append(sources[l.source], l)
	}
	for _, name := range slices.Sorted(maps.Keys(sources)) {
		p := collection.GetProjection(name)
		switch {
		case p == nil:
			return nil, fmt.Errorf("%q: the collection %q has no field %q to hold the keys of the records %q points at. Add a projection by that name, or set %q", "relations", collection.Name, name, sources[name][0].field, "source_field")
		case p.IsPrimaryKey || p.IsRootDocumentProjection():
			return nil, fmt.Errorf("%q: the collection key or document %q can't hold the keys of linked records. Add a projection of that location under another name", "relations", name)
		}
		types := slices.DeleteFunc(slices.Clone(p.Inference.Types), func(t string) bool { return t == "null" })
		slices.Sort(types)
		if len(types) == 0 || slices.ContainsFunc(types, func(t string) bool { return t != "string" && t != "integer" }) {
			return nil, fmt.Errorf("%q: the field %q holds the keys of linked records, which must be strings or integers, not %s", "relations", name, describe(types, ""))
		}
	}
	return sources, nil
}

// validateBinding returns the constraints for a binding's projections.
func validateBinding(res resource, obj *objectMeta, links []link, collection *pf.CollectionSpec) (map[string][]*pm.Response_Validated_Constraint, error) {
	if err := checkOwnFields(res, obj); err != nil {
		return nil, err
	}
	if len(collection.Key) == 0 {
		return nil, fmt.Errorf("the collection %q has no key", collection.Name)
	}
	sources, err := linkSources(links, collection)
	if err != nil {
		return nil, err
	}

	for _, name := range res.CreateFields {
		p := collection.GetProjection(name)
		if slices.Contains(res.ownFields(), name) {
			continue // the connector's own field, not a collection field
		} else if p == nil {
			return nil, fmt.Errorf("%q: the collection %q has no field %q", "create_fields", collection.Name, name)
		} else if p.IsPrimaryKey {
			return nil, fmt.Errorf("%q: %q is the key of the collection %q, which the connector writes to the ID Field %q", "create_fields", name, collection.Name, res.IDField)
		} else if p.IsRootDocumentProjection() {
			return nil, fmt.Errorf("%q: %q is the whole document of the collection %q, which the connector doesn't write", "create_fields", name, collection.Name)
		}
	}

	var adopt fieldMeta
	if res.AdoptBy != "" {
		f, ok := obj.field(res.AdoptBy)
		if !ok || !f.IsUnique || !f.IsActive || uniquePath(f) == "" {
			return nil, fmt.Errorf("%q: %q must be an active unique Text, Links or Emails field of %q", "adopt_by", res.AdoptBy, obj.NamePlural)
		}
		adopt = f
	}
	adoptFrom := ""

	constraints := make(map[string][]*pm.Response_Validated_Constraint)
	for i := range collection.Projections {
		p := &collection.Projections[i]
		c := &pm.Response_Validated_Constraint{}
		switch {
		case p.IsPrimaryKey:
			if err := checkKey(p); err != nil {
				return nil, fmt.Errorf("the collection %q: %w", collection.Name, err)
			}
			c.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
			c.Reason = fmt.Sprintf("The connector writes the collection key to the ID Field %q", res.IDField)
		case p.IsRootDocumentProjection():
			c.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
			c.Reason = "The connector reads the document to tell a missing field from a null one"
		case len(sources[p.Field]) > 0:
			var uses []string
			for _, l := range sources[p.Field] {
				uses = append(uses, fmt.Sprintf("%q to the %s record whose %s is this value", l.field, l.target, l.idField))
			}
			c.Type = pm.Response_Validated_Constraint_FIELD_REQUIRED
			c.Reason = "The connector sets the relation " + strings.Join(uses, ", and ")
		default:
			if f, why, ok := res.creates(obj, p); ok {
				if why != "" {
					c.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
					c.Reason = why
				} else {
					// The binding names it, so it is required rather than left to the default selection.
					c.Type = pm.Response_Validated_Constraint_FIELD_REQUIRED
					c.Reason = fmt.Sprintf("The connector creates the Twenty field %q as %s when it applies", f.name, f.typ)
				}
			} else if m, why := mapProjection(res, obj, p); why != "" {
				c.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
				c.Reason = why
			} else if res.AdoptBy != "" && m.meta.Name == adopt.Name && (m.target.sub == "" || m.target.sub == primarySubfield[adopt.Type]) && adoptFrom == "" {
				adoptFrom = p.Field
				c.Type = pm.Response_Validated_Constraint_FIELD_REQUIRED
				c.Reason = fmt.Sprintf("The connector finds existing records to adopt by the Twenty field %s", m.target)
			} else {
				c.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
				c.Reason = fmt.Sprintf("This field can be written to the Twenty field %s", m.target)
				if m.kind == kindAmount {
					c.Reason += ", converted from units to micros"
				}
			}
		}
		constraints[p.Field] = []*pm.Response_Validated_Constraint{c}
	}
	if res.AdoptBy != "" && adoptFrom == "" {
		return nil, fmt.Errorf("%q: the collection %q has no field that writes %q", "adopt_by", collection.Name, res.AdoptBy)
	}
	return constraints, nil
}

// checkKey checks that the key always has a value the ID field can hold.
func checkKey(p *pf.Projection) error {
	mustExist := p.Inference.Exists == pf.Inference_MUST && !slices.Contains(p.Inference.Types, "null")
	if !mustExist && p.Inference.DefaultJson == nil {
		return fmt.Errorf("the key field %q may be missing or null, which needs a default value annotation", p.Field)
	}
	types := slices.DeleteFunc(slices.Clone(p.Inference.Types), func(t string) bool { return t == "null" })
	if !slices.Equal(types, []string{"string"}) && !slices.Equal(types, []string{"integer"}) {
		return fmt.Errorf("the key field %q must be a string or an integer, not %s", p.Field, describe(types, ""))
	}
	return nil
}

// mappedField is a projection and the Twenty field, or subfield, it writes to, or the links it
// holds the keys for.
type mappedField struct {
	field  string // the projection's name
	ptr    string // its location in the document
	target target
	meta   fieldMeta
	kind   kind
	links  []link // for kindLink
	// The schema gives the location a default, which Flow sends in place of a missing value, so
	// only the document tells whether it was set.
	defaulted bool
}

// mapProjection maps a projection to the Twenty field its name gives, or returns why it can't.
func mapProjection(res resource, obj *objectMeta, p *pf.Projection) (mappedField, string) {
	t, ok := parseTarget(p.Field)
	if !ok {
		return mappedField{}, fmt.Sprintf("%q isn't a Twenty field's API name. To write this location, give the collection a projection named after the field, like \"domainName\" or \"name/firstName\"", p.Field)
	}
	f, ok := obj.field(t.field)
	if !ok {
		return mappedField{}, fmt.Sprintf("The Twenty object %q has no field %q", obj.NamePlural, t.field)
	}
	if slices.Contains(res.ownFields(), t.field) {
		return mappedField{}, fmt.Sprintf("The connector writes the field %q itself", t.field)
	}
	for _, rel := range res.Relations {
		if rel.Field == t.field {
			return mappedField{}, fmt.Sprintf("The binding links %q from the collection field %q", t.field, rel.source())
		}
	}
	if ok, why := writable(f); !ok {
		return mappedField{}, why
	}
	if f.IsUnique && uniquePath(f) == "" {
		return mappedField{}, fmt.Sprintf("The connector can't check the unique %s field %q against other records yet", f.Type, f.Name)
	}
	k, why := mapKind(p, f, t.sub)
	if why != "" {
		return mappedField{}, why
	}
	if k == kindAmount {
		// The amount is written to amountMicros, so it claims that subfield.
		t.sub = "amountMicros"
	}
	return mappedField{field: p.Field, ptr: p.Ptr, target: t, meta: f, kind: k, defaulted: len(p.Inference.DefaultJson) > 0}, ""
}

// uniquePath is where the connector looks up a unique field's values: a text field itself, or
// the primary subfield of a links or emails field, which is what Twenty's unique index covers.
func uniquePath(f fieldMeta) string {
	switch f.Type {
	case "TEXT":
		return f.Name
	case "LINKS", "EMAILS":
		return f.Name + "." + primarySubfield[f.Type]
	}
	return ""
}

// uniqueValue reads a unique field's value from a record the connector built or Twenty returned.
func uniqueValue(rec record, f fieldMeta) string {
	if f.Type == "TEXT" {
		s, _ := rec[f.Name].(string)
		return s
	}
	obj, _ := rec[f.Name].(map[string]any)
	s, _ := obj[primarySubfield[f.Type]].(string)
	return s
}

// dropUnique takes a unique field's value out of a record, so Twenty keeps the value it has.
func dropUnique(rec record, f fieldMeta) {
	if f.Type == "TEXT" {
		delete(rec, f.Name)
		return
	}
	if obj, ok := rec[f.Name].(map[string]any); ok {
		delete(obj, primarySubfield[f.Type])
		if len(obj) == 0 {
			delete(rec, f.Name)
		}
	}
}
