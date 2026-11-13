package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// binding is a materialized collection and the Twenty object it writes to.
type binding struct {
	res    resource
	object string
	path   []string
	fields []mappedField // in the order of the stored values
	unique []fieldMeta   // the unique Twenty fields the binding writes
	adopt  *fieldMeta    // the unique field that finds records to adopt, if adopt_by is set
	links  []link        // the relations the binding writes

	phoneCountry string // for phone numbers without + or a country, from default_phone_country

	waiting map[linkSlot]waitingLink // links whose records weren't in Twenty yet, kept in the state
	retries retryBudget              // the waiting links linkLater tried lately
}

// linkRef stands in a record for the Twenty id of the record a relation points at, until the
// connector looks that record up by its key.
type linkRef struct{ key string }

// MarshalJSON fails, so that a link that wasn't looked up is never sent.
func (linkRef) MarshalJSON() ([]byte, error) {
	return nil, errors.New("a relation's record wasn't looked up before the write")
}

// newBinding maps a binding's selected fields to the object's Twenty fields and links. Twenty's
// schema may have changed since Validate, so it checks each field again.
func newBinding(res resource, obj *objectMeta, links []link, spec *pf.MaterializationSpec, b *pf.MaterializationSpec_Binding) (*binding, error) {
	if err := checkOwnFields(res, obj); err != nil {
		return nil, err
	}
	if len(b.FieldSelection.Keys) == 0 {
		return nil, fmt.Errorf("the binding for %q selects no key fields", res.Object)
	}
	if b.FieldSelection.Document == "" {
		return nil, fmt.Errorf("the binding for %q must select the root document", res.Object)
	}
	collection := spec.BindingCollection(b)
	if collection == nil {
		return nil, fmt.Errorf("the binding for %q has no collection", res.Object)
	}

	sources, err := linkSources(links, collection)
	if err != nil {
		return nil, err
	}
	for _, name := range slices.Sorted(maps.Keys(sources)) {
		if !slices.Contains(b.FieldSelection.Values, name) {
			return nil, fmt.Errorf("the binding links %q from the field %q, which isn't selected", sources[name][0].field, name)
		}
	}

	out := &binding{res: res, object: obj.NamePlural, path: res.path(), links: links}
	targets := make(map[string]string) // Twenty field or subfield -> projection
	for _, name := range b.FieldSelection.Values {
		p := collection.GetProjection(name)
		if p == nil {
			return nil, fmt.Errorf("the collection %q has no projection %q", collection.Name, name)
		}
		m, why := mappedField{field: name, ptr: p.Ptr, kind: kindLink, links: sources[name], defaulted: len(p.Inference.DefaultJson) > 0}, ""
		if len(m.links) == 0 {
			m, why = mapProjection(res, obj, p)
		}
		if why != "" {
			return nil, fmt.Errorf("the field %q can't be written: %s", name, why)
		}
		for _, t := range m.claims() {
			if other, ok := targets[t]; ok {
				return nil, fmt.Errorf("the fields %q and %q both write to the Twenty field %s", other, name, t)
			}
			targets[t] = name
		}
		out.fields = append(out.fields, m)
		if m.meta.IsUnique && !slices.ContainsFunc(out.unique, func(f fieldMeta) bool { return f.Name == m.meta.Name }) {
			out.unique = append(out.unique, m.meta)
		}
	}
	if res.AdoptBy != "" {
		i := slices.IndexFunc(out.unique, func(f fieldMeta) bool { return f.Name == res.AdoptBy })
		if i < 0 {
			return nil, fmt.Errorf("%q: the binding for %q doesn't write %q, or it isn't a unique Text, Links or Emails field", "adopt_by", res.Object, res.AdoptBy)
		}
		out.adopt = &out.unique[i]
	}
	return out, nil
}

// record builds the Twenty record for a stored document. A field that is missing from the
// document is left out, so Twenty keeps its value. A field that is null clears it, unless the
// binding keeps nulls: then it is left out too.
func (b *binding) record(key string, values tuple.Tuple, doc json.RawMessage) (record, error) {
	if len(values) != len(b.fields) {
		return nil, fmt.Errorf("got %d values for %d fields", len(values), len(b.fields))
	}
	rec := record{b.res.IDField: key}
	if b.res.deleteMode() == deleteFlag {
		rec[b.res.DeletedField] = false
		if b.res.DeletedAtField != "" {
			rec[b.res.DeletedAtField] = nil
		}
	}

	var parsed any
	decoded := false
	// Whole fields go first, so a subfield's value lands in its field's object.
	for _, subs := range []bool{false, true} {
		for i, m := range b.fields {
			if (m.target.sub != "") != subs {
				continue
			}
			v := values[i]
			if v == nil && b.res.keepNulls() {
				continue
			}
			if v == nil || m.defaulted {
				if !decoded {
					if err := decode(doc, &parsed); err != nil {
						return nil, fmt.Errorf("reading the document: %w", err)
					}
					decoded = true
				}
				if !present(parsed, m.ptr) {
					continue
				}
			}
			out, err := convert(m.kind, v)
			if err != nil {
				return nil, fmt.Errorf("the field %q: %w", m.field, err)
			}
			if err := m.place(rec, out, b.res.keepNulls()); err != nil {
				return nil, err
			}
		}
	}
	b.normalize(rec)
	return rec, nil
}

// claims lists the Twenty fields and subfields a mapped field writes. A whole composite field
// writes each of its subfields, and a string for a link or email writes the primary one.
func (m mappedField) claims() []string {
	subs, composite := compositeSubfields[m.meta.Type]
	switch {
	case m.kind == kindLink:
		var out []string
		for _, l := range m.links {
			out = append(out, l.field)
		}
		return out
	case m.target.sub != "":
		return []string{m.target.String()}
	case m.kind == kindPrimary:
		return []string{target{m.target.field, primarySubfield[m.meta.Type]}.String()}
	case composite:
		var out []string
		for sub := range subs {
			out = append(out, target{m.target.field, sub}.String())
		}
		return out
	}
	return []string{m.target.String()}
}

// place puts a converted value into the record. With keepNulls, a whole composite value's null
// subfields are left out, and one with nothing else is left out whole.
func (m mappedField) place(rec record, v any, keepNulls bool) error {
	name := m.target.field
	_, composite := compositeSubfields[m.meta.Type]
	switch {
	case m.kind == kindLink:
		for _, l := range m.links {
			// A null or empty key clears the link: no record has an empty key.
			if key, _ := v.(string); key != "" {
				rec[l.joinColumn] = linkRef{key: key}
			} else {
				rec[l.joinColumn] = nil
			}
		}
	case m.target.sub != "":
		obj, ok := rec[name].(map[string]any)
		if !ok {
			obj = make(map[string]any)
			rec[name] = obj
		}
		obj[m.target.sub] = v
	case m.kind == kindPrimary:
		rec[name] = map[string]any{primarySubfield[m.meta.Type]: v}
	case composite && v != nil:
		var obj map[string]any
		if err := decode(v.(json.RawMessage), &obj); err != nil {
			return fmt.Errorf("the field %q: %w", m.field, err)
		}
		if keepNulls {
			maps.DeleteFunc(obj, func(_ string, sub any) bool { return sub == nil })
			if len(obj) == 0 {
				return nil
			}
		}
		// A whole currency's amount gets the same check as one mapped on its own.
		if n, ok := obj["amountMicros"].(json.Number); ok && m.meta.Type == "CURRENCY" {
			if _, err := convert(kindInteger, string(n)); err != nil {
				return fmt.Errorf("the field %q: amountMicros: %w", m.field, err)
			}
		}
		rec[name] = obj
	case composite:
		// Twenty ignores a null composite field, so a null clears each subfield instead.
		obj := make(map[string]any)
		for sub := range compositeSubfields[m.meta.Type] {
			obj[sub] = nil
		}
		rec[name] = obj
	default:
		rec[name] = v
	}
	return nil
}

// normalize writes domains and emails the way Twenty stores them, so the unique check looks up
// the values Twenty will hold.
func (b *binding) normalize(rec record) {
	for _, m := range b.fields {
		obj, ok := rec[m.target.field].(map[string]any)
		if !ok {
			continue
		}
		switch {
		case m.meta.Type == "LINKS" && m.meta.Settings.Type == "domain":
			if s, ok := obj["primaryLinkUrl"].(string); ok {
				obj["primaryLinkUrl"] = normalizeDomain(s)
			}
		case m.meta.Type == "EMAILS":
			if s, ok := obj["primaryEmail"].(string); ok {
				obj["primaryEmail"] = normalizeEmail(s)
			}
		case m.meta.Type == "PHONES":
			b.normalizePhone(obj)
		}
		if m.meta.Type == "LINKS" {
			// A cleared link is cleared whole. Twenty would otherwise make its first secondary
			// link the primary one (local probe, Oct 2026).
			if url, set := obj["primaryLinkUrl"]; set && (url == nil || url == "") {
				for _, sub := range []string{"primaryLinkLabel", "secondaryLinks"} {
					if _, set := obj[sub]; !set {
						obj[sub] = nil
					}
				}
			}
		}
	}
}

// normalizePhone gives Twenty a phone number it can read. Twenty reads the country and calling code
// from a number that starts with "+", and refuses codes that conflict with it, so a mapped default
// code can't clash with a foreign number. A number without "+" needs a country, and with one
// Twenty strips its punctuation itself: "(415) 555-0101" with US is stored as 4155550101 (local
// probe, Oct 2026).
func (b *binding) normalizePhone(obj map[string]any) {
	s, ok := obj["primaryPhoneNumber"].(string)
	if !ok {
		return
	}
	s = strings.TrimSpace(s)
	hasCode := nonEmpty(obj["primaryPhoneCountryCode"]) || nonEmpty(obj["primaryPhoneCallingCode"])
	if !hasCode && strings.HasPrefix(s, "00") {
		// The international prefix most countries dial, which Twenty refuses in place of "+".
		s = "+" + s[2:]
		obj["primaryPhoneNumber"] = s
	}
	if strings.HasPrefix(s, "+") {
		delete(obj, "primaryPhoneCountryCode")
		delete(obj, "primaryPhoneCallingCode")
	} else if s != "" && !hasCode && b.phoneCountry != "" {
		obj["primaryPhoneCountryCode"] = b.phoneCountry
	}
}

func nonEmpty(v any) bool {
	s, ok := v.(string)
	return ok && s != ""
}

// keepSubfields copies into rec, from the record Twenty holds, the links and phones subfields
// that rec leaves out of a field it sets. Twenty would clear them, and the connector leaves alone
// what a document doesn't set.
func (b *binding) keepSubfields(rec, held record) {
	for _, m := range b.fields {
		subs := clearedSubfields[m.meta.Type]
		obj, ok := rec[m.target.field].(map[string]any)
		if len(subs) == 0 || !ok {
			continue
		}
		heldObj, _ := held[m.target.field].(map[string]any)
		for _, sub := range subs {
			if _, set := obj[sub]; !set {
				if v, ok := heldObj[sub]; ok {
					obj[sub] = v
				}
			}
		}
	}
}

// decode reads JSON with numbers kept exact, as for a currency's amountMicros.
func decode(raw []byte, into any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(into)
}

// present reports whether a JSON pointer names a location in a decoded document, even one that
// holds null.
func present(doc any, ptr string) bool {
	if ptr == "" {
		return true
	}
	for _, token := range strings.Split(strings.TrimPrefix(ptr, "/"), "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch node := doc.(type) {
		case map[string]any:
			v, ok := node[token]
			if !ok {
				return false
			}
			doc = v
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i < 0 || i >= len(node) {
				return false
			}
			doc = node[i]
		default:
			return false
		}
	}
	return true
}

// keyString returns a key as the text the ID field holds. A key of one field is its value as
// text. A key of several fields joins its parts, in the binding's key order, with "/", each
// percent-encoded so that a "/" in a part can't make two keys the same text.
func keyString(key tuple.Tuple) (string, error) {
	if len(key) == 0 {
		return "", errors.New("expected a key, got none")
	}
	parts := make([]string, len(key))
	for i, elem := range key {
		var s string
		switch v := elem.(type) {
		case string:
			s = v
		case int64:
			s = strconv.FormatInt(v, 10)
		case uint64:
			s = strconv.FormatUint(v, 10)
		default:
			return "", fmt.Errorf("unexpected key of type %T", elem)
		}
		if len(key) == 1 {
			return s, nil
		}
		parts[i] = escape(s)
	}
	return strings.Join(parts, "/"), nil
}

// linkLookup is where the binding's relation with the join column finds its records.
func (b *binding) linkLookup(column string) string {
	for _, l := range b.links {
		if l.joinColumn == column {
			return l.lookup()
		}
	}
	return ""
}
