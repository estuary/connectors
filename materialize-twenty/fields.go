package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/net/idna"
)

// kind is how the connector writes a projection's values.
type kind int

const (
	kindText     kind = iota // a string, or a number or boolean written as text
	kindNumber               // a number, or a string holding one
	kindInteger              // a whole number, or a string holding one
	kindBool                 // a boolean
	kindString               // a string as is: dates, select options, UUIDs
	kindDateTime             // an RFC 3339 date-time, written in UTC
	kindJSON                 // an object or array as is
	kindPrimary              // a string written to a composite field's primary subfield
	kindLink                 // the key of the record a relation points at, a string or an integer
	kindAmount               // a currency amount in units, a number or a string holding one, written as micros
)

// amountSubfield is the subfield name that maps a number in currency units, like 19.99, to a
// currency field's amountMicros.
const amountSubfield = "amount"

// Subfields of Twenty's composite field types, as of v2.52, and the kind each one takes.
var compositeSubfields = map[string]map[string]kind{
	"LINKS":     {"primaryLinkUrl": kindString, "primaryLinkLabel": kindString, "secondaryLinks": kindJSON},
	"EMAILS":    {"primaryEmail": kindString, "additionalEmails": kindJSON},
	"PHONES":    {"primaryPhoneNumber": kindString, "primaryPhoneCountryCode": kindString, "primaryPhoneCallingCode": kindString, "additionalPhones": kindJSON},
	"FULL_NAME": {"firstName": kindString, "lastName": kindString},
	"ADDRESS": {
		"addressStreet1": kindString, "addressStreet2": kindString, "addressCity": kindString, "addressPostcode": kindString,
		"addressState": kindString, "addressCountry": kindString, "addressLat": kindNumber, "addressLng": kindNumber,
	},
	"CURRENCY": {"amountMicros": kindInteger, "currencyCode": kindString},
	// Rich text also has blocknote and tiptap, the editor's formats, which Twenty makes from the
	// markdown: tiptap from v2.51, blocknote before. Any write replaces the whole value (local
	// probes of v2.0 to v2.52, Nov 2026).
	"RICH_TEXT": {"markdown": kindString},
}

// primarySubfield is where a plain string goes in a links, emails or rich text field. For links
// and emails, these are also the subfields that Twenty's unique index covers.
var primarySubfield = map[string]string{"LINKS": "primaryLinkUrl", "EMAILS": "primaryEmail", "RICH_TEXT": "markdown"}

// clearedSubfields are the subfields that Twenty v2.52 clears when an update sets other
// subfields of the field but not them (local probe, Oct 2026). Other composite fields keep the
// subfields an update leaves out.
var clearedSubfields = map[string][]string{
	"LINKS":  {"primaryLinkUrl", "primaryLinkLabel", "secondaryLinks"},
	"PHONES": {"additionalPhones"},
	"EMAILS": {"primaryEmail"},
}

// target is the Twenty field, and optional composite subfield, that a projection maps to. A
// projection named "name/firstName" maps to the subfield firstName of the field name.
type target struct {
	field, sub string
}

func parseTarget(projectionField string) (target, bool) {
	parts := strings.Split(projectionField, "/")
	if len(parts) > 2 || !fieldName.MatchString(parts[0]) {
		return target{}, false
	}
	t := target{field: parts[0]}
	if len(parts) == 2 {
		if !fieldName.MatchString(parts[1]) {
			return target{}, false
		}
		t.sub = parts[1]
	}
	return t, true
}

func (t target) String() string {
	if t.sub == "" {
		return t.field
	}
	return t.field + "." + t.sub
}

// writable reports whether the connector may write a Twenty field as a value, and why not.
// A many-to-one relation is written through the binding's relations instead.
func writable(f fieldMeta) (bool, string) {
	switch {
	case !f.IsActive:
		return false, fmt.Sprintf("The Twenty field %q is inactive", f.Name)
	case f.IsSystem || (f.Writability != "" && f.Writability != "OPEN"):
		return false, fmt.Sprintf("Twenty manages the field %q", f.Name)
	}
	switch f.Type {
	case "ACTOR", "POSITION", "TS_VECTOR":
		return false, fmt.Sprintf("Twenty manages %s fields like %q", f.Type, f.Name)
	case "RELATION":
		if f.Settings.RelationType == "MANY_TO_ONE" {
			return false, fmt.Sprintf("%q is a relation. To link records through it, add it to the binding's relations", f.Name)
		}
		return false, fmt.Sprintf("%q lists the records that point at this one. Link them from their own object's binding instead", f.Name)
	case "MORPH_RELATION":
		if f.Settings.JoinColumnName != "" {
			return false, fmt.Sprintf("%q is a relation. To link records through it, add it to the binding's relations", f.Name)
		}
		return false, fmt.Sprintf("%q is a relation that can point at more than one object, which the connector doesn't write", f.Name)
	case "FILES":
		return false, fmt.Sprintf("%q holds files uploaded to Twenty, which the connector doesn't write", f.Name)
	case "RICH_TEXT_V2":
		// Twenty calls rich text RICH_TEXT from v2.0 (local probes); the connector needs v2.30.
		return false, fmt.Sprintf("%q is a rich text field of a Twenty older than v2.0, which the connector doesn't support", f.Name)
	}
	return true, ""
}

// mapKind returns how to write a projection's values to a Twenty field, or why it can't.
func mapKind(p *pf.Projection, f fieldMeta, sub string) (kind, string) {
	types := slices.DeleteFunc(slices.Clone(p.Inference.Types), func(t string) bool { return t == "null" })
	slices.Sort(types)
	single := ""
	if len(types) == 1 {
		single = types[0]
	} else if slices.Equal(types, []string{"integer", "number"}) {
		single = "number"
	}
	format := ""
	if p.Inference.String_ != nil {
		format = p.Inference.String_.Format
	}
	numeric := single == "integer" || single == "number" ||
		(single == "string" && (format == "integer" || format == "number"))
	whole := single == "integer" || (single == "string" && format == "integer")

	if f.Type == "CURRENCY" && sub == amountSubfield {
		if numeric {
			return kindAmount, ""
		}
		return 0, fmt.Sprintf("The subfield %s.%s can't hold %s values", f.Name, sub, describe(types, format))
	}
	if sub != "" {
		subs, ok := compositeSubfields[f.Type]
		if !ok {
			return 0, fmt.Sprintf("The Twenty field %q is a %s field, which has no subfields", f.Name, f.Type)
		}
		k, ok := subs[sub]
		if !ok {
			return 0, fmt.Sprintf("The Twenty %s field %q has no subfield %q", f.Type, f.Name, sub)
		}
		switch {
		case k == kindString && single == "string",
			k == kindNumber && numeric,
			k == kindInteger && whole,
			k == kindJSON && (single == "array" || single == "object"):
			return k, ""
		}
		return 0, fmt.Sprintf("The subfield %s.%s can't hold %s values", f.Name, sub, describe(types, format))
	}

	switch f.Type {
	case "TEXT":
		if single == "string" || numeric || single == "boolean" {
			return kindText, ""
		}
	case "NUMBER", "NUMERIC":
		if numeric {
			return kindNumber, ""
		}
	case "BOOLEAN":
		if single == "boolean" {
			return kindBool, ""
		}
	case "DATE_TIME":
		if single == "string" && format == "date-time" {
			return kindDateTime, ""
		}
	case "DATE":
		if single == "string" && format == "date" {
			return kindString, ""
		}
	case "SELECT", "UUID":
		if single == "string" {
			return kindString, ""
		}
	case "RATING":
		if single == "string" {
			return kindString, ""
		}
		return 0, fmt.Sprintf("The Twenty RATING field %q takes the strings RATING_1 to RATING_5, not %s values", f.Name, describe(types, format))
	case "MULTI_SELECT", "ARRAY":
		if single != "array" {
			break
		}
		// Twenty refuses an item that isn't a string, null included (local probe, Oct 2026). Items
		// the schema allows to be null pass, as few arrays hold a null: Twenty rejects a record
		// that does.
		if items := arrayItems(p); len(items) > 0 && !slices.Equal(items, []string{"string"}) {
			return 0, fmt.Sprintf("The Twenty %s field %q holds strings, and this array can hold %s items", f.Type, f.Name, describe(items, ""))
		}
		return kindJSON, ""
	case "RAW_JSON":
		if single == "array" || single == "object" {
			return kindJSON, ""
		}
	case "LINKS", "EMAILS", "RICH_TEXT":
		if single == "string" {
			return kindPrimary, ""
		} else if single == "object" {
			return kindJSON, ""
		}
	default:
		if _, ok := compositeSubfields[f.Type]; ok && single == "object" {
			return kindJSON, ""
		}
	}
	return 0, fmt.Sprintf("The Twenty %s field %q can't hold %s values", f.Type, f.Name, describe(types, format))
}

// arrayItems returns the types an array projection's items can have, other than null, or nothing
// if the schema doesn't say.
func arrayItems(p *pf.Projection) []string {
	if p.Inference.Array == nil {
		return nil
	}
	items := slices.DeleteFunc(slices.Clone(p.Inference.Array.ItemTypes), func(t string) bool { return t == "null" })
	slices.Sort(items)
	return items
}

func describe(types []string, format string) string {
	if len(types) == 0 {
		return "null"
	}
	s := strings.Join(types, " or ")
	if format != "" {
		s += " (" + format + ")"
	}
	return s
}

// convert turns a value from the document into what the connector sends for kind k.
func convert(k kind, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch k {
	case kindText:
		switch v := v.(type) {
		case string:
			return stripNUL(v), nil
		case bool:
			return strconv.FormatBool(v), nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		case uint64:
			return strconv.FormatUint(v, 10), nil
		case float64:
			// 'f', since 'g' writes 2500000 as "2.5e+06".
			return strconv.FormatFloat(v, 'f', -1, 64), nil
		}
	case kindNumber, kindInteger:
		switch v := v.(type) {
		// A whole number is amountMicros, which takes anything up to maxMicros.
		case int64:
			if k == kindNumber && (v > maxExact || v < -maxExact) {
				return nil, errInexact(strconv.FormatInt(v, 10))
			}
			return v, nil
		case uint64:
			if k == kindNumber && v > maxExact {
				return nil, errInexact(strconv.FormatUint(v, 10))
			}
			if v > maxMicros {
				return nil, errAmount(strconv.FormatUint(v, 10))
			}
			return v, nil
		case float64:
			if k == kindNumber && !math.IsInf(v, 0) && !math.IsNaN(v) {
				return v, nil
			}
		case string:
			n := json.Number(strings.TrimSpace(v))
			if _, err := strconv.ParseFloat(string(n), 64); err != nil || !jsonNumber.MatchString(string(n)) {
				return nil, fmt.Errorf("%q is not a number", v)
			}
			if k == kindInteger {
				if _, err := strconv.ParseInt(string(n), 10, 64); errors.Is(err, strconv.ErrRange) {
					return nil, errAmount(string(n))
				} else if err != nil {
					return nil, fmt.Errorf("%q is not a whole number", v)
				}
				return n, nil
			}
			if !strings.ContainsAny(string(n), ".eE") {
				if i, err := strconv.ParseInt(string(n), 10, 64); err != nil || i > maxExact || i < -maxExact {
					return nil, errInexact(string(n))
				}
			}
			return n, nil
		}
	case kindAmount:
		switch v := v.(type) {
		case int64:
			return micros(strconv.FormatInt(v, 10))
		case uint64:
			return micros(strconv.FormatUint(v, 10))
		case float64:
			if !math.IsInf(v, 0) && !math.IsNaN(v) {
				return micros(strconv.FormatFloat(v, 'g', -1, 64))
			}
		case string:
			n := strings.TrimSpace(v)
			if !jsonNumber.MatchString(n) {
				return nil, fmt.Errorf("%q is not a number", v)
			}
			return micros(n)
		}
	case kindBool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case kindString, kindPrimary:
		if s, ok := v.(string); ok {
			return stripNUL(s), nil
		}
	case kindLink:
		switch v := v.(type) {
		case string:
			return v, nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		case uint64:
			return strconv.FormatUint(v, 10), nil
		}
	case kindDateTime:
		if s, ok := v.(string); ok {
			// Twenty's REST API takes UTC times ending in "Z".
			t, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				return nil, fmt.Errorf("%q is not an RFC 3339 date-time", s)
			}
			// Twenty v2.52 stores a date-time before the year 100 in another century, like 0050
			// as 1950 (local probe, Oct 2026).
			if t.UTC().Year() < 100 {
				return nil, fmt.Errorf("%q is before the year 100, which Twenty stores as a different year", s)
			}
			return t.UTC().Format(time.RFC3339Nano), nil
		}
	case kindJSON:
		switch v := v.(type) {
		case json.RawMessage:
			return stripJSONNUL(v)
		case []byte:
			return stripJSONNUL(json.RawMessage(v))
		}
	}
	return nil, fmt.Errorf("unexpected value of type %T", v)
}

// maxExact is the largest whole number a NUMBER field holds exactly: Twenty stores it as a double,
// so it would round a larger one.
const maxExact = 1 << 53

// maxMicros is the largest currency amount in micros the connector writes, about 9.2 trillion units.
// Twenty keeps amountMicros as a double too, but there the rounding above 2^53 micros stays under
// 0.002 units, so a big amount is written rather than refused.
const maxMicros = math.MaxInt64

// micros turns an amount in currency units, written as a JSON number, into whole micros, rounding
// half away from zero.
func micros(n string) (any, error) {
	if len(n) > 100 {
		// A longer number is no real amount, and parsing it exactly costs time with its length.
		return nil, fmt.Errorf("%.20s... is more than 100 characters, too long for a currency amount", n)
	}
	f, err := strconv.ParseFloat(n, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return nil, fmt.Errorf("%q is not a number", n)
	}
	switch {
	case math.Abs(f) > maxMicros/1e6+1:
		// Checked as a float first, so an exponent like 1e999999999 isn't expanded.
		return nil, errAmount(n)
	case math.Abs(f) < 1e-7:
		return int64(0), nil
	}
	r, ok := new(big.Rat).SetString(n)
	if !ok {
		return nil, fmt.Errorf("%q is not a number", n)
	}
	r.Mul(r, big.NewRat(1_000_000, 1))
	q, rem := new(big.Int).QuoRem(new(big.Int).Abs(r.Num()), r.Denom(), new(big.Int))
	if rem.Lsh(rem, 1).Cmp(r.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() {
		return nil, errAmount(n)
	}
	if r.Sign() < 0 {
		return -q.Int64(), nil
	}
	return q.Int64(), nil
}

func errAmount(n string) error {
	return fmt.Errorf("%s is out of range for a Twenty currency amount, which the connector writes up to %d micros either way", n, int64(maxMicros))
}

func errInexact(n string) error {
	return fmt.Errorf("%s is too large for a Twenty number field to hold exactly; map it to a text field", n)
}

// stripNUL removes NUL characters, which Postgres refuses: Twenty v2.52 answers such a value with
// HTTP 500 "Data validation error." (local probe, Oct 2026).
func stripNUL(s string) string {
	return strings.ReplaceAll(s, "\x00", "")
}

// stripJSONNUL removes NUL characters from the strings in a JSON value.
func stripJSONNUL(raw json.RawMessage) (json.RawMessage, error) {
	if !bytes.Contains(raw, []byte(`\u0000`)) {
		return raw, nil
	}
	var v any
	if err := decode(raw, &v); err != nil {
		return nil, err
	}
	var strip func(any) any
	strip = func(v any) any {
		switch v := v.(type) {
		case string:
			return stripNUL(v)
		case []any:
			for i := range v {
				v[i] = strip(v[i])
			}
		case map[string]any:
			out := make(map[string]any, len(v))
			for k, x := range v {
				out[stripNUL(k)] = strip(x)
			}
			return out
		}
		return v
	}
	return json.Marshal(strip(v))
}

var jsonNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

var otherScheme = regexp.MustCompile(`^[a-z][a-z0-9+.-]*://`)

// normalizeDomain returns a domain the way Twenty v2.52 stores a domain-typed link (local probe,
// Oct 2026): the host only, lowercased, in punycode, without scheme, user, port, path, query or
// leading "www."s. The connector sends this value, so its unique check looks up exactly what
// Twenty will hold. It returns junk unchanged, for Twenty to reject.
func normalizeDomain(raw string) string {
	if host, ok := domainHost(raw); ok {
		return host
	}
	return strings.TrimSpace(raw)
}

// domainHost is normalizeDomain's host, and whether raw holds one.
func domainHost(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", false
	}
	lower := strings.ToLower(s)
	hasScheme := otherScheme.MatchString(lower)
	if hasScheme && !strings.HasPrefix(lower, "http://") && !strings.HasPrefix(lower, "https://") {
		return "", false
	}
	withScheme := s
	if !hasScheme {
		withScheme = "http://" + s
	}
	u, err := url.Parse(withScheme)
	if err != nil {
		return "", false
	}
	host, err := idna.Lookup.ToASCII(strings.ToLower(u.Hostname()))
	if err != nil || host == "" || strings.Contains(host, " ") || strings.HasSuffix(host, ".") {
		return "", false
	}
	for strings.HasPrefix(host, "www.") {
		host = host[4:]
	}
	if host == "" {
		return "", false
	}
	return host, true
}

// normalizeEmail returns an email the way Twenty stores it: trimmed and lowercased.
func normalizeEmail(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}
