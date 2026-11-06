package connector

import (
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/estuary/connectors/go/common"
	"github.com/invopop/jsonschema"
)

var featureFlagDefaults = map[string]common.FlagDefault{}

const (
	defaultBaseURL           = "https://api.twenty.com"
	defaultRequestsPerMinute = 40

	deleteIgnore = "ignore"
	deleteFlag   = "flag"
	deleteTrash  = "trash"
	deleteDelete = "delete"

	conflictSkipField = "skip_field"
	conflictFail      = "fail"

	rejectedFail = "fail"
	rejectedSkip = "skip"

	nullClear = "clear"
	nullKeep  = "keep"
)

// phoneCountry is the form of an ISO 3166 country code. Twenty checks that the country exists.
var phoneCountry = regexp.MustCompile(`^[A-Za-z]{2}$`)

// fieldName is the form of a Twenty field's API name.
var fieldName = regexp.MustCompile(`^[a-z][a-zA-Z0-9]*$`)

type config struct {
	BaseURL string `json:"base_url,omitempty" jsonschema:"title=Twenty URL,description=The address of the Twenty API. Use the default for Twenty Cloud or your self-hosted Twenty's address.,default=https://api.twenty.com" jsonschema_extras:"order=0"`
	APIKey  string `json:"api_key" jsonschema:"title=API Key,description=An API key from Settings > APIs & Webhooks in Twenty. Its role needs read and write access to the objects you materialize and read access to their metadata." jsonschema_extras:"secret=true,order=1"`
	// Twenty refuses a phone number without a country, like "(415) 555-0101", but cleans up its
	// punctuation once it has one (local probe, Oct 2026).
	DefaultPhoneCountry string         `json:"default_phone_country,omitempty" jsonschema:"title=Default Phone Country,description=The two-letter country code (like US or GB) for phone numbers that don't start with + and whose document gives no country. Twenty refuses such numbers without one.,pattern=^[A-Za-z]{2}$" jsonschema_extras:"order=2"`
	Advanced            advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	RequestsPerMinute int    `json:"requests_per_minute,omitempty" jsonschema:"title=Requests Per Minute,description=The most requests per minute the connector sends to Twenty across all bindings. Keep it under your Twenty plan's API limit.,default=40,minimum=1,maximum=6000"`
	FeatureFlags      string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
}

func (c config) Validate() error {
	if c.APIKey == "" {
		return fmt.Errorf("missing %q", "api_key")
	}
	u, err := url.Parse(c.baseURL())
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return fmt.Errorf("%q must be an http or https address, like %s", "base_url", defaultBaseURL)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("%q must not have a query or fragment", "base_url")
	}
	if c.DefaultPhoneCountry != "" && !phoneCountry.MatchString(c.DefaultPhoneCountry) {
		return fmt.Errorf("%q must be a two-letter country code, like US", "default_phone_country")
	}
	if c.Advanced.RequestsPerMinute < 0 {
		return fmt.Errorf("%q must be positive", "requests_per_minute")
	}
	return nil
}

func (c config) DefaultNamespace() string { return "" }

func (c config) FeatureFlags() (string, map[string]common.FlagDefault) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c config) baseURL() string {
	if c.BaseURL == "" {
		return defaultBaseURL
	}
	return c.BaseURL
}

func (c config) defaultPhoneCountry() string {
	return strings.ToUpper(c.DefaultPhoneCountry)
}

func (c config) requestsPerMinute() float64 {
	if c.Advanced.RequestsPerMinute == 0 {
		return defaultRequestsPerMinute
	}
	return float64(c.Advanced.RequestsPerMinute)
}

// resource is a binding's config. Its schema comes from the tags; JSONSchemaExtend fixes what
// tags can't say.
type resource struct {
	// Name is the collection name. The UI needs an x-collection-name field, but the connector
	// doesn't use it.
	Name           string     `json:"name" jsonschema_extras:"x-collection-name=true,x-hidden-field=true"`
	Object         string     `json:"object" jsonschema:"title=Object" jsonschema_description:"The Twenty object to write to, by its plural API name, like \"companies\", \"people\" or a custom object's name." jsonschema_extras:"order=0"`
	IDField        string     `json:"id_field" jsonschema:"title=ID Field" jsonschema_description:"A unique Text field on the object that holds the collection key. The connector finds each record by this field, so re-sent documents update the same record." jsonschema_extras:"order=1"`
	DeleteMode     string     `json:"delete_mode,omitempty" jsonschema:"title=Deletes,enum=ignore,enum=flag,enum=trash,enum=delete,default=ignore" jsonschema_description:"What a deleted document does. \"ignore\" leaves the record as it is. \"flag\" sets the Deleted Field on the record. \"trash\" moves the record to Twenty's trash, where it can be restored; if the document comes back, it is skipped unless Restore Records Deleted in Twenty is on. \"delete\" deletes the record for good; if the document comes back, it creates a new record." jsonschema_extras:"order=2"`
	DeletedField   string     `json:"deleted_field,omitempty" jsonschema:"title=Deleted Field" jsonschema_description:"A Boolean field the connector sets to true when the document is deleted, and back to false when the document comes back. Required when Deletes is \"flag\"." jsonschema_extras:"order=3"`
	DeletedAtField string     `json:"deleted_at_field,omitempty" jsonschema:"title=Deleted At Field" jsonschema_description:"An optional Date and Time field the connector sets to the time it flags a record as deleted." jsonschema_extras:"order=4"`
	RestoreDeleted bool       `json:"restore_deleted,omitempty" jsonschema:"title=Restore Records Deleted in Twenty,default=false" jsonschema_description:"If true, a change to a record in Twenty's trash restores it, whether a Twenty user or the connector trashed it. If false, the connector leaves the record in the trash and skips the change." jsonschema_extras:"order=5"`
	UniqueConflict string     `json:"unique_conflict,omitempty" jsonschema:"title=Unique Value Conflicts,enum=skip_field,enum=fail,default=skip_field" jsonschema_description:"What happens when a value for a unique field, like a company's domain or a person's email, already belongs to another record. \"skip_field\" writes the record without that value. \"fail\" stops the task." jsonschema_extras:"order=6"`
	RejectedRecord string     `json:"rejected_records,omitempty" jsonschema:"title=Rejected Records,enum=fail,enum=skip,default=fail" jsonschema_description:"What happens when Twenty rejects a record. \"fail\" stops the task. \"skip\" first leaves out an email, phone or link value that Twenty calls invalid, then logs and skips the record if Twenty still rejects it. The task stops if a binding skips more than 10 records and 10% of its documents in a transaction." jsonschema_extras:"order=7"`
	Relations      []relation `json:"relations,omitempty" jsonschema:"title=Relations" jsonschema_description:"Relation fields to link, like a person's company. The connector finds the linked record by its key and writes its Twenty id. A key that isn't in Twenty leaves the link as it is." jsonschema_extras:"order=8"`
	// CreateFields are collection fields the connector adds to the object in Twenty when it
	// applies, if the object lacks them and the field is selected.
	CreateFields []string `json:"create_fields,omitempty" jsonschema:"title=Create Fields" jsonschema_description:"Collection fields to add to the Twenty object as new fields, if it doesn't have them. The connector requires them, and creates each one when it applies, with a type from the collection's schema, and never changes or deletes a field. It may also list the binding's ID Field, created as a unique Text field, and its delete fields. Needs an API key whose role can change the data model." jsonschema_extras:"order=9"`
	NullValues   string   `json:"null_values,omitempty" jsonschema:"title=Null Values,enum=clear,enum=keep,default=clear" jsonschema_description:"What a null in a document does to the Twenty value. \"clear\" clears it. \"keep\" leaves it as it is, like a field missing from the document, so values people or other tools set in Twenty stay until the source has one." jsonschema_extras:"order=10"`
	AdoptBy      string   `json:"adopt_by,omitempty" jsonschema:"title=Match Existing Records By" jsonschema_description:"A unique field the binding writes, like \"emails\" on people or \"domainName\" on companies. A document with no record takes the live record that holds the same value and has an empty ID Field, such as one a person made in Twenty, rather than leaving the value out. Deletes must be \"ignore\" or \"flag\", and no other binding may write the object." jsonschema_extras:"order=11"`
	// This connector always uses delta updates. The read-only option shows that, as in other
	// materializations.
	DeltaUpdates bool `json:"delta_updates,omitempty" jsonschema:"title=Delta Update,default=true" jsonschema_description:"Should updates to this object be done via delta updates. This connector always uses delta updates." jsonschema_extras:"order=12"`
}

// JSONSchemaExtend makes the hidden-field flag a boolean, which schemagen leaves as text, marks
// delta_updates read-only, and lets a relation's settings have other keys, as the rest of the
// schema does.
func (resource) JSONSchemaExtend(s *jsonschema.Schema) {
	if name, ok := s.Properties.Get("name"); ok {
		name.Extras["x-hidden-field"] = true
	}
	if delta, ok := s.Properties.Get("delta_updates"); ok {
		delta.ReadOnly = true
	}
	if rels, ok := s.Properties.Get("relations"); ok {
		rels.Items.AdditionalProperties = nil
	}
}

// relation is a many-to-one relation field the binding writes, like a person's company. A
// collection field holds the key of the record it points at, which the connector looks up by
// the unique text field IDField of that record's object.
type relation struct {
	Field       string `json:"field" jsonschema:"title=Relation Field" jsonschema_description:"A relation field of the object that points at one record, like \"company\" on people."`
	IDField     string `json:"id_field" jsonschema:"title=Linked ID Field" jsonschema_description:"The unique Text field that holds the keys of the records the relation points at, like the ID Field of their binding. Use \"id\" for Twenty ids, or \"userEmail\" or \"name\" for workspace members."`
	SourceField string `json:"source_field,omitempty" jsonschema:"title=Collection Field" jsonschema_description:"The collection field that holds the key of the record to link. Defaults to the relation field's name."`
}

// source is the collection field that holds the key of the record to link.
func (r relation) source() string {
	if r.SourceField == "" {
		return r.Field
	}
	return r.SourceField
}

func (r resource) Validate() error {
	if r.Object == "" {
		return fmt.Errorf("missing %q", "object")
	}
	if !fieldName.MatchString(r.Object) {
		return fmt.Errorf("%q must be a Twenty object's plural API name, like \"companies\"", "object")
	}
	if !fieldName.MatchString(r.IDField) {
		return fmt.Errorf("%q must name a Twenty field, like \"externalId\"", "id_field")
	}
	switch r.deleteMode() {
	case deleteIgnore, deleteTrash, deleteDelete:
		if r.DeletedField != "" || r.DeletedAtField != "" {
			return fmt.Errorf("%q and %q apply only when %q is %q", "deleted_field", "deleted_at_field", "delete_mode", deleteFlag)
		}
	case deleteFlag:
		if !fieldName.MatchString(r.DeletedField) {
			return fmt.Errorf("%q %q needs %q, a Twenty BOOLEAN field", "delete_mode", deleteFlag, "deleted_field")
		}
		if r.DeletedAtField != "" && !fieldName.MatchString(r.DeletedAtField) {
			return fmt.Errorf("%q must name a Twenty field", "deleted_at_field")
		}
	default:
		return fmt.Errorf("%q must be %q, %q, %q or %q", "delete_mode", deleteIgnore, deleteFlag, deleteTrash, deleteDelete)
	}
	for _, f := range []string{r.DeletedField, r.DeletedAtField} {
		if f != "" && f == r.IDField {
			return fmt.Errorf("%q can't also be a delete field", "id_field")
		}
	}
	if r.DeletedField != "" && r.DeletedField == r.DeletedAtField {
		return fmt.Errorf("%q and %q must be different fields", "deleted_field", "deleted_at_field")
	}
	if c := r.uniqueConflict(); c != conflictSkipField && c != conflictFail {
		return fmt.Errorf("%q must be %q or %q", "unique_conflict", conflictSkipField, conflictFail)
	}
	if p := r.rejectedRecords(); p != rejectedFail && p != rejectedSkip {
		return fmt.Errorf("%q must be %q or %q", "rejected_records", rejectedFail, rejectedSkip)
	}
	if r.NullValues != "" && r.NullValues != nullClear && r.NullValues != nullKeep {
		return fmt.Errorf("%q must be %q or %q", "null_values", nullClear, nullKeep)
	}
	if r.AdoptBy != "" {
		switch {
		case !fieldName.MatchString(r.AdoptBy):
			return fmt.Errorf("%q must name a Twenty field, like \"emails\"", "adopt_by")
		case slices.Contains(r.ownFields(), r.AdoptBy):
			return fmt.Errorf("%q must be a field the binding writes from the collection, not its ID Field or a delete field", "adopt_by")
		case r.deleteMode() == deleteTrash || r.deleteMode() == deleteDelete:
			// A deleted document would then remove a record a person made.
			return fmt.Errorf("%q works only with %q %q or %q", "adopt_by", "delete_mode", deleteIgnore, deleteFlag)
		}
	}
	seen := make(map[string]bool)
	for _, rel := range r.Relations {
		if !fieldName.MatchString(rel.Field) {
			return fmt.Errorf("%q: each needs %q, a relation field's API name, like \"company\"", "relations", "field")
		}
		if !fieldName.MatchString(rel.IDField) {
			return fmt.Errorf("%q: %q needs %q, the field on the object it points at that holds the keys, like \"externalId\"", "relations", rel.Field, "id_field")
		}
		if seen[rel.Field] {
			return fmt.Errorf("%q: %q is listed twice", "relations", rel.Field)
		}
		seen[rel.Field] = true
	}
	listed := make(map[string]bool)
	for _, name := range r.CreateFields {
		switch {
		case !fieldName.MatchString(name) || len(name) > maxFieldName:
			return fmt.Errorf("%q: %q isn't a name Twenty takes: a lowercase letter, then letters and digits, at most %d characters", "create_fields", name, maxFieldName)
		case slices.Contains(reservedFieldNames, name):
			return fmt.Errorf("%q: Twenty reserves the name %q", "create_fields", name)
		case listed[name]:
			return fmt.Errorf("%q: %q is listed twice", "create_fields", name)
		}
		for _, rel := range r.Relations {
			if name == rel.Field || name == rel.source() {
				return fmt.Errorf("%q: %q is a relation of the binding", "create_fields", name)
			}
		}
		listed[name] = true
	}
	return nil
}

// path names the binding by its object and ID field, so that collections with their own key
// fields, like a HubSpot and a Salesforce one, can write the same object in one materialization.
func (r resource) path() []string { return []string{r.Object, r.IDField} }

func (r resource) deleteMode() string {
	if r.DeleteMode == "" {
		return deleteIgnore
	}
	return r.DeleteMode
}

func (r resource) uniqueConflict() string {
	if r.UniqueConflict == "" {
		return conflictSkipField
	}
	return r.UniqueConflict
}

// keepNulls says whether a null leaves the Twenty value as it is, like a missing field.
func (r resource) keepNulls() bool {
	return r.NullValues == nullKeep
}

func (r resource) rejectedRecords() string {
	if r.RejectedRecord == "" {
		return rejectedFail
	}
	return r.RejectedRecord
}

// ownFields are the fields the connector writes itself, so no projection may map to them.
func (r resource) ownFields() []string {
	var out []string
	for _, f := range []string{r.IDField, r.DeletedField, r.DeletedAtField} {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}
