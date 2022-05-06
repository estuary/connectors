package main

import (
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	sqlDriver "github.com/estuary/flow/go/protocols/materialize/sql"
)

func ConstraintsForNewBinding(collection *pf.CollectionSpec, deltaUpdates bool) map[string]*pm.Constraint {
	return sqlDriver.ValidateNewSQLProjections(collection, deltaUpdates)
}

func ConstraintsForExistingBinding(
	existing *pf.MaterializationSpec_Binding,
	collection *pf.CollectionSpec,
	deltaUpdates bool,
) (
	map[string]*pm.Constraint,
	error,
) {

	// There's no particular reason why we _need_ to constrain this, but it seems smart to only
	// relax it if we need to. We previously disallowed all changes to the delta_updates
	// configuration, and relaxed it because we wanted to enable delta_updates for an existing
	// binding, and couldn't think of why it would hurt anything. But disabling delta_updates
	// for an existing binding might not be as simple, since Load implementations may not be
	// prepared to deal with the potential for duplicate primary keys. So I'm leaving this
	// validation in place for now, since there's no need to relax it further at the moment.
	//
	// Copied from https://github.com/estuary/flow/blob/358a640df83ef2e3dab200505804cec155851887/go/protocols/materialize/sql/driver.go#L114-L120
	if existing.DeltaUpdates && !deltaUpdates {
		return nil, fmt.Errorf(
			"cannot disable delta-updates mode of existing target %s", strings.Join(existing.ResourcePath, "."))
	}

	return sqlDriver.ValidateMatchesExisting(existing, collection), nil
}

// Table represent the Table inside BigQuery.
type Table struct {
	// Fields holds the transformation required for a Field to go from a Flow Document (JSON)
	// to a SQL field. The slice matches
	Fields []*Field

	// Keys is a subset of Fields that only includes the
	// Keys as defined by the MaterializationSpec_Binding
	Keys []*Field

	// Values is a subset of Fields that only includes the Values as defined
	// by the MaterializationSpec_Binding. Note that if there's a root document,
	// it will be included in Values
	Values []*Field

	// RootDocument is the flow document's field that is used
	// to store the Flow document in bigquery in the form of a
	// marshalled JSON Object
	RootDocument *Field

	// Name of the table
	name string

	deltaUpdates bool

	// Reference to the definition of this binding as it was generated from
	// MaterializatioSpec. It is stored here so the individual values inside this
	// spec doesn't need to be explicitely referenced in this Binding struct and can
	// just be accessed when needed.
	spec *pf.MaterializationSpec_Binding

	// The compiled BigQuery schema that is associated with this Table. This schema can
	// be used to either create a table, or to use it as a definition for cloud storage
	// when storing data there before merging it into BQ. The Schema stored here should be validated
	// by calling Validate() on the table. Once the Validate() call returns, it is safe to use this schema.
	bigquery.Schema
}

func NewTable(binding *pf.MaterializationSpec_Binding, resource *bindingResource) (*Table, error) {
	fields := binding.FieldSelection.AllFields()

	table := &Table{
		deltaUpdates: resource.Delta,
		name:         identifierSanitizer(resource.Table),
		Schema:       make(bigquery.Schema, len(fields)),
		Fields:       make([]*Field, len(fields)),
		Keys:         make([]*Field, len(binding.FieldSelection.Keys)),
		Values:       make([]*Field, len(binding.FieldSelection.Values)+1), // The extra entry is for the RootDocument inclusion
		spec:         binding,
	}

	for idx, fieldName := range fields {
		var (
			err        error
			field      *Field
			projection *pf.Projection
		)

		for _, p := range binding.Collection.Projections {
			if p.Field == fieldName {
				projection = &p
				break
			}
		}

		if projection == nil {
			return nil, fmt.Errorf("couldn't find a projection for field name: %s", fieldName)
		}

		field, err = NewField(projection)

		// This is a fatal error. Panicking.
		if err != nil {
			panic(err)
		}

		schema, err := field.FieldSchema(projection)
		if err != nil {
			panic(err)
		}

		table.Fields[idx] = field
		table.Schema[idx] = schema

		if idx < cap(table.Keys) {
			table.Keys[idx] = field
		} else {
			table.Values[idx-cap(table.Keys)] = field
		}

		if fieldName == binding.FieldSelection.Document {
			table.RootDocument = field
		}
	}

	return table, nil
}

func (t *Table) Validate(existing *pf.MaterializationSpec_Binding) error {
	if existing == nil {
		return sqlDriver.ValidateSelectedFields(ConstraintsForNewBinding(&t.spec.Collection, t.DeltaUpdates()), t.spec)
	} else {
		constraints, err := ConstraintsForExistingBinding(existing, &t.spec.Collection, t.DeltaUpdates())
		if err != nil {
			return err
		}
		return sqlDriver.ValidateSelectedFields(constraints, t.spec)
	}
}

// Whether this Table is configured to be only receive
// delta updates
func (t *Table) DeltaUpdates() bool {
	return t.deltaUpdates
}

// The name of the Table as created in BigQuery
func (t *Table) Name() string {
	return t.name
}

// Field represents a field in BigQuery. The underlying Projection
// needs to be analyzed and sanitized so that it can be safely used inside
// BigQuery.
type Field struct {
	name      string
	fieldType bigquery.FieldType

	// Sanitizers are executed in the order
	// they are added in the slice.
	sanitizers []sanitizerFunc
}

type sanitizerFunc func(string) string

// NewField returns a field constructed from a projection provided by a BindingSpec
// The Field returned includes logic that allows a field to map a Key and its associated value
// into SQL safely. The field gets sanitized and quoted according to the type of field
// bigquery will see.
func NewField(projection *pf.Projection) (*Field, error) {
	f := &Field{
		name:       identifierSanitizer(projection.Field),
		sanitizers: []sanitizerFunc{},
	}

	// This is a test to make sure that there is only 1 infered type for this projection
	// ie. Inference.Types = [string, integer] is wrong. [string, null] is OK. [string, integer, null] is wrong
	if !projection.Inference.IsSingleType() {
		return nil, fmt.Errorf("don't expect multiple types besides Null and a real type, can't proceed: %s", &projection.Inference.Types)
	}

	for _, p := range projection.Inference.Types {
		switch p {
		// The string format can be further expanded so that it can be stored in a more specific format
		// When a projection is a string and it has the Inference_String set, it's possible for the user
		// to pass along more information about the string field to let the connector create a more
		// appropriate field for that string. If an Inference_String{} is present, it doesn't default back
		// to a normal string as it is prefered to let the user know that the inference could not be matched.
		case "string":
			if projection.Inference.String_ != nil {
				s := projection.Inference.String_
				switch s.Format {
				case "date-time":
					f.fieldType = bigquery.TimestampFieldType
				case "date":
					f.fieldType = bigquery.DateFieldType
				// There is a case where a String_ is present but the values aren't set.
				// I'm not sure how this can happen since the protobuf struct should
				// be optional and nil, but it did happen in my testing and I couldn't
				// figured out why, so this is here as a precaution (P-O)
				case "":
					f.fieldType = bigquery.StringFieldType
					f.sanitizers = append(f.sanitizers, sqlDriver.DefaultQuoteSanitizer)

				default:
					return nil, fmt.Errorf("inferring string column type as the format is not supported: %s", s.Format)
				}
			} else {
				f.fieldType = bigquery.StringFieldType
				f.sanitizers = append(f.sanitizers, sqlDriver.DefaultQuoteSanitizer)
			}
		case "boolean":
			f.fieldType = bigquery.BooleanFieldType
		case "integer":
			f.fieldType = bigquery.IntegerFieldType
		case "number":
			f.fieldType = bigquery.BigNumericFieldType
		case "object":
			f.fieldType = bigquery.StringFieldType
			f.sanitizers = append(f.sanitizers, sqlDriver.DefaultQuoteSanitizer, sqlDriver.SingleQuotesWrapper())
		}
	}

	if f.fieldType == "" {
		return nil, fmt.Errorf("could not map the field to a big query type: %s", projection.Field)
	}

	return f, nil
}

func (f *Field) FieldType() bigquery.FieldType {
	return bigquery.FieldType(f.fieldType)
}

func (f *Field) Name() string {
	return f.name
}

func (f *Field) Render(val interface{}) (str string, err error) {
	str = fmt.Sprint(val)

	for _, sanitizer := range f.sanitizers {
		str = sanitizer(str)
	}

	return str, err
}

func (f *Field) FieldSchema(projection *pf.Projection) (*bigquery.FieldSchema, error) {
	var includesNull bool

	// This is a test to make sure that their is only 1 infered type for this projection
	// ie. Inference.Types = [string, integer] is wrong. [string, null] is OK. [string, integer, null] is wrong
	if !projection.Inference.IsSingleType() {
		return nil, fmt.Errorf("don't expect multiple types besides Null and a real type, can't proceed: %s", &projection.Inference.Types)
	}

	for _, p := range projection.Inference.Types {
		if p == "null" {
			includesNull = true
		}
	}

	return &bigquery.FieldSchema{
		Name:        f.name,
		Type:        f.FieldType(),
		Required:    projection.Inference.Exists == pf.Inference_MUST && !includesNull, // If Required is False, it means it's nullable
		Description: projection.Inference.Description,
	}, nil
}

var identifierSanitizerRegexp = regexp.MustCompile(`[^\-\._0-9a-zA-Z]`)

func identifierSanitizer(text string) string {
	return identifierSanitizerRegexp.ReplaceAllString(text, "_")
}
