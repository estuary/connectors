package main

import (
	"fmt"
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
	// Reference to the definition of this binding as it was generated from
	// MaterializatioSpec. It is stored here so the individual values inside this
	// spec doesn't need to be explicitely referenced in this Binding struct and can
	// just be accessed when needed.
	spec *pf.MaterializationSpec_Binding

	// The binding resource associated with this table. The Table name and the mode for
	// this table is stored here
	resource *bindingResource

	// The compiled BigQuery schema that is associated with this Table. This schema can
	// be used to either create a table, or to use it as a definition for cloud storage
	// when storing data there before merging it into BQ. The Schema stored here should be validated
	// by calling Validate() on the table. Once the Validate() call returns, it is safe to use this schema.
	bigquery.Schema
}

func NewTable(binding *pf.MaterializationSpec_Binding, resource *bindingResource) (*Table, error) {
	fields := binding.FieldSelection.AllFields()
	table := &Table{
		resource: resource,
		spec:     binding,
		Schema:   make(bigquery.Schema, len(fields)),
	}

	for idx, fieldName := range fields {
		var (
			err   error
			field *bigquery.FieldSchema
		)
		field, err = fieldSchema(binding.Collection.Projections, fieldName)

		// This is a fatal error. Panicking.
		if err != nil {
			panic(err)
		}

		table.Schema[idx] = field
	}

	return table, nil
}

func (t *Table) Validate(existing *pf.MaterializationSpec_Binding) error {
	if existing == nil {
		return sqlDriver.ValidateSelectedFields(ConstraintsForNewBinding(&t.spec.Collection, t.resource.Delta), t.spec)
	} else {
		constraints, err := ConstraintsForExistingBinding(existing, &t.spec.Collection, t.resource.Delta)
		if err != nil {
			return err
		}
		return sqlDriver.ValidateSelectedFields(constraints, t.spec)
	}
}

// Whether this Table is configured to be only receive
// delta updates
func (t *Table) DeltaUpdates() bool {
	return t.resource.Delta
}

// The name of the Table as created in BigQuery
func (t *Table) Name() string {
	return t.resource.Table
}

// fieldSchema inspect each projection and builds up a bigquery.FieldSchema that can then be
// used within a bigquery.Schema. A field in bigquery is analogous to a column, in RDBMS parlance.
// Although a slice of projection is passed in, fieldSchema only consumes 1 Projection within the slice
// that matches the fieldName.
// A projection will hold an inference types slice that is expected to contains only 1 or 2 values, depending
// on whether the projection is nullable or not. If the field is nullable, the type slice will contain
// one entry called "null". Otherwise, the slice needs to contain only 1 element, representing the type
// for this projection
func fieldSchema(projections []pf.Projection, fieldName string) (*bigquery.FieldSchema, error) {
	var fieldType bigquery.FieldType
	var projection *pf.Projection
	var includesNull bool

	for _, p := range projections {
		if p.Field == fieldName {
			projection = &p
			break
		}
	}

	if projection == nil {
		return nil, fmt.Errorf("Couldn't find a projection for field name: %s", fieldName)
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
					fieldType = bigquery.DateTimeFieldType
				case "date":
					fieldType = bigquery.DateFieldType
				case "geography-wkt":
					fieldType = bigquery.GeographyFieldType
				default:
					return nil, fmt.Errorf("inferring string column type as the format is not supported: %s", s.Format)
				}
			} else {
				fieldType = bigquery.StringFieldType
			}
		case "boolean":
			fieldType = bigquery.BooleanFieldType
		case "integer":
			fieldType = bigquery.IntegerFieldType
		case "number":
			fieldType = bigquery.BigNumericFieldType
		case "null":
			includesNull = true
		}
	}

	if fieldType == "" {
		return nil, fmt.Errorf("could not map the field to a big query type: %s", fieldName)
	}

	// This is a test to make sure that the only value in the infered type is either 1
	// or 2 but it includes a nullable field. If it has more than the expected count, it returns an
	// error back, ie. Inference.Types = [string, integer] is wrong. [string, null] is OK. [string, integer, null] is wrong
	expectedSize := 1
	if includesNull {
		expectedSize += 1
	}

	if len(projection.Inference.Types) != expectedSize {
		return nil, fmt.Errorf("don't expect multiple types besides Null and a real type, can't proceed: %s", &projection.Inference.Types)
	}

	return &bigquery.FieldSchema{
		Name:        projection.Field,
		Type:        fieldType,
		Required:    projection.Inference.Exists == pf.Inference_MUST && !includesNull, // If Required is False, it means it's nullable
		Description: projection.Inference.Description,
	}, nil
}
