package main

import (
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	pf "github.com/estuary/flow/go/protocols/flow"
)

var (
	errPrivateProjection = errors.New("projection is private and should not be included")
	errNotFound          = errors.New("not found")
)

// Because binding.Collection.Projections doesn't respect the same order as
// binding.FieldSelection.AllFields(), the schema needs to be generated by having an inner loop
// matching the projection then assigning it the index as defined in AllFields().
// If binding.Collection.Projections would honor the same ordering, this function could
// be simplified to just iterate over the projection slice and remove the need for a few
// extra error handling as defined in fieldSchemaForNameAndProjections
func schemaForBinding(binding *pf.MaterializationSpec_Binding) (bigquery.Schema, error) {
	fields := binding.FieldSelection.AllFields()
	schema := make(bigquery.Schema, len(fields))

	for idx, fieldName := range fields {
		var (
			err   error
			field *bigquery.FieldSchema
		)

		if field, err = fieldSchemaForNameAndProjections(fieldName, binding.Collection.Projections); err == errPrivateProjection {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("generating a field schema for field: %s, %w", fieldName, err)
		}

		schema[idx] = field
	}

	return schema, nil
}

func fieldSchemaForNameAndProjections(fieldName string, projections []pf.Projection) (*bigquery.FieldSchema, error) {
	var projection *pf.Projection

	for _, p := range projections {
		if p.Field == fieldName {
			projection = &p
			break
		}
	}

	if projection == nil {
		return nil, errNotFound
	}

	if projection.Inference.Exists == pf.Inference_INVALID || projection.Inference.Exists == pf.Inference_CANNOT {
		return nil, errNotFound
	}

	return &bigquery.FieldSchema{
		Name:        projection.Field,
		Type:        preferredFieldType(projection.Inference.Types),
		Required:    projection.Inference.Exists == pf.Inference_MUST,
		Description: projection.Inference.Description,
	}, nil
}

func preferredFieldType(possibleFields []string) bigquery.FieldType {
	switch possibleFields[0] {
	case "binary":
		return bigquery.BytesFieldType
	case "string":
		return bigquery.StringFieldType
	case "integer":
		return bigquery.IntegerFieldType
	case "number":
		return bigquery.BigNumericFieldType
	case "timestamp":
		return bigquery.TimestampFieldType
	default:
		return bigquery.BytesFieldType
	}
}
