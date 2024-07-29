package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type field struct {
	name       string
	scalarType types.ScalarAttributeType
}

// mapType maps a Projection to a DynamoDB ScalarAttributeType. The
// ScalarAttributeType will be an empty string unless the Projection can be used
// as a hash or sort key.
func mapType(p boilerplate.Projection, fc *any) (field, boilerplate.ElementConverter) {
	out := field{name: p.Field}

	if p.NumericString != nil {
		switch *p.NumericString {
		case boilerplate.StringFormatInteger:
			return out, convertStringInteger
		case boilerplate.StringFormatNumber:
			// DynamoDB doesn't support non-numeric float values.
			// https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/dynamodb/types.html
			return out, boilerplate.StrToFloat(nil, nil, nil)
		}
	}

	if len(p.TypesWithoutNull) != 1 {
		// Multiple possible types, a single null type, or completely unconstrained types.
		return out, nil
	}

	// Single type. Map it to a DynamoDB scalar type if possible, and establish additional
	// conversions for storing its data in the database.
	switch p.TypesWithoutNull[0] {
	case "string":
		if p.Inference.String_.ContentEncoding == "base64" {
			out.scalarType = types.ScalarAttributeTypeB
			return out, boilerplate.DecodeBase64String
		} else {
			out.scalarType = types.ScalarAttributeTypeS
			return out, nil
		}
	case "boolean":
		// For boolean key fields to be used as DynamoDB key fields, they must be converted to
		// strings. Otherwise they can be directly as booleans.
		if p.IsPrimaryKey {
			out.scalarType = types.ScalarAttributeTypeS
			return out, boilerplate.ToStr
		} else {
			return out, nil
		}
	case "integer":
		out.scalarType = types.ScalarAttributeTypeN
		return out, nil
	case "number":
		return out, nil
	case "array", "object":
		return out, boilerplate.HydrateObject
	default:
		panic(fmt.Sprintf("unsupported type %s", p.Inference.Types))
	}
}

var typeMapper = boilerplate.NewTypeMapper(mapType)

func mapBinding(bindingSpec *pf.MaterializationSpec_Binding) ([]field, []boilerplate.ElementConverter, error) {
	fields := make([]field, 0, len(bindingSpec.FieldSelection.AllFields()))
	converters := make([]boilerplate.ElementConverter, 0, len(bindingSpec.FieldSelection.AllFields()))

	for _, f := range bindingSpec.FieldSelection.AllFields() {
		mapped, err := typeMapper.Map(bindingSpec.Collection.GetProjection(f), bindingSpec.FieldSelection.FieldConfigJsonMap)
		if err != nil {
			return nil, nil, err
		}
		fields = append(fields, mapped.EndpointType)
		converters = append(converters, mapped.Converter)
	}

	return fields, converters, nil
}

// wrappedInteger provides handling for integer values that may be
// string-encoded. These integer values will may be very large and their string
// representation will be preserved if possible. It is constructed using the
// element converter convertStringInteger.
type wrappedInteger struct {
	stringified string
}

var _ attributevalue.Marshaler = (*wrappedInteger)(nil)

func (w *wrappedInteger) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	return &types.AttributeValueMemberN{
		Value: w.stringified,
	}, nil
}

func convertStringInteger(te tuple.TupleElement) (any, error) {
	out := &wrappedInteger{}

	switch tt := te.(type) {
	case string:
		out.stringified = tt
	case int64, uint64:
		c, err := boilerplate.ToStr(te)
		if err != nil {
			return nil, err
		}
		out.stringified = c.(string)
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported type %T (%#v)", te, te)
	}

	return out, nil
}

type constrainter struct{}

func (constrainter) NewConstraints(p boilerplate.Projection, deltaUpdates bool, fc *any) *pm.Response_Validated_Constraint {
	// By default only the collection key and root document fields are materialized, due to
	// DynamoDB's 400kb single item size limit. Additional fields are optional and may be selected
	// to materialize as top-level properties with the applicable conversion applied, if desired.
	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Primary key locations are required"
	case p.IsRootDocumentProjection() && !deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document is required for a standard updates materialization"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"

	default:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "This field is able to be materialized"
	}

	return &constraint
}

func (constrainter) Compatible(existing boilerplate.EndpointField, proposed boilerplate.Projection, fc *any) (bool, error) {
	// Non-key fields have no compatibility restrictions and can be changed in any way at any time.
	// This relies on the assumption that the key of an establish Flow collection cannot be changed
	// after the fact.
	if !proposed.IsPrimaryKey {
		return true, nil
	}

	field, _ := mapType(proposed, fc)
	return strings.EqualFold(existing.Type, string(field.scalarType)), nil
}

func (constrainter) DescriptionForType(p boilerplate.Projection, fc *any) (string, error) {
	field, _ := mapType(p, fc)

	out := ""
	switch t := field.scalarType; t {
	case types.ScalarAttributeTypeS:
		out = "string"
	case types.ScalarAttributeTypeN:
		out = "numeric"
	case types.ScalarAttributeTypeB:
		out = "binary"
	}

	return out, nil
}

func infoSchema(ctx context.Context, db *dynamodb.Client, tableNames []string) (*boilerplate.InfoSchema, error) {
	is := boilerplate.NewInfoSchema(
		func(rp []string) []string {
			// Pass-through the index name as-is, since it is the only component of the resource
			// path and the required transformations are assumed to already be done as part of the
			// Validate response.
			return rp
		},
		func(f string) string { return f },
	)

	for _, t := range tableNames {
		d, err := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(t),
		})
		if err != nil {
			var errNotFound *types.ResourceNotFoundException
			if errors.As(err, &errNotFound) {
				// Table hasn't been created yet.
				continue
			}
			return nil, fmt.Errorf("describing table %q: %w", t, err)
		}

		for _, def := range d.Table.AttributeDefinitions {
			is.PushField(boilerplate.EndpointField{
				Name:               *def.AttributeName,
				Nullable:           false,                     // Table keys can never be nullable.
				Type:               string(def.AttributeType), // "B", "S", or "N".
				CharacterMaxLength: 0,
			}, t)
		}
	}

	return is, nil
}
