package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

type mappedType struct {
	// Name of the field.
	field string

	// The equivalent DynamoDB scalar attribute type if this JSON type is able to be used as a
	// DynamoDB partition key or sort key. Unset otherwise.
	ddbScalarType types.ScalarAttributeType

	// Converts tuple values into database-friendly values.
	converter func(tuple.TupleElement) (any, error)
}

func (m mappedType) String() string {
	out := ""
	switch t := m.ddbScalarType; t {
	case types.ScalarAttributeTypeS:
		out = "string"
	case types.ScalarAttributeTypeN:
		out = "numeric"
	case types.ScalarAttributeTypeB:
		out = "binary"
	}
	return out
}

func (m mappedType) Compatible(existing boilerplate.ExistingField) bool {
	// Non-key fields have no compatibility restrictions and can be changed in any way at any time.
	// This relies on the assumption that the key of an establish Flow collection cannot be changed
	// after the fact.
	return strings.EqualFold(existing.Type, string(m.ddbScalarType))
}

func (m mappedType) CanMigrate(existing boilerplate.ExistingField) bool {
	return false
}

func mapType(p pf.Projection) mappedType {
	out := mappedType{
		field: p.Field,
	}

	if _, ok := boilerplate.AsFormattedNumeric(&p); ok {
		// A string field formatted as an integer or number, with a possible additional
		// corresponding integer or number type.
		out.converter = convertNumeric
		return out
	}

	jsonTypes := typesWithoutNull(p.Inference.Types)

	if len(jsonTypes) != 1 {
		// Multiple possible types, a single null type, or completely unconstrained types.
		out.converter = passthrough
		return out
	}

	// Single type. Map it to a DynamoDB scalar type if possible, and establish additional
	// conversions for storing its data in the database.
	switch t := jsonTypes[0]; t {
	case pf.JsonTypeString:
		if p.Inference.String_.ContentEncoding == "base64" {
			out.ddbScalarType = types.ScalarAttributeTypeB
			out.converter = convertBase64
		} else {
			out.ddbScalarType = types.ScalarAttributeTypeS
			out.converter = passthrough
		}
	case pf.JsonTypeBoolean:
		// For boolean key fields to be used as DynamoDB key fields, they must be converted to
		// strings. Otherwise they can be directly as booleans.
		if p.IsPrimaryKey {
			out.ddbScalarType = types.ScalarAttributeTypeS
			out.converter = stringify
		} else {
			out.converter = passthrough
		}
	case pf.JsonTypeInteger:
		out.ddbScalarType = types.ScalarAttributeTypeN
		out.converter = convertNumeric
	case pf.JsonTypeNumber:
		out.ddbScalarType = types.ScalarAttributeTypeN
		out.converter = convertNumeric
	case pf.JsonTypeArray, pf.JsonTypeObject:
		out.converter = convertObject
	case "":
		// Empty type string - treat as passthrough
		out.converter = passthrough
	default:
		panic(fmt.Errorf("invalid JSON type %s", t))
	}

	return out
}

func typesWithoutNull(ts []string) []string {
	out := []string{}
	for _, t := range ts {
		if t != "null" {
			out = append(out, t)
		}
	}

	return out
}

func stringify(te tuple.TupleElement) (any, error) {
	b, err := json.Marshal(te)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func convertObject(te tuple.TupleElement) (any, error) {
	var out any

	switch b := te.(type) {
	case []byte:
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, err
		}
	case json.RawMessage:
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, err
		}
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid type %T (%#v) for variant", te, te)
	}

	return out, nil
}

func passthrough(te tuple.TupleElement) (any, error) {
	return te, nil
}

// wrappedNumeric provides handling for numeric values that can either be the actual numeric value
// (integer or number), or a string with an applicable format.
type wrappedNumeric struct {
	innerNumeric string
}

var _ attributevalue.Marshaler = (*wrappedNumeric)(nil)

func (w *wrappedNumeric) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	return &types.AttributeValueMemberN{
		Value: w.innerNumeric,
	}, nil
}

func convertNumeric(te tuple.TupleElement) (any, error) {
	out := &wrappedNumeric{}

	switch tt := te.(type) {
	case string:
		switch tt {
		case "NaN", "Infinity", "-Infinity":
			// DynamoDB doesn't support non-numeric float values.
			// https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/dynamodb/types.html
			return nil, nil
		default:
			out.innerNumeric = tt
		}
	case int:
		out.innerNumeric = strconv.Itoa(tt)
	case int64:
		out.innerNumeric = strconv.FormatInt(tt, 10)
	case float64:
		out.innerNumeric = strconv.FormatFloat(tt, 'f', -1, 64)
	case nil:
		return nil, nil
	default:
		return out, fmt.Errorf("unsupported type %T (%#v)", te, te)
	}

	return out, nil
}

func convertBase64(te tuple.TupleElement) (any, error) {
	switch t := te.(type) {
	case string:
		bytes, err := base64.StdEncoding.DecodeString(t)
		if err != nil {
			return nil, fmt.Errorf("decoding bytes from string: %w", err)
		}

		return bytes, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("convertBase64 unsupported type %T (%#v)", te, te)
	}
}

type fieldConfig struct{}

func (fieldConfig) Validate() error {
	return nil
}

func (fieldConfig) CastToString() bool {
	return false
}
