package hubspot

import (
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestNewMappedFieldConvert(t *testing.T) {
	tests := []struct {
		name        string
		projection  pf.Projection
		fc          FieldConfig
		expected    *MappedField
		expectedErr bool
	}{
		{
			name: "string",
			projection: pf.Projection{
				Field:        "email",
				Ptr:          "/email",
				IsPrimaryKey: true,
				Inference: pf.Inference{
					Types:       []string{"string", "null"},
					String_:     &pf.Inference_String{},
					Title:       "My Title",
					Description: "My Description",
				},
			},
			expected: &MappedField{
				Name: "email",
				Ptr:  "/email",
				Property: &Property{
					Name:           "email",
					GroupName:      "flow",
					Label:          "My Title",
					Description:    "My Description",
					HasUniqueValue: true,
					Type:           StringPropertyType,
					FieldType:      TextPropertyFieldType,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, err := NewMappedField(tt.projection, tt.fc)
			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, field)
			}
		})
	}
}

func TestMappedFieldConvert(t *testing.T) {
	tests := []struct {
		name        string
		field       MappedField
		elem        tuple.TupleElement
		property    *Property
		expected    any
		expectedErr bool
	}{
		{
			name: "",
			field: MappedField{
				Name: "email",
				Ptr:  "/email",
				Property: &Property{
					Name:           "email",
					GroupName:      "flow",
					Label:          "My Title",
					Description:    "My Description",
					HasUniqueValue: true,
					Type:           StringPropertyType,
					FieldType:      TextPropertyFieldType,
				},
			},
			elem:     tuple.TupleElement("foo@example.org"),
			property: contactsProperties["email"],
			expected: "foo@example.org",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.field.Convert(tt.elem, tt.property)
			require.Equal(t, tt.expected, actual)
			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, actual)
			}

		})
	}
}

func TestPropertyName(t *testing.T) {
	tests := []struct {
		name        string
		field       string
		expected    string
		expectedErr bool
	}{
		{
			name:     "core metadata",
			field:    "_meta/op",
			expected: "meta_op",
		},
		{
			name:     "basic property",
			field:    "properties/domain",
			expected: "domain",
		},
		{
			name:     "document",
			field:    "flow_document",
			expected: "flow_document",
		},
		{
			name:     "has uppercase",
			field:    "Company",
			expected: "company",
		},
		{
			name:     "symbols",
			field:    "xyz@123",
			expected: "xyz_123",
		},
		{
			name:        "invalid",
			field:       "42",
			expected:    "",
			expectedErr: true,
		},
		{
			name:     "too long",
			field:    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
			expected: "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuv",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := PropertyName(tt.field)
			require.Equal(t, tt.expected, actual)
			if tt.expectedErr {
				require.Error(t, err)
			}
		})
	}
}
