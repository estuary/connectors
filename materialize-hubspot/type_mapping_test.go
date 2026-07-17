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
					Label:          "My Title",
					Description:    "My Description",
					HasUniqueValue: true,
					Type:           StringPropertyType,
					FieldType:      TextPropertyFieldType,
				},
			},
		},
		{
			name: "null only",
			projection: pf.Projection{
				Field: "firstname",
				Ptr:   "/firstname",
				Inference: pf.Inference{
					Types:       []string{"null"},
					String_:     &pf.Inference_String{},
					Title:       "My Title",
					Description: "My Description",
				},
			},
			expected: &MappedField{
				Name: "firstname",
				Ptr:  "/firstname",
				Property: &Property{
					Name:        "firstname",
					Label:       "My Title",
					Description: "My Description",
					Type:        StringPropertyType,
					FieldType:   TextPropertyFieldType,
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
			name: "string",
			field: MappedField{
				Name:     "email",
				Ptr:      "/email",
				Property: contactsProperties["email"],
			},
			elem:     tuple.TupleElement("foo@example.org"),
			property: contactsProperties["email"],
			expected: "foo@example.org",
		},
		{
			name: "null",
			field: MappedField{
				Name:     "firstname",
				Ptr:      "/firstname",
				Property: contactsProperties["firstname"],
			},
			elem:     tuple.TupleElement(nil),
			property: contactsProperties["firstname"],
			expected: nil,
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
			expected: "metaop",
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
			expected: "xyz123",
		},
		{
			name:     "symbols dash",
			field:    "my-name",
			expected: "myname",
		},
		{
			name:     "starts with number",
			field:    "22name",
			expected: "n22name",
		},
		{
			name:     "number",
			field:    "42",
			expected: "n42",
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
