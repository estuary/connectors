package hubspot

import (
	"encoding/json"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/stretchr/testify/require"
)

func TestFieldConfig(t *testing.T) {
	stringPropertyType := StringPropertyType

	tests := []struct {
		name     string
		raw      json.RawMessage
		expected FieldConfig
		isError  bool
	}{
		{
			name: "string",
			raw:  json.RawMessage(`{"property_type": "string"}`),
			expected: FieldConfig{
				PropertyType: &stringPropertyType,
			},
		},
		{
			name:    "invalid",
			raw:     json.RawMessage(`{"property_type": "invalid"}`),
			isError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fc FieldConfig
			err := boilerplate.UnmarshalStrict(tt.raw, &fc)
			if tt.isError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, fc)
			}
		})
	}
}
