package hubspot

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchRequest(t *testing.T) {
	tests := []struct {
		name          string
		searchRequest *SearchRequest
		expected      json.RawMessage
	}{
		{
			name: "",
			searchRequest: &SearchRequest{
				FilterGroups: NewFilterGroupsEquals("domain", []string{
					"foo.example.com",
					"bar.example.com",
				}),
				Properties: []string{"property1", "property2"},
			},
			expected: json.RawMessage(`
				{
				  "filterGroups": [
					{
					  "filters": [
						{
						  "propertyName": "domain",
						  "operator": "EQ",
						  "value": "foo.example.com"
						}
					  ]
					},
					{
					  "filters": [
						{
						  "propertyName": "domain",
						  "operator": "EQ",
						  "value": "bar.example.com"
						}
					  ]
					}
				  ],
				  "properties": ["property1", "property2"]
				}
			`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := json.Marshal(tt.searchRequest)
			require.NoError(t, err)

			expected, err := json.Marshal(tt.expected)
			require.NoError(t, err)

			require.Equal(t, string(expected), string(actual))
		})

	}
}
