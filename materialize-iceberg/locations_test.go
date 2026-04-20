package connector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocationSuffix(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		table     string
		style     LocationStyle
		expected  string
	}{
		{
			name:      "flat style",
			namespace: "flow",
			table:     "mytable",
			style:     FlatLocationStyle,
			expected:  "flow_mytable_15EEAE2C604ABEC4",
		},
		{
			name:      "flat style needs sanitize",
			namespace: "flow",
			table:     "my.table",
			style:     FlatLocationStyle,
			expected:  "flow_my_table_D5723292493D5622",
		},
		{
			name:      "flat style hyphen sanitize",
			namespace: "flow",
			table:     "my-table",
			style:     FlatLocationStyle,
			expected:  "flow_my_table_839FE986CB099DF4",
		},
		{
			name:      "nested dot hash style",
			namespace: "flow",
			table:     "mytable",
			style:     NestedDotHashLocationStyle,
			expected:  "flow/mytable.D7D053D01D253AEB",
		},
		{
			name:      "nested dot hash style needs sanitize",
			namespace: "flow",
			table:     "my.table",
			style:     NestedDotHashLocationStyle,
			expected:  "flow/my_table.25FBA56C761DB31B",
		},
		{
			name:      "nested dot hash style hyphen no sanitize",
			namespace: "flow",
			table:     "my-table",
			style:     NestedDotHashLocationStyle,
			expected:  "flow/my_table.25FBA56C761DB31B",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := createLocationSuffix(tt.namespace, tt.table, tt.style)
			require.Equal(t, tt.expected, actual)
		})
	}
}
