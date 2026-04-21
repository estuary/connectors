package main

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
			name:      "nested style",
			namespace: "flow",
			table:     "mytable",
			style:     NestedLocationStyle,
			expected:  "flow/mytable_3A6FA27EC5D2E5B2",
		},
		{
			name:      "nested style needs sanitize",
			namespace: "flow",
			table:     "my.table",
			style:     NestedLocationStyle,
			expected:  "flow/my_table_1224DB2BF670A673",
		},
		{
			name:      "nested style hyphen no sanitize",
			namespace: "flow",
			table:     "my-table",
			style:     NestedLocationStyle,
			expected:  "flow/my-table_8B5C97BB6111DDEE",
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
			expected:  "flow/my-table.47E2A0063A659F99",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := createLocationSuffix(tt.namespace, tt.table, tt.style)
			require.Equal(t, tt.expected, actual)
		})
	}
}
