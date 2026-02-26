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
			name:      "nested dot hash style",
			namespace: "flow",
			table:     "mytable",
			style:     NestedDotHashLocationStyle,
			expected:  "flow/mytable.D7D053D01D253AEB",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := createLocationSuffix(tt.namespace, tt.table, tt.style)
			require.Equal(t, tt.expected, actual)
		})
	}
}
