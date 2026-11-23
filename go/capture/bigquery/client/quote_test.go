package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuoteIdentifier(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
	}{
		{"my_dataset", "`my_dataset`"},
		{"a`b", "`a\\`b`"},
		{"a\\b", "`a\\\\b`"},
		{"a\\`b", "`a\\\\\\`b`"},
		{"a\\", "`a\\\\`"},
	} {
		require.Equal(t, tc.want, QuoteIdentifier(tc.name), "name %q", tc.name)
	}
}
