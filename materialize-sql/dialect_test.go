package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuoteTransform(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		quote    string
		escape   string
		expected string
	}{
		{
			name:     "no escape",
			input:    `foo`,
			quote:    "'",
			escape:   "\\'",
			expected: `'foo'`,
		},
		{
			name:     "single quote",
			input:    `foo'bar`,
			quote:    "'",
			escape:   "''",
			expected: `'foo''bar'`,
		},
		{
			name:     "backslash quote",
			input:    `foo\'bar`,
			quote:    "'",
			escape:   "\\'",
			expected: `'foo\\'bar'`,
		},
		{
			name:     "backslash",
			input:    `foo\\bar`,
			quote:    "'",
			escape:   "\\'",
			expected: `'foo\\bar'`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transform := QuoteTransform(tt.quote, tt.escape)
			actual := transform(tt.input)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestQuoteTransformEscapedBackslash(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		quote    string
		escape   string
		expected string
	}{
		{
			name:     "no escape",
			input:    `foo`,
			quote:    "'",
			escape:   "\\'",
			expected: `'foo'`,
		},
		{
			name:     "single quote",
			input:    `foo'bar`,
			quote:    "'",
			escape:   "\\'",
			expected: `'foo\'bar'`,
		},
		{
			name:     "backslash quote",
			input:    `foo\'bar`,
			quote:    "'",
			escape:   "\\'",
			expected: `'foo\\\'bar'`,
		},
		{
			name:     "backslash",
			input:    `foo\\bar`,
			quote:    "'",
			escape:   "\\'",
			expected: `'foo\\\\bar'`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transform := QuoteTransformEscapedBackslash(tt.quote, tt.escape)
			actual := transform(tt.input)
			require.Equal(t, tt.expected, actual)
		})
	}
}
