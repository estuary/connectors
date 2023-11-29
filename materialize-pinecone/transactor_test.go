package main

import (
	"strings"
	"testing"

	"github.com/pkoukk/tiktoken-go"
	"github.com/stretchr/testify/require"
)

func TestTruncateInput(t *testing.T) {
	tokenizer, err := tiktoken.EncodingForModel(textEmbeddingAda002)
	require.NoError(t, err)

	tests := []struct {
		name      string
		input     string
		maxTokens int
		want      string
		truncated bool
	}{
		{
			name:      "one hello one token",
			input:     "hello",
			maxTokens: 1,
			want:      "hello",
			truncated: false,
		},
		{
			name:      "two hellos one token",
			input:     "hellohello",
			maxTokens: 1,
			want:      "hello",
			truncated: true,
		},
		{
			name:      "three hellos two tokens",
			input:     "hellohellohello",
			maxTokens: 2,
			want:      "hellohello",
			truncated: true,
		},
		{
			name:      "ascii then non-ascii",
			input:     "a中",
			maxTokens: 1,
			want:      "a",
			truncated: true,
		},
		{
			name:      "non-ascii then ascii",
			input:     "中a",
			maxTokens: 1,
			want:      "",
			truncated: true,
		},
		{
			name:      "hello plus 1 a",
			input:     "helloa",
			maxTokens: 1,
			want:      "hel",
			truncated: true,
		},
		{
			name:      "hello plus 2 a's",
			input:     "helloaa",
			maxTokens: 1,
			want:      "hel",
			truncated: true,
		},
		{
			name:      "hello plus 3 a's",
			input:     "helloaaa",
			maxTokens: 1,
			want:      "hell",
			truncated: true,
		},
		{
			name:      "long input",
			input:     strings.Repeat("hello", 8193),
			maxTokens: 8192,
			want:      strings.Repeat("hello", 8192),
			truncated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, truncated := truncateInput(tt.input, tokenizer, tt.maxTokens)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.truncated, truncated)
		})
	}
}
