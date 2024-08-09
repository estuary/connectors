package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeUnistr(t *testing.T) {
	var cases = [][]string{
		{"\\2764\\FE0F", "❤️"},
		{"\\D83D\\DD25\\FE0F", "🔥️"},
		{"\\D83D\\DD25\\FE0F\\2764\\FE0F", "🔥️❤️"},
		{"\\005C \\\\ \\2764\\FE0F \\\\", "\\ \\ ❤️ \\"},
	}

	for _, c := range cases {
		out, err := decodeUnistr(c[0])
		require.NoError(t, err)
		require.Equal(t, c[1], out)
	}
}
