package smb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPath(t *testing.T) {
	for _, tt := range []struct{ path, smb string }{
		{"/", ""},
		{"/inbox", "inbox"},
		{"/inbox/sub/b.csv", `inbox\sub\b.csv`},
		{"/My Files/日次.csv", `My Files\日次.csv`},
	} {
		t.Run(tt.path, func(t *testing.T) {
			require.Equal(t, tt.smb, Path(tt.path))
		})
	}
}

func TestNormalizePath(t *testing.T) {
	for _, tt := range []struct{ path, normalized string }{
		{"", "/"},
		{"/", "/"},
		{`\`, "/"},
		{"inbox", "/inbox"},
		{"/inbox", "/inbox"},
		{`\inbox`, "/inbox"},
		{`inbox\sub\`, "/inbox/sub"},
		{`\inbox\sub/`, "/inbox/sub"},
		{"/inbox//./sub/../", "/inbox"},
		{"/inbox/../other", "/other"},
		{"../inbox", "/inbox"},
		{".", "/"},
		{`My Files\日次`, "/My Files/日次"},
	} {
		t.Run(tt.path, func(t *testing.T) {
			normalized, err := NormalizePath(tt.path)
			require.NoError(t, err)
			require.Equal(t, tt.normalized, normalized)
			require.NoError(t, ValidatePath(normalized))
			again, err := NormalizePath(normalized)
			require.NoError(t, err)
			require.Equal(t, normalized, again)
		})
	}
	for _, p := range []string{"/inbox\x00", "\x00/../inbox"} {
		_, err := NormalizePath(p)
		require.ErrorContains(t, err, "NUL")
	}
}

func TestValidatePath(t *testing.T) {
	for _, p := range []string{"", "inbox", "/inbox/", "/inbox/../other", `/inbox\sub`, "/inbox\x00"} {
		t.Run(p, func(t *testing.T) {
			require.Error(t, ValidatePath(p))
		})
	}
}
