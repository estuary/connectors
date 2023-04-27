package sqlcapture

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecommendedCatalogName(t *testing.T) {
	tests := []struct {
		schema string
		table  string
		want   string
	}{
		{
			schema: "something",
			table:  "typical",
			want:   "something.typical",
		},
		{
			schema: "Capital",
			table:  "LetterS",
			want:   "capital.letters",
		},
		{
			schema: "for$bidden",
			table:  "char%s",
			want:   "for_bidden.char_s",
		},
		{
			schema: "schema",
			table:  "/table",
			want:   "schema._table",
		},
		{
			schema: "path-like",
			table:  "/../this",
			want:   "path-like._.._this",
		},
		{
			schema: "@",
			table:  "!",
			want:   "_._",
		},
		{
			schema: ".",
			table:  "/",
			want:   ".._",
		},
		{
			schema: ".",
			table:  "", // This should not be possible, but even if it were it would not be a problem as a collection name.
			want:   "..",
		},
		{
			schema: "/",
			table:  "./",
			want:   "_.._",
		},
		{
			schema: "multiple////",
			table:  "slashes",
			want:   "multiple____.slashes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.schema+"."+tt.table, func(t *testing.T) {
			got := recommendedCatalogName(tt.schema, tt.table)
			require.Equal(t, path.Clean(got), got)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestKeyRoundTrip(t *testing.T) {
	for _, pk := range []string{
		"standard",
		"with/slash",
		"with~tilde",
		"/start",
		"~start",
		"end/",
		"end~",
		"has~0escape~1already",
		"~/",
		"~0~1",
		"~1~0",
		"~~01",
		"~~10",
		"~01~",
		"~0",
		"~1",
		"~",
		"/",
		"",
	} {
		t.Run(pk, func(t *testing.T) {
			require.Equal(t, pk, collectionKeyToPrimaryKey(primaryKeyToCollectionKey(pk)))
		})
	}
}

func TestParseVersion(t *testing.T) {
	tests := []struct {
		version   string
		major     int
		minor     int
		shouldErr bool
	}{
		{
			version:   "invalid",
			major:     0,
			minor:     0,
			shouldErr: true,
		},
		{
			version:   "",
			major:     0,
			minor:     0,
			shouldErr: true,
		},
		{
			version:   "10",
			major:     0,
			minor:     0,
			shouldErr: true,
		},
		{
			version:   "10.0",
			major:     10,
			minor:     0,
			shouldErr: false,
		},
		{
			version:   "11.0 (Debian 11.0-1.pgdg90+2)",
			major:     11,
			minor:     0,
			shouldErr: false,
		},
		{
			version:   "v11.0 (Debian 11.0-1.pgdg90+2)",
			major:     11,
			minor:     0,
			shouldErr: false,
		},
		{
			version:   "10.3.38-MariaDB-1:10.3.38+maria~ubu2004-log",
			major:     10,
			minor:     3,
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			major, minor, err := ParseVersion(tt.version)
			if tt.shouldErr {
				require.Error(t, err)
				return
			}

			require.Equal(t, tt.major, major)
			require.Equal(t, tt.minor, minor)
		})
	}
}

func TestValidVersion(t *testing.T) {
	tests := []struct {
		name               string
		major, minor       int
		reqMajor, reqMinor int
		valid              bool
	}{
		{
			name:     "equal major and minor",
			major:    2,
			minor:    3,
			reqMajor: 2,
			reqMinor: 3,
			valid:    true,
		},
		{
			name:     "bigger major, smaller minor",
			major:    2,
			minor:    3,
			reqMajor: 1,
			reqMinor: 4,
			valid:    true,
		},
		{
			name:     "smaller major, bigger minor",
			major:    2,
			minor:    3,
			reqMajor: 3,
			reqMinor: 2,
			valid:    false,
		},
		{
			name:     "bigger major, bigger minor",
			major:    2,
			minor:    3,
			reqMajor: 1,
			reqMinor: 2,
			valid:    true,
		},
		{
			name:     "smaller major, smaller minor",
			major:    2,
			minor:    3,
			reqMajor: 3,
			reqMinor: 4,
			valid:    false,
		},
		{
			name:     "bigger major, equal minor",
			major:    2,
			minor:    3,
			reqMajor: 1,
			reqMinor: 3,
			valid:    true,
		},
		{
			name:     "smaller major, equal minor",
			major:    2,
			minor:    3,
			reqMajor: 3,
			reqMinor: 3,
			valid:    false,
		},
		{
			name:     "equal major, bigger minor",
			major:    2,
			minor:    3,
			reqMajor: 2,
			reqMinor: 2,
			valid:    true,
		},
		{
			name:     "equal major, smaller minor",
			major:    2,
			minor:    3,
			reqMajor: 2,
			reqMinor: 4,
			valid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.valid, ValidVersion(tt.major, tt.minor, tt.reqMajor, tt.reqMinor))
		})
	}
}
