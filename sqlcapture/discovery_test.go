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
