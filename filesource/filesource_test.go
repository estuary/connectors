package filesource

import (
	"testing"

	"github.com/estuary/flow/go/parser"
	"github.com/stretchr/testify/require"
)

func TestPathParts(t *testing.T) {
	var bucket, key = PathToParts("foo/bar/baz")
	require.Equal(t, bucket, "foo")
	require.Equal(t, key, "bar/baz")

	require.Equal(t, "foo/bar/baz/", PartsToPath("foo", "bar/baz/"))
}

func TestIsParquetFile(t *testing.T) {
	for _, tc := range []struct {
		path   string
		format string
		expect bool
	}{
		{path: "bucket/data/catalog_sales.parquet", expect: true},
		{path: "bucket/data/catalog_sales.parquet.gz", expect: true},
		{path: "bucket/data/.parquet", expect: true},
		{path: "bucket/data/rows.csv", expect: false},
		{path: "bucket/data/rows.jsonl.gz", expect: false},
		{path: "bucket/data/parquet", expect: false},
		{path: "bucket/dir.parquet/rows.csv", expect: false},
		{path: "bucket/data/unknown.bin", expect: false},
		// A recognized format extension after a parquet segment decides,
		// while an unrecognized one is skipped over, as in the parser.
		{path: "bucket/data/data.parquet.csv", expect: false},
		{path: "bucket/data/data.parquet.bak", expect: true},
		// The parser infers from dots anywhere in the path, so an
		// extensionless file under a dotted directory resolves parquet.
		{path: "bucket/dir.parquet.d/file", expect: true},
		// An explicitly configured format wins over the extension.
		{path: "bucket/data/rows.csv", format: "parquet", expect: true},
		{path: "bucket/data/catalog_sales.parquet", format: "csv", expect: false},
		// Format "auto" defers to extension inference.
		{path: "bucket/data/catalog_sales.parquet", format: "auto", expect: true},
	} {
		var cfg *parser.Config
		if tc.format != "" {
			cfg = &parser.Config{Format: map[string]any{"type": tc.format}}
		}
		require.Equal(t, tc.expect, isParquetFile(tc.path, cfg), "path %q format %q", tc.path, tc.format)
	}
}
