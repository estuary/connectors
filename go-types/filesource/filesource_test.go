package filesource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathParts(t *testing.T) {
	var bucket, key = PathToParts("foo/bar/baz")
	require.Equal(t, bucket, "foo")
	require.Equal(t, key, "bar/baz")

	require.Equal(t, "foo/bar/baz/", PartsToPath("foo", "bar/baz/"))
}
