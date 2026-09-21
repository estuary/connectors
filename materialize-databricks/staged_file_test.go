package connector

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStagedFileCompression covers the local-file half of stagedFile: staged files are gzipped and
// named so that Databricks decompresses them on read.
func TestStagedFileCompression(t *testing.T) {
	var f = &stagedFile{
		fields: []string{"first", "second"},
		dir:    t.TempDir(),
	}

	require.NoError(t, f.newFile())
	require.NoError(t, f.writer.Write([]any{"hello", 42}))
	require.NoError(t, f.writer.Close())

	require.Len(t, f.uploaded, 1)
	require.True(t, strings.HasSuffix(f.uploaded[0], ".json.gz"), "got %q", f.uploaded[0])

	contents, err := os.ReadFile(filepath.Join(f.dir, f.uploaded[0]))
	require.NoError(t, err)

	gz, err := gzip.NewReader(bytes.NewReader(contents))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(gz)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(decompressed, &got))
	require.Equal(t, map[string]any{"first": "hello", "second": float64(42)}, got)
}
