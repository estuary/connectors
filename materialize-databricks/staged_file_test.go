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

// writeOneRow exercises the local-file half of stagedFile: newFile, a single row, and closing the
// writer. It returns the name and contents of the file that was written.
func writeOneRow(t *testing.T, cfg config) (string, []byte) {
	t.Helper()

	var f = &stagedFile{
		fields: []string{"first", "second"},
		dir:    t.TempDir(),
		cfg:    cfg,
	}

	require.NoError(t, f.newFile())
	require.NoError(t, f.writer.Write([]any{"hello", 42}))
	require.NoError(t, f.writer.Close())

	require.Len(t, f.uploaded, 1)
	var contents, err = os.ReadFile(filepath.Join(f.dir, f.uploaded[0]))
	require.NoError(t, err)

	return f.uploaded[0], contents
}

func TestStagedFileCompression(t *testing.T) {
	var wantRow = map[string]any{"first": "hello", "second": float64(42)}

	t.Run("enabled by default", func(t *testing.T) {
		var name, contents = writeOneRow(t, config{})
		require.True(t, strings.HasSuffix(name, ".json.gz"), "got %q", name)

		gz, err := gzip.NewReader(bytes.NewReader(contents))
		require.NoError(t, err)
		decompressed, err := io.ReadAll(gz)
		require.NoError(t, err)

		var got map[string]any
		require.NoError(t, json.Unmarshal(decompressed, &got))
		require.Equal(t, wantRow, got)
	})

	t.Run("disabled", func(t *testing.T) {
		var cfg = config{Advanced: advancedConfig{DisableStagedFileCompression: true}}
		var name, contents = writeOneRow(t, cfg)
		require.True(t, strings.HasSuffix(name, ".json"), "got %q", name)
		require.False(t, strings.HasSuffix(name, ".gz"), "got %q", name)

		var got map[string]any
		require.NoError(t, json.Unmarshal(contents, &got))
		require.Equal(t, wantRow, got)
	})
}
