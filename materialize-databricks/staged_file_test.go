package connector

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/files"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

type fakeFiles struct {
	files.FilesInterface
	created []string
}

func (f *fakeFiles) CreateDirectory(_ context.Context, req files.CreateDirectoryRequest) error {
	f.created = append(f.created, req.DirectoryPath)
	return nil
}

// TestStagedFileCompression covers the local-file half of stagedFile: staged files are gzipped and
// named so that Databricks decompresses them on read.
func TestStagedFileCompression(t *testing.T) {
	var f = newStagedFile(config{}, "/Volumes/c/s/v/root", []string{"first", "second"}, nil)
	f.dir = t.TempDir()
	f.txnDir = "txn-1"

	require.NoError(t, f.newFile())
	require.NoError(t, f.writer.Write([]any{"hello", 42}))
	require.NoError(t, f.writer.Close())

	require.Len(t, f.uploaded, 1)
	require.True(t, strings.HasPrefix(f.uploaded[0], "txn-1/"), "got %q", f.uploaded[0])
	require.True(t, strings.HasSuffix(f.uploaded[0], ".json.gz"), "got %q", f.uploaded[0])
	require.Equal(t, "/Volumes/c/s/v/root/txn-1", f.remoteDir())
	require.Equal(t, "/Volumes/c/s/v/root/txn-1/"+filepath.Base(f.uploaded[0]), pathsWithRoot(f.root, f.uploaded)[0])

	contents, err := os.ReadFile(filepath.Join(f.dir, filepath.Base(f.uploaded[0])))
	require.NoError(t, err)

	gz, err := gzip.NewReader(bytes.NewReader(contents))
	require.NoError(t, err)
	decompressed, err := io.ReadAll(gz)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(decompressed, &got))
	require.Equal(t, map[string]any{"first": "hello", "second": float64(42)}, got)
}

func TestStagedFileStartCreatesDirectory(t *testing.T) {
	var api = &fakeFiles{}
	var f = newStagedFile(config{}, "/Volumes/c/s/v/root", []string{"id"}, api)
	f.dir = filepath.Join(t.TempDir(), "local")

	require.NoError(t, f.start(context.Background(), nil))
	require.Len(t, api.created, 1)
	require.Equal(t, f.remoteDir(), api.created[0])
	require.True(t, strings.HasPrefix(api.created[0], "/Volumes/c/s/v/root/"), api.created[0])
	require.NotEqual(t, "/Volumes/c/s/v/root", api.created[0])
	close(f.putFiles)
	require.NoError(t, f.group.Wait())
}

func TestStagedSchemaDDL(t *testing.T) {
	var col = func(field, ddl string) *sql.Column {
		var c = &sql.Column{MappedType: sql.MappedType{DDL: ddl}}
		c.Field = field
		return c
	}
	require.Equal(t,
		"`a_key` STRING, `int` BIGINT, `big` STRING, `num` STRING, `bool` BOOLEAN, `ts` STRING, `bin` STRING, `we``ird` STRING, `_flow_delete` BOOLEAN",
		stagedSchemaDDL([]*sql.Column{
			col("a key", "STRING NOT NULL"),
			col("int", "LONG"),
			col("big", "NUMERIC(38,0)"),
			col("num", "DOUBLE"),
			col("bool", "BOOLEAN NOT NULL"),
			col("ts", "TIMESTAMP"),
			col("bin", "BINARY"),
			col("we`ird", "STRING"),
		}, true))
}
