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
	"sync"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/files"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

// fakeFiles records directory and file operations; directories list as empty.
type fakeFiles struct {
	files.FilesInterface
	mu               sync.Mutex
	created, deleted []string
	// entries are the listed contents of directories.
	entries map[string][]files.DirectoryEntry
}

func (f *fakeFiles) CreateDirectory(_ context.Context, req files.CreateDirectoryRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, req.DirectoryPath)
	return nil
}

func (f *fakeFiles) ListDirectoryContentsAll(_ context.Context, req files.ListDirectoryContentsRequest) ([]files.DirectoryEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.entries[req.DirectoryPath], nil
}

func (f *fakeFiles) DeleteByFilePath(_ context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, path)
	return nil
}

func (f *fakeFiles) DeleteDirectoryByDirectoryPath(_ context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, path)
	return nil
}

// TestStagedFileCompression covers the local-file half of stagedFile: staged files are gzipped and
// named so that Databricks decompresses them on read.
func TestStagedFileCompression(t *testing.T) {
	var f = newStagedFile(config{}, "/Volumes/c/s/v/root", "abc", []string{"first", "second"}, nil)
	f.dir = t.TempDir()
	f.txnDir = "txn-1"

	require.NoError(t, f.newFile())
	require.NoError(t, f.writer.Write([]any{"hello", 42}))
	require.NoError(t, f.writer.Close())

	local, err := os.ReadDir(f.dir)
	require.NoError(t, err)
	require.Len(t, local, 1)
	require.True(t, strings.HasSuffix(local[0].Name(), ".json.gz"), "got %q", local[0].Name())
	require.Equal(t, "/Volumes/c/s/v/root/txn-1", f.remoteDir())

	contents, err := os.ReadFile(filepath.Join(f.dir, local[0].Name()))
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
	var f = newStagedFile(config{}, "/Volumes/c/s/v/root", "abc", []string{"id"}, api)
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

// TestStagingStatementsQuoteNames covers the statements that name a catalog or
// schema outside of a target table's identifier.
func TestStagingStatementsQuoteNames(t *testing.T) {
	const root = "/Volumes/o'cat/sch`ema/flow_staging/flow_temp_tables"

	t.Run("create volume", func(t *testing.T) {
		var db = recordingDB(t, nil)
		require.NoError(t, createStagingVolume(t.Context(), db, testDialect, "sch`ema"))
		require.Equal(t, []string{"CREATE VOLUME IF NOT EXISTS `sch``ema`.flow_staging;"}, recording.executed)
	})

	t.Run("put", func(t *testing.T) {
		var dir = t.TempDir()
		var path = filepath.Join(dir, "file.json.gz")
		require.NoError(t, os.WriteFile(path, []byte("{}"), 0o644))

		var db = recordingDB(t, nil)
		var paths = make(chan string, 1)
		paths <- path
		close(paths)
		var f = &stagedFile{dir: dir, root: root, txnDir: "txn"}
		require.NoError(t, f.putWorker(t.Context(), db, paths))
		require.Equal(t, []string{
			"PUT '" + path + "' INTO '/Volumes/o\\'cat/sch`ema/flow_staging/flow_temp_tables/txn/file.json.gz' OVERWRITE",
		}, recording.executed)
	})

	t.Run("merge of root-level files", func(t *testing.T) {
		query, err := RenderTableWithStaged(renderableTable(), nil, pathsWithRoot(root, []string{"old.json.gz"}), "", testTemplates.mergeInto, nil)
		require.NoError(t, err)
		require.Contains(t, query, "FROM json.`/Volumes/o'cat/sch``ema/flow_staging/flow_temp_tables/old.json.gz`")
	})
}
