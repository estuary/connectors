package main

import (
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	parserSpec, err := os.ReadFile("tests/parser_spec.json")
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(configSchema(json.RawMessage(parserSpec)), "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

type tree struct {
	fs fstest.MapFS
}

// ReadDir is an adapter for transforming the fstest.MapFS ReadDir output []fs.dirEntry into
// []os.FileInfo to satisfy the dirEntry interface. This whole apparatus is in place so the
// fstest.MapFS can be used for these unit tests.
func (t *tree) ReadDir(s string) ([]os.FileInfo, error) {
	entries, err := t.fs.ReadDir(s)
	if err != nil {
		return nil, err
	}

	out := make([]os.FileInfo, 0, len(entries))

	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}

	return out, nil
}

var testFs = fstest.MapFS{
	"root/sub/nested/a.jsonl": &fstest.MapFile{
		Data:    []byte(`{"some": "value"}`),
		ModTime: time.UnixMilli(1000000000000).UTC(),
	},
	"root/sub/y.csv": &fstest.MapFile{
		Data:    []byte(`first,second,third`),
		ModTime: time.UnixMilli(1500000000000).UTC(),
	},
	"root/sub/z.csv": &fstest.MapFile{
		Data:    []byte(`first,second,third`),
		ModTime: time.UnixMilli(1500000000000).UTC(),
	},
	"root/b.csv.gz": &fstest.MapFile{
		Data:    []byte(`gzippedstuff`),
		ModTime: time.UnixMilli(2000000000000).UTC(),
	},
	"root/emptyDir1": &fstest.MapFile{
		Mode: fs.ModeDir,
	},
	"root/emptyDir2": &fstest.MapFile{
		Mode: fs.ModeDir,
	},
	"otherDir": &fstest.MapFile{
		Mode: fs.ModeDir,
	},
}

func TestListing(t *testing.T) {
	testTree := &tree{
		fs: testFs,
	}

	tests := []struct {
		name      string
		root      string
		recursive bool
		startAt   string
	}{
		{
			name:      "recursive",
			root:      "root",
			recursive: true,
			startAt:   "",
		},
		{
			name:      "non-recursive",
			root:      "root",
			recursive: false,
			startAt:   "",
		},
		{
			name:      "recursive startAt root/sub",
			root:      "root",
			recursive: true,
			startAt:   "root/sub",
		},
		{
			name:      "recursive startAt z",
			root:      "root",
			recursive: true,
			startAt:   "z",
		},
		{
			name:      "recursive startAt root/sub/y.csv",
			root:      "root",
			recursive: true,
			startAt:   "root/sub/y.csv",
		},
		{
			name:      "recursive otherDir",
			root:      "otherDir",
			recursive: true,
			startAt:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := newSftpListing(nil, testTree, tt.root, tt.recursive, tt.startAt)
			require.NoError(t, err)

			var snap strings.Builder
			for info, err := l.Next(); err != io.EOF; info, err = l.Next() {
				require.NoError(t, err)
				bytes, err := json.MarshalIndent(info, "", "\t")
				require.NoError(t, err)
				snap.Write(bytes)
				snap.WriteByte('\n')
			}
			cupaloy.SnapshotT(t, snap.String())
		})
	}
}
