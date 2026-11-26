package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/estuary/connectors/filesource"
	"github.com/hirochachacha/go-smb2"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

type tree struct {
	fs         fstest.MapFS
	reads      []string
	links      map[string]string
	readErrors map[string]error
	statErrors map[string]error
	stats      []string
}

func (t *tree) ReadDir(p string) ([]os.FileInfo, error) {
	t.reads = append(t.reads, p)
	if err := t.readErrors[p]; err != nil {
		return nil, err
	}
	entries, err := t.fs.ReadDir(path.Clean(strings.TrimPrefix(p, "/")))
	if err != nil {
		return nil, err
	}
	var infos []os.FileInfo
	// Reverse the input order to check that the listing sorts server responses.
	for i := len(entries) - 1; i >= 0; i-- {
		info, err := entries[i].Info()
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// Stat resolves links the way a server opening a non-symlink reparse point does.
func (t *tree) Stat(p string) (os.FileInfo, error) {
	if target, ok := t.links[p]; ok {
		p = target
	}
	return t.fs.Stat(path.Clean(strings.TrimPrefix(p, "/")))
}

func (t *tree) Lstat(p string) (os.FileInfo, error) {
	return t.fs.Lstat(path.Clean(strings.TrimPrefix(p, "/")))
}

// Readlink returns link targets as SMB reports them, relative to the link's
// directory with backslash separators. Other reparse points are not links.
func (t *tree) Readlink(p string) (string, error) {
	t.stats = append(t.stats, p)
	if err := t.statErrors[p]; err != nil {
		return "", err
	}
	if target, ok := t.links[p]; ok {
		return target, nil
	}
	return "", &os.PathError{Op: "readlink", Path: p, Err: &smb2.InvalidResponseError{Message: "broken symbolic link response data buffer format"}}
}

func TestListing(t *testing.T) {
	var modified = time.Unix(1600000000, 0).UTC()
	var testFs = fstest.MapFS{
		"inbox/a.csv":       &fstest.MapFile{Data: []byte("id,canary\n1,a1\n"), ModTime: modified},
		"inbox/a/first.csv": &fstest.MapFile{Data: []byte("id,canary\n2,a2\n"), ModTime: modified},
		"inbox/a0.csv":      &fstest.MapFile{Data: []byte("id,canary\n3,a3\n"), ModTime: modified},
		"inbox/sub/b.csv":   &fstest.MapFile{Data: []byte("id,canary\n4,b4\n"), ModTime: modified},
		"inbox/sub/c.csv":   &fstest.MapFile{Data: []byte("id,canary\n5,b5\n"), ModTime: modified},
		"inbox/sub2/d.json": &fstest.MapFile{Data: []byte(`{"id":"6"}`), ModTime: modified},
		"inbox/notes.txt":   &fstest.MapFile{Data: []byte("notes"), ModTime: modified},
		"inbox/empty":       &fstest.MapFile{Mode: fs.ModeDir},
	}
	for _, tt := range []struct {
		name      string
		recursive bool
		startAt   string
		paths     []string
	}{
		{"recursive", true, "", []string{"/inbox/a.csv", "/inbox/a/first.csv", "/inbox/a0.csv", "/inbox/notes.txt", "/inbox/sub/b.csv", "/inbox/sub/c.csv", "/inbox/sub2/d.json"}},
		{"non-recursive", false, "", []string{"/inbox/a", "/inbox/a.csv", "/inbox/a0.csv", "/inbox/empty", "/inbox/notes.txt", "/inbox/sub", "/inbox/sub2"}},
		{"inclusive startAt", true, "/inbox/sub/b.csv", []string{"/inbox/sub/b.csv", "/inbox/sub/c.csv", "/inbox/sub2/d.json"}},
		{"inside directory", true, "/inbox/sub/bb.csv", []string{"/inbox/sub/c.csv", "/inbox/sub2/d.json"}},
		{"after directory", true, "/inbox/sub0.csv", []string{"/inbox/sub2/d.json"}},
		{"directory prefix", true, "/inbox/a", []string{"/inbox/a.csv", "/inbox/a/first.csv", "/inbox/a0.csv", "/inbox/notes.txt", "/inbox/sub/b.csv", "/inbox/sub/c.csv", "/inbox/sub2/d.json"}},
		{"non-recursive startAt", false, "/inbox/sub/b.csv", []string{"/inbox/sub2"}},
		{"past end", true, "/z", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var tree = &tree{fs: testFs}
			listing, err := newSmbListing(tree, "/inbox", tt.recursive, tt.startAt)
			require.NoError(t, err)
			var paths []string
			for {
				obj, err := listing.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				paths = append(paths, obj.Path)
				if obj.IsPrefix {
					require.False(t, tt.recursive)
				} else {
					require.Equal(t, modified, obj.ModTime)
					require.Equal(t, int64(len(testFs[strings.TrimPrefix(obj.Path, "/")].Data)), obj.Size)
				}
			}
			require.Equal(t, tt.paths, paths)
			if tt.startAt == "/inbox/sub/b.csv" {
				require.NotContains(t, tree.reads, "/inbox/a")
				require.NotContains(t, tree.reads, "/inbox/empty")
			}
			_, err = listing.Next()
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestListingUnsetModTime(t *testing.T) {
	var hook = logtest.NewGlobal()
	defer hook.Reset()
	modTimeWarnings.paths, modTimeWarnings.full = nil, false

	// go-smb2 converts a FILETIME to Unix nanoseconds as (filetime-116444736000000000)*100,
	// which overflows for an unset (zero) or pre-1677 FILETIME.
	var filetime int64
	var wrapped = time.Unix(0, (filetime-116444736000000000)*100)
	require.Equal(t, 2185, wrapped.UTC().Year())

	var modified = time.Unix(1600000000, 0).UTC()
	var changed = time.Unix(1700000000, 0).UTC()
	var created = time.Unix(1500000000, 0).UTC()
	var tree = &tree{fs: fstest.MapFS{
		"inbox/a.csv":     &fstest.MapFile{Data: []byte("id\n1\n"), ModTime: modified},
		"inbox/unset.csv": &fstest.MapFile{Data: []byte("id\n2\n"), ModTime: wrapped},
		"inbox/max.csv":   &fstest.MapFile{Data: []byte("id\n3\n"), ModTime: time.Unix(0, math.MaxInt64)},
		"inbox/changed.csv": &fstest.MapFile{Data: []byte("id\n4\n"), ModTime: wrapped, Sys: &smb2.FileStat{
			LastWriteTime: wrapped, ChangeTime: changed, CreationTime: created,
		}},
		"inbox/created.csv": &fstest.MapFile{Data: []byte("id\n5\n"), ModTime: wrapped, Sys: &smb2.FileStat{
			LastWriteTime: wrapped, ChangeTime: wrapped, CreationTime: created,
		}},
		"inbox/none.csv": &fstest.MapFile{Data: []byte("id\n6\n"), ModTime: wrapped, Sys: &smb2.FileStat{
			LastWriteTime: wrapped, ChangeTime: wrapped, CreationTime: wrapped,
		}},
	}}
	listing, err := newSmbListing(tree, "/inbox", true, "")
	require.NoError(t, err)
	var modTimes = map[string]time.Time{}
	for {
		obj, err := listing.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		modTimes[obj.Path] = obj.ModTime
	}
	var epoch = time.Unix(0, 0).UTC()
	require.Equal(t, map[string]time.Time{
		"/inbox/a.csv":       modified,
		"/inbox/changed.csv": changed,
		"/inbox/created.csv": created,
		"/inbox/max.csv":     epoch,
		"/inbox/none.csv":    epoch,
		"/inbox/unset.csv":   epoch,
	}, modTimes)

	// The listing warns once about each file whose time does not track changes,
	// since filesource skips such a file without reading it.
	var warned = func() []string {
		var paths []string
		for _, entry := range hook.AllEntries() {
			if entry.Level == log.WarnLevel {
				paths = append(paths, entry.Data["path"].(string))
			}
		}
		return paths
	}
	var want = []string{"/inbox/created.csv", "/inbox/max.csv", "/inbox/none.csv", "/inbox/unset.csv"}
	require.ElementsMatch(t, want, warned())
	listing, err = newSmbListing(tree, "/inbox", true, "")
	require.NoError(t, err)
	for _, err = listing.Next(); err == nil; _, err = listing.Next() {
	}
	require.ErrorIs(t, err, io.EOF)
	require.ElementsMatch(t, want, warned())

	// Read reports the same time as the listing for the same file.
	for _, name := range []string{"changed.csv", "created.csv", "none.csv"} {
		info, err := fs.Stat(tree.fs, "inbox/"+name)
		require.NoError(t, err)
		modTime, _ := fileModTime(info)
		require.Equal(t, modTimes["/inbox/"+name], modTime)
	}
}

func TestModTimeWarningsBounded(t *testing.T) {
	var hook = logtest.NewGlobal()
	defer hook.Reset()
	modTimeWarnings.paths, modTimeWarnings.full = nil, false
	defer func() { modTimeWarnings.paths, modTimeWarnings.full = nil, false }()

	for i := 0; i < maxModTimeWarnings+10; i++ {
		warnModTimeOnce(fmt.Sprintf("/inbox/%d.csv", i), "warning")
	}
	warnModTimeOnce("/inbox/0.csv", "warning")
	require.Len(t, modTimeWarnings.paths, maxModTimeWarnings)
	require.Len(t, hook.AllEntries(), maxModTimeWarnings+1)
	require.NotContains(t, hook.LastEntry().Data, "path")
}

func TestListingErrors(t *testing.T) {
	_, err := newSmbListing(&tree{fs: fstest.MapFS{}}, "/missing", true, "")
	require.ErrorIs(t, err, fs.ErrNotExist)

	var tree = &tree{fs: fstest.MapFS{"inbox/sub/a.csv": &fstest.MapFile{}}}
	listing, err := newSmbListing(tree, "/inbox", true, "")
	require.NoError(t, err)
	delete(tree.fs, "inbox/sub/a.csv")
	_, err = listing.Next()
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestListingPermissionErrors(t *testing.T) {
	for _, root := range []string{"/", "/inbox"} {
		t.Run(root, func(t *testing.T) {
			var tree = &tree{
				fs: fstest.MapFS{
					"$RECYCLE.BIN":              &fstest.MapFile{Mode: fs.ModeDir},
					"RECYCLER":                  &fstest.MapFile{Mode: fs.ModeDir},
					"System Volume Information": &fstest.MapFile{Mode: fs.ModeDir},
					"inbox/DfsrPrivate":         &fstest.MapFile{Mode: fs.ModeDir},
					"inbox/lost+found":          &fstest.MapFile{Mode: fs.ModeDir},
					"inbox/a.csv":               &fstest.MapFile{},
					"inbox/private/hidden.csv":  &fstest.MapFile{},
					"inbox/z.csv":               &fstest.MapFile{},
				},
				readErrors: map[string]error{
					"/$RECYCLE.BIN":              os.ErrPermission,
					"/RECYCLER":                  os.ErrPermission,
					"/System Volume Information": os.ErrPermission,
					"/inbox/DfsrPrivate":         os.ErrPermission,
					"/inbox/lost+found":          os.ErrPermission,
				},
			}
			listing, err := newSmbListing(tree, root, true, "")
			require.NoError(t, err)
			for _, expected := range []string{"/inbox/a.csv", "/inbox/private/hidden.csv", "/inbox/z.csv"} {
				obj, err := listing.Next()
				require.NoError(t, err)
				require.Equal(t, expected, obj.Path)
			}
			_, err = listing.Next()
			require.ErrorIs(t, err, io.EOF)

			// Other unreadable subdirectories fail the listing rather than being
			// skipped behind capture progress.
			tree.readErrors["/inbox/private"] = &os.PathError{
				Op: "readdir", Path: "/inbox/private", Err: os.ErrPermission,
			}
			listing, err = newSmbListing(tree, root, true, "")
			require.NoError(t, err)
			obj, err := listing.Next()
			require.NoError(t, err)
			require.Equal(t, "/inbox/a.csv", obj.Path)
			_, err = listing.Next()
			require.ErrorIs(t, err, os.ErrPermission)
			delete(tree.readErrors, "/inbox/private")

			tree.readErrors[root] = &os.PathError{Op: "readdir", Path: root, Err: os.ErrPermission}
			_, err = newSmbListing(tree, root, true, "")
			require.ErrorIs(t, err, os.ErrPermission)
		})
	}
}

func TestIsSystemDirectory(t *testing.T) {
	for _, p := range []string{"/System Volume Information", "/$Recycle.Bin", "/RECYCLER", "/data/DfsrPrivate", "/nas/lost+found"} {
		require.True(t, isSystemDirectory(p), p)
	}
	// A user folder named like the old recycle bin is not a system folder.
	for _, p := range []string{"/plant/Recycler", "/inbox", "/private"} {
		require.False(t, isSystemDirectory(p), p)
	}
}

func TestMatchFiles(t *testing.T) {
	var cfg = config{Directory: "/inbox", MatchFiles: `^/inbox/sub/.*\.csv$`}
	var tree = &tree{fs: fstest.MapFS{"inbox/sub/a.csv": &fstest.MapFile{}}}
	listing, err := newSmbListing(tree, cfg.Directory, true, "")
	require.NoError(t, err)
	obj, err := listing.Next()
	require.NoError(t, err)
	require.Equal(t, "/inbox/sub/a.csv", obj.Path)
	require.Regexp(t, cfg.PathRegex(), obj.Path)
	require.NotRegexp(t, `^sub/.*\.csv$`, obj.Path)

	var schema struct {
		Properties map[string]struct {
			Description string `json:"description"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(configSchema(json.RawMessage(`{}`)), &schema))
	require.Contains(t, schema.Properties["matchFiles"].Description, "path relative to the share root, starting with /")
	require.Contains(t, schema.Properties["matchFiles"].Description, obj.Path)
}

func TestListingShareRoot(t *testing.T) {
	var tree = &tree{fs: fstest.MapFS{"inbox/a.csv": &fstest.MapFile{}}}
	listing, err := newSmbListing(tree, "/", true, "")
	require.NoError(t, err)
	obj, err := listing.Next()
	require.NoError(t, err)
	require.Equal(t, "/inbox/a.csv", obj.Path)
	_, err = listing.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestListingSymlinks(t *testing.T) {
	var modified = time.Unix(1600000000, 0).UTC()
	var tree = &tree{
		fs: fstest.MapFS{
			"inbox/a.csv":    &fstest.MapFile{Data: []byte("id\n1\n"), ModTime: modified},
			"inbox/copy.csv": &fstest.MapFile{Mode: fs.ModeSymlink},
			"inbox/loop":     &fstest.MapFile{Mode: fs.ModeSymlink},
		},
		links: map[string]string{"/inbox/copy.csv": "a.csv", "/inbox/loop": "."},
	}
	listing, err := newSmbListing(tree, "/inbox", true, "")
	require.NoError(t, err)
	for _, expected := range []string{"/inbox/a.csv", "/inbox/copy.csv"} {
		obj, err := listing.Next()
		require.NoError(t, err)
		require.Equal(t, expected, obj.Path)
		require.Equal(t, modified, obj.ModTime)
		require.Equal(t, int64(5), obj.Size)
	}
	_, err = listing.Next()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, []string{"/inbox"}, tree.reads)
}

func TestListingUnresolvableSymlinks(t *testing.T) {
	var tree = &tree{
		fs: fstest.MapFS{
			"inbox/a.csv":       &fstest.MapFile{Data: []byte("id\n1\n")},
			"inbox/archive":     &fstest.MapFile{Mode: fs.ModeDir | fs.ModeSymlink},
			"inbox/gone.csv":    &fstest.MapFile{Mode: fs.ModeSymlink},
			"inbox/other.csv":   &fstest.MapFile{Mode: fs.ModeSymlink},
			"inbox/private.csv": &fstest.MapFile{Mode: fs.ModeSymlink},
		},
		links: map[string]string{
			"/inbox/archive":  `D:\archive`,
			"/inbox/gone.csv": "missing.csv",
		},
		statErrors: map[string]error{
			"/inbox/other.csv": &os.PathError{Op: "stat", Path: "other.csv", Err: &smb2.ResponseError{Code: 0xc0000033}},
		},
	}
	listing, err := newSmbListing(tree, "/inbox", true, "")
	require.NoError(t, err)
	obj, err := listing.Next()
	require.NoError(t, err)
	require.Equal(t, "/inbox/a.csv", obj.Path)

	// A transport or permission error still fails the listing.
	tree.statErrors["/inbox/private.csv"] = &os.PathError{Op: "stat", Path: "private.csv", Err: os.ErrPermission}
	_, err = listing.Next()
	require.ErrorIs(t, err, os.ErrPermission)
	_, err = listing.Next()
	require.ErrorIs(t, err, io.EOF)
	// The directory link is skipped without resolving it.
	require.Equal(t, []string{"/inbox/gone.csv", "/inbox/other.csv", "/inbox/private.csv"}, tree.stats)
	require.Equal(t, []string{"/inbox"}, tree.reads)
}

func TestListingLinkTargets(t *testing.T) {
	var modified = time.Unix(1600000000, 0).UTC()
	var older = time.Unix(1500000000, 0).UTC()
	var tree = &tree{
		fs: fstest.MapFS{
			"inbox/2026-11.csv": &fstest.MapFile{Data: []byte("id\n1\n"), ModTime: modified},
			"inbox/chain.csv":   &fstest.MapFile{Mode: fs.ModeSymlink, ModTime: older},
			"inbox/cycle.csv":   &fstest.MapFile{Mode: fs.ModeSymlink},
			// A reparse point other than a link, which the server opens directly.
			"inbox/deduped.csv":     &fstest.MapFile{Mode: fs.ModeSymlink, Data: []byte("2026-11.csv")},
			"inbox/escape.csv":      &fstest.MapFile{Mode: fs.ModeSymlink},
			"inbox/latest.csv":      &fstest.MapFile{Mode: fs.ModeSymlink, ModTime: older},
			"inbox/other-drive.csv": &fstest.MapFile{Mode: fs.ModeSymlink},
			"inbox/sub/up.csv":      &fstest.MapFile{Mode: fs.ModeSymlink, ModTime: older},
		},
		links: map[string]string{
			"/inbox/chain.csv":       "latest.csv",
			"/inbox/cycle.csv":       `sub\..\cycle.csv`,
			"/inbox/escape.csv":      `..\..\secret.csv`,
			"/inbox/latest.csv":      "2026-11.csv",
			"/inbox/other-drive.csv": `C:\data\2026-11.csv`,
			"/inbox/sub/up.csv":      `..\2026-11.csv`,
		},
	}
	listing, err := newSmbListing(tree, "/inbox", true, "")
	require.NoError(t, err)
	var objs []filesource.ObjectInfo
	for {
		obj, err := listing.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		objs = append(objs, obj)
	}
	var paths []string
	for _, obj := range objs {
		paths = append(paths, obj.Path)
		// Links report the referent file's metadata under the link's path.
		require.Equal(t, modified, obj.ModTime, obj.Path)
		require.Equal(t, int64(5), obj.Size, obj.Path)
	}
	require.Equal(t, []string{"/inbox/2026-11.csv", "/inbox/chain.csv", "/inbox/deduped.csv", "/inbox/latest.csv", "/inbox/sub/up.csv"}, paths)

	resolved, _, err := resolveLink(tree, "/inbox/sub/up.csv")
	require.NoError(t, err)
	require.Equal(t, "/inbox/2026-11.csv", resolved)
	_, _, err = resolveLink(tree, "/inbox/cycle.csv")
	require.ErrorIs(t, err, errTooManyLinks)
}

// parentLinkTree serves /current as a link to /archive/2026. Like a Windows
// server, it follows the link when listing or opening a path beneath it, but
// stops on it when a path beneath it is read without following links.
type parentLinkTree struct{ *tree }

func (t parentLinkTree) follow(p string) string {
	if rest, ok := strings.CutPrefix(p, "/current/"); ok {
		return "/archive/2026/" + rest
	}
	return p
}

func (t parentLinkTree) stopped(p string) error {
	if strings.HasPrefix(p, "/current/") {
		return &os.PathError{Op: "stat", Path: p, Err: &smb2.ResponseError{Code: 0x8000002d}}
	}
	return nil
}

func (t parentLinkTree) ReadDir(p string) ([]os.FileInfo, error) {
	if p == "/current" {
		p = "/archive/2026"
	}
	return t.tree.ReadDir(t.follow(p))
}

func (t parentLinkTree) Stat(p string) (os.FileInfo, error) { return t.tree.Stat(t.follow(p)) }

func (t parentLinkTree) Lstat(p string) (os.FileInfo, error) {
	if err := t.stopped(p); err != nil {
		return nil, err
	}
	return t.tree.Lstat(p)
}

func (t parentLinkTree) Readlink(p string) (string, error) {
	if err := t.stopped(p); err != nil {
		t.stats = append(t.stats, p)
		return "", err
	}
	return t.tree.Readlink(p)
}

func TestListingUnderDirectoryLink(t *testing.T) {
	var modified = time.Unix(1600000000, 0).UTC()
	var linked = parentLinkTree{&tree{
		fs: fstest.MapFS{
			"archive/2026/a.csv": &fstest.MapFile{Data: []byte("id\n1\n"), ModTime: modified},
			// A deduplicated file, which is a reparse point but not a link.
			"archive/2026/deduped.csv": &fstest.MapFile{Mode: fs.ModeSymlink, Data: []byte("a.csv")},
			"archive/2026/escape.csv":  &fstest.MapFile{Mode: fs.ModeSymlink},
			"archive/2026/latest.csv":  &fstest.MapFile{Mode: fs.ModeSymlink},
			"current":                  &fstest.MapFile{Mode: fs.ModeDir | fs.ModeSymlink},
		},
		links: map[string]string{
			"/archive/2026/escape.csv": `..\..\..\secret.csv`,
			"/archive/2026/latest.csv": "a.csv",
			"/current":                 `archive\2026`,
		},
	}}
	listing, err := newSmbListing(linked, "/current", true, "")
	require.NoError(t, err)
	var paths []string
	for {
		obj, err := listing.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		paths = append(paths, obj.Path)
		require.Equal(t, modified, obj.ModTime, obj.Path)
		require.Equal(t, int64(5), obj.Size, obj.Path)
	}
	// Files keep their paths under the configured directory. Only the link
	// leading outside the share is skipped.
	require.Equal(t, []string{"/current/a.csv", "/current/deduped.csv", "/current/latest.csv"}, paths)

	resolved, _, err := resolveLink(linked, "/current/latest.csv")
	require.NoError(t, err)
	require.Equal(t, "/archive/2026/a.csv", resolved)

	dir, err := realDir(linked, "/current")
	require.NoError(t, err)
	require.Equal(t, "/archive/2026", dir)
	linked.links["/current"] = `D:\archive`
	_, err = realDir(linked, "/current")
	require.ErrorIs(t, err, errLinkOutsideShare)

	// A server which still stops on a link once its parents are resolved fails
	// the listing rather than skipping the file.
	_, _, err = resolveLink(&tree{fs: fstest.MapFS{"inbox/a.csv": &fstest.MapFile{Mode: fs.ModeSymlink}}, statErrors: map[string]error{
		"/inbox/a.csv": &os.PathError{Op: "stat", Path: `inbox\a.csv`, Err: &smb2.ResponseError{Code: 0x8000002d}},
	}}, "/inbox/a.csv")
	require.True(t, isStoppedOnSymlink(err))
	require.False(t, isUnresolvableLink(err))
}

func TestListingLinkTargetsUnderDirectoryLink(t *testing.T) {
	var modified = time.Unix(1600000000, 0).UTC()
	var linked = parentLinkTree{&tree{
		fs: fstest.MapFS{
			"archive/2026/a.csv":   &fstest.MapFile{Data: []byte("id\n1\n"), ModTime: modified},
			"archive/2026/hop.csv": &fstest.MapFile{Mode: fs.ModeSymlink},
			"current":              &fstest.MapFile{Mode: fs.ModeDir | fs.ModeSymlink},
			"inbox/chain.csv":      &fstest.MapFile{Mode: fs.ModeSymlink},
			"inbox/gone.csv":       &fstest.MapFile{Mode: fs.ModeSymlink},
			"inbox/hop1.csv":       &fstest.MapFile{Mode: fs.ModeSymlink},
			"inbox/hop2.csv":       &fstest.MapFile{Mode: fs.ModeSymlink},
			"inbox/latest.csv":     &fstest.MapFile{Mode: fs.ModeSymlink},
		},
		links: map[string]string{
			"/archive/2026/hop.csv": "a.csv",
			"/current":              `archive\2026`,
			// The target sits under the directory link.
			"/inbox/latest.csv": `..\current\a.csv`,
			// The first hop of the chain sits under the directory link.
			"/inbox/chain.csv": `..\current\hop.csv`,
			// A later hop of the chain sits under the directory link.
			"/inbox/hop1.csv": "hop2.csv",
			"/inbox/hop2.csv": `..\current\a.csv`,
			"/inbox/gone.csv": `..\current\missing.csv`,
		},
	}}
	listing, err := newSmbListing(linked, "/inbox", true, "")
	require.NoError(t, err)
	var paths []string
	for {
		obj, err := listing.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		paths = append(paths, obj.Path)
		require.Equal(t, modified, obj.ModTime, obj.Path)
		require.Equal(t, int64(5), obj.Size, obj.Path)
	}
	// Only the link whose target is missing is skipped.
	require.Equal(t, []string{"/inbox/chain.csv", "/inbox/hop1.csv", "/inbox/hop2.csv", "/inbox/latest.csv"}, paths)

	for _, link := range []string{"/inbox/chain.csv", "/inbox/hop1.csv", "/inbox/latest.csv"} {
		resolved, _, err := resolveLink(linked, link)
		require.NoError(t, err, link)
		require.Equal(t, "/archive/2026/a.csv", resolved, link)
	}
	_, _, err = resolveLink(linked, "/inbox/gone.csv")
	require.True(t, isUnresolvableLink(err))
}

func TestLinkTarget(t *testing.T) {
	for _, tt := range []struct{ link, target, resolved string }{
		{"/latest.csv", "2026-11.csv", "/2026-11.csv"},
		{"/inbox/latest.csv", "2026-11.csv", "/inbox/2026-11.csv"},
		{"/a/b/latest.csv", `..\c\2026-11.csv`, "/a/c/2026-11.csv"},
		{"/inbox/latest.csv", `..\2026-11.csv`, "/2026-11.csv"},
		{"/inbox/here", ".", "/inbox"},
		{"/inbox/latest.csv", `..\..\2026-11.csv`, ""},
		{"/inbox/latest.csv", `C:\data\2026-11.csv`, ""},
		{"/inbox/latest.csv", `\\server\share\2026-11.csv`, ""},
		{"/inbox/latest.csv", `\inbox\2026-11.csv`, ""},
		{"/inbox/latest.csv", "", ""},
	} {
		t.Run(tt.link+" "+tt.target, func(t *testing.T) {
			resolved, err := linkTarget(tt.link, tt.target)
			if tt.resolved == "" {
				require.ErrorIs(t, err, errLinkOutsideShare)
				require.True(t, isUnresolvableLink(err))
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.resolved, resolved)
			}
		})
	}
}

func TestIsStoppedOnSymlink(t *testing.T) {
	var stopped = &os.PathError{Op: "stat", Path: `current\a.csv`, Err: &smb2.ResponseError{Code: 0x8000002d}}
	require.True(t, isStoppedOnSymlink(stopped))
	require.False(t, isStoppedOnSymlink(&os.PathError{Op: "stat", Path: "a.csv", Err: &smb2.ResponseError{Code: 0xc0000034}}))
	require.False(t, isStoppedOnSymlink(os.ErrNotExist))
	require.False(t, isStoppedOnSymlink(nil))
}

func TestListingDirectoryLinksWarn(t *testing.T) {
	var hook = logtest.NewGlobal()
	defer hook.Reset()
	var tree = &tree{
		fs: fstest.MapFS{
			"inbox/a.csv":          &fstest.MapFile{},
			"inbox/projects":       &fstest.MapFile{Mode: fs.ModeDir | fs.ModeSymlink},
			"inbox/projects/b.csv": &fstest.MapFile{},
		},
	}
	listing, err := newSmbListing(tree, "/inbox", true, "")
	require.NoError(t, err)
	obj, err := listing.Next()
	require.NoError(t, err)
	require.Equal(t, "/inbox/a.csv", obj.Path)
	_, err = listing.Next()
	require.ErrorIs(t, err, io.EOF)

	var warned bool
	for _, entry := range hook.AllEntries() {
		if entry.Level == log.WarnLevel && entry.Data["path"] == "/inbox/projects" {
			warned = true
		}
	}
	require.True(t, warned)
}

func TestClosedSource(t *testing.T) {
	var source smbSource
	require.NoError(t, source.Close())
	_, err := source.List(context.Background(), filesource.Query{Prefix: "/inbox", Recursive: true})
	require.ErrorIs(t, err, os.ErrClosed)
	_, _, err = source.Read(context.Background(), filesource.ObjectInfo{Path: "/inbox/a.csv"})
	require.ErrorIs(t, err, os.ErrClosed)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = source.List(ctx, filesource.Query{Prefix: "/inbox"})
	require.ErrorIs(t, err, context.Canceled)
}
