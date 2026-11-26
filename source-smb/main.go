package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/filesource"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/smb"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/flow/go/parser"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/hirochachacha/go-smb2"
	log "github.com/sirupsen/logrus"
)

type config struct {
	Host          string                      `json:"host"`
	Port          int                         `json:"port,omitempty"`
	Share         string                      `json:"share"`
	Username      string                      `json:"username"`
	Password      string                      `json:"password"`
	Domain        string                      `json:"domain,omitempty"`
	Directory     string                      `json:"directory,omitempty"`
	MatchFiles    string                      `json:"matchFiles,omitempty"`
	Advanced      advancedConfig              `json:"advanced,omitempty"`
	Parser        *parser.Config              `json:"parser,omitempty"`
	NetworkTunnel *networkTunnel.TunnelConfig `json:"networkTunnel,omitempty"`
}

type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys,omitempty" jsonschema:"title=Ascending Keys" jsonschema_extras:"nonsensitive=true"`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"host", c.Host},
		{"share", c.Share},
		{"username", c.Username},
		{"password", c.Password},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("invalid port %d: must be between 1 and 65535", c.Port)
	}
	if strings.ContainsAny(c.Share, `/\`) || c.Share == "." || c.Share == ".." {
		return fmt.Errorf("invalid share %q: must be a share name, not a path", c.Share)
	}
	if _, err := smb.NormalizePath(c.Directory); err != nil {
		return err
	}
	if c.MatchFiles != "" {
		if _, err := regexp.Compile(c.MatchFiles); err != nil {
			return fmt.Errorf("invalid regex pattern '%s' for Match Files: %w", c.MatchFiles, err)
		}
	}
	return validateNetworkTunnel(c.NetworkTunnel)
}

func (c config) DiscoverRoot() string {
	return c.Directory
}

func (c config) RecommendedName() string {
	var name = strings.Trim(c.DiscoverRoot(), "/")
	if name == "" {
		return "root"
	} else {
		return name
	}
}

func (c config) FilesAreMonotonic() bool {
	return c.Advanced.AscendingKeys
}

func (c config) ParserConfig() *parser.Config {
	return c.Parser
}

func (c config) PathRegex() string {
	return c.MatchFiles
}

func newSmbSource(ctx context.Context, cfg config, address string) (*smbSource, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := smb.Dial(ctx, smb.Config{
		Host: cfg.Host, Share: cfg.Share,
		Username: cfg.Username, Password: cfg.Password, Domain: cfg.Domain,
		DialContext: smbDialContext,
	}, address)
	if err != nil {
		return nil, err
	}
	var s = &smbSource{Connection: conn}
	var dir = smbDirectory{source: s, ctx: ctx}

	// Surface invalid directories during discovery and validation, rather than at runtime.
	if info, err := dir.Stat(cfg.Directory); err != nil {
		s.Close()
		return nil, fmt.Errorf("stat'ing directory %s: %w", cfg.Directory, err)
	} else if !info.IsDir() {
		s.Close()
		return nil, fmt.Errorf("'%s' is not a directory", cfg.Directory)
	}
	return s, nil
}

var smbDialContext func(context.Context, string, string) (net.Conn, error)

type smbSource struct {
	*smb.Connection
}

// shareFS implements dirLister on a share using our absolute paths.
type shareFS struct{ *smb2.Share }

func (s shareFS) ReadDir(p string) ([]os.FileInfo, error) { return s.Share.ReadDir(smb.Path(p)) }
func (s shareFS) Stat(p string) (os.FileInfo, error)      { return s.Share.Stat(smb.Path(p)) }
func (s shareFS) Lstat(p string) (os.FileInfo, error)     { return s.Share.Lstat(smb.Path(p)) }
func (s shareFS) Readlink(p string) (string, error)       { return s.Share.Readlink(smb.Path(p)) }

type smbDirectory struct {
	source *smbSource
	ctx    context.Context
}

func onShare[T any](d smbDirectory, fn func(shareFS) (T, error)) (T, error) {
	var out T
	err := d.source.WithShare(d.ctx, func(share *smb2.Share) error {
		var err error
		out, err = fn(shareFS{share})
		return err
	})
	return out, err
}

func (d smbDirectory) ReadDir(p string) ([]os.FileInfo, error) {
	return onShare(d, func(s shareFS) ([]os.FileInfo, error) { return s.ReadDir(p) })
}

func (d smbDirectory) Stat(p string) (os.FileInfo, error) {
	return onShare(d, func(s shareFS) (os.FileInfo, error) { return s.Stat(p) })
}

func (d smbDirectory) Lstat(p string) (os.FileInfo, error) {
	return onShare(d, func(s shareFS) (os.FileInfo, error) { return s.Lstat(p) })
}

func (d smbDirectory) Readlink(p string) (string, error) {
	return onShare(d, func(s shareFS) (string, error) { return s.Readlink(p) })
}

func (s *smbSource) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	if err := smb.ValidatePath(query.Prefix); err != nil {
		return nil, err
	}
	return newSmbListing(smbDirectory{source: s, ctx: ctx}, query.Prefix, query.Recursive, query.StartAt)
}

type dirLister interface {
	ReadDir(string) ([]os.FileInfo, error)
	Stat(string) (os.FileInfo, error)
	Lstat(string) (os.FileInfo, error)
	Readlink(string) (string, error)
}

// smbListing lazily descends into directories, returning files in lexical order
// and pruning directories which cannot contain paths at or after StartAt.
type smbListing struct {
	fs        dirLister
	stack     []visit
	recursive bool
	startAt   string
}

func newSmbListing(fs dirLister, root string, recursive bool, startAt string) (*smbListing, error) {
	var l = &smbListing{fs: fs, recursive: recursive, startAt: startAt}
	if err := l.pushDir(root); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *smbListing) Next() (filesource.ObjectInfo, error) {
	for len(l.stack) != 0 {
		var next = l.stack[len(l.stack)-1]
		l.stack = l.stack[:len(l.stack)-1]

		if next.Mode()&os.ModeSymlink != 0 {
			// Do not follow directory links, which can introduce cycles or point
			// outside the share. The listing already marks them as directories.
			// Windows reports junctions, mount points and cloud placeholder folders as
			// directory links too.
			if next.IsDir() {
				log.WithField("path", next.path).Warn("skipping directory link, junction or mount point; capture its target directory instead to capture its files")
				continue
			}
			_, info, err := resolveLink(l.fs, next.path)
			if isUnresolvableLink(err) {
				log.WithField("path", next.path).WithError(err).Warn("skipping symbolic link with a missing or unreachable target")
				continue
			} else if err != nil {
				return filesource.ObjectInfo{}, fmt.Errorf("stat'ing symbolic link %s: %w", next.path, err)
			}
			if info.IsDir() {
				continue
			}
			next.FileInfo = info
		}

		if next.IsDir() && l.recursive {
			if err := l.pushDir(next.path); err != nil {
				if errors.Is(err, os.ErrPermission) && isSystemDirectory(next.path) {
					log.WithField("path", next.path).WithError(err).Warn("skipping unreadable system directory; backfill the binding after granting access to capture its files")
					continue
				}
				return filesource.ObjectInfo{}, fmt.Errorf("listing directory %s: %w", next.path, err)
			}
			continue
		}

		var modTime, warning = fileModTime(next.FileInfo)
		if warning != "" && !next.IsDir() {
			warnModTimeOnce(next.path, warning)
		}
		return filesource.ObjectInfo{
			Path:     next.path,
			IsPrefix: next.IsDir(),
			Size:     next.Size(),
			ModTime:  modTime,
		}, nil
	}
	return filesource.ObjectInfo{}, io.EOF
}

// unsetModTimes start where go-smb2 places a file time which is unset
// (FILETIME 0) or before 1677: its conversion to Unix nanoseconds overflows
// int64 and wraps to the years 2185 through 2262.
var unsetModTimes = time.Date(2185, time.January, 1, 0, 0, 0, 0, time.UTC)

// fileModTime returns the modification time of info. When the server reports
// an unset or pre-1677 modification time, it falls back to the file's change
// time and then its creation time, which go-smb2 converts the same way. A file
// would otherwise be deferred on every sweep as modified in the future. It
// returns a warning when the time does not advance on later changes: the
// creation time, or the Unix epoch when no time is valid, with which filesource
// captures the file only in a sweep whose lower bound precedes the epoch, which
// is the initial capture or a backfill.
func fileModTime(info fs.FileInfo) (time.Time, string) {
	if t := info.ModTime(); t.Before(unsetModTimes) {
		return t, ""
	}
	if stat, ok := info.Sys().(*smb2.FileStat); ok {
		if stat.ChangeTime.Before(unsetModTimes) {
			return stat.ChangeTime, ""
		} else if stat.CreationTime.Before(unsetModTimes) {
			return stat.CreationTime, "file has no valid modification or change time; it is captured by its creation time, and later changes to it are not captured"
		}
	}
	return time.Unix(0, 0).UTC(), "file has no valid modification, change or creation time; it is captured only by the initial capture or a backfill, and later changes to it are not captured"
}

// modTimeWarnings holds the files already warned about, since every sweep lists
// them again, including files which the binding's filters exclude. It holds at
// most maxModTimeWarnings files, after which one more warning is logged and
// further files are not warned about, so it does not grow with the share.
var modTimeWarnings struct {
	sync.Mutex
	paths map[string]bool
	full  bool
}

const maxModTimeWarnings = 1000

func warnModTimeOnce(p, warning string) {
	modTimeWarnings.Lock()
	defer modTimeWarnings.Unlock()
	if modTimeWarnings.full || modTimeWarnings.paths[p] {
		return
	}
	if modTimeWarnings.paths == nil {
		modTimeWarnings.paths = make(map[string]bool)
	}
	if len(modTimeWarnings.paths) == maxModTimeWarnings {
		modTimeWarnings.full = true
		log.WithField("files", maxModTimeWarnings).Warn("more files have no valid modification time; no further warnings about them are logged")
		return
	}
	modTimeWarnings.paths[p] = true
	log.WithField("path", p).Warn(warning)
}

// systemDirectories are folders which Windows and NAS file systems create and
// ordinary users cannot read: Windows volume folders, DFS Replication's staging
// folder and the lost+found folder of Linux file systems. Other unreadable
// directories fail the sweep, so their files are captured once access is
// restored instead of falling behind capture progress.
var systemDirectories = map[string]bool{
	"system volume information": true,
	"$recycle.bin":              true,
	"dfsrprivate":               true,
	"lost+found":                true,
}

// isSystemDirectory reports whether p is a system folder which is skipped when
// it cannot be read. The generic name RECYCLER is matched only at the share root.
func isSystemDirectory(p string) bool {
	var name = strings.ToLower(path.Base(p))
	return systemDirectories[name] || (name == "recycler" && path.Dir(p) == "/")
}

var (
	errLinkOutsideShare = errors.New("link target is outside the share")
	errTooManyLinks     = errors.New("too many levels of symbolic links")
)

// maxLinkDepth bounds chains of links, which may form cycles.
const maxLinkDepth = 8

// resolveLink follows a file link and any links it leads to, returning the path
// and metadata of the file it names. go-smb2 mis-joins relative targets of links
// in subdirectories when following them, so we read and resolve each target.
func resolveLink(lister dirLister, p string) (string, os.FileInfo, error) {
	for range maxLinkDepth {
		target, err := lister.Readlink(p)
		if isStoppedOnSymlink(err) {
			// A parent directory is a link, which the server does not follow when
			// reading a link. Read it again through the directory the link names.
			if dir, dirErr := realDir(lister, path.Dir(p)); dirErr != nil {
				return "", nil, dirErr
			} else if dir != path.Dir(p) {
				p = path.Join(dir, path.Base(p))
				target, err = lister.Readlink(p)
			}
		}
		var invalid *smb2.InvalidResponseError
		if errors.As(err, &invalid) {
			// Other reparse points, such as deduplicated files, are not symbolic
			// links and the server opens them directly.
			info, err := lister.Stat(p)
			return p, info, err
		} else if err != nil {
			return "", nil, err
		}
		if p, err = linkTarget(p, target); err != nil {
			return "", nil, err
		}
		var info os.FileInfo
		if p, info, err = lstatLink(lister, p); err != nil {
			return "", nil, err
		} else if info.Mode()&os.ModeSymlink == 0 || info.IsDir() {
			return p, info, nil
		}
	}
	return "", nil, errTooManyLinks
}

// lstatLink looks up p without following a link it names. When a parent
// directory is a link, which the server does not follow, it looks p up again
// through the directory the link names and returns the path it used.
func lstatLink(lister dirLister, p string) (string, os.FileInfo, error) {
	info, err := lister.Lstat(p)
	if !isStoppedOnSymlink(err) {
		return p, info, err
	}
	dir, dirErr := realDir(lister, path.Dir(p))
	if dirErr != nil {
		return "", nil, dirErr
	} else if dir == path.Dir(p) {
		return "", nil, err
	}
	p = path.Join(dir, path.Base(p))
	info, err = lister.Lstat(p)
	return p, info, err
}

// realDir replaces directory symbolic links among the components of dir with
// the directories they name. Junctions, mount points and other reparse points
// which the server follows itself are kept.
func realDir(lister dirLister, dir string) (string, error) {
	var walked = "/"
	var rest = strings.Split(strings.Trim(dir, "/"), "/")
	for links := 0; len(rest) != 0; {
		var next = path.Join(walked, rest[0])
		if rest = rest[1:]; next == walked {
			continue
		}
		info, err := lister.Lstat(next)
		if err != nil {
			return "", err
		} else if info.Mode()&os.ModeSymlink == 0 || !info.IsDir() {
			walked = next
			continue
		}
		target, err := lister.Readlink(next)
		var invalid *smb2.InvalidResponseError
		if errors.As(err, &invalid) {
			walked = next
			continue
		} else if err != nil {
			return "", err
		}
		if links++; links > maxLinkDepth {
			return "", errTooManyLinks
		}
		resolved, err := linkTarget(next, target)
		if err != nil {
			return "", err
		}
		// The target may itself sit under links, so walk it from the share root.
		rest = append(strings.Split(strings.Trim(resolved, "/"), "/"), rest...)
		walked = "/"
	}
	return walked, nil
}

// linkTarget resolves a link's relative target against the link's directory.
// Absolute targets name a drive, volume or UNC path and cannot be mapped into
// the share, nor can relative targets which climb above the share root.
func linkTarget(link, target string) (string, error) {
	target = strings.ReplaceAll(target, `\`, "/")
	if target == "" || strings.HasPrefix(target, "/") || strings.Contains(target, ":") {
		return "", errLinkOutsideShare
	}
	var resolved = path.Join(strings.TrimPrefix(path.Dir(link), "/"), target)
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return "", errLinkOutsideShare
	}
	return path.Join("/", resolved), nil
}

// isUnresolvableLink reports whether a link's target is missing or names a path
// the server cannot open through the share, such as another drive, whether
// for the link itself or for a directory link above it.
func isUnresolvableLink(err error) bool {
	var responseErr *smb2.ResponseError
	if errors.As(err, &responseErr) {
		switch responseErr.Code {
		case 0xc0000033, // STATUS_OBJECT_NAME_INVALID
			0xc000003b, // STATUS_OBJECT_PATH_SYNTAX_BAD
			0xc0000039, // STATUS_OBJECT_PATH_INVALID
			0xc0000280: // STATUS_REPARSE_POINT_NOT_RESOLVED
			return true
		}
	}
	var internalErr *smb2.InternalError
	if errors.As(err, &internalErr) && internalErr.Message == "Too many levels of symbolic links" {
		return true
	}
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, errLinkOutsideShare) || errors.Is(err, errTooManyLinks)
}

// isStoppedOnSymlink reports whether the server stopped at a link in a parent
// of the path, which go-smb2 only follows when it is asked to follow links.
func isStoppedOnSymlink(err error) bool {
	var responseErr *smb2.ResponseError
	return errors.As(err, &responseErr) && responseErr.Code == 0x8000002d // STATUS_STOPPED_ON_SYMLINK
}

func (l *smbListing) pushDir(dir string) error {
	infos, err := l.fs.ReadDir(dir)
	if err != nil {
		return err
	}

	var visits []visit
	for _, info := range infos {
		var v = visit{FileInfo: info, path: path.Join(dir, info.Name())}
		var key = v.path
		if info.IsDir() && l.recursive {
			key += "/"
			if strings.HasPrefix(l.startAt, key) {
				visits = append(visits, v)
				continue
			}
		}
		if key >= l.startAt {
			visits = append(visits, v)
		}
	}

	// Include the directory separator when ordering recursive visits: a.csv must
	// precede a/b.csv. Reverse sorting lets us pop the earliest path off the stack.
	key := func(v visit) string {
		if v.IsDir() && l.recursive {
			return v.path + "/"
		}
		return v.path
	}
	sort.Slice(visits, func(i, j int) bool { return key(visits[i]) > key(visits[j]) })
	l.stack = append(l.stack, visits...)
	return nil
}

// visit retains the full path, since FileInfo only provides the file name.
type visit struct {
	fs.FileInfo
	path string
}

func (s *smbSource) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	if err := smb.ValidatePath(obj.Path); err != nil {
		return nil, obj, err
	}
	var file *smb2.File
	err := s.WithShare(ctx, func(share *smb2.Share) error {
		// Open the file a link names, since go-smb2 may not follow it correctly.
		var p = obj.Path
		info, err := share.Lstat(smb.Path(p))
		if isStoppedOnSymlink(err) {
			// A parent directory, such as the configured directory, is a link.
			// Look the file up through the directory the link names, so a file
			// link beneath it is resolved as the listing resolved it. Otherwise
			// Open follows the parent link the same way the listing's ReadDir did.
			if dir, dirErr := realDir(shareFS{share}, path.Dir(p)); dirErr == nil && dir != path.Dir(p) {
				p = path.Join(dir, path.Base(p))
				info, err = share.Lstat(smb.Path(p))
			} else {
				if dirErr != nil {
					log.WithField("path", p).WithError(dirErr).Warn("resolving directory links above a file; opening it through the server's link instead")
				}
				info, err = nil, nil
			}
		}
		if err != nil {
			return err
		}
		if info != nil && info.Mode()&os.ModeSymlink != 0 && !info.IsDir() {
			if p, _, err = resolveLink(shareFS{share}, p); err != nil {
				return err
			}
		}
		if file, err = share.Open(smb.Path(p)); err != nil {
			return err
		}
		// Refresh metadata in case the file changed after it was listed.
		info, err = file.Stat()
		if err != nil {
			file.Close()
			return err
		}
		obj.Size = info.Size()
		obj.ModTime, _ = fileModTime(info)
		return nil
	})
	if err != nil {
		return nil, obj, fmt.Errorf("opening path %s for reading: %w", obj.Path, err)
	}
	return file, obj, nil
}

type smbDriver struct {
	filesource.Source
	tunnelMu sync.Mutex
	tunnels  map[smbTunnelKey]int
}

// filesource.Store has no Close method. Scope the SMB session to each RPC so
// discovery, validation, and completed or failed captures all release it.
func (d *smbDriver) sourceWithClose() (*filesource.Source, func()) {
	var src = d.Source
	var closer io.Closer
	src.Connect = func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
		store, err := d.Connect(ctx, cfg)
		if err == nil {
			closer, _ = store.(io.Closer)
		}
		return store, err
	}
	return &src, func() {
		if closer != nil {
			if err := closer.Close(); err != nil {
				log.WithError(err).Warn("closing SMB connection")
			}
		}
	}
}

func (d *smbDriver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	src, close := d.sourceWithClose()
	defer close()
	return src.Discover(ctx, req)
}

func (d *smbDriver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	src, close := d.sourceWithClose()
	defer close()
	return src.Validate(ctx, req)
}

func (d *smbDriver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	src, close := d.sourceWithClose()
	defer close()
	return src.Pull(open, stream)
}

func newDriver() *smbDriver {
	var d = &smbDriver{}
	d.Source = filesource.Source{
		NewConfig: func(raw json.RawMessage) (filesource.Config, error) {
			var cfg = config{Port: 445}
			if err := pf.UnmarshalStrict(raw, &cfg); err != nil {
				return nil, fmt.Errorf("parsing config json: %w", err)
			}
			var err error
			cfg.Directory, err = smb.NormalizePath(cfg.Directory)
			return cfg, err
		},
		Connect:          d.connect,
		ConfigSchema:     configSchema,
		DocumentationURL: "https://go.estuary.dev/source-smb",
		// Match source-sftp's guard against files at the current time horizon.
		TimeHorizonDelta: time.Second * -30,
	}
	return d
}

func main() {
	boilerplate.RunMain(newDriver())
}

func configSchema(parserSchema json.RawMessage) json.RawMessage {
	return json.RawMessage(`{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "properties": {
          "host": {
            "type": "string",
            "title": "Host",
            "description": "Hostname or IP address of your SMB server.",
            "order": 0
          },
          "port": {
            "type": "integer",
            "title": "Port",
            "description": "Port of your SMB server. Defaults to 445.",
            "default": 445,
            "minimum": 1,
            "maximum": 65535,
            "order": 1
          },
          "share": {
            "type": "string",
            "title": "Share",
            "description": "Name of the share to capture from, for example data.",
            "order": 2
          },
          "username": {
            "type": "string",
            "title": "Username",
            "description": "Username for authentication.",
            "order": 3
          },
          "password": {
            "type": "string",
            "title": "Password",
            "description": "Password for authentication.",
            "secret": true,
            "order": 4
          },
          "domain": {
            "type": "string",
            "title": "Domain",
            "description": "Optional NTLM domain for authentication.",
            "order": 5
          },
          "directory": {
            "type": "string",
            "title": "Directory",
            "default": "/",
            "description": "Directory inside the share to capture files from. Accepts forward or backslashes, with or without a leading separator, and normalizes the path. Defaults to / (the share root). All files in this directory and any subdirectories will be included.",
            "order": 6
          },
          "matchFiles": {
            "type": "string",
            "title": "Match Files Regex",
            "description": "Filter applied to file paths. If provided, only files whose path relative to the share root, starting with / (for example /inbox/sub/a.csv), matches this regex will be read. For example, .*\\.csv$ captures CSV files.",
            "order": 7
          },
          "networkTunnel": {
            "type": "object",
            "title": "Network Tunnel",
            "description": "Connect to your system through an SSH server that acts as a bastion host for your network.",
            "advanced": true,
            "order": 8,
            "additionalProperties": false,
            "properties": {
              "sshForwarding": {
                "type": "object",
                "title": "SSH Forwarding",
                "description": "Forward connections to your SMB server through an SSH bastion host.",
                "additionalProperties": false,
                "required": ["sshEndpoint", "privateKey"],
                "properties": {
                  "sshEndpoint": {
                    "type": "string",
                    "title": "SSH Endpoint",
                    "description": "Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port]).",
                    "pattern": "^ssh://.+@.+$",
                    "order": 0
                  },
                  "privateKey": {
                    "type": "string",
                    "title": "SSH Private Key",
                    "description": "Unencrypted private key to connect to the remote SSH server.",
                    "secret": true,
                    "multiline": true,
                    "order": 1
                  }
                }
              }
            }
          },
          "advanced": {
            "properties": {
              "ascendingKeys": {
                "type": "boolean",
                "title": "Ascending Keys",
                "description": "May improve sync speeds by listing files from the end of the last sync, rather than listing all files in the configured directory. This requires that you write files in ascending lexicographic order, such as an RFC-3339 timestamp, so that lexical path ordering matches modification time ordering."
              }
            },
            "additionalProperties": false,
            "type": "object",
            "description": "Options for advanced users. You should not typically need to modify these.",
            "advanced": true
          },
          "parser": ` + string(parserSchema) + `
        },
        "type": "object",
        "required": ["host", "share", "username", "password"],
        "title": "SMB Source"
      }`)
}
