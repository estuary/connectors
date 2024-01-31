package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/pkg/sftp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"golang.org/x/sync/errgroup"
)

type config struct {
	Address     string            `json:"address" jsonschema:"title=Address" jsonschema_extras:"order=0"`
	Username    string            `json:"username" jsonschema:"-"`
	Password    string            `json:"password" jsonschema:"-"`
	Directory   string            `json:"directory" jsonschema:"title=Directory" jsonschema_extras:"order=4"`
	MatchFiles  string            `json:"matchFiles,omitempty" jsonschema:"title=Match Files Regex" jsonschema_extras:"order=5"`
	Credentials credentialsConfig `json:"credentials" jsonschema_extras:"order=6,oneOf=true"`
	Advanced    advancedConfig    `json:"advanced,omitempty" jsonschema_extras:"advanced=true"`
	Parser      *parser.Config    `json:"parser,omitempty"`
}

func (config) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Address":
		return "Host and port of the SFTP server. Example: myserver.com:22"
	case "Credentials":
		return "Credentials for authentication"
	case "Directory":
		return "Directory to capture files from. All files in this directory and any subdirectories will be included."
	case "MatchFiles":
		return "Filter applied to all file names in the directory. If provided, only files whose path (relative to the directory) matches this regex will be read."
	case "Advanced":
		return "Options for advanced users. You should not typically need to modify these."
	default:
		return ""
	}
}

type credentialsConfig struct {
	// one of "password" or "sshKey"
	Type string `json:"type"`

	Username string `json:"username"`
	Password string `json:"password"`
	SSHKey   string `json:"sshKey"`
}

type advancedConfig struct {
	// Although possibly less useful here than for the cloud storage connectors, this advanced
	// configuration may have some utility for cases where new files are added in lexically newer
	// directories. For example, if `a/` has been processed and a new directory `b/` is added, a
	// connector configured with ascendingKeys will able able to skip the full directory listing of
	// `a/` when completing the sweep that processes `b/` (and for all subsequent sweeps).
	AscendingKeys bool `json:"ascendingKeys,omitempty" jsonschema:"title=Ascending Keys"`
}

func (advancedConfig) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "AscendingKeys":
		return "May improve sync speeds by listing files from the end of the last sync, rather than listing all files in the configured directory. This requires that you write files in ascending lexicographic order, such as an RFC-3339 timestamp, so that lexical path ordering matches modification time ordering."
	default:
		return ""
	}
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"directory", c.Directory},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Credentials.Password == "" && c.Credentials.SSHKey == "" && c.Password == "" {
		return fmt.Errorf("missing password and sshKey, one must be provided for authentication")
	}

	if c.Credentials.Username == "" && c.Username == "" {
		return fmt.Errorf("missing username")
	}

	if c.Directory != path.Clean(c.Directory) {
		return fmt.Errorf("invalid configured directory '%s': must be a clean path (calculated clean path is '%s')", c.Directory, path.Clean(c.Directory))
	}

	if !strings.HasPrefix(c.Directory, "/") {
		return fmt.Errorf("invalid configured directory '%s': path must be absolute (must start with a '/')", c.Directory)
	}

	if c.MatchFiles != "" {
		if _, err := regexp.Compile(c.MatchFiles); err != nil {
			return fmt.Errorf("invalid regex pattern '%s' for Match Files: %w", c.MatchFiles, err)
		}
	}

	return nil
}

func (c config) DiscoverRoot() string {
	return c.Directory
}

func (c config) RecommendedName() string {
	return strings.Trim(c.DiscoverRoot(), "/")
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

func newSftpSource(ctx context.Context, cfg config) (filesource.Store, error) {
	var user = cfg.Credentials.Username
	if len(user) == 0 && len(cfg.Username) > 0 {
		user = cfg.Username
	}

	sshConfig := ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// Legacy authentication method
	if cfg.Credentials.Type == "" && cfg.Password != "" {
		sshConfig.Auth = append(sshConfig.Auth, ssh.Password(cfg.Password))
	} else if cfg.Credentials.Type == "password" {
		sshConfig.Auth = append(sshConfig.Auth, ssh.Password(cfg.Credentials.Password))
	} else if cfg.Credentials.Type == "sshKey" {
		if signer, err := ssh.ParsePrivateKey([]byte(cfg.Credentials.SSHKey)); err != nil {
			return nil, fmt.Errorf("parsing ssh key: %w", err)
		} else {
			sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeys(signer))
		}
	} else {
		return nil, fmt.Errorf("invalid credentials.type %q", cfg.Credentials.Type)
	}

	conn, err := ssh.Dial("tcp", cfg.Address, &sshConfig)
	if err != nil {
		return nil, fmt.Errorf("tcp connection to '%s': %w", cfg.Address, err)
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, fmt.Errorf("creating sftp client: %w", err)
	}

	// Surface errors with the configuration here so they are caught in discover & publish commands,
	// rather than at runtime. We can verify that the configured root directory is actually a
	// directory - note that Stat here will follow symlinks, so a symlink to a directory works as
	// the configured "root" directory.
	info, err := client.Stat(cfg.Directory)
	if err != nil {
		return nil, fmt.Errorf("error reading directory '%s': %w", cfg.Directory, err)
	}

	if !info.IsDir() {
		log.WithFields(log.Fields{
			"Name":    info.Name(),
			"IsDir":   info.IsDir(),
			"ModTime": info.ModTime().UTC(),
			"Mode":    info.Mode().String(),
			"Size":    info.Size(),
		}).Info("invalid directory")
		return nil, fmt.Errorf("'%s' is not a directory", cfg.Directory)
	}

	return &sftpSource{
		conn:   conn,
		client: client,
	}, nil
}

type sftpSource struct {
	conn   *ssh.Client
	client *sftp.Client
}

func (s *sftpSource) List(_ context.Context, query filesource.Query) (filesource.Listing, error) {
	return newSftpListing(s.client, s.client, query.Prefix, query.Recursive, query.StartAt)
}

type dirLister interface {
	ReadDir(string) ([]os.FileInfo, error)
}

// sftpListing provides a mechanism for listing all files from an SFTP filesystem in lexical order
// in a somewhat performant way. A simpler implementation would be possible using
// (*sftp.Client).Walk and reading the entire tree into memory at once, sorting it, and filtering
// out items per the StartAt query parameter. This implementation lazily descends into
// subdirectories, and applies StartAt while traversing the tree to avoid reading directories that
// are not needed. The two are equivalent into two cases: 1) A pathological case where nested
// directories are lexically "before" any of the files in the directories and there is no StartAt,
// and 2) A more common case where all files are in the root directory and there are no
// subdirectories.
type sftpListing struct {
	client    *sftp.Client
	fs        dirLister
	stack     []visit
	recursive bool
	startAt   string
	root      string
}

func newSftpListing(client *sftp.Client, fs dirLister, root string, recursive bool, startAt string) (*sftpListing, error) {
	l := &sftpListing{
		client:    client,
		fs:        fs,
		recursive: recursive,
		startAt:   startAt,
		root:      root,
	}

	// Do the initial listing of the root directory we are starting at, which will be provided by
	// query.Prefix.
	if err := l.pushDir(root); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *sftpListing) Next() (filesource.ObjectInfo, error) {
	for {
		if len(l.stack) == 0 {
			// No more files to list. io.EOF is the expected error in this case.
			return filesource.ObjectInfo{}, io.EOF
		}

		// Pop the next file in lexical order.
		next := l.stack[len(l.stack)-1]
		l.stack = l.stack[:len(l.stack)-1]

		// If this file is a symlink, it might be a symlink to a directory. We don't descend into
		// directories so they should not be included in any listing output. If the file is a
		// symlink to a non-directory, we should report the info of the referent file.
		if next.Mode()&os.ModeSymlink != 0 {
			info, err := l.client.Stat(next.path)
			if err != nil {
				return filesource.ObjectInfo{}, fmt.Errorf("stat'ing symbolic link: %w", err)
			}
			if info.IsDir() {
				log.WithFields(log.Fields{
					"Name": info.Name(),
					"Mode": info.Mode().String(),
				}).Info("skipping symbolic link to directory in listing output")
				continue
			}

			next = visit{FileInfo: info, path: next.path}
		}

		if next.IsDir() {
			// If the query is recursive, descend into this subdirectory to list its files but do
			// not return info the subdirectory file entry itself.
			if l.recursive {
				if err := l.pushDir(next.path); err != nil {
					return filesource.ObjectInfo{}, fmt.Errorf("pushing next path %s: %w", next.path, err)
				}
				continue
			}

			// If the query is not recursive, do not descend into this subdirectory but do return
			// info with IsPrefix=true for it. This is accomplished the the next.IsDir() call in the
			// struct below.
		}

		return filesource.ObjectInfo{
			Path:            next.path,
			IsPrefix:        next.IsDir(),
			ContentSum:      "",
			Size:            next.Size(),
			ContentType:     "",
			ContentEncoding: "",
			ModTime:         next.ModTime(),
		}, nil
	}
}

func (l *sftpListing) pushDir(dir string) error {
	infos, err := l.fs.ReadDir(dir)
	if err != nil {
		return err
	}

	// Filter out file names < query.StartAt and map to visits.
	visits := []visit{}
	for _, info := range infos {
		filePath := filepath.Join(dir, info.Name())
		startAt := l.startAt

		// If this is a directory, filter it out based on the directory part of startAt. If we've
		// process files like `a/b.csv` already, we still need to process files like `a/c.csv` and
		// need to include the directory `a/` in the listing, even though `a/` is lexically before
		// `a/b.csv`. But if we've process a file like `b/b.csv` `a/` should not be included since
		// `a/` is lexically before `b/`.
		if info.IsDir() {
			// NB: IsDir() doesn't work for symlink directors, but symlink directories are already
			// removed from consideration by Next().
			startAt = filepath.Dir(filePath)
		}

		if filePath >= startAt {
			visits = append(visits, visit{FileInfo: info, path: filePath})
		}
	}

	// Ensure files are lexically sorted in reverse order so earliest files are popped first. We
	// make no assumption about lexical ordering of files returned from ReadDir etc. as this
	// ordering is filesystem dependent. The filesource framework requires listings to be in lexical
	// order, so an explicit sort is required.
	sort.Slice(visits, func(i, j int) bool {
		return visits[i].path > visits[j].path
	})

	l.stack = append(l.stack, visits...)

	return nil
}

// visit wraps fs.FileInfo and retains the full path to the file, since fs.FileInfo only provides
// the file name.
type visit struct {
	fs.FileInfo
	path string
}

func (s *sftpSource) Read(_ context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	file, err := s.client.Open(obj.Path)
	if err != nil {
		return nil, obj, fmt.Errorf("opening path %s for reading: %w", obj.Path, err)
	}

	// Update file info in case it has changed since the original listing.
	info, err := file.Stat()
	if err != nil {
		return nil, obj, fmt.Errorf("stat'ing path %s for reading: %w", obj.Path, err)
	}
	obj.Size = info.Size()
	obj.ModTime = info.ModTime()

	r, w := io.Pipe()

	f := &sftpFile{
		file:   file,
		reader: r,
		writer: w,
		group:  new(errgroup.Group),
	}

	f.group.Go(f.transfer)

	return f, obj, nil
}

// sftpFile handles piping the data from a remote sftp file to a consumer. This is mostly here to
// allow the use of (*sftp.File).WriteTo for reading data, which improves network performance by
// executing concurrent buffered reads which are fed to the reader while maintaining an accurate
// streamed representation of the file.
type sftpFile struct {
	file   *sftp.File
	reader *io.PipeReader
	writer *io.PipeWriter
	group  *errgroup.Group
}

func (f *sftpFile) transfer() error {
	// Pump data to the pipe using WriteTo to utilize concurrent read workers.
	_, err := f.file.WriteTo(f.writer)

	// Errors from WriteTo will be propagated to calls to read. CloseWithError always returns `nil`.
	return f.writer.CloseWithError(err)
}

func (f *sftpFile) Read(p []byte) (n int, err error) {
	return f.reader.Read(p)
}

func (f *sftpFile) Close() error {
	if err := f.file.Close(); err != nil {
		return err
	} else if f.reader.Close(); err != nil {
		return err
	}

	return f.group.Wait()
}

func main() {
	var src = filesource.Source{
		NewConfig: func(raw json.RawMessage) (filesource.Config, error) {
			var cfg config
			if err := pf.UnmarshalStrict(raw, &cfg); err != nil {
				return nil, fmt.Errorf("parsing config json: %w", err)
			}
			return cfg, nil
		},
		Connect: func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
			return newSftpSource(ctx, cfg.(config))
		},
		ConfigSchema:     configSchema,
		DocumentationURL: "https://go.estuary.dev/source-sftp",
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
	}

	src.Main()
}

func configSchema(parserSchema json.RawMessage) json.RawMessage {
	return json.RawMessage(`{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "properties": {
          "address": {
            "type": "string",
            "title": "Address",
            "description": "Host and port of the SFTP server. Example: myserver.com:22",
            "order": 0
          },
          "credentials": {
            "type": "object",
            "discriminator": {
              "propertyName": "type"
            },
            "oneOf": [{
              "title": "SSH Key",
              "properties": {
                "type": {
                  "type": "string",
                  "title": "Authentication Method",
                  "const": "sshKey",
                  "default": "sshKey",
                  "order": 1
                },
                "username": {
                  "type": "string",
                  "title": "Username",
                  "description": "Username for authentication.",
                  "order": 2
                },
                "sshKey": {
                  "type": "string",
                  "title": "SSH Key",
                  "description": "SSH Key for authentication",
                  "multiline": true,
                  "order": 3,
                  "secret": true
                }
              },
              "required": [
                "type",
                "username",
                "sshKey"
              ]
            }, {
              "title": "Password",
              "properties": {
                "type": {
                  "type": "string",
                  "title": "Authentication Method",
                  "const": "password",
                  "default": "password",
                  "order": 1
                },
                "username": {
                  "type": "string",
                  "title": "Username",
                  "description": "Username for authentication.",
                  "order": 2
                },
                "password": {
                  "type": "string",
                  "title": "Password",
                  "description": "Password for authentication",
                  "order": 3,
                  "secret": true
                }
              },
              "required": [
                "type",
                "username",
                "password"
              ]
            }]
          },
          "directory": {
            "type": "string",
            "title": "Directory",
            "description": "Directory to capture files from. All files in this directory and any subdirectories will be included.",
            "order": 4
          },
          "matchFiles": {
            "type": "string",
            "title": "Match Files Regex",
            "description": "Filter applied to all file names in the directory. If provided, only files whose path (relative to the directory) matches this regex will be read.",
            "order": 5
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
        "required": [
          "address",
          "credentials",
          "directory"
        ],
        "title": "SFTP Source"
      }`)
}
