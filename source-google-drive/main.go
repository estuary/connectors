package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/estuary/connectors/filesource"
	google_auth "github.com/estuary/connectors/go/auth/google"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

var (
	scopes            = []string{drive.DriveReadonlyScope}
	folderURLReString = "https://drive.google.com/drive/folders/([^/?]+).*"
	folderURLRe       = regexp.MustCompilePOSIX(folderURLReString)
)

type config struct {
	FolderURL   string                        `json:"folderUrl"`
	Credentials *google_auth.CredentialConfig `json:"credentials"`
	MatchKeys   string                        `json:"matchKeys"`
	Parser      *parser.Config                `json:"parser"`
	Advanced    advancedConfig                `json:"advanced"`
}

type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys"`
}

func (c config) Validate() error {
	if folderURLRe.FindStringSubmatch(c.FolderURL) == nil {
		return fmt.Errorf("invalid or missing `folderUrl`: must match regex %s", folderURLReString)
	}
	return nil
}

func (c config) DiscoverRoot() string {
	return folderURLRe.FindStringSubmatch(c.FolderURL)[1]
}

func (c config) FilesAreMonotonic() bool {
	return c.Advanced.AscendingKeys
}

func (c config) ParserConfig() *parser.Config {
	return c.Parser
}

func (c config) PathRegex() string {
	return c.MatchKeys
}

func (c config) buildService(ctx context.Context) (*drive.FilesService, error) {
	if creds, err := c.Credentials.GoogleCredentials(ctx, scopes...); err != nil {
		return nil, fmt.Errorf("initializing Google credentials: %w", err)
	} else if client, err := drive.NewService(ctx, option.WithCredentials(creds)); err != nil {
		return nil, fmt.Errorf("initializing drive file service: %w", err)
	} else if page, err := readdirPage(client.Files, ctx, c.DiscoverRoot(), ""); err != nil {
		return nil, fmt.Errorf("failed to list folder: %w", err)
	} else if len(page.Files) == 0 {
		return nil, fmt.Errorf("listing of folder returned no results.\n" +
			"Please verify that the folder has been shared with the OAuth user or service account,\n" +
			"and that it has at least one file or sub-folder")
	} else {
		return client.Files, nil
	}
}

type gdStore struct {
	client *drive.FilesService
}

func (s *gdStore) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	// progressively walk folder structure
	// skip all *drive.File which produce a path that's < startAt
	// if !recursive, return directories. Otherwise transparently walk them.

	type folderEntry struct {
		path  string
		files []*drive.File
	}
	var stack []folderEntry

	// `query.Prefix` is a Google Drive FolderID.
	if root, err := readdirAll(s.client, ctx, query.Prefix); err != nil {
		return nil, fmt.Errorf("listing root folder: %w", err)
	} else {
		stack = append(stack, folderEntry{path: "", files: root})
	}

	return filesource.ListingFunc(func() (filesource.ObjectInfo, error) {
		for {
			var depth = len(stack)
			if depth == 0 {
				return filesource.ObjectInfo{}, io.EOF
			}
			var folder = &stack[depth-1]

			if len(folder.files) == 0 {
				stack = stack[:depth-1] // Pop up stack to parent folder.
				continue
			}

			// Pop next file.
			var file = folder.files[0]
			folder.files = folder.files[1:]

			var path = path.Join(folder.path, file.Name)

			modTime, err := time.Parse(time.RFC3339, file.ModifiedTime)
			if err != nil {
				return filesource.ObjectInfo{},
					fmt.Errorf("file %q has invalid ModifiedTime %q", path, file.ModifiedTime)
			}

			if query.StartAt > path && !strings.HasPrefix(query.StartAt, path) {
				// File or folder is entirely skipped by StartAt.
				continue
			} else if file.MimeType != "application/vnd.google-apps.folder" {

				if strings.HasPrefix(file.MimeType, "application/vnd.google-apps") {
					logrus.WithFields(logrus.Fields{
						"path":     path,
						"mimeType": file.MimeType,
					}).Warn("skipping unsupported Google Apps document")
					continue
				}

				return filesource.ObjectInfo{
					Path:            path,
					IsPrefix:        false,
					ContentSum:      file.Md5Checksum,
					Size:            file.Size,
					ContentType:     file.MimeType,
					ContentEncoding: "",
					ModTime:         modTime,
					Extra:           file.Id,
				}, nil
			} else if !query.Recursive {
				return filesource.ObjectInfo{
					Path:            path,
					IsPrefix:        true,
					ContentSum:      file.Md5Checksum,
					Size:            file.Size,
					ContentType:     file.MimeType,
					ContentEncoding: "",
					ModTime:         modTime,
					Extra:           file.Id,
				}, nil
			}

			// Push child folder onto the stack.
			if files, err := readdirAll(s.client, ctx, file.Id); err != nil {
				return filesource.ObjectInfo{},
					fmt.Errorf("listing folder %s (Drive ID %s): %w", path, file.Id, err)
			} else {
				stack = append(stack, folderEntry{path: path, files: files})
			}
		}
	}), nil
}

func (s *gdStore) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	if resp, err := s.client.Get(obj.Extra.(string)).Download(); err != nil {
		return nil, obj, fmt.Errorf("client.Get(ID %s): %w", obj.Extra, err)
	} else {
		return resp.Body, obj, nil
	}
}

func readdirPage(client *drive.FilesService, ctx context.Context, folderID, pageToken string) (*drive.FileList, error) {
	var call = client.List().
		Context(ctx).
		Q(fmt.Sprintf("'%s' in parents", folderID)).
		// We support files stored on either "My Drives" and also organization "Shared Drives".
		// These are different entities in Drive's API model for ... $reasons.
		// See: https://developers.google.com/drive/api/guides/enable-shareddrives
		SupportsAllDrives(true).
		// Surface files stored in Shared Drives, not just My Drives.
		IncludeItemsFromAllDrives(true).
		// PageSize(1). // Default is ~100. Uncomment to exercise multi-page listing.
		Fields("nextPageToken, files(id, name, md5Checksum, mimeType, modifiedTime, size)")

	// Is this a continuation? Pass its token.
	if pageToken != "" {
		call = call.PageToken(pageToken)
	}

	return call.Do()
}

func readdirAll(client *drive.FilesService, ctx context.Context, folderID string) ([]*drive.File, error) {
	var files []*drive.File
	var pageToken string

	for {
		var resp, err = readdirPage(client, ctx, folderID, pageToken)

		if err != nil {
			return nil, fmt.Errorf("listing folder %s: %w", folderID, err)
		} else if resp.IncompleteSearch {
			return nil, fmt.Errorf("file listing was unexpectedly incomplete (please contact Estuary support)")
		}
		files = append(files, resp.Files...)

		if pageToken = resp.NextPageToken; pageToken == "" {
			break // All done.
		}
	}

	// Ensure files are in Name order.
	sort.SliceStable(files, func(i, j int) bool { return files[i].Name < files[j].Name })

	return files, nil
}

func configSchema(parserSchema json.RawMessage) json.RawMessage {
	var credentialSchema, _ = json.Marshal(google_auth.CredentialConfig{}.JSONSchema())

	return json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "Google Drive Source",
		"type":    "object",
		"required": [
			"folderUrl",
			"credentials"
		],
		"properties": {
			"folderUrl": {
				"type":        "string",
				"title":       "Folder URL",
				"description": "URL of the Google Drive Folder to be captured from.",
				"order":       1
			},
			"credentials": ` + string(credentialSchema) + `,
			"matchKeys": {
				"type":        "string",
				"title":       "Match Keys",
				"format":      "regex",
				"description": "Filter applied to file paths under the folder URL. If provided, only files whose paths (relative to the folder) match this regex will be read. For example, you can use \".*\\.json\" to only capture json files.",
				"order":       3
			},
			"advanced": {
				"properties": {
				  "ascendingKeys": {
					"type":        "boolean",
					"title":       "Ascending Keys",
					"description": "Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. For more information see https://go.estuary.dev/fOMT4s.",
					"default":     false
				  }
				},
				"additionalProperties": false,
				"type": "object",
				"description": "Options for advanced users. You should not typically need to modify these.",
				"advanced": true
			},
			"parser": ` + string(parserSchema) + `
		}
    }`)
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
			if client, err := cfg.(config).buildService(ctx); err != nil {
				return nil, err
			} else {
				return &gdStore{client: client}, nil
			}
		},
		ConfigSchema:     configSchema,
		DocumentationURL: "https://go.estuary.dev/source-google-drive",
		Oauth2:           google_auth.Spec(scopes...),
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
	}
	src.Main()
}
