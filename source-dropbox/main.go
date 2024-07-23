package main

import (
	"cmp"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
)

type config struct {
	Path        string         `json:"path"`
	Credentials *Credentials   `json:"credentials"`
	Parser      *parser.Config `json:"parser"`
	MatchKeys   string         `json:"matchKeys,omitempty"`
	Advanced    advancedConfig `json:"advanced"`
}

type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys,omitempty"`
}

func (c config) Validate() error {
	requiredProperties := [][]string{
		{"Path", c.Path},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

func (c config) DiscoverRoot() string {
	return c.Path
}

func (c config) RecommendedName() string {
	return c.Path[1:]
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

func newDropboxStore(ctx context.Context, cfg config) (*dropboxStore, error) {
	oauthClient := cfg.Credentials.GetOauthClient(ctx)
	config := dropbox.Config{
		Client:   oauthClient,
		LogLevel: dropbox.LogDebug,
	}
	client := files.New(config)

	store := dropboxStore{
		client: client,
		config: cfg,
	}

	if err := store.check(); err != nil {
		return &dropboxStore{}, err
	}

	return &store, nil
}

type dropboxStore struct {
	client files.Client
	config config
}

// check checks if the specified path exists in the Dropbox store.
// It returns an error if the path does not exist or if there was an error
// while checking the path.
func (dbStore *dropboxStore) check() error {
	_, err := dbStore.client.ListFolder(&files.ListFolderArg{Path: dbStore.config.Path, Limit: 1})
	if err != nil {
		return err
	}
	return nil
}

func (dbStore *dropboxStore) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	// Get all files at the specified path from Dropbox.
	arg := &files.ListFolderArg{Path: query.Prefix, Recursive: query.Recursive}
	f, err := dbStore.client.ListFolder(arg)
	if err != nil {
		return nil, fmt.Errorf("retrieving folder: %w", err)
	}

	entries := f.Entries

	for f.HasMore {
		arg := files.NewListFolderContinueArg(f.Cursor)

		f, err = dbStore.client.ListFolderContinue(arg)
		if err != nil {
			return nil, fmt.Errorf("retrieving additional files: %w", err)
		}

		entries = append(entries, f.Entries...)
	}

	// Convert each file and folder entry to an ObjectInfo.
	var objects []filesource.ObjectInfo
	for _, entry := range entries {
		switch e := entry.(type) {
		case *files.FileMetadata:
			obj := filesource.ObjectInfo{
				Path:       e.PathLower,
				IsPrefix:   false,
				ContentSum: e.ContentHash,
				Size:       int64(e.Size),
				ModTime:    e.ServerModified,
			}
			objects = append(objects, obj)

		case *files.FolderMetadata:
			if !query.Recursive {
				obj := filesource.ObjectInfo{
					Path:     e.PathLower,
					IsPrefix: true,
				}
				objects = append(objects, obj)
			}

		default:
			// Ignore all other metadata types (like DeletedMetadata).
			continue
		}
	}

	// Remove files that are lexicographically less than the query's StartAt property.
	if query.StartAt != "" {
		var filtered []filesource.ObjectInfo
		for _, obj := range objects {
			if query.StartAt < obj.Path {
				filtered = append(filtered, obj)
			}
		}

		objects = filtered
	}

	// Sort the remaining objects by path.
	slices.SortStableFunc(objects, func(a, b filesource.ObjectInfo) int {
		return cmp.Compare(a.Path, b.Path)
	})

	return &dropboxListing{
		objects:   objects,
		index:     0,
		recursive: query.Recursive,
	}, nil
}

func (dbStore *dropboxStore) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	file, reader, err := dbStore.client.Download(&files.DownloadArg{Path: obj.Path})
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	obj.Path = file.PathLower
	obj.ContentSum = file.ContentHash
	obj.Size = int64(file.Size)
	obj.ModTime = file.ServerModified

	return reader, obj, nil
}

type dropboxListing struct {
	objects   []filesource.ObjectInfo
	index     int
	recursive bool
}

func (l *dropboxListing) Next() (filesource.ObjectInfo, error) {
	// When there are no entries left to return, return an io.EOF.
	if l.index > len(l.objects)-1 {
		return filesource.ObjectInfo{}, io.EOF
	}

	obj := l.objects[l.index]
	l.index++
	return obj, nil
}

func configSchema(parserSchema json.RawMessage) json.RawMessage {

	return json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "Dropbox Source",
		"type": "object",
		"required": [
			"path",
			"credentials"
		],
		"properties": {
			"path": {
				"type": "string",
				"title": "Path",
				"format": "string",
				"pattern": "^/.+",
				"description": "The path to the Dropbox folder to read from. For example, \"/my-folder\".",
				"order": 1
			},
			"credentials": {
				"title": "Credentials",
				"description": "OAuth2 credentials for Dropbox. Those are automatically handled by the Web UI.",
				"type": "object",
				"x-oauth2-provider": "dropbox",
				"properties": {
					"refresh_token": {
						"title": "Refresh Token",
						"description": "The refresh token for the Dropbox account.",
						"type": "string",
						"secret": true
					},
					"client_id": {
						"title": "Client ID",
						"description": "The client ID for the Dropbox account.",
						"type": "string",
						"secret": true
					},
					"client_secret": {
						"title": "Client Secret",
						"description": "The client secret for the Dropbox account.",
						"type": "string",
						"secret": true
					}
				},
				"required": ["refresh_token", "client_id", "client_secret"],
				"order": 2
			},
			"matchKeys": {
				"type": "string",
				"title": "Match Keys",
				"format": "regex",
				"description": "Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files.",
				"order": 3
			},
			"advanced": {
				"properties": {
				  "ascendingKeys": {
					"type":        "boolean",
					"title":       "Ascending Keys",
					"description": "Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering.",
					"default":     false
				  }
				},
				"additionalProperties": false,
				"type": "object",
				"description": "Options for advanced users. You should not typically need to modify these.",
				"advanced": true,
				"order": 4
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
			return newDropboxStore(ctx, cfg.(config))
		},
		ConfigSchema:     configSchema,
		DocumentationURL: "https://go.estuary.dev/source-dropbox",
		Oauth2:           oauth2Spec(),
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
	}

	src.Main()
}
