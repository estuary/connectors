package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
)

type config struct {
	Path        string         `json:"path" yaml:"path"`
	Credentials *Credentials   `json:"credentials" yaml:"credentials"`
	Parser      *parser.Config `json:"parser" yaml:"parser"`
	MatchKeys   string         `json:"matchKeys,omitempty" yaml:"matchKeys,omitempty"`
	Advanced    advancedConfig `json:"advanced" yaml:"advanced"`
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
		//! Note: This Client property was originally build for testing purposes. So, it can change in the future.
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
func (dbx *dropboxStore) check() error {
	_, err := dbx.client.ListFolder(&files.ListFolderArg{Path: dbx.config.Path, Limit: 1})
	if err != nil {
		return err
	}
	return nil
}

func (dbx *dropboxStore) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	files, err := dbx.client.ListFolder(&files.ListFolderArg{Path: query.Prefix, Recursive: query.Recursive, Limit: 2})
	if err != nil {
		return nil, err
	}

	return &dropboxListing{
		ctx:       ctx,
		client:    dbx.client,
		files:     *files,
		index:     0,
		recursive: query.Recursive,
	}, nil
}

func (dbx *dropboxStore) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	meta, reader, err := dbx.client.Download(&files.DownloadArg{Path: obj.Path})
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	obj.Path = meta.PathDisplay
	obj.ModTime = meta.ClientModified
	if meta.ExportInfo != nil {
		obj.ContentType = meta.ExportInfo.ExportAs
	}
	obj.Size = int64(meta.Size)

	return reader, obj, nil
}

type dropboxListing struct {
	ctx       context.Context
	client    files.Client
	files     files.ListFolderResult
	index     int
	recursive bool
}

func (l *dropboxListing) Next() (filesource.ObjectInfo, error) {
	if len(l.files.Entries) == 0 {
		return filesource.ObjectInfo{}, io.EOF
	}
	if l.index > len(l.files.Entries)-1 {
		if !l.files.HasMore {
			return filesource.ObjectInfo{}, io.EOF
		} else {
			// TODO: Fetch next page
			resp, err := l.client.ListFolderContinue(&files.ListFolderContinueArg{Cursor: l.files.Cursor})
			if err != nil {
				return filesource.ObjectInfo{}, err
			}
			l.files = *resp
		}
		return filesource.ObjectInfo{}, io.EOF
	}

	switch entry := l.files.Entries[l.index].(type) {
	case *files.FileMetadata:
		l.index++
		obj := filesource.ObjectInfo{
			Path:    entry.PathDisplay,
			ModTime: entry.ClientModified,
			Size:    int64(entry.Size),
		}
		if entry.ExportInfo != nil {
			obj.ContentType = entry.ExportInfo.ExportAs
		}
		return obj, nil
	case *files.FolderMetadata:
		l.index++
		if !l.recursive {
			return filesource.ObjectInfo{
				Path:     entry.PathDisplay,
				IsPrefix: true,
			}, nil
		}
		return l.Next()
	case *files.DeletedMetadata:
		l.index++
		return l.Next()
	default:
		l.index++
		return filesource.ObjectInfo{}, fmt.Errorf("unexpected entry type")
	}
}
func configSchema(parserSchema json.RawMessage) json.RawMessage {

	return json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "Dropbox Source",
		"type": "object",
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
				"required": ["access_token"],
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
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
		Oauth2:           oauth2Spec(),
	}

	src.Main()
}
