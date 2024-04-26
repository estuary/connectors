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
	log "github.com/sirupsen/logrus"
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
	token, err := cfg.Credentials.GetAccessToken(ctx)
	if err != nil {
		return &dropboxStore{}, err
	}
	config := dropbox.Config{
		Token:    token,
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

// check checks the connection to the Azure Blob Storage container.
// It returns an error if the container is not found, the account is disabled, or if there is an authorization failure.
// If the listing is successful, it returns nil.
func (dbx *dropboxStore) check() error {
	_, err := dbx.client.ListFolder(&files.ListFolderArg{Path: dbx.config.Path})
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}
	return nil
}

func (dbx *dropboxStore) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	log.Debug("Listing files in path: ", dbx.config.Path)
	log.Debug("Query: ", query)
	files, err := dbx.client.ListFolder(&files.ListFolderArg{Path: query.Prefix})
	if err != nil {
		return nil, err
	}

	return &dropboxListing{
		ctx:    ctx,
		client: dbx.client,
		files:  *files,
		index:  0,
	}, nil
}

func (dbx *dropboxStore) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	log.Debug("Reading object: ", obj.Path)
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
	ctx    context.Context
	client files.Client
	files  files.ListFolderResult
	index  int
}

func (l *dropboxListing) Next() (filesource.ObjectInfo, error) {
	log.Debug("Listing next file")
	log.Debug(l.files.Entries)

	if len(l.files.Entries) == 0 {
		return filesource.ObjectInfo{}, io.EOF
	}
	if !l.files.HasMore && l.index > 0 {
		log.Debug("No more files to list")
		return filesource.ObjectInfo{}, io.EOF
	}
	entry, ok := l.files.Entries[l.index].(*files.FileMetadata)
	if !ok {
		return filesource.ObjectInfo{}, fmt.Errorf("unexpected entry type")
	}
	obj := filesource.ObjectInfo{}

	log.Debug("Entry: ", entry)

	obj.Path = entry.PathDisplay
	obj.ModTime = entry.ClientModified
	if entry.ExportInfo != nil {
		obj.ContentType = entry.ExportInfo.ExportAs
	}
	obj.Size = int64(entry.Size)

	log.Debug("Listing object: ", obj.Path)

	l.index++

	return obj, nil
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
				"type": "object",
				"x-oauth2-provider": "dropbox",
				"properties": {
					"access_token": {
						"type": "string",
						"secret": true
					},
					"refresh_token": {
						"type": "string",
						"secret": true
					},
					"client_id": {
						"type": "string",
						"secret": true
					},
					"client_secret": {
						"type": "string",
						"secret": true
					},
					"token_type": {
						"type": "string"
					},
					"expires_in": {
						"type": "integer"
					},
					"scope": {
						"type": "string"
					},
					"uid": {
						"type": "string"
					},
					"account_id": {
						"type": "string"
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
				"order": 2
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
				"order": 3
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
		Oauth2:           OAuth2Spec(),
	}

	src.Main()
}
