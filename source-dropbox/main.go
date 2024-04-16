package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

type config struct {
	Credentials credentials    `json:"credentials"`
	Path        string         `json:"path"`
	Parser      *parser.Config `json:"parser"`
	MatchKeys   string         `json:"matchKeys,omitempty"`
	Advanced    advancedConfig `json:"advanced"`
}
type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys,omitempty"`
}

type credentials struct {
	apiToken string `json:"apiToken"`
}

func (c config) Validate() error {
	var requiredProperties [][]string
	requiredProperties = [][]string{
		{"AzureTenantID", c.Credentials.apiToken},
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
	return c.Path
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

func newDropboxStore(ctx context.Context, cfg config) (files.Client, error) {
	config := dropbox.Config{
		Token:    cfg.Credentials.apiToken,
		LogLevel: dropbox.LogInfo, // if needed, set the desired logging level. Default is off
	}
	client := files.New(config)

	return client, nil
}

type dropboxStore struct {
	client files.Client
	cfg    config
}

// check checks the connection to the Azure Blob Storage container.
// It returns an error if the container is not found, the account is disabled, or if there is an authorization failure.
// If the listing is successful, it returns nil.
func (db *dropboxStore) check(ctx context.Context) error {
	_, err := db.client.ListFolder(&files.ListFolderArg{})
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}
	return nil
}

func (db *dropboxStore) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	files, err := db.client.ListFolder(&files.ListFolderArg{})
	if err != nil {
		return nil, err
	}

	return &dropboxListing{
		ctx:    ctx,
		client: db.client,
		files:  *files,
	}, nil
}

// func (s *dropboxStore) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
// 	resp, err := s.client.DownloadStream(ctx, s.cfg.ContainerName, obj.Path, nil)
// 	if err != nil {
// 		return nil, filesource.ObjectInfo{}, err
// 	}
// 	retryReader := resp.NewRetryReader(ctx, &azblob.RetryReaderOptions{})

// 	obj.ModTime = *resp.LastModified
// 	obj.ContentType = *resp.ContentType
// 	obj.Size = *resp.ContentLength

// 	if resp.ContentEncoding != nil {
// 		obj.ContentEncoding = *resp.ContentEncoding
// 	}

// 	return retryReader, obj, nil
// }

type dropboxListing struct {
	ctx    context.Context
	client files.Client
	files  files.ListFolderResult
}

func (l *dropboxListing) Next() (filesource.ObjectInfo, error) {
	// page, err := l.getPage()

	// if err != nil {
	// 	return filesource.ObjectInfo{}, err
	// }

	// if page == nil {
	// 	log.Debug("No more files to list")
	// 	return filesource.ObjectInfo{}, io.EOF
	// }

	// blob := page.Segment.BlobItems[l.index]
	// l.index++

	obj := filesource.ObjectInfo{}

	// obj.Path = *blob.Name
	// obj.ModTime = *blob.Properties.LastModified
	// obj.ContentType = *blob.Properties.ContentType
	// obj.Size = *blob.Properties.ContentLength

	// if blob.Properties.ContentEncoding != nil {
	// 	obj.ContentEncoding = *blob.Properties.ContentEncoding
	// }
	log.Debug("Listing object: ", obj.Path)

	return obj, nil
}

// func (l *dropboxListing) getPage() (*azblob.ListBlobsFlatResponse, error) {
// 	if l.index < l.currentPageLength {
// 		log.Debug("Returning current page")
// 		return &l.page, nil
// 	}
// 	if !l.pager.More() {
// 		log.Debug("No more pages")
// 		return nil, nil
// 	}
// 	log.Debug("Fetching next page")
// 	page, err := l.pager.NextPage(l.ctx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	l.page = page
// 	l.currentPageLength = len(page.Segment.BlobItems)

// 	return &page, nil
// }

func getConfigSchema(parserSchema json.RawMessage) json.RawMessage {

	return json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "Azure Blob Storage Source",
		"type": "object",
		"properties": {
			"credentials": {
				"type": "object",
				"title": "Credentials",
				"description": "Azure credentials used to authenticate with Azure Blob Storage.",
				"order": 0,
				"anyOf": [
					{
						"title": "OAuth2 Credentials",
						"required": [
							"azureClientID",
							"azureClientSecret",
							"azureTenantID",
							"azureSubscriptionID",
							"storageAccountName"
						],
						"properties": {
							"azureClientID": {
								"type": "string",
								"title": "Azure Client ID",
								"description": "The client ID used to authenticate with Azure Blob Storage.",
								"order": 0
							},
							"azureClientSecret": {
								"type": "string",
								"title": "Azure Client Secret",
								"description": "The client secret used to authenticate with Azure Blob Storage.",
								"secret": true,
								"order": 1
							},
							"azureTenantID": {
								"type": "string",
								"title": "Azure Tenant ID",
								"description": "The ID of the Azure tenant where the Azure Blob Storage account is located.",
								"order": 2
							},
							"azureSubscriptionID": {
								"type": "string",
								"title": "Azure Subscription ID",
								"description": "The ID of the Azure subscription that contains the Azure Blob Storage account.",
								"order": 3
							},
							"storageAccountName": {
								"type": "string",
								"title": "Storage Account Name",
								"description": "The name of the Azure Blob Storage account.",
								"order": 1
							}
						}
					},
					{
						"title": "Connection String",
						"required": [
							"ConnectionString",
							"storageAccountName"
						],
						"properties": {
							"ConnectionString": {
								"type": "string",
								"title": "Connection String",
								"description": "The connection string used to authenticate with Azure Blob Storage.",
								"order": 0
							},
							"storageAccountName": {
								"type": "string",
								"title": "Storage Account Name",
								"description": "The name of the Azure Blob Storage account.",
								"order": 1
							}
						}
					}
				],
				"title": "Credentials",
				"description": "Azure credentials used to authenticate with Azure Blob Storage."
			},
			"containerName": {
				"type": "string",
				"title": "Container Name",
				"description": "The name of the Azure Blob Storage container to read from.",
				"order": 1
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
		ConfigSchema:     getConfigSchema,
		DocumentationURL: "https://go.estuary.dev/source-azure-blob-storage",
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
	}

	src.Main()
}
