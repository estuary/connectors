package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

type config struct {
	Credentials   credentials    `json:"credentials"`
	Parser        *parser.Config `json:"parser"`
	MatchKeys     string         `json:"matchKeys,omitempty"`
	Advanced      advancedConfig `json:"advanced"`
	ContainerName string         `json:"containerName"`
}
type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys,omitempty"`
}

type credentials struct {
	StorageAccountName  string `json:"storageAccountName"`
	AzureClientID       string `json:"azureClientID"`
	AzureClientSecret   string `json:"azureClientSecret"`
	AzureTenantID       string `json:"azureTenantID"`
	AzureSubscriptionID string `json:"azureSubscriptionID"`
	ConnectionString    string `json:"connectionString"`
}

func (c config) Validate() error {
	var requiredProperties [][]string
	if c.Credentials.ConnectionString == "" {
		requiredProperties = [][]string{
			{"AzureTenantID", c.Credentials.AzureTenantID},
			{"AzureClientID", c.Credentials.AzureClientID},
			{"AzureClientSecret", c.Credentials.AzureClientSecret},
			{"AzureSubscriptionID", c.Credentials.AzureSubscriptionID},
			{"StorageAccountName", c.Credentials.StorageAccountName},
			{"StorageAccountName", c.Credentials.StorageAccountName},
		}
	} else {
		requiredProperties = [][]string{
			{"ConnectionString", c.Credentials.ConnectionString},
			{"StorageAccountName", c.Credentials.StorageAccountName},
		}
	}

	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

func (c config) DiscoverRoot() string {
	return c.ContainerName
}

func (c config) RecommendedName() string {
	return c.ContainerName
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

func newAzureBlobStore(ctx context.Context, cfg config) (*azureBlobStore, error) {
	blobUrl := fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.Credentials.StorageAccountName)
	var client *azblob.Client
	var err error

	if cfg.Credentials.ConnectionString != "" {
		client, err = azblob.NewClientFromConnectionString(cfg.Credentials.ConnectionString, nil)
		if err != nil {
			return nil, err
		}
	} else {
		credential, err := azidentity.NewClientSecretCredential(cfg.Credentials.AzureTenantID, cfg.Credentials.AzureClientID, cfg.Credentials.AzureClientSecret, nil)
		if err != nil {
			return nil, err
		}

		client, err = azblob.NewClient(blobUrl, credential, nil)
		if err != nil {
			return nil, err
		}
	}

	store := &azureBlobStore{client: *client, cfg: cfg}

	if err = store.check(ctx); err != nil {
		return nil, err
	}

	return store, err
}

type azureBlobStore struct {
	client azblob.Client
	cfg    config
}

// check checks the connection to the Azure Blob Storage container.
// It returns an error if the container is not found, the account is disabled, or if there is an authorization failure.
// If the listing is successful, it returns nil.
func (az *azureBlobStore) check(ctx context.Context) error {
	// All we care about is a successful listing rather than iterating on all objects

	maxResults := int32(1)
	listingOptions := azblob.ListBlobsFlatOptions{MaxResults: &maxResults}

	pager := az.client.NewListBlobsFlatPager(az.cfg.ContainerName, &listingOptions)
	if !pager.More() {
		return nil
	}
	_, err := pager.NextPage(ctx)
	if bloberror.HasCode(err, bloberror.AccountIsDisabled) {
		return fmt.Errorf("account is disabled: %w", err)
	} else if bloberror.HasCode(err, bloberror.AuthorizationFailure) {
		return fmt.Errorf("authorization failure: %w", err)
	} else if bloberror.HasCode(err, bloberror.ContainerNotFound) {
		return fmt.Errorf("container '%q' not found: %w", az.cfg.ContainerName, err)
	} else if bloberror.HasCode(err, bloberror.AuthorizationFailure) {
		return fmt.Errorf("authentication failure: %w", err)
	} else if bloberror.HasCode(err, bloberror.InvalidResourceName) {
		return fmt.Errorf("invalid resource name: %w", err)
	} else if bloberror.HasCode(err, bloberror.AuthorizationPermissionMismatch) {
		return fmt.Errorf("authorization permission mismatch: %w", err)
	} else if bloberror.HasCode(err, bloberror.AuthorizationSourceIPMismatch) {
		return fmt.Errorf("authorization source IP mismatch: %w", err)
	} else if err != nil {
		return fmt.Errorf("unable to list objects in container %q: %w", az.cfg.ContainerName, err)
	}

	// If we can list a object, we can read it too
	return nil
}

func (az *azureBlobStore) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	pager := az.client.NewListBlobsFlatPager(az.cfg.ContainerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	page, err := pager.NextPage(ctx)
	if err != nil {
		return nil, err
	}

	return &azureBlobListing{
		ctx:               ctx,
		client:            az.client,
		pager:             *pager,
		index:             0,
		currentPageLength: len(page.Segment.BlobItems),
		page:              page,
	}, nil
}

func (s *azureBlobStore) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	resp, err := s.client.DownloadStream(ctx, s.cfg.ContainerName, obj.Path, nil)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}
	retryReader := resp.NewRetryReader(ctx, &azblob.RetryReaderOptions{})

	obj.ModTime = *resp.LastModified
	obj.ContentType = *resp.ContentType
	obj.Size = *resp.ContentLength

	if resp.ContentEncoding != nil {
		obj.ContentEncoding = *resp.ContentEncoding
	}

	return retryReader, obj, nil
}

type azureBlobListing struct {
	ctx               context.Context
	client            azblob.Client
	pager             runtime.Pager[azblob.ListBlobsFlatResponse]
	index             int
	currentPageLength int
	page              azblob.ListBlobsFlatResponse
}

func (l *azureBlobListing) Next() (filesource.ObjectInfo, error) {
	page, err := l.getPage()

	if err != nil {
		return filesource.ObjectInfo{}, err
	}

	if page == nil {
		log.Debug("No more files to list")
		return filesource.ObjectInfo{}, io.EOF
	}

	blob := page.Segment.BlobItems[l.index]
	l.index++

	obj := filesource.ObjectInfo{}

	obj.Path = *blob.Name
	obj.ModTime = *blob.Properties.LastModified
	obj.ContentType = *blob.Properties.ContentType
	obj.Size = *blob.Properties.ContentLength

	if blob.Properties.ContentEncoding != nil {
		obj.ContentEncoding = *blob.Properties.ContentEncoding
	}
	log.Debug("Listing object: ", obj.Path)

	return obj, nil
}

func (l *azureBlobListing) getPage() (*azblob.ListBlobsFlatResponse, error) {
	if l.index < l.currentPageLength {
		log.Debug("Returning current page")
		return &l.page, nil
	}
	if !l.pager.More() {
		log.Debug("No more pages")
		return nil, nil
	}
	log.Debug("Fetching next page")
	page, err := l.pager.NextPage(l.ctx)
	if err != nil {
		return nil, err
	}

	l.page = page
	l.currentPageLength = len(page.Segment.BlobItems)

	return &page, nil
}

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
			return newAzureBlobStore(ctx, cfg.(config))
		},
		ConfigSchema:     getConfigSchema,
		DocumentationURL: "https://go.estuary.dev/source-azure-blob-storage",
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
	}

	src.Main()
}
