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
)

type config struct {
	Credentials        credentials    `json:"credentials"`
	StorageAccountName string         `json:"storageAccountName"`
	Parser             *parser.Config `json:"parser"`
	MatchKeys          string         `json:"matchKeys,omitempty"`
	Advanced           advancedConfig `json:"advanced"`
}
type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys,omitempty"`
}

type credentials struct {
	ContainerName       string `json:"containerName"`
	AzureClientID       string `json:"azureClientID"`
	AzureClientSecret   string `json:"azureClientSecret"`
	AzureTenantID       string `json:"azureTenantID"`
	AzureSubscriptionID string `json:"azureSubscriptionID"`
	ConnectionString    string `json:"connectionString"`
}

func (c config) Validate() error {
	if c.Credentials.ConnectionString != "" {
		return nil
	}
	var requiredProperties [][]string
	if c.Credentials.ConnectionString == "" {
		requiredProperties = [][]string{
			{"AzureTenantID", c.Credentials.AzureTenantID},
			{"AzureClientID", c.Credentials.AzureClientID},
			{"AzureClientSecret", c.Credentials.AzureClientSecret},
			{"AzureSubscriptionID", c.Credentials.AzureSubscriptionID},
			{"StorageAccountName", c.StorageAccountName},
			{"ContainerName", c.Credentials.ContainerName},
		}
	} else {
		requiredProperties = [][]string{
			{"ConnectionString", c.Credentials.ConnectionString},
			{"ContainerName", c.Credentials.ContainerName},
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
	return c.Credentials.ContainerName
}

func (c config) RecommendedName() string {
	return c.Credentials.ContainerName
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
	blobUrl := fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.StorageAccountName)
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

	pager := az.client.NewListBlobsFlatPager(az.cfg.Credentials.ContainerName, &listingOptions)
	if !pager.More() {
		return nil
	}
	_, err := pager.NextPage(ctx)
	if bloberror.HasCode(err, bloberror.AccountIsDisabled) {
		return fmt.Errorf("account is disabled: %w", err)
	} else if bloberror.HasCode(err, bloberror.AuthorizationFailure) {
		return fmt.Errorf("authorization failure: %w", err)
	} else if bloberror.HasCode(err, bloberror.ContainerNotFound) {
		return fmt.Errorf("container '%q' not found: %w", az.cfg.Credentials.ContainerName, err)
	} else if bloberror.HasCode(err, bloberror.AuthorizationFailure) {
		return fmt.Errorf("authentication failure: %w", err)
	} else if bloberror.HasCode(err, bloberror.InvalidResourceName) {
		return fmt.Errorf("invalid resource name: %w", err)
	} else if bloberror.HasCode(err, bloberror.AuthorizationPermissionMismatch) {
		return fmt.Errorf("authorization permission mismatch: %w", err)
	} else if bloberror.HasCode(err, bloberror.AuthorizationSourceIPMismatch) {
		return fmt.Errorf("authorization source IP mismatch: %w", err)
	} else if err != nil {
		return fmt.Errorf("unable to list objects in container %q: %w", az.cfg.Credentials.ContainerName, err)
	}

	// If we can list a object, we can read it too
	return nil
}

func (az *azureBlobStore) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	pager := az.client.NewListBlobsFlatPager(az.cfg.Credentials.ContainerName, &azblob.ListBlobsFlatOptions{
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
	resp, err := s.client.DownloadStream(ctx, s.cfg.Credentials.ContainerName, obj.Path, nil)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}
	retryReader := resp.NewRetryReader(ctx, &azblob.RetryReaderOptions{})

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
		return filesource.ObjectInfo{}, io.EOF
	}

	blob := page.Segment.BlobItems[l.index]
	l.index++

	return filesource.ObjectInfo{Path: *blob.Name, Size: *blob.Properties.ContentLength}, nil
}

func (l *azureBlobListing) getPage() (*azblob.ListBlobsFlatResponse, error) {
	if l.index < l.currentPageLength {
		return &l.page, nil
	}
	if !l.pager.More() {
		return nil, nil
	}
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
		"title": "S3 Source",
		"type": "object",
		"required": [
			"AzureClientID",
			"AzureClientSecret",
			"AzureTenantID",
			"AzureSubscriptionID",
			"StorageAccountName"
		],
		"properties": {
			"credentials": {
				"type": "object",
				"anyOf": [
					{
						"required": [
							"ConnectionString"
						],
						"properties": {
							"ConnectionString": {
								"type": "string",
								"title": "Connection String",
								"description": "The connection string used to authenticate with Azure Blob Storage.",
								"order": 0
							}
						}
					},
					{
						"required": [
							"azureClientID",
							"azureClientSecret",
							"azureTenantID",
							"azureSubscriptionID"
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
							}
						}
					}
				],
				"title": "Credentials",
				"description": "Azure credentials used to authenticate with Azure Blob Storage."
			},
			"storageAccountName": {
				"type": "string",
				"title": "Storage Account Name",
				"description": "The name of the Azure Blob Storage account.",
				"order": 4
			},
			"matchKeys": {
				"type": "string",
				"title": "Match Keys",
				"format": "regex",
				"description": "Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files.",
				"order": 5
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
