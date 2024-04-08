package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
)

//go:embed schema.json
var jsonSchema string

type config struct {
	AzureClientID       string         `json:"azureClientID"`
	AzureClientSecret   string         `json:"azureClientSecret"`
	AzureTenantID       string         `json:"azureTenantID"`
	AzureSubscriptionID string         `json:"azureSubscriptionID"`
	StorageAccountName  string         `json:"storageAccountName"`
	ContainerName       string         `json:"containerName"`
	Parser              *parser.Config `json:"parser"`
	MatchKeys           string         `json:"matchKeys"`
}

func (c config) Validate() error {
	if c.AzureTenantID == "" {
		return fmt.Errorf("missing AzureTenantID")
	}
	if c.AzureClientID == "" {
		return fmt.Errorf("missing AzureClientID")
	}
	if c.AzureClientSecret == "" {
		return fmt.Errorf("missing AzureClientSecret")
	}
	if c.AzureSubscriptionID == "" {
		return fmt.Errorf("missing AzureSubscriptionID")
	}
	if c.StorageAccountName == "" {
		return fmt.Errorf("missing StorageAccountName")
	}
	if c.ContainerName == "" {
		return fmt.Errorf("missing ContainerName")
	}
	return nil
}

func (c config) DiscoverRoot() string {
	return c.ContainerName
}

func (c config) RecommendedName() string {
	return c.ContainerName
}

// Azure Blob Storage does not inherently enforce a monotonic order on the files
// it stores. The order of files in Azure Blob Storage is not determined by the
// system, but rather by how the user or application uploads, names, and organizes
// the files. See https://learn.microsoft.com/en-us/azure/storage/blobs/storage-performance-checklist.
func (c config) FilesAreMonotonic() bool {
	return false
}

func (c config) ParserConfig() *parser.Config {
	return c.Parser
}

func (c config) PathRegex() string {
	return c.MatchKeys
}

// TODO: Test this
func newAzureBlobStore(cfg config) (*azureBlobStore, error) {
	os.Setenv("AZURE_CLIENT_ID", cfg.AzureClientID)
	os.Setenv("AZURE_CLIENT_SECRET", cfg.AzureClientSecret)
	os.Setenv("AZURE_TENANT_ID", cfg.AzureTenantID)
	os.Setenv("AZURE_SUBSCRIPTION_ID", cfg.AzureSubscriptionID)
	blobUrl := fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.StorageAccountName)

	// For this to work, we need to have the azure client installed
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := azblob.NewClient(blobUrl, credential, nil)
	if err != nil {
		return nil, err
	}

	store := &azureBlobStore{client: client, cfg: &cfg}

	err = store.check(context.TODO())
	if err != nil {
		return nil, err
	}

	return store, err
}

type azureBlobStore struct {
	client *azblob.Client
	cfg    *config
}

// check verifies that we can list objects in the bucket and potentially read an object in
// the bucket. This is done in a way that requires only s3:ListBucket and s3:GetObject permissions,
// since these are the permissions required by the connector.
func (az *azureBlobStore) check(ctx context.Context) error {
	// All we care about is a successful listing rather than iterating on all objects

	maxResults := int32(1)
	listingOptions := azblob.ListBlobsFlatOptions{MaxResults: &maxResults}

	var blobName string
	pager := az.client.NewListBlobsFlatPager(az.cfg.ContainerName, &listingOptions)
	if !pager.More() {
		log.Printf("The container '%q' is empty", az.cfg.ContainerName)
		return nil
	}
	page, err := pager.NextPage(ctx)
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

	if len(page.Segment.BlobItems) == 0 {
		return nil
	}
	blobName = *page.Segment.BlobItems[0].Name

	// Read a blob from the container
	resp, err := az.client.DownloadStream(ctx, az.cfg.ContainerName, blobName, nil)
	if err != nil {
		return fmt.Errorf("unable to download blob %q: %w", blobName, err)
	}
	downloadedData := bytes.Buffer{}
	retryReader := resp.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)

	if err != nil {
		return fmt.Errorf("unable to read blob content: %w", err)
	}

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
		pager:             pager,
		index:             0,
		currentPageLength: len(page.Segment.BlobItems),
		page:              &page,
	}, nil
}

func (s *azureBlobStore) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	resp, err := s.client.DownloadStream(ctx, s.cfg.ContainerName, obj.Path, nil)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}
	retryReader := resp.NewRetryReader(ctx, &azblob.RetryReaderOptions{})

	return retryReader, obj, nil
}

type azureBlobListing struct {
	ctx               context.Context
	client            *azblob.Client
	pager             *runtime.Pager[azblob.ListBlobsFlatResponse]
	index             int
	currentPageLength int
	page              *azblob.ListBlobsFlatResponse
}

func (l *azureBlobListing) Next() (filesource.ObjectInfo, error) {
	page, err := l.getPage()

	if err != nil {
		return filesource.ObjectInfo{}, err
	}

	if page == nil {
		return filesource.ObjectInfo{}, io.EOF
	}

	for _, blob := range page.Segment.BlobItems {
		fmt.Println(*blob.Name)
	}
	blob := page.Segment.BlobItems[l.index]
	l.index++

	return filesource.ObjectInfo{Path: *blob.Name, Size: *blob.Properties.ContentLength}, nil
}

func (l *azureBlobListing) getPage() (*azblob.ListBlobsFlatResponse, error) {
	if l.index < l.currentPageLength {
		return l.page, nil
	}
	if !l.pager.More() {
		return nil, nil
	}
	page, err := l.pager.NextPage(l.ctx)
	if err != nil {
		return nil, err
	}

	l.page = &page
	l.currentPageLength = len(page.Segment.BlobItems)

	return &page, nil
}

type property struct {
	JsonType    string `json:"type"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Order       int    `json:"order"`
	Secret      bool   `json:"secret"`
}

type schema struct {
	SchemaType string   `json:"$schema"`
	Title      string   `json:"title"`
	JsonType   string   `json:"type"`
	Required   []string `json:"required"`
	Properties map[string]property
}

func getConfigSchema(parserJsonSchema json.RawMessage) json.RawMessage {
	schema := schema{}
	err := json.Unmarshal([]byte(jsonSchema), &schema)
	if err != nil {
		log.Fatalf("Error unmarshalling schema: %v", err)
		return nil
	}
	parserSchema := property{}

	err = json.Unmarshal(parserJsonSchema, &parserSchema)
	if err != nil {
		log.Fatalf("Error unmarshalling schema: %v", err)
		return nil
	}
	schema.Properties["parser"] = parserSchema

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		log.Fatalf("Error marshalling schema: %v", err)
		return nil
	}

	return json.RawMessage(schemaJSON)
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
			return newAzureBlobStore(cfg.(config))
		},
		ConfigSchema:     getConfigSchema,
		DocumentationURL: "https://go.estuary.dev/source-azure-blob-storage",
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
	}

	src.Main()
}
