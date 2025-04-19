package obj

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const defaultAzureEndpoint = "blob.core.windows.net"

type azureBlobStoreConfig struct {
	storageAccountKey string
	sasToken          string
	endpoint          string
}

type AzureConfigOption func(*azureBlobStoreConfig)

func WithAzureStorageAccountKey(sak string) AzureConfigOption {
	return func(cfg *azureBlobStoreConfig) {
		cfg.storageAccountKey = sak
	}
}

func WithAzureSasToken(token string) AzureConfigOption {
	return func(cfg *azureBlobStoreConfig) {
		cfg.sasToken = token
	}
}

func WithAzureEndpoint(ep string) AzureConfigOption {
	return func(cfg *azureBlobStoreConfig) {
		cfg.endpoint = ep
	}
}

var _ Store = (*AzureBlobStore)(nil)

type AzureBlobStore struct {
	client    *azblob.Client
	container string
}

// NewAzureBlobStore creates an Azure Blob object store. At least one
// AzureConfigOption must be provided to specify authentication; either storage
// account key or SAS token.
func NewAzureBlobStore(ctx context.Context, container string, accountName string, opts []AzureConfigOption) (*AzureBlobStore, error) {
	cfg := azureBlobStoreConfig{endpoint: defaultAzureEndpoint}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.sasToken != "" && cfg.storageAccountKey != "" {
		return nil, fmt.Errorf("cannot specify both sas token and storage account key")
	}

	serviceUrl, err := url.Parse(fmt.Sprintf("https://%s.%s/", accountName, cfg.endpoint))
	if err != nil {
		log.Fatal(err)
	}

	var client *azblob.Client
	if cfg.sasToken != "" {
		serviceUrl.RawQuery = strings.TrimPrefix(cfg.sasToken, "?")
		if client, err = azblob.NewClientWithNoCredential(serviceUrl.String(), nil); err != nil {
			return nil, fmt.Errorf("failed to create client with SAS token: %w", err)
		}
	} else if cfg.storageAccountKey != "" {
		if cred, err := azblob.NewSharedKeyCredential(accountName, cfg.storageAccountKey); err != nil {
			return nil, fmt.Errorf("failed to create storage client shared key credential: %w", err)
		} else if client, err = azblob.NewClientWithSharedKeyCredential(serviceUrl.String(), cred, nil); err != nil {
			return nil, fmt.Errorf("failed to create client with storage account key: %w", err)
		}
	} else {
		return nil, fmt.Errorf("must specify either SAS token or storage account key")
	}

	return &AzureBlobStore{
		client:    client,
		container: container,
	}, nil
}

func (s *AzureBlobStore) PutStream(ctx context.Context, key string, r io.Reader, opts ...PutStreamOption) error {
	cfg := getPutStreamConfig(opts)

	var uploadOpts *azblob.UploadStreamOptions
	if cfg.metadata != nil {
		uploadOpts = new(azblob.UploadStreamOptions)
		meta := make(map[string]*string)
		for k, v := range cfg.metadata {
			meta[k] = &v
		}
		uploadOpts.Metadata = meta
	}

	_, err := s.client.UploadStream(ctx, s.container, key, r, uploadOpts)

	return err
}

func (s *AzureBlobStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.client.DownloadStream(ctx, s.container, key, nil)
	if err != nil {
		return nil, err
	}

	return r.Body, nil
}
