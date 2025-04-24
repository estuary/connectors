package blob

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/url"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"golang.org/x/sync/errgroup"
)

const defaultAzureEndpoint = "blob.core.windows.net"

type azureBlobConfig struct {
	storageAccountKey string
	sasToken          string
	endpoint          string
}

type AzureConfigOption func(*azureBlobConfig)

func WithAzureStorageAccountKey(sak string) AzureConfigOption {
	return func(cfg *azureBlobConfig) {
		cfg.storageAccountKey = sak
	}
}

func WithAzureSasToken(token string) AzureConfigOption {
	return func(cfg *azureBlobConfig) {
		cfg.sasToken = token
	}
}

func WithAzureEndpoint(ep string) AzureConfigOption {
	return func(cfg *azureBlobConfig) {
		cfg.endpoint = ep
	}
}

var _ Bucket = (*AzureBlobBucket)(nil)

type AzureBlobBucket struct {
	client    *azblob.Client
	container string
}

// NewAzureBlobBucket creates an Azure Blob object storage bucket. At least one
// AzureConfigOption must be provided to specify authentication; either storage
// account key or SAS token.
func NewAzureBlobBucket(ctx context.Context, container string, accountName string, authOpt AzureConfigOption, otherOpts ...AzureConfigOption) (*AzureBlobBucket, error) {
	cfg := azureBlobConfig{endpoint: defaultAzureEndpoint}
	for _, opt := range append(otherOpts, authOpt) {
		opt(&cfg)
	}
	if cfg.sasToken != "" && cfg.storageAccountKey != "" {
		return nil, fmt.Errorf("cannot specify both sas token and storage account key")
	}

	serviceUrl, err := url.Parse(fmt.Sprintf("https://%s.%s/", accountName, cfg.endpoint))
	if err != nil {
		return nil, fmt.Errorf("failed to parse service url: %w", err)
	}

	var client *azblob.Client
	if tok := cfg.sasToken; tok != "" {
		serviceUrl.RawQuery = strings.TrimPrefix(tok, "?")
		if client, err = azblob.NewClientWithNoCredential(serviceUrl.String(), nil); err != nil {
			return nil, fmt.Errorf("failed to create client with SAS token: %w", err)
		}
	} else if sak := cfg.storageAccountKey; sak != "" {
		if _, err := base64.StdEncoding.DecodeString(sak); err != nil {
			return nil, fmt.Errorf("invalid storage account key: must be base64-encoded")
		} else if cred, err := azblob.NewSharedKeyCredential(accountName, sak); err != nil {
			return nil, fmt.Errorf("failed to create storage client shared key credential: %w", err)
		} else if client, err = azblob.NewClientWithSharedKeyCredential(serviceUrl.String(), cred, nil); err != nil {
			return nil, fmt.Errorf("failed to create client with storage account key: %w", err)
		}
	} else {
		return nil, fmt.Errorf("must specify either SAS token or storage account key")
	}

	return &AzureBlobBucket{
		client:    client,
		container: container,
	}, nil
}

func (b *AzureBlobBucket) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := b.client.DownloadStream(ctx, b.container, key, nil)
	if err != nil {
		return nil, err
	}

	return r.Body, nil
}

func (b *AzureBlobBucket) NewWriter(ctx context.Context, key string, opts ...WriterOption) io.WriteCloser {
	return newBlobWriteCloser(ctx, b.Upload, key, opts...)
}

func (b *AzureBlobBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...WriterOption) error {
	cfg := getWriterConfig(opts)

	var uploadOpts *azblob.UploadStreamOptions
	if cfg.metadata != nil {
		uploadOpts = new(azblob.UploadStreamOptions)
		meta := make(map[string]*string)
		for k, v := range cfg.metadata {
			meta[k] = &v
		}
		uploadOpts.Metadata = meta
	}

	_, err := b.client.UploadStream(ctx, b.container, key, r, uploadOpts)

	return err
}

func (b *AzureBlobBucket) URI(key string) string {
	return strings.TrimSuffix(b.client.URL(), "/") + "/" + path.Join(b.container, key)
}

func (b *AzureBlobBucket) Delete(ctx context.Context, uris []string) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, uri := range uris {
		trimmed := strings.TrimPrefix(strings.TrimPrefix(uri, b.client.URL()), "/")
		container, blobName, ok := strings.Cut(trimmed, "/")
		if !ok {
			return fmt.Errorf("invalid uri %q", uri)
		}
		group.Go(func() error {
			if _, err := b.client.DeleteBlob(groupCtx, container, blobName, nil); err != nil {
				return fmt.Errorf("deleting blob %q: %w", uri, err)
			}
			return nil
		})
	}

	return group.Wait()
}

func (b *AzureBlobBucket) List(ctx context.Context, query Query) iter.Seq2[ObjectInfo, error] {
	input := &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Metadata: true},
	}
	if query.Prefix != "" {
		input.Prefix = &query.Prefix
	}
	pager := b.client.NewListBlobsFlatPager(b.container, input)

	return func(yield func(ObjectInfo, error) bool) {
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				yield(ObjectInfo{}, err)
				return
			}

			for _, obj := range page.Segment.BlobItems {
				if isFolder, ok := obj.Metadata["hdi_isfolder"]; ok && *isFolder == "true" {
					// Exclude directories.
					continue
				}

				if !yield(ObjectInfo{
					Key: *obj.Name,
				}, nil) {
					return
				}

			}
		}
	}
}

func (b *AzureBlobBucket) CheckPermissions(ctx context.Context, cfg CheckPermissionsConfig) error {
	checkReadOnlyFn := func(key string) error {
		_, err := b.client.DownloadBuffer(ctx, b.container, key, []byte{}, nil)
		if !bloberror.HasCode(err, bloberror.BlobNotFound) {
			return err
		}

		return nil
	}

	handleErr := func(err error) error {
		if bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return errors.New("no such container") // azure calls them containers, not buckets
		} else if bloberror.HasCode(err, bloberror.AuthorizationFailure) {
			return permissionsErrorUnauthorized
		}

		return err
	}

	return checkPermissions(ctx, b, cfg, checkReadOnlyFn, handleErr)
}
