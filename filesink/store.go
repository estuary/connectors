// This file contains helpers for building common Store implementations.
package filesink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	storage "cloud.google.com/go/storage"
	"github.com/estuary/connectors/go/blob"
	"google.golang.org/api/option"
)

const (
	// Timeout for file upload operations across all cloud providers.
	// This prevents uploads from hanging indefinitely due to network issues.
	uploadTimeout = 30 * time.Minute
)

type GCSStore struct {
	bucket string
	client *storage.Client
}

type GCSStoreConfig struct {
	Bucket          string `json:"bucket" jsonschema:"title=Bucket,description=Bucket to store materialized objects." jsonschema_extras:"order=0"`
	CredentialsJSON string `json:"credentialsJson" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization." jsonschema_extras:"secret=true,multiline=true,order=1"`

	UploadInterval string `json:"uploadInterval" jsonschema:"title=Upload Interval,description=Frequency at which files will be uploaded. Must be a valid Go duration string.,enum=5m,enum=15m,enum=30m,enum=1h,default=5m" jsonschema_extras:"order=2"`
	Prefix         string `json:"prefix,omitempty" jsonschema:"title=Prefix,description=Optional prefix that will be used to store objects." jsonschema_extras:"order=3"`
	FileSizeLimit  int    `json:"fileSizeLimit,omitempty" jsonschema:"title=File Size Limit,description=Approximate maximum size of materialized files in bytes. Defaults to 10737418240 (10 GiB) if blank." jsonschema_extras:"order=4"`
}

func (c GCSStoreConfig) Validate() error {
	var requiredProperties = [][]string{
		{"bucket", c.Bucket},
		{"credentialsJson", c.CredentialsJSON},
		{"uploadInterval", c.UploadInterval},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if !json.Valid([]byte(c.CredentialsJSON)) {
		return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
	} else if _, err := time.ParseDuration(c.UploadInterval); err != nil {
		return fmt.Errorf("parsing upload interval %q: %w", c.UploadInterval, err)
	} else if c.Prefix != "" {
		if strings.HasPrefix(c.Prefix, "/") {
			return fmt.Errorf("prefix %q cannot start with /", c.Prefix)
		}
	} else if c.FileSizeLimit < 0 {
		return fmt.Errorf("fileSizeLimit '%d' cannot be negative", c.FileSizeLimit)
	}

	return nil
}

func NewGCSStore(ctx context.Context, cfg GCSStoreConfig) (*GCSStore, error) {
	opts := []option.ClientOption{
		option.WithCredentialsJSON([]byte(cfg.CredentialsJSON)),
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &GCSStore{
		bucket: cfg.Bucket,
		client: client,
	}, nil
}

func (s *GCSStore) PutStream(ctx context.Context, r io.Reader, key string) error {
	uploadCtx, cancel := context.WithTimeout(ctx, uploadTimeout)
	defer cancel()

	w := s.client.Bucket(s.bucket).Object(key).NewWriter(uploadCtx)

	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("copying r to w: %w", err)
	} else if err := w.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}

	return nil
}

type AzureBlob struct {
	bucket *blob.AzureBlobBucket
}

type AzureBlobConfig struct {
	StorageAccountName string `json:"storageAccountName" jsonschema:"title=Storage Account Name,description=Name of the storage account that files will be written to." jsonschema_extras:"order=0"`
	StorageAccountKey  string `json:"storageAccountKey" jsonschema:"title=Storage Account Key,description=Storage account key for the storage account that files will be written to." jsonschema_extras:"order=1,secret=true"`
	ContainerName      string `json:"containerName" jsonschema:"title=Storage Account Container Name,description=Name of the container in the storage account where files will be written." jsonschema_extras:"order=2"`

	UploadInterval string `json:"uploadInterval" jsonschema:"title=Upload Interval,description=Frequency at which files will be uploaded. Must be a valid Go duration string.,enum=5m,enum=15m,enum=30m,enum=1h,default=5m" jsonschema_extras:"order=3"`
	Prefix         string `json:"prefix,omitempty" jsonschema:"title=Prefix,description=Optional prefix that will be used to store objects." jsonschema_extras:"order=4"`
	FileSizeLimit  int    `json:"fileSizeLimit,omitempty" jsonschema:"title=File Size Limit,description=Approximate maximum size of materialized files in bytes. Defaults to 10737418240 (10 GiB) if blank." jsonschema_extras:"order=5"`
}

func (c AzureBlobConfig) Validate() error {
	var requiredProperties = [][]string{
		{"containerName", c.ContainerName},
		{"storageAccountName", c.StorageAccountName},
		{"storageAccountKey", c.StorageAccountKey},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if _, err := time.ParseDuration(c.UploadInterval); err != nil {
		return fmt.Errorf("parsing upload interval %q: %w", c.UploadInterval, err)
	} else if c.Prefix != "" {
		if strings.HasPrefix(c.Prefix, "/") {
			return fmt.Errorf("prefix %q cannot start with /", c.Prefix)
		}
	} else if c.FileSizeLimit < 0 {
		return fmt.Errorf("fileSizeLimit '%d' cannot be negative", c.FileSizeLimit)
	}

	return nil
}

func NewAzureBlob(ctx context.Context, cfg AzureBlobConfig) (*AzureBlob, error) {
	bucket, err := blob.NewAzureBlobBucket(ctx, cfg.ContainerName, cfg.StorageAccountName, blob.WithAzureStorageAccountKey(cfg.StorageAccountKey))
	if err != nil {
		return nil, err
	}

	return &AzureBlob{
		bucket: bucket,
	}, nil
}

func (s *AzureBlob) PutStream(ctx context.Context, r io.Reader, key string) error {
	uploadCtx, cancel := context.WithTimeout(ctx, uploadTimeout)
	defer cancel()

	return s.bucket.Upload(uploadCtx, key, r)
}
