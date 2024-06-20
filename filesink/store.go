package filesink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	storage "cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	m "github.com/estuary/connectors/go/protocols/materialize"
	"google.golang.org/api/option"
)

// This file contains helpers for building common Store implementations.

type S3Store struct {
	uploader *manager.Uploader
	bucket   string
}

type S3StoreConfig struct {
	Bucket             string `json:"bucket" jsonschema:"title=Bucket,description=Bucket to store materialized objects." jsonschema_extras:"order=0"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=AWS Access Key ID,description=Access Key ID for writing data to the bucket." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=AWS Secret Access key,description=Secret Access Key for writing data to the bucket." jsonschema_extras:"order=2"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region of the bucket to write to." jsonschema_extras:"order=3"`

	UploadInterval string `json:"uploadInterval" jsonschema:"title=Upload Interval,description=Frequency at which files will be uploaded. Must be a valid Go duration string.,enum=5m,enum=15m,enum=30m,enum=1h,default=5m" jsonschema_extras:"order=4"`
	Prefix         string `json:"prefix,omitempty" jsonschema:"title=Prefix,description=Optional prefix that will be used to store objects." jsonschema_extras:"order=5"`
	FileSizeLimit  int    `json:"fileSizeLimit,omitempty" jsonschema:"title=File Size Limit,description=Approximate maximum size of materialized files in bytes. Defaults to 10737418240 (10 GiB) if blank." jsonschema_extras:"order=6"`

	Endpoint string `json:"endpoint,omitempty" jsonschema:"title=Custom S3 Endpoint,description=The S3 endpoint URI to connect to. Use if you're materializing to a compatible API that isn't provided by AWS. Should normally be left blank." jsonschema_extras:"order=7"`
}

func (c S3StoreConfig) Validate() error {
	var requiredProperties = [][]string{
		{"bucket", c.Bucket},
		{"awsAccessKeyId", c.AWSAccessKeyID},
		{"awsSecretAccessKey", c.AWSSecretAccessKey},
		{"region", c.Region},
		{"uploadInterval", c.UploadInterval},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if _, err := m.ParseDelay(c.UploadInterval); err != nil {
		return err
	} else if c.Prefix != "" {
		if strings.HasPrefix(c.Prefix, "/") {
			return fmt.Errorf("prefix %q cannot start with /", c.Prefix)
		}
	} else if c.FileSizeLimit < 0 {
		return fmt.Errorf("fileSizeLimit '%d' cannot be negative", c.FileSizeLimit)
	}

	return nil
}

func NewS3Store(ctx context.Context, cfg S3StoreConfig) (*S3Store, error) {
	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(cfg.Region),
	}

	if cfg.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: cfg.Endpoint}, nil
		})

		opts = append(opts, awsConfig.WithEndpointResolverWithOptions(customResolver))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	uploader := manager.NewUploader(s3.NewFromConfig(awsCfg), func(u *manager.Uploader) {
		u.Concurrency = 1
		u.PartSize = manager.MinUploadPartSize
	})

	return &S3Store{
		uploader: uploader,
		bucket:   cfg.Bucket,
	}, nil
}

func (s *S3Store) PutStream(ctx context.Context, r io.Reader, key string) error {
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	return err
}

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
	} else if _, err := m.ParseDelay(c.UploadInterval); err != nil {
		return err
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
	w := s.client.Bucket(s.bucket).Object(key).NewWriter(ctx)

	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("copying r to w: %w", err)
	} else if err := w.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}

	return nil
}
