package obj

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var _ Store = (*S3Store)(nil)

type S3Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func NewS3Store(
	ctx context.Context,
	credentialsProvider aws.CredentialsProvider,
	bucket string,
	region string,
	endpoint *string,
) (*S3Store, error) {
	configOpts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(credentialsProvider),
		awsConfig.WithRegion(region),
	}

	var clientOpts []func(*s3.Options)
	if endpoint != nil {
		clientOpts = append(clientOpts, func(opts *s3.Options) { opts.BaseEndpoint = endpoint })
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, clientOpts...)

	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.Concurrency = 1
		u.PartSize = manager.MinUploadPartSize
	})

	return &S3Store{
		client:   s3Client,
		uploader: uploader,
		bucket:   bucket,
	}, nil
}

func (s *S3Store) PutStream(ctx context.Context, key string, r io.Reader, opts ...PutStreamOption) error {
	cfg := getPutStreamConfig(opts)
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		Body:     r,
		Metadata: cfg.metadata,
	})

	return err
}
