package obj

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var _ Store = (*S3Store)(nil)

type S3Store struct {
	client   *s3.Client
	bucket   string
	uploader *manager.Uploader
}

// NewS3Store creates an S3 object storage. clientOpts are optional, but may be
// useful for things like an alternate endpoint. The region of the bucket is
// determined automatically, if possible. If using an S3 endpoint other than
// s3.amazonaws.com, set the region in one of the clientOpts if it is required
// by the alternate endpoint.
func NewS3Store(ctx context.Context, bucket string, creds aws.CredentialsProvider, clientOpts ...func(*s3.Options)) (*S3Store, error) {
	configOpts := []func(*config.LoadOptions) error{
		config.WithCredentialsProvider(creds),
	}

	if region, err := getBucketRegion(bucket); err == nil {
		configOpts = append(configOpts, config.WithRegion(region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, clientOpts...)

	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.Concurrency = 1
		u.PartSize = manager.MinUploadPartSize
	})

	return &S3Store{
		client:   s3Client,
		bucket:   bucket,
		uploader: uploader,
	}, nil
}

func (s *S3Store) PutStream(ctx context.Context, key string, r io.Reader, opts ...PutStreamOption) error {
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		Body:     r,
		Metadata: getPutStreamConfig(opts).metadata,
	})

	return err
}

func (s *S3Store) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return r.Body, nil
}

func getBucketRegion(bucket string) (string, error) {
	// It's apparently impossible to get the region of a bucket if you don't
	// already know it using the AWS Go SDKs, but this silly mechanism actually
	// seems to work 100% of the time.
	resp, err := http.DefaultClient.Head(fmt.Sprintf("https://%s.s3.amazonaws.com", bucket))
	if resp != nil && resp.Header != nil {
		if bucketRegion := resp.Header.Get("x-amz-bucket-region"); bucketRegion != "" {
			return bucketRegion, nil
		}
	}
	if err != nil {
		return "", err
	}

	return "", fmt.Errorf("could not find region for bucket %s", bucket)
}
