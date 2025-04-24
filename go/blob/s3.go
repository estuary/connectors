package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"path"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	awsHttp "github.com/aws/smithy-go/transport/http"
)

var _ Bucket = (*S3Bucket)(nil)

type S3Bucket struct {
	client   *s3.Client
	bucket   string
	uploader *manager.Uploader
}

// NewS3Bucket creates an S3 object storage bucket. clientOpts are optional, but
// may be useful for things like an alternate endpoint. The region of the bucket
// is determined automatically, if possible. If using an S3 endpoint other than
// s3.amazonaws.com, set the region in one of the clientOpts if it is required
// by the alternate endpoint.
func NewS3Bucket(ctx context.Context, bucket string, creds aws.CredentialsProvider, clientOpts ...func(*s3.Options)) (*S3Bucket, error) {
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

	return &S3Bucket{
		client:   s3Client,
		bucket:   bucket,
		uploader: manager.NewUploader(s3Client),
	}, nil
}

func (b *S3Bucket) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return r.Body, nil
}

func (b *S3Bucket) NewWriter(ctx context.Context, key string, opts ...WriterOption) io.WriteCloser {
	return newBlobWriteCloser(ctx, b.Upload, key, opts...)
}

func (b *S3Bucket) Upload(ctx context.Context, key string, r io.Reader, opts ...WriterOption) error {
	_, err := b.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(b.bucket),
		Key:      aws.String(key),
		Body:     r,
		Metadata: getWriterConfig(opts).metadata,
	})

	return err
}

func (b *S3Bucket) URI(key string) string {
	return "s3://" + path.Join(b.bucket, key)
}

func (b *S3Bucket) Delete(ctx context.Context, uris []string) error {
	for batch := range slices.Chunk(uris, 1000) {
		toDelete := make([]types.ObjectIdentifier, 0, len(batch))
		for _, uri := range batch {
			trimmed := strings.TrimPrefix(uri, "s3://")
			bucket, key, ok := strings.Cut(trimmed, "/")
			if !ok {
				return fmt.Errorf("invalid uri %q", uri)
			} else if bucket != b.bucket {
				return fmt.Errorf("bucket mismatch from %q: %q (got) vs %q (want)", uri, bucket, b.bucket)
			}
			toDelete = append(toDelete, types.ObjectIdentifier{Key: aws.String(key)})
		}

		if _, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(b.bucket),
			Delete: &types.Delete{
				Objects: toDelete,
			},
		}); err != nil {
			return fmt.Errorf("deleting blob batch: %w", err)
		}
	}

	return nil
}

func (b *S3Bucket) List(ctx context.Context, query Query) iter.Seq2[ObjectInfo, error] {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
	}
	if query.Prefix != "" {
		input.Prefix = aws.String(query.Prefix)
	}
	paginator := s3.NewListObjectsV2Paginator(b.client, input)

	return func(yield func(ObjectInfo, error) bool) {
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				yield(ObjectInfo{}, err)
				return
			}

			for _, obj := range page.Contents {
				if !yield(ObjectInfo{
					Key: *obj.Key,
				}, nil) {
					return
				}
			}
		}
	}
}

func (b *S3Bucket) CheckPermissions(ctx context.Context, cfg CheckPermissionsConfig) error {
	checkReadOnlyFn := func(key string) error {
		var awsErr *awsHttp.ResponseError
		if _, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &b.bucket, Key: aws.String(key)}); err != nil {
			if !errors.As(err, &awsErr) || awsErr.Response.Response.StatusCode != http.StatusNotFound {
				return err
			}
		}

		return nil
	}

	handleErr := func(err *awsHttp.ResponseError) error {
		if err.Response.Response.StatusCode == http.StatusNotFound {
			return permissionsErrorNoSuchBucket
		} else if err.Response.Response.StatusCode == http.StatusForbidden {
			return permissionsErrorUnauthorized
		}

		return err
	}

	return checkPermissions(ctx, b, cfg, checkReadOnlyFn, handleErr)
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
