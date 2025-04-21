package blob

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

var _ Bucket = (*GCSBucket)(nil)

type GCSBucket struct {
	client *storage.Client
	bucket string
}

// NewGCSBucket creates a GCS object storage bucket. At least one ClientOption
// must be provided to specify authentication.
func NewGCSBucket(ctx context.Context, bucket string, opts []option.ClientOption) (*GCSBucket, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &GCSBucket{
		client: client,
		bucket: bucket,
	}, nil
}

func (s *GCSBucket) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.client.Bucket(s.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (s *GCSBucket) NewWriter(ctx context.Context, key string, opts ...WriterOption) io.WriteCloser {
	return newBlobWriteCloser(ctx, s.Upload, key, opts...)
}

func (s *GCSBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...WriterOption) error {
	w := s.client.Bucket(s.bucket).Object(key).NewWriter(ctx)
	w.Metadata = getWriterConfig(opts).metadata
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("copying r to w: %w", err)
	} else if err := w.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}

	return nil
}
