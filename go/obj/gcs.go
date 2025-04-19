package obj

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

var _ Store = (*GCSStore)(nil)

type GCSStore struct {
	client *storage.Client
	bucket string
}

// NewGCSStore creates a GCS object store. At least one ClientOption must be
// provided to specify authentication.
func NewGCSStore(ctx context.Context, bucket string, opts []option.ClientOption) (*GCSStore, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &GCSStore{
		client: client,
		bucket: bucket,
	}, nil
}

func (s *GCSStore) PutStream(ctx context.Context, key string, r io.Reader, opts ...PutStreamOption) error {
	w := s.client.Bucket(s.bucket).Object(key).NewWriter(ctx)
	w.Metadata = getPutStreamConfig(opts).metadata
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("copying r to w: %w", err)
	} else if err := w.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}

	return nil
}

func (s *GCSStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.client.Bucket(s.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return r, nil
}
