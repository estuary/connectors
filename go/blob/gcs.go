package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var _ Bucket = (*GCSBucket)(nil)

type GCSBucket struct {
	client *storage.Client
	bucket string
}

// NewGCSBucket creates a GCS object storage bucket. At least one ClientOption
// must be provided to specify authentication.
func NewGCSBucket(ctx context.Context, bucket string, authOpt option.ClientOption, otherOpts ...option.ClientOption) (*GCSBucket, error) {
	client, err := storage.NewClient(ctx, append(otherOpts, authOpt)...)
	if err != nil {
		return nil, err
	}

	return &GCSBucket{
		client: client,
		bucket: bucket,
	}, nil
}

func (b *GCSBucket) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := b.client.Bucket(b.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (b *GCSBucket) NewWriter(ctx context.Context, key string, opts ...WriterOption) io.WriteCloser {
	return b.client.Bucket(b.bucket).Object(key).NewWriter(ctx)
}

func (b *GCSBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...WriterOption) error {
	w := b.client.Bucket(b.bucket).Object(key).NewWriter(ctx)
	w.Metadata = getWriterConfig(opts).metadata
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("copying r to w: %w", err)
	} else if err := w.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}

	return nil
}

func (b *GCSBucket) URI(key string) string {
	return "gs://" + path.Join(b.bucket, key)
}

func (b *GCSBucket) Delete(ctx context.Context, uris []string) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, uri := range uris {
		trimmed := strings.TrimPrefix(uri, "gs://")
		bucket, key, ok := strings.Cut(trimmed, "/")
		if !ok {
			return fmt.Errorf("invalid uri %q", uri)
		}
		group.Go(func() error {
			if err := b.client.Bucket(bucket).Object(key).Delete(groupCtx); err != nil {
				var gErr *googleapi.Error
				if errors.As(err, &gErr) && gErr.Code == http.StatusNotFound {
					return nil
				}

				return fmt.Errorf("deleting blob %q: %w", uri, err)
			}
			return nil
		})
	}

	return group.Wait()
}

func (b *GCSBucket) List(ctx context.Context, query Query) iter.Seq2[ObjectInfo, error] {
	input := &storage.Query{Prefix: query.Prefix}
	iter := b.client.Bucket(b.bucket).Objects(ctx, input)

	return func(yield func(ObjectInfo, error) bool) {
		for {
			obj, err := iter.Next()
			if err == iterator.Done {
				return
			} else if err != nil {
				yield(ObjectInfo{}, err)
				return
			}

			if strings.HasSuffix(obj.Name, "/") {
				// Exclude directories.
				continue
			}

			if !yield(ObjectInfo{
				Key: obj.Name,
			}, nil) {
				return
			}
		}
	}
}

func (b *GCSBucket) CheckPermissions(ctx context.Context, cfg CheckPermissionsConfig) error {
	bucketHandle := b.client.Bucket(b.bucket)

	checkReadOnlyFn := func(key string) error {
		var apiErr *googleapi.Error
		_, err := bucketHandle.Object(key).Attrs(ctx)
		if !errors.As(err, &apiErr) || apiErr.Code != 404 {
			return err
		}

		return nil
	}

	handleErr := func(err *googleapi.Error) error {
		if err.Code == http.StatusNotFound {
			return permissionsErrorNoSuchBucket
		} else if err.Code == http.StatusForbidden {
			return permissionsErrorUnauthorized
		}

		return err
	}

	return checkPermissions(ctx, b, cfg, checkReadOnlyFn, handleErr)
}
