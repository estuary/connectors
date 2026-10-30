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
	client        *storage.Client
	bucket        string
	singleRequest bool
}

// NewGCSBucket creates a GCS object storage bucket. At least one ClientOption
// must be provided to specify authentication.
func NewGCSBucket(ctx context.Context, bucket string, authOpt option.ClientOption, otherOpts ...option.ClientOption) (*GCSBucket, error) {
	var client, err = storage.NewClient(ctx, append(otherOpts, authOpt)...)
	if err != nil {
		return nil, err
	}

	return &GCSBucket{
		client: client,
		bucket: bucket,
	}, nil
}

// NewGCSSingleRequestBucket creates a GCS object storage bucket whose uploads
// are each a single request, so an object's MD5 attribute is the MD5 of its
// bytes. At least one ClientOption must be provided to specify authentication.
func NewGCSSingleRequestBucket(ctx context.Context, bucket string, authOpt option.ClientOption, otherOpts ...option.ClientOption) (*GCSBucket, error) {
	var b, err = NewGCSBucket(ctx, bucket, authOpt, otherOpts...)
	if err != nil {
		return nil, err
	}
	b.singleRequest = true
	return b, nil
}

func (b *GCSBucket) newObjectWriter(ctx context.Context, key string) *storage.Writer {
	var w = b.client.Bucket(b.bucket).Object(key).NewWriter(ctx)
	if b.singleRequest {
		// A ChunkSize of 0 buffers nothing and sends the object in one request.
		w.ChunkSize = 0
	}
	return w
}

func (b *GCSBucket) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	var r, err = b.client.Bucket(b.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (b *GCSBucket) NewWriter(ctx context.Context, key string, opts ...WriterOption) io.WriteCloser {
	return b.newObjectWriter(ctx, key)
}

func (b *GCSBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...WriterOption) error {
	var cfg = getWriterConfig(opts)
	var w = b.newObjectWriter(ctx, key)
	w.Metadata = cfg.metadata
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("copying r to w: %w", err)
	} else if err := w.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}
	cfg.setETag(w.Attrs().Etag)

	return nil
}

func (b *GCSBucket) URI(key string) string {
	return "gs://" + path.Join(b.bucket, key)
}

func (b *GCSBucket) Delete(ctx context.Context, uris []string) error {
	var group, groupCtx = errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, uri := range uris {
		var trimmed = strings.TrimPrefix(uri, "gs://")
		var bucket, key, ok = strings.Cut(trimmed, "/")
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
	var input = &storage.Query{Prefix: query.Prefix}
	var iter = b.client.Bucket(b.bucket).Objects(ctx, input)

	return func(yield func(ObjectInfo, error) bool) {
		for {
			var obj, err = iter.Next()
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
	var bucketHandle = b.client.Bucket(b.bucket)

	var checkReadOnlyFn = func(key string) error {
		var apiErr *googleapi.Error
		var _, err = bucketHandle.Object(key).Attrs(ctx)
		if !errors.As(err, &apiErr) || apiErr.Code != 404 {
			return err
		}

		return nil
	}

	var handleErr = func(err *googleapi.Error) error {
		if err.Code == http.StatusNotFound {
			return permissionsErrorNoSuchBucket
		} else if err.Code == http.StatusForbidden {
			return permissionsErrorUnauthorized
		}

		return err
	}

	return checkPermissions(ctx, b, cfg, checkReadOnlyFn, handleErr)
}
