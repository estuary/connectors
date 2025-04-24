// Package blob provides a high-level interface and specific implementations for
// working with blobs on the common object storage providers.
//
// This may eventually replace the fragmented versions of these we have
// scattered throughout the various captures and materializations, and it's
// expected that the basic initial structure is enhanced as additional
// requirements are incorporated.
package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"path"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type writerConfig struct {
	metadata map[string]string
}

type WriterOption func(*writerConfig)

func WithObjectMetadata(metadata map[string]string) WriterOption {
	return func(cfg *writerConfig) {
		cfg.metadata = metadata
	}
}

func getWriterConfig(opts []WriterOption) writerConfig {
	var cfg writerConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// CheckPermissionsConfig specifies which operations will be validated in
// CheckPermissions.
//
// A typical materialization that writes staged files, reads from those files
// with known key, and then deletes them by key should leave `ReadOnly` and
// `Lister` unset to validate writing, reading, and deleting a key.
//
// A materialization requiring the ability to also list keys should set
// `Lister`.
//
// A typical capture that only lists and reads from files should set `ReadOnly`.
type CheckPermissionsConfig struct {
	ReadOnly bool   // only verify that blobs can be read & listed; does not write or delete
	Prefix   string // prefix for reading/writing a test blob, and listing
	Lister   bool   // check that the list operation can succeed; implied by ReadOnly
}

// Bucket is a common interface for working with blob storage. Conceptually, a
// Bucket is aligned with an S3 or GCS bucket, or an Azure Blob storage
// "container".
type Bucket interface {
	// NewReader returns a stream of bytes for reading from the given key. The
	// caller is responsible for closing the returned ReadCloser.
	NewReader(ctx context.Context, key string) (io.ReadCloser, error)

	// NewWriter returns a WriteCloser to write an object to the bucket. The
	// object will be readable after Close returns.
	NewWriter(ctx context.Context, key string, opts ...WriterOption) io.WriteCloser

	// Upload uploads an object as a stream.
	Upload(ctx context.Context, key string, r io.Reader, opts ...WriterOption) error

	// URI creates an identifier from a file key, usually be prepending the URI
	// scheme, for example "s3://".
	URI(key string) string

	// Delete deletes the objects per the list of URIs.
	Delete(ctx context.Context, uris []string) error

	// CheckPermissions performs test operations to validate the bucket's access
	// per `cfg`.
	CheckPermissions(ctx context.Context, cfg CheckPermissionsConfig) error

	// List lists the items in the bucket.
	List(ctx context.Context, query Query) iter.Seq2[ObjectInfo, error]
}

// Query of objects to be returned by List.
type Query struct {
	// Prefix constrains the listing to paths which begin with the prefix.
	Prefix string
}

// ObjectInfo is returned by List.
type ObjectInfo struct {
	// Key is the path of the object or common prefix (if IsPrefix). It does not
	// include the bucket name.
	Key string
}

type uploadFn func(context.Context, string, io.Reader, ...WriterOption) error

// blobWriteCloser adapts a synchronous uploadFn into an io.WriterCloser.
type blobWriteCloser struct {
	w     *io.PipeWriter
	errCh chan (error)
}

func newBlobWriteCloser(ctx context.Context, u uploadFn, key string, opts ...WriterOption) *blobWriteCloser {
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		err := u(ctx, key, pr, opts...)
		if err != nil {
			pr.CloseWithError(err)
		}
		errCh <- err
	}()

	return &blobWriteCloser{
		w:     pw,
		errCh: errCh,
	}
}

func (w *blobWriteCloser) Close() error {
	if err := w.w.Close(); err != nil {
		return err
	}

	return <-w.errCh
}

func (w *blobWriteCloser) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

// Sanitized versions of user-facing errors for the two common cases of the
// wrong bucket input, or incorrect / insufficient authorization to do various
// actions on that bucket.
var (
	permissionsErrorNoSuchBucket = errors.New("no such bucket")
	permissionsErrorUnauthorized = errors.New("unauthorized")
)

func checkPermissions[T error](
	ctx context.Context,
	bucket Bucket,
	cfg CheckPermissionsConfig,
	// checkReadOnlyFn is a system-specific function for reading from a key that
	// we know doesn't exist to determine if we can read from any key. This is
	// done instead of writing a file and then trying to read it, because we
	// want to check reading permissions but not writing permissions. It will
	// always be called after a successful `List`, and generally a "not found"
	// response should be considered as success.
	checkReadOnlyFn func(key string) error,
	// handleErr adapts the error of type T into a more user-friendly error.
	handleErr func(err T) error,
) error {
	var asErr T
	adaptErr := func(err error) error {
		if errors.As(err, &asErr) {
			// Log the original error message, since it can sometimes contain
			// useful (although potentially confusing) information.
			log.WithField("err", err).Error("bucket permissions check failed")
			err = handleErr(asErr)
		}
		return err
	}

	bucketAndPrefixURI := bucket.URI(cfg.Prefix)

	if cfg.ReadOnly || cfg.Lister {
		for _, err := range bucket.List(ctx, Query{Prefix: cfg.Prefix}) {
			if err != nil {
				return fmt.Errorf("unable to list %q: %w", bucketAndPrefixURI, adaptErr(err))
			}
			break
		}
	}

	if cfg.ReadOnly {
		if err := checkReadOnlyFn(path.Join(cfg.Prefix, uuid.NewString())); err != nil {
			return fmt.Errorf("unable to read from %q: %w", bucketAndPrefixURI, adaptErr(err))
		}

		return nil
	}

	testData := []byte("test")
	testKey := path.Join(cfg.Prefix, uuid.NewString())
	testWriter := bucket.NewWriter(ctx, testKey)
	got := make([]byte, len(testData))

	if n, err := testWriter.Write(testData); err != nil {
		return fmt.Errorf("unable to write to %q: %w", bucketAndPrefixURI, adaptErr(err))
	} else if n != len(testData) {
		return fmt.Errorf("short write: %d vs %d", n, len(testData))
	} else if err := testWriter.Close(); err != nil {
		return fmt.Errorf("unable to write to %q: %w", bucketAndPrefixURI, adaptErr(err))
	} else if reader, err := bucket.NewReader(ctx, testKey); err != nil {
		return fmt.Errorf("unable to read from %q: %w", bucketAndPrefixURI, adaptErr(err))
	} else if n, err := reader.Read(got); err != nil && err != io.EOF {
		return fmt.Errorf("unable to read from %q: %w", bucketAndPrefixURI, adaptErr(err))
	} else if n != len(testData) {
		return fmt.Errorf("short read: %d vs %d", n, len(testData))
	} else if !bytes.Equal(testData, got) {
		return fmt.Errorf("io error: read bytes do not match written bytes")
	} else if err := reader.Close(); err != nil {
		return fmt.Errorf("unexpected error when closing reader: %w", err)
	} else if err := bucket.Delete(ctx, []string{bucket.URI(testKey)}); err != nil {
		return fmt.Errorf("unable to delete from %q: %w", bucketAndPrefixURI, adaptErr(err))
	}

	return nil
}
