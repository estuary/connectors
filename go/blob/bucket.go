// Package blob provides a high-level interface and specific implementations for
// working with blobs on the common object storage providers.
//
// This may eventually replace the fragmented versions of these we have
// scattered throughout the various captures and materializations, and it's
// expected that the basic initial structure is enhanced as additional
// requirements are incorporated.
package blob

import (
	"context"
	"io"
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
