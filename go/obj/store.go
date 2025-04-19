// Package obj provides a high-level interface and specific implementations for
// working with object storage providers.
//
// This may eventually replace the fragmented versions of these we have
// scattered throughout the various captures and materializations, and it is
// expected that the basic initial structure is enhanced as additional
// requirements are incorporated.
package obj

import (
	"context"
	"io"
)

type putStreamConfig struct {
	metadata map[string]string
}

type PutStreamOption func(*putStreamConfig)

func WithPutStreamMetadata(metadata map[string]string) PutStreamOption {
	return func(cfg *putStreamConfig) {
		cfg.metadata = metadata
	}
}

func getPutStreamConfig(opts []PutStreamOption) putStreamConfig {
	var cfg putStreamConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// Store is a common interface for object storage implementations.
type Store interface {
	// PutStream uploads an object as a stream, reading from `r` and writing an
	// object named `key`.
	PutStream(ctx context.Context, key string, r io.Reader, opts ...PutStreamOption) error

	// GetStream downloads an object as a stream. The caller is responsible for
	// closing the returned ReadCloser.
	GetStream(ctx context.Context, key string) (io.ReadCloser, error)
}
