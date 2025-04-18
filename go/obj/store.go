// Package obj provides a high-level interface and specific implementations for
// working with object storage providers.
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

type Store interface {
	PutStream(ctx context.Context, key string, r io.Reader, opts ...PutStreamOption) error
}
