package main

import (
	"io"
	"path"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

type stagedFileClient struct {
	disableGzip bool
}

func (c stagedFileClient) NewWriter(w io.WriteCloser, fields []string) boilerplate.Writer {
	opts := []writer.JsonOption{writer.WithJsonSkipNulls()}
	if c.disableGzip {
		opts = append(opts, writer.WithJsonDisableCompression())
	}
	return writer.NewJsonWriter(w, fields, opts...)
}

func (stagedFileClient) NewKey(keyParts []string) string {
	return path.Join(keyParts...)
}

func edc(uris []string, schema bigquery.Schema, isEmulator bool) *bigquery.ExternalDataConfig {
	cfg := &bigquery.ExternalDataConfig{
		SourceFormat: bigquery.JSON,
		SourceURIs:   uris,
		Schema:       schema,
	}
	if !isEmulator {
		cfg.Compression = bigquery.Gzip
	}
	return cfg
}
