package main

import (
	"io"
	"path"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

type stagedFileClient struct {
	// disableGzip is set for the goccy emulator, whose native GCS load jobs
	// read staged files as-is and do not decompress gzip. Production staging
	// always compresses, matched by Compression on the external data config.
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

func edc(uris []string, schema bigquery.Schema) *bigquery.ExternalDataConfig {
	return &bigquery.ExternalDataConfig{
		SourceFormat: bigquery.JSON,
		SourceURIs:   uris,
		Schema:       schema,
		Compression:  bigquery.Gzip,
	}
}
