package main

import (
	"io"
	"path"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

type stagedFileClient struct{}

func (stagedFileClient) NewWriter(w io.WriteCloser, fields []string) boilerplate.Writer {
	return writer.NewJsonWriter(w, fields, writer.WithJsonSkipNulls())
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
