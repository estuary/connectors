package connector

import (
	"io"
	"path"

	"cloud.google.com/go/bigquery"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
)

type stagedFileClient struct{}

func (stagedFileClient) NewEncoder(w io.WriteCloser, fields []string) boilerplate.Encoder {
	return enc.NewJsonEncoder(w, fields, enc.WithJsonSkipNulls())
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
