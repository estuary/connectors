package connector

import (
	"io"
	"path"
	"strings"

	enc "github.com/estuary/connectors/go/stream-encode"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

const fileSizeLimit = 250 * 1024 * 1024

type stagedFileClient struct{}

func (stagedFileClient) NewEncoder(w io.WriteCloser, fields []string) boilerplate.Encoder {
	return enc.NewCsvEncoder(w, fields, enc.WithCsvSkipHeaders(), enc.WithCsvQuoteChar('`'))
}

func (stagedFileClient) NewKey(keyParts []string) string {
	return path.Join(keyParts...) + ".csv.gz"
}

func s3UriToParts(uri string) (bucket, key string) {
	uri = strings.TrimPrefix(uri, "s3://")
	bucket, key, _ = strings.Cut(uri, "/")
	return
}
