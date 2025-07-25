package main

import (
	"io"
	"path"

	enc "github.com/estuary/connectors/go/stream-encode"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

// Multiple files are loaded faster by COPY INTO than a single large file.
// Splitting files into 250MiB chunks (after compression) seems to work well
// enough for larger transactions.
const fileSizeLimit = 250 * 1024 * 1024

type stagedFileClient struct{}

func (stagedFileClient) NewEncoder(w io.WriteCloser, fields []string) boilerplate.Encoder {
	return enc.NewCsvEncoder(w, fields, enc.WithCsvSkipHeaders(), enc.WithCsvQuoteChar('`'))
}

func (stagedFileClient) NewKey(keyParts []string) string {
	return path.Join(keyParts...)
}
