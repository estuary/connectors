package main

import (
	"io"
	"path"

	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

type stagedFileClient struct{}

func (stagedFileClient) NewWriter(w io.WriteCloser, fields []string) boilerplate.Writer {
	return writer.NewJsonWriter(w, fields)
}

func (stagedFileClient) NewKey(keyParts []string) string {
	return path.Join(keyParts...)
}
