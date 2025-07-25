package main

import (
	"io"
	"path"

	enc "github.com/estuary/connectors/go/stream-encode"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

type stagedFileClient struct{}

func (stagedFileClient) NewEncoder(w io.WriteCloser, fields []string) boilerplate.Encoder {
	return enc.NewJsonEncoder(w, fields)
}

func (stagedFileClient) NewKey(keyParts []string) string {
	return path.Join(keyParts...)
}
