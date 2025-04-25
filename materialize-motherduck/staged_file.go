package main

import (
	"io"
	"path"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
)

type stagedFileClient struct{}

func (stagedFileClient) NewEncoder(w io.WriteCloser, fields []string) boilerplate.Encoder {
	return enc.NewJsonEncoder(w, fields)
}

func (stagedFileClient) NewKey(keyParts []string) string {
	return path.Join(keyParts...)
}
