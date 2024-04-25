package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

func TestDropbox_getConfigSchema(t *testing.T) {
	parserJsonSchema := []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`)

	result := configSchema(parserJsonSchema)

	cupaloy.SnapshotT(t, string(result))
}
