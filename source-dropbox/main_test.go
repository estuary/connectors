package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

func TestAzureBlobStore_getConfigSchema(t *testing.T) {
	parserJsonSchema := []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`)

	result := getConfigSchema(parserJsonSchema)

	cupaloy.SnapshotT(t, string(result))
}
