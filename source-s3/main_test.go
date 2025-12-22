package main

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestConfigSchema(t *testing.T) {
	parserSchema := json.RawMessage("{}")
	data := src.ConfigSchema(parserSchema)

	formatted, err := json.MarshalIndent(data, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)

}
