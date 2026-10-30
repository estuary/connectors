package main

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestConfigSchema(t *testing.T) {
	var parserSchema = json.RawMessage("{}")
	var data = src.ConfigSchema(parserSchema)

	var formatted, err = json.MarshalIndent(data, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)

}
