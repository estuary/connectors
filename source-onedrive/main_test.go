package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	parserSpec, err := os.ReadFile("tests/parser_spec.json")
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(configSchema(json.RawMessage(parserSpec)), "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}
