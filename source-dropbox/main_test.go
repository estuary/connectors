package main

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/filesource"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func getTestConfig(t *testing.T) config {
	wd, err := os.Getwd()
	if err != nil {
		require.NoError(t, err)
	}
	const kms = "projects/estuary-theatre/locations/us-central1/keyRings/connector-keyring/cryptoKeys/connector-repository"
	var configFilePath = filepath.Join(wd, "connector_config.yaml")

	cmd := exec.Command(
		"sops",
		"--decrypt",
		"--gcp-kms",
		kms,
		configFilePath,
	)
	output, err := cmd.Output()
	require.NoError(t, err)

	var cfg config
	err = yaml.Unmarshal(output, &cfg)
	require.NoError(t, err)

	return cfg
}
func TestDropbox_getConfigSchema(t *testing.T) {
	parserJsonSchema := []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`)

	result := configSchema(parserJsonSchema)

	cupaloy.SnapshotT(t, string(result))
}
func TestNewDropboxStore(t *testing.T) {
	ctx := context.TODO()
	cfg := getTestConfig(t)

	store, err := newDropboxStore(ctx, cfg)
	require.NoError(t, err)

	err = store.check()
	require.NoError(t, err)
}

func TestDropboxStore_List(t *testing.T) {
	ctx := context.TODO()
	cfg := getTestConfig(t)

	query := filesource.Query{
		Prefix:    cfg.Path,
		Recursive: true,
	}

	dbx, err := newDropboxStore(ctx, cfg)
	require.NoError(t, err)

	listing, err := dbx.List(ctx, query)
	require.NoError(t, err)

	require.NotNil(t, listing)

	obj, err := listing.Next()
	require.NoError(t, err)
	require.NotNil(t, obj)

	for err == nil {
		obj, err = listing.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotNil(t, obj)
	}
}
