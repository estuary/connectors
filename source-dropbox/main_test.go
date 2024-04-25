package main

import (
	"context"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestDropbox_getConfigSchema(t *testing.T) {
	parserJsonSchema := []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`)

	result := configSchema(parserJsonSchema)

	cupaloy.SnapshotT(t, string(result))
}
func TestDropbox_newDropboxStore(t *testing.T) {
	token := os.Getenv("DROPBOX_TOKEN")
	if token == "" {
		t.Skip("DROPBOX token not set")
	}
	ctx := context.TODO()
	cfg := config{
		Credentials: &Credentials{
			AccessToken: token,
		},
		Path: "",
	}

	store, err := newDropboxStore(ctx, cfg)

	if err != nil {
		require.NoError(t, err)
	}

	err = store.check()
	require.NoError(t, err)
}
