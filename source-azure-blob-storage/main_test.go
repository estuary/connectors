package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/filesource"
	"github.com/stretchr/testify/require"
)

const connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

func getTestConfig(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}
	cfg := config{
		Credentials: credentials{
			ConnectionString:   connectionString,
			StorageAccountName: "devstoreaccount1",
		},
		ContainerName: "example",
	}
	return cfg
}

func TestAzureBlobStore_newAzureBlobStore(t *testing.T) {
	// Create a test instance of the azureBlobStore
	cfg := getTestConfig(t)
	ctx := context.TODO()
	az, err := newAzureBlobStore(ctx, cfg)
	require.NoError(t, err)
	require.NotNil(t, az)
}

func TestAzureBlobStore_getConfigSchema(t *testing.T) {
	parserJsonSchema := []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`)

	result := getConfigSchema(parserJsonSchema)

	cupaloy.SnapshotT(t, string(result))
}

func TestAzureBlobStore_List(t *testing.T) {
	cfg := getTestConfig(t)
	ctx := context.TODO()
	az, err := newAzureBlobStore(ctx, cfg)

	require.NoError(t, err)
	require.NotNil(t, az)

	query := filesource.Query{}
	listing, err := az.List(ctx, query)

	require.NoError(t, err)
	require.NotNil(t, listing)

	file, err := listing.Next()
	require.NoError(t, err)
	require.NotNil(t, file)

	fmt.Println(file.Path)
}
func TestAzureBlobStore_Read(t *testing.T) {
	cfg := getTestConfig(t)

	ctx := context.TODO()
	az, err := newAzureBlobStore(ctx, cfg)

	require.NoError(t, err)
	require.NotNil(t, az)

	obj := filesource.ObjectInfo{
		Path: "l00cmqdvppy01-4060447306.jpg",
	}

	reader, info, err := az.Read(ctx, obj)
	require.NoError(t, err)
	require.NotNil(t, reader)
	downloadedData := bytes.Buffer{}

	_, err = downloadedData.ReadFrom(reader)
	require.NoError(t, err)
	cupaloy.SnapshotT(t, downloadedData)

	require.Equal(t, obj, info)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	err = reader.Close()
	require.NoError(t, err)
}
