package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/estuary/connectors/filesource"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/assert"
)

const connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

func getTestConfig(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}
	cfg := config{
		Credentials: &credentials{
			ConnectionString: connectionString,
		},
		StorageAccountName: "devstoreaccount1",
		ContainerName:      "example",
	}
	return cfg
}

func TestAzureBlobStore_newAzureBlobStore(t *testing.T) {
	// Create a test instance of the azureBlobStore
	cfg := getTestConfig(t)
	az, err := newAzureBlobStore(cfg) // Pass cfg directly instead of its address
	assert.NoError(t, err)
	assert.NotNil(t, az)
}

func TestGetConfigSchema(t *testing.T) {
	parserJsonSchema := []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`)

	result := getConfigSchema(parserJsonSchema)

	snaps.WithConfig(
		snaps.Dir("snapshots"),
		snaps.Filename("config_schema"),
	).MatchJSON(t, string(result))
}

func TestAzureBlobStore_List(t *testing.T) {
	// Create a test instance of the azureBlobStore
	cfg := getTestConfig(t)

	az, err := newAzureBlobStore(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, az)

	// Create a test context
	ctx := context.TODO()

	// Create a test query
	query := filesource.Query{}
	listing, err := az.List(ctx, query)

	assert.NoError(t, err)
	assert.NotNil(t, listing)

	file, err := listing.Next()
	assert.NoError(t, err)
	assert.NotNil(t, file)

	fmt.Println(file.Path)
}
func TestAzureBlobStore_Read(t *testing.T) {
	// Create a test instance of the azureBlobStore
	cfg := getTestConfig(t)

	az, err := newAzureBlobStore(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, az)

	// Create a test context
	ctx := context.TODO()

	// Create a test object
	obj := filesource.ObjectInfo{
		Path: "l00cmqdvppy01-4060447306.jpg",
	}

	// Call the Read function
	reader, info, err := az.Read(ctx, obj)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, obj, info)

	// Read from the reader
	data, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	// Close the reader
	err = reader.Close()
	assert.NoError(t, err)
}