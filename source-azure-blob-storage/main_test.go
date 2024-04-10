package main

import (
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/assert"
)

const connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

func TestAzureBlobStore_Read(t *testing.T) {
	// Create a test instance of the azureBlobStore
	cfg := config{
		Credentials: &credentials{
			ConnectionString: connectionString,
		},
		StorageAccountName: "devstoreaccount1",
		ContainerName:      "example",
	}
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
