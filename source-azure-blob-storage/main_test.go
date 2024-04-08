package main

import (
	"log"
	"os"
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
)

func TestAzureBlobStore_Read(t *testing.T) {
	t.Skip("Skipping test as it demands a manually set storage environment.")
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// Create a test instance of the azureBlobStore

	cfg := config{
		Credentials: &credentials{
			AzureClientID:       os.Getenv("AZURE_CLIENT_ID"),
			AzureClientSecret:   os.Getenv("AZURE_CLIENT_SECRET"),
			AzureTenantID:       os.Getenv("AZURE_TENANT_ID"),
			AzureSubscriptionID: os.Getenv("AZURE_SUBSCRIPTION_ID"),
		},
		StorageAccountName: os.Getenv("STORAGE_ACCOUNT_NAME"),
		ContainerName:      os.Getenv("CONTAINER_NAME"),
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
