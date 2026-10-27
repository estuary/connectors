package blob

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntegrationAzure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var cfg = loadSopsConfig[struct {
		Container          string `json:"container"`
		StorageAccountName string `json:"storageAccountName"`
		StorageAccountKey  string `json:"storageAccountKey"`
	}](t, "testdata/azure.yaml")

	bucket, err := NewAzureBlobBucket(context.Background(), cfg.Container, cfg.StorageAccountName, WithAzureStorageAccountKey(cfg.StorageAccountKey))
	require.NoError(t, err)
	testBucket(t, bucket)
}
