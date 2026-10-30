package blob

import (
	"context"
	"encoding/hex"
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

	var authOpt = WithAzureStorageAccountKey(cfg.StorageAccountKey)

	t.Run("default", func(t *testing.T) {
		var bucket, err = NewAzureBlobBucket(context.Background(), cfg.Container, cfg.StorageAccountName, authOpt)
		require.NoError(t, err)
		testBucket(t, bucket)
	})

	t.Run("single request", func(t *testing.T) {
		var bucket, err = NewAzureSingleRequestBucket(context.Background(), cfg.Container, cfg.StorageAccountName, authOpt)
		require.NoError(t, err)
		testBucket(t, bucket)

		t.Run("stored md5", func(t *testing.T) {
			testStoredMD5(t, bucket, func(t *testing.T, key string) string {
				var props, err = bucket.client.ServiceClient().NewContainerClient(bucket.container).NewBlobClient(key).GetProperties(context.Background(), nil)
				require.NoError(t, err)
				return hex.EncodeToString(props.ContentMD5)
			})
		})
	})
}
