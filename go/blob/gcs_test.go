package blob

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestIntegrationGCS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var cfg = loadSopsConfig[struct {
		Bucket          string `json:"bucket"`
		CredentialsJSON string `json:"credentialsJson"`
	}](t, "testdata/gcs.yaml")

	bucket, err := NewGCSBucket(context.Background(), cfg.Bucket, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(cfg.CredentialsJSON)))
	require.NoError(t, err)
	testBucket(t, bucket)

	t.Run("stored md5", func(t *testing.T) {
		testStoredMD5(t, bucket, func(t *testing.T, key string) string {
			attrs, err := bucket.client.Bucket(bucket.bucket).Object(key).Attrs(context.Background())
			require.NoError(t, err)
			return hex.EncodeToString(attrs.MD5)
		})
	})
}
