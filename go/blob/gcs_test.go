package blob

import (
	"context"
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
}
