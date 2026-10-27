package blob

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestIntegrationS3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var cfg = loadSopsConfig[struct {
		Bucket             string `json:"bucket"`
		Region             string `json:"region"`
		AWSAccessKeyID     string `json:"awsAccessKeyId"`
		AWSSecretAccessKey string `json:"awsSecretAccessKey"`
	}](t, "testdata/s3.yaml")

	var creds = credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, "")

	t.Run("explicit region", func(t *testing.T) {
		bucket, err := NewS3Bucket(context.Background(), cfg.Bucket, creds, func(o *s3.Options) { o.Region = cfg.Region })
		require.NoError(t, err)
		testBucket(t, bucket)
	})

	t.Run("resolved region", func(t *testing.T) {
		bucket, err := NewS3Bucket(context.Background(), cfg.Bucket, creds)
		require.NoError(t, err)
		testBucket(t, bucket)
	})
}
