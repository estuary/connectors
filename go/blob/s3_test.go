package blob

import (
	"context"
	"strings"
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
		var bucket, err = NewS3Bucket(context.Background(), cfg.Bucket, creds, func(o *s3.Options) { o.Region = cfg.Region })
		require.NoError(t, err)
		testBucket(t, bucket)
	})

	t.Run("resolved region", func(t *testing.T) {
		var bucket, err = NewS3Bucket(context.Background(), cfg.Bucket, creds)
		require.NoError(t, err)
		testBucket(t, bucket)
	})

	t.Run("single request", func(t *testing.T) {
		var bucket = NewS3SingleRequestBucket(cfg.Bucket, cfg.Region, creds)
		testBucket(t, bucket)

		t.Run("stored md5", func(t *testing.T) {
			testStoredMD5(t, bucket, func(t *testing.T, key string) string {
				var head, err = bucket.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket.bucket, Key: &key})
				require.NoError(t, err)
				return strings.Trim(*head.ETag, `"`)
			})
		})
	})
}
