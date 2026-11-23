package blob

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"
)

func TestURI(t *testing.T) {
	var s3Bucket = NewS3SingleRequestBucket("bucket", "us-east-1", credentials.NewStaticCredentialsProvider("id", "secret", ""))
	var gcsBucket = &GCSBucket{bucket: "bucket"}

	azureClient, err := azblob.NewClientWithNoCredential("https://account.blob.core.windows.net/", nil)
	require.NoError(t, err)
	var azureBucket = &AzureBlobBucket{client: azureClient, container: "bucket"}

	for _, tt := range []struct {
		name string
		key  string
		want string
	}{
		{"empty key", "", "bucket"},
		{"file", "file", "bucket/file"},
		{"file under a prefix", "estuary/staging/file", "bucket/estuary/staging/file"},
		{"trailing slash", "estuary/staging/", "bucket/estuary/staging"},
		{"leading slash", "/estuary/file", "bucket/estuary/file"},
		{"repeated slash", "estuary//file", "bucket/estuary/file"},
		{"parent segments do not replace the bucket", "../staging/file", "bucket/staging/file"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, "s3://"+tt.want, s3Bucket.URI(tt.key))
			require.Equal(t, "gs://"+tt.want, gcsBucket.URI(tt.key))
			require.Equal(t, "https://account.blob.core.windows.net/"+tt.want, azureBucket.URI(tt.key))
		})
	}
}
