package connector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketFromS3Path(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"s3://bucket/warehouse/ns/table/", "bucket"},
		{"s3://bucket/a/b/data/x.parquet", "bucket"},
		{"s3://bucket", "bucket"},
		{"s3://bucket/", "bucket"},
		{"s3a://bucket/x", ""},
		{"gs://bucket/x", ""},
		{"", ""},
	} {
		require.Equal(t, tc.want, bucketFromS3Path(tc.in), tc.in)
	}
}

func TestCheckTableLocationBucket(t *testing.T) {
	require.NoError(t, checkTableLocationBucket("ns.t", "s3://bucket/warehouse/ns/t", "bucket"))

	err := checkTableLocationBucket("ns.t", "s3://other/warehouse/ns/t", "bucket")
	require.ErrorContains(t, err, `in bucket "other"`)
	require.ErrorContains(t, err, `configured bucket "bucket"`)

	require.Error(t, checkTableLocationBucket("ns.t", "gs://bucket/warehouse/ns/t", "bucket"))
}
