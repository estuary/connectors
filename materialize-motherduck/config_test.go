package main

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS3ConfigWithEndpoint(t *testing.T) {
	baseConfig := func() config {
		return config{
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
			Database: "test_db",
			Schema:   "main",
			StagingBucket: stagingBucketConfig{
				StagingBucketType: stagingBucketTypeS3,
				stagingBucketS3Config: stagingBucketS3Config{
					BucketS3:           "mybucket",
					AWSAccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
					AWSSecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					Region:             "us-east-1",
				},
			},
		}
	}

	t.Run("valid config without endpoint", func(t *testing.T) {
		cfg := baseConfig()
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("valid config with endpoint", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.Endpoint = "https://abc123.r2.cloudflarestorage.com"
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("valid config with endpoint and auto region", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.Endpoint = "https://abc123.r2.cloudflarestorage.com"
		cfg.StagingBucket.Region = "auto"
		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestBucketPathValidation(t *testing.T) {
	// Validate() only checks that each dot-separated part is valid base64url,
	// so this doesn't need to look like a real JWT.
	baseToken := "YQ.YQ.YQ"

	s3Config := func() config {
		return config{
			Token:    baseToken,
			Database: "test_db",
			Schema:   "main",
			StagingBucket: stagingBucketConfig{
				StagingBucketType: stagingBucketTypeS3,
				stagingBucketS3Config: stagingBucketS3Config{
					BucketS3:           "mybucket",
					AWSAccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
					AWSSecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					Region:             "us-east-1",
				},
			},
		}
	}

	gcsConfig := func() config {
		return config{
			Token:    baseToken,
			Database: "test_db",
			Schema:   "main",
			StagingBucket: stagingBucketConfig{
				StagingBucketType: stagingBucketTypeGCS,
				stagingBucketGCSConfig: stagingBucketGCSConfig{
					BucketGCS:       "mybucket",
					CredentialsJSON: "{}",
					GCSHMACAccessID: strings.Repeat("a", 61),
					GCSHMACSecret:   base64.StdEncoding.EncodeToString(make([]byte, 30)), // 40 base64 chars
				},
			},
		}
	}

	azureConfig := func() config {
		return config{
			Token:    baseToken,
			Database: "test_db",
			Schema:   "main",
			StagingBucket: stagingBucketConfig{
				StagingBucketType: stagingBucketTypeAzure,
				stagingBucketAzureConfig: stagingBucketAzureConfig{
					StorageAccountName: "mystorageaccount",
					StorageAccountKey:  base64.StdEncoding.EncodeToString([]byte("test-storage-account-key-bytes-here")),
					ContainerName:      "mycontainer",
				},
			},
		}
	}

	cases := []struct {
		name       string
		baseConfig func() config
		setPath    func(cfg *config, path string)
	}{
		{"S3", s3Config, func(cfg *config, path string) { cfg.StagingBucket.BucketPathS3 = path }},
		{"GCS", gcsConfig, func(cfg *config, path string) { cfg.StagingBucket.BucketPathGCS = path }},
		{"Azure", azureConfig, func(cfg *config, path string) { cfg.StagingBucket.BucketPathAzure = path }},
	}

	for _, tc := range cases {
		t.Run(tc.name+" valid bucket path prefix", func(t *testing.T) {
			cfg := tc.baseConfig()
			tc.setPath(&cfg, "some/prefix/path")
			err := cfg.Validate()
			require.NoError(t, err)
		})

		t.Run(tc.name+" rejects leading slash", func(t *testing.T) {
			cfg := tc.baseConfig()
			tc.setPath(&cfg, "/some/prefix")
			err := cfg.Validate()
			require.ErrorContains(t, err, "cannot start with /")
		})

		t.Run(tc.name+" rejects full URI", func(t *testing.T) {
			cfg := tc.baseConfig()
			tc.setPath(&cfg, "s3://bucket/prefix")
			err := cfg.Validate()
			require.ErrorContains(t, err, "must be a bare key prefix within the bucket")
		})
	}
}
