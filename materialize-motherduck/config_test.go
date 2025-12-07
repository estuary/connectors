package main

import (
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
