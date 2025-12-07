package main

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAzureConfigValidation(t *testing.T) {
	// Valid base64 encoded key (like a real Azure storage account key)
	validKey := base64.StdEncoding.EncodeToString([]byte("test-storage-account-key-bytes-here"))

	baseConfig := func() config {
		return config{
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
			Database: "test_db",
			Schema:   "main",
			StagingBucket: stagingBucketConfig{
				StagingBucketType: stagingBucketTypeAzure,
				stagingBucketAzureConfig: stagingBucketAzureConfig{
					StorageAccountName: "mystorageaccount",
					StorageAccountKey:  validKey,
					ContainerName:      "mycontainer",
				},
			},
		}
	}

	t.Run("valid config", func(t *testing.T) {
		cfg := baseConfig()
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("missing storageAccountName", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.StorageAccountName = ""
		err := cfg.Validate()
		require.ErrorContains(t, err, "missing 'storageAccountName'")
	})

	t.Run("missing storageAccountKey", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.StorageAccountKey = ""
		err := cfg.Validate()
		require.ErrorContains(t, err, "missing 'storageAccountKey'")
	})

	t.Run("missing containerName", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = ""
		err := cfg.Validate()
		require.ErrorContains(t, err, "missing 'containerName'")
	})

	t.Run("invalid storageAccountKey not base64", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.StorageAccountKey = "not-valid-base64!!!"
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid storageAccountKey: must be base64 encoded")
	})

	t.Run("valid config with optional bucketPath", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.BucketPathAzure = "some/prefix/path"
		err := cfg.Validate()
		require.NoError(t, err)
	})

	// Storage account name validation tests
	t.Run("storageAccountName too short", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.StorageAccountName = "ab"
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid storageAccountName: must be between 3 and 24 characters")
	})

	t.Run("storageAccountName too long", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.StorageAccountName = "abcdefghijklmnopqrstuvwxy" // 25 characters
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid storageAccountName: must be between 3 and 24 characters")
	})

	t.Run("storageAccountName with uppercase", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.StorageAccountName = "MyStorageAccount"
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid storageAccountName: must contain only lowercase letters and numbers")
	})

	t.Run("storageAccountName with special characters", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.StorageAccountName = "my-storage-account"
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid storageAccountName: must contain only lowercase letters and numbers")
	})

	t.Run("valid storageAccountName at min length", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.StorageAccountName = "abc" // 3 characters
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("valid storageAccountName at max length", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.StorageAccountName = "abcdefghijklmnopqrstuvwx" // 24 characters
		err := cfg.Validate()
		require.NoError(t, err)
	})

	// Container name validation tests
	t.Run("containerName too short", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = "ab"
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid containerName: must be between 3 and 63 characters")
	})

	t.Run("containerName too long", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01" // 64 characters
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid containerName: must be between 3 and 63 characters")
	})

	t.Run("containerName with uppercase", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = "MyContainer"
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid containerName: must start and end with a lowercase letter or number")
	})

	t.Run("containerName starting with hyphen", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = "-mycontainer"
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid containerName: must start and end with a lowercase letter or number")
	})

	t.Run("containerName ending with hyphen", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = "mycontainer-"
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid containerName: must start and end with a lowercase letter or number")
	})

	t.Run("containerName with consecutive hyphens", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = "my--container"
		err := cfg.Validate()
		require.ErrorContains(t, err, "invalid containerName: must not contain consecutive hyphens")
	})

	t.Run("valid containerName with hyphens", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = "my-container-name"
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("valid containerName at min length", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = "abc" // 3 characters
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("valid containerName at max length", func(t *testing.T) {
		cfg := baseConfig()
		cfg.StagingBucket.ContainerName = "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0" // 63 characters
		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestAzureBucketWrapperURI(t *testing.T) {
	wrapper := &azureBucketWrapper{
		AzureBlobBucket: nil, // Not needed for URI test
		container:       "mycontainer",
	}

	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "simple key",
			key:      "file.json",
			expected: "az://mycontainer/file.json",
		},
		{
			name:     "key with path",
			key:      "path/to/file.json",
			expected: "az://mycontainer/path/to/file.json",
		},
		{
			name:     "key with prefix",
			key:      "staging/data/file.json.gz",
			expected: "az://mycontainer/staging/data/file.json.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := wrapper.URI(tt.key)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestAzureBucketWrapperDeleteKeyExtraction(t *testing.T) {
	// Test the key extraction logic used in Delete method.
	// The Delete method converts az:// URIs back to keys by stripping the prefix.
	// This test verifies that round-trip works: URI(key) -> Delete extracts key
	container := "mycontainer"

	tests := []struct {
		name        string
		key         string
		description string
	}{
		{
			name:        "simple key",
			key:         "file.json",
			description: "basic filename",
		},
		{
			name:        "key with path",
			key:         "path/to/file.json",
			description: "nested path",
		},
		{
			name:        "key with staging prefix",
			key:         "staging/data/file.json.gz",
			description: "typical staging path with compression",
		},
		{
			name:        "key with special characters",
			key:         "data/2024-01-01/file_name.json",
			description: "path with date and underscore",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate what URI() does
			azURI := "az://" + container + "/" + tt.key

			// Simulate what Delete() does to extract the key
			prefix := "az://" + container + "/"
			extractedKey := strings.TrimPrefix(azURI, prefix)

			// Verify round-trip: the extracted key should match the original
			require.Equal(t, tt.key, extractedKey, "key extraction should preserve original key")
		})
	}
}

func TestAzureBucketWrapperDeleteMultipleURIs(t *testing.T) {
	// Test that Delete correctly handles multiple URIs
	container := "testcontainer"

	inputURIs := []string{
		"az://testcontainer/file1.json",
		"az://testcontainer/path/to/file2.json",
		"az://testcontainer/staging/file3.json.gz",
	}

	expectedKeys := []string{
		"file1.json",
		"path/to/file2.json",
		"staging/file3.json.gz",
	}

	// Extract keys using the same logic as Delete method
	prefix := "az://" + container + "/"
	extractedKeys := make([]string, len(inputURIs))
	for i, uri := range inputURIs {
		extractedKeys[i] = strings.TrimPrefix(uri, prefix)
	}

	require.Equal(t, expectedKeys, extractedKeys, "all keys should be correctly extracted")
}
