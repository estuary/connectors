//go:build !nodb

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	ctx := context.Background()

	// Create the test bucket.
	s3Client := newS3Client(t, ctx)
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("test-bucket"),
	})
	require.NoError(t, err)

	// Drive the connector with test data.
	boilerplate.RunFlowctl(
		t,
		"preview",
		"--source", "testdata/test.flow.yaml",
		"--fixture", "testdata/fixture.json",
		"--name", "acmeCo/tests/materialize-s3-parquet",
	)

	// Download parquet files from S3 to a temp dir and read them with parquet-tools.
	tmpDir := t.TempDir()

	var snap strings.Builder
	for _, prefix := range []string{"simple/", "multiple-data-types/"} {
		snap.WriteString(fmt.Sprintf("=== %s ===\n", prefix))
		rows := downloadAndReadParquet(t, ctx, s3Client, "test-bucket", prefix, tmpDir)
		for _, row := range rows {
			snap.WriteString(row)
			snap.WriteString("\n")
		}
		snap.WriteString("\n")
	}

	cupaloy.SnapshotT(t, snap.String())
}

func newS3Client(t *testing.T, ctx context.Context) *s3.Client {
	t.Helper()

	cfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion("us-east-1"),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test_key", "test_secret", "")),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:4566")
		o.UsePathStyle = true
	})
}

func downloadAndReadParquet(t *testing.T, ctx context.Context, client *s3.Client, bucket, prefix, tmpDir string) []string {
	t.Helper()

	// List all parquet files under the prefix.
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	require.NoError(t, err)

	var keys []string
	for _, obj := range listOut.Contents {
		if strings.HasSuffix(*obj.Key, ".parquet") {
			keys = append(keys, *obj.Key)
		}
	}
	sort.Strings(keys)

	// Download each parquet file.
	localDir := filepath.Join(tmpDir, prefix)
	require.NoError(t, os.MkdirAll(localDir, 0o755))

	var localFiles []string
	for _, key := range keys {
		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		data, err := io.ReadAll(getOut.Body)
		getOut.Body.Close()
		require.NoError(t, err)

		localPath := filepath.Join(localDir, filepath.Base(key))
		require.NoError(t, os.WriteFile(localPath, data, 0o644))
		localFiles = append(localFiles, localPath)
	}

	// Read parquet files using parquet-tools docker image.
	var allRows []string
	for _, f := range localFiles {
		out, err := exec.Command(
			"docker", "run", "--rm",
			"-v", fmt.Sprintf("%s:/data", filepath.Dir(f)),
			"nathanhowell/parquet-tools",
			"cat", "-json", fmt.Sprintf("/data/%s", filepath.Base(f)),
		).Output()
		require.NoError(t, err)

		// Each line is a JSON object. Parse and re-serialize for consistent formatting.
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line == "" {
				continue
			}
			var row map[string]any
			require.NoError(t, json.Unmarshal([]byte(line), &row))
			sorted, err := json.Marshal(row)
			require.NoError(t, err)
			allRows = append(allRows, string(sorted))
		}
	}

	sort.Strings(allRows)
	return allRows
}
