package connector

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/estuary/connectors/go/writer"
	"github.com/estuary/connectors/materialize-iceberg/catalog"
	"github.com/estuary/connectors/materialize-iceberg/python"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/require"
)

// testAll controls whether integration tests run against every catalog
// configuration (rest-local plus the cloud-based REST/Polaris-managed, Glue,
// and S3 Tables variants) or only against the local docker-compose stack.
// Running the full matrix is slow and requires SOPS decryption keys plus
// live AWS credentials, so by default we only exercise the local stack.
// Opt in to the full matrix by setting the `ICEBERG_TEST_ALL` environment
// variable or passing `-iceberg.test-all` to `go test`.
var testAll = flag.Bool("iceberg.test-all", false, "run integration tests against all catalog configurations (rest-local plus REST/Glue/S3 Tables) instead of the local stack only")

const (
	composeFile    = "docker-compose.yaml"
	composeProject = "materialize-iceberg"
	credsPath      = "testdata/.local/polaris-creds.json"
	configTemplate = "testdata/config.rest-local.yaml"
	localConfig    = "testdata/.local/config.rest-local.yaml"
)

var (
	dockerOnce sync.Once
	dockerUp   bool
)

func ensureDockerUp(t *testing.T) {
	t.Helper()
	dockerOnce.Do(func() {
		if err := composeUpAndBootstrap(); err != nil {
			t.Logf("docker compose setup failed: %v", err)
			return
		}
	})
	if !dockerUp {
		t.Fatal("docker compose setup previously failed")
	}
}

func composeUpAndBootstrap() error {
	if err := os.MkdirAll(filepath.Dir(localConfig), 0o755); err != nil {
		return fmt.Errorf("creating .local dir: %w", err)
	}

	// Ensure a clean slate: any leftover compose state plus a stale credentials
	// file from a prior run would silently produce credentials that don't match
	// the current Polaris instance. Tearing down explicitly is cheap relative
	// to debugging an "unauthorized_client" failure later.
	exec.Command("docker", "compose", "-f", composeFile, "-p", composeProject, "down", "-v").Run()
	_ = os.Remove(credsPath)

	upCmd := exec.Command("docker", "compose", "-f", composeFile, "-p", composeProject, "up", "--wait", "--wait-timeout", "180")
	dockerUp = true
	if out, err := upCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up failed: %s\n%s", err, out)
	}

	creds, err := readPolarisCreds()
	if err != nil {
		return fmt.Errorf("reading polaris bootstrap credentials: %w", err)
	}

	tmpl, err := os.ReadFile(configTemplate)
	if err != nil {
		return fmt.Errorf("reading %s: %w", configTemplate, err)
	}

	credential := creds.ClientID + ":" + creds.ClientSecret
	rendered := strings.ReplaceAll(string(tmpl), "CREDENTIAL_PLACEHOLDER", credential)
	if err := os.WriteFile(localConfig, []byte(rendered), 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", localConfig, err)
	}

	return nil
}

type polarisCreds struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

// readPolarisCreds polls the bootstrap credentials volume until the
// `polaris-bootstrap` service has finished writing the file. The compose
// `--wait` flag returns when the bootstrap container exits successfully, but
// the bind-mounted file may briefly lag the container's exit on some hosts.
func readPolarisCreds() (*polarisCreds, error) {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		bs, err := os.ReadFile(credsPath)
		if err == nil && len(bs) > 0 {
			var c polarisCreds
			if err := json.Unmarshal(bs, &c); err != nil {
				return nil, fmt.Errorf("parsing %s: %w", credsPath, err)
			}
			if c.ClientID != "" && c.ClientSecret != "" {
				return &c, nil
			}
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("timed out reading %s: %v", credsPath, lastErr)
}

func TestMain(m *testing.M) {
	code := m.Run()
	if dockerUp {
		exec.Command("docker", "compose", "-f", composeFile, "-p", composeProject, "down", "-v").Run()
	}
	os.Exit(code)
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ensureDockerUp(t)

	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table}
	}

	// Normalize S3 file paths that contain UUIDs which change on every run.
	actionDescSanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`"s3://[^"]+\.csv\.gz"`).ReplaceAllString(s, `"s3://<bucket>/<uuid>.csv.gz"`)
		},
	}

	all := *testAll || os.Getenv("ICEBERG_TEST_ALL") != ""

	materializeSpec := "testdata/materialize-rest-local.flow.yaml"
	applySpec := "testdata/apply-rest-local.flow.yaml"
	migrateSpec := "testdata/migrate-rest-local.flow.yaml"
	if all {
		materializeSpec = "testdata/materialize.flow.yaml"
		applySpec = "testdata/apply.flow.yaml"
		migrateSpec = "testdata/migrate.flow.yaml"
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTestParallel(t, newMaterialization, materializeSpec, makeResourceFn, actionDescSanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTestParallel(t, &Driver{}, newMaterialization, applySpec, makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		boilerplate.RunMigrationTestParallel(t, newMaterialization, migrateSpec, makeResourceFn, nil)
	})

	t.Run("ts-overflow-regression", func(t *testing.T) {
		runTimestampOverflowRegression(t)
	})

	t.Run("date-overflow-regression", func(t *testing.T) {
		runDateOverflowRegression(t)
	})

	t.Run("empty-string-null-count-regression", func(t *testing.T) {
		runEmptyStringNullCountRegression(t)
	})
}

// runTimestampOverflowRegression checks whether a year-10000 timestamp - the
// value class that broke materialize-s3-iceberg through pyiceberg/pyarrow's
// Python datetime path - round-trips through this connector's CSV → Spark →
// Iceberg-Java pipeline without crashing.
func runTimestampOverflowRegression(t *testing.T) {
	ctx := context.Background()

	creds, err := readPolarisCreds()
	require.NoError(t, err)
	credential := creds.ClientID + ":" + creds.ClientSecret
	scope := "PRINCIPAL_ROLE:flow_user_role"

	cat, err := catalog.New(ctx, "http://localhost:9802/api/catalog", "quickstart_catalog",
		catalog.WithClientCredential(credential, "v1/oauth/tokens", &scope))
	require.NoError(t, err)

	const (
		ns        = "ts_overflow_regression"
		tableName = "profile"
	)

	// Tolerate leftovers from a prior run.
	_ = cat.DeleteTable(ctx, ns, tableName)
	if err := cat.CreateNamespace(ctx, ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	schema := iceberg.NewSchemaWithIdentifiers(0, nil, iceberg.NestedField{
		ID:       1,
		Name:     "ts",
		Type:     iceberg.PrimitiveTypes.TimestampTz,
		Required: false,
	})
	require.NoError(t, cat.CreateTable(ctx, ns, tableName, schema, nil, nil, nil))
	t.Cleanup(func() { _ = cat.DeleteTable(context.Background(), ns, tableName) })

	// "9999-12-31T23:59:59-14:00" normalizes to UTC year 10000.
	csvPath := filepath.Join(t.TempDir(), "data.csv.gz")
	csvFile, err := os.Create(csvPath)
	require.NoError(t, err)
	csvw := writer.NewCsvWriter(csvFile, []string{"ts"},
		writer.WithCsvSkipHeaders(), writer.WithCsvQuoteChar('`'))
	require.NoError(t, csvw.Write([]any{"9999-12-31T23:59:59-14:00"}))
	require.NoError(t, csvw.Close())

	csvBody, err := os.ReadFile(csvPath)
	require.NoError(t, err)

	s3client := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("flow", "flow", ""),
		BaseEndpoint: aws.String("http://localhost:9800"),
		UsePathStyle: true,
	})
	csvKey := "staging/ts-overflow-" + uuid.New().String() + ".csv.gz"
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("warehouse"),
		Key:    aws.String(csvKey),
		Body:   bytes.NewReader(csvBody),
	})
	require.NoError(t, err)

	mergeInput := python.MergeInput{
		Bindings: []python.MergeBinding{{
			Binding: 0,
			Query:   fmt.Sprintf("INSERT INTO estuary.%s.%s SELECT ts FROM merge_view_0", ns, tableName),
			Columns: []python.NestedField{{Name: "ts", Type: "timestamptz"}},
			Files:   []string{"s3://warehouse/" + csvKey},
		}},
	}
	body, err := json.Marshal(struct {
		Action string             `json:"action"`
		Input  python.MergeInput `json:"input"`
	}{Action: "merge", Input: mergeInput})
	require.NoError(t, err)

	resp, err := http.Post("http://localhost:9806/run", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "daemon response: %s", respBody)

	var result struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(respBody, &result))
	require.Truef(t, result.Success, "merge failed: %s", result.Error)
}

// runDateOverflowRegression is the date analog of
// runTimestampOverflowRegression - same year-10000 question, INT32-days
// encoding instead of INT64-micros.
func runDateOverflowRegression(t *testing.T) {
	ctx := context.Background()

	creds, err := readPolarisCreds()
	require.NoError(t, err)
	credential := creds.ClientID + ":" + creds.ClientSecret
	scope := "PRINCIPAL_ROLE:flow_user_role"

	cat, err := catalog.New(ctx, "http://localhost:9802/api/catalog", "quickstart_catalog",
		catalog.WithClientCredential(credential, "v1/oauth/tokens", &scope))
	require.NoError(t, err)

	const (
		ns        = "date_overflow_regression"
		tableName = "profile"
	)

	_ = cat.DeleteTable(ctx, ns, tableName)
	if err := cat.CreateNamespace(ctx, ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	schema := iceberg.NewSchemaWithIdentifiers(0, nil, iceberg.NestedField{
		ID:       1,
		Name:     "d",
		Type:     iceberg.PrimitiveTypes.Date,
		Required: false,
	})
	require.NoError(t, cat.CreateTable(ctx, ns, tableName, schema, nil, nil, nil))
	t.Cleanup(func() { _ = cat.DeleteTable(context.Background(), ns, tableName) })

	csvPath := filepath.Join(t.TempDir(), "data.csv.gz")
	csvFile, err := os.Create(csvPath)
	require.NoError(t, err)
	csvw := writer.NewCsvWriter(csvFile, []string{"d"},
		writer.WithCsvSkipHeaders(), writer.WithCsvQuoteChar('`'))
	require.NoError(t, csvw.Write([]any{"10000-01-01"}))
	require.NoError(t, csvw.Close())

	csvBody, err := os.ReadFile(csvPath)
	require.NoError(t, err)

	s3client := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("flow", "flow", ""),
		BaseEndpoint: aws.String("http://localhost:9800"),
		UsePathStyle: true,
	})
	csvKey := "staging/date-overflow-" + uuid.New().String() + ".csv.gz"
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("warehouse"),
		Key:    aws.String(csvKey),
		Body:   bytes.NewReader(csvBody),
	})
	require.NoError(t, err)

	mergeInput := python.MergeInput{
		Bindings: []python.MergeBinding{{
			Binding: 0,
			Query:   fmt.Sprintf("INSERT INTO estuary.%s.%s SELECT d FROM merge_view_0", ns, tableName),
			Columns: []python.NestedField{{Name: "d", Type: "date"}},
			Files:   []string{"s3://warehouse/" + csvKey},
		}},
	}
	body, err := json.Marshal(struct {
		Action string            `json:"action"`
		Input  python.MergeInput `json:"input"`
	}{Action: "merge", Input: mergeInput})
	require.NoError(t, err)

	resp, err := http.Post("http://localhost:9806/run", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "daemon response: %s", respBody)

	var result struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(respBody, &result))
	require.Truef(t, result.Success, "merge failed: %s", result.Error)
}

// runEmptyStringNullCountRegression verifies that empty strings written by the
// Go connector survive the CSV → Spark → Iceberg pipeline without becoming
// NULLs. The bug was "emptyValue": '""' in read_csv_opts, which set the marker
// to two double-quote characters instead of the default empty string, causing
// Spark to fall through to nullValue="" and convert every backtick-quoted empty
// field to NULL.
func runEmptyStringNullCountRegression(t *testing.T) {
	ctx := context.Background()

	creds, err := readPolarisCreds()
	require.NoError(t, err)
	credential := creds.ClientID + ":" + creds.ClientSecret
	scope := "PRINCIPAL_ROLE:flow_user_role"

	cat, err := catalog.New(ctx, "http://localhost:9802/api/catalog", "quickstart_catalog",
		catalog.WithClientCredential(credential, "v1/oauth/tokens", &scope))
	require.NoError(t, err)

	const (
		ns        = "empty_string_null_count"
		tableName = "events"
	)

	_ = cat.DeleteTable(ctx, ns, tableName)
	if err := cat.CreateNamespace(ctx, ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	// Required (non-nullable) string column. Snowflake/Bauplan rejects the table
	// if null_value_counts[body] > 0 because the column is declared NOT NULL.
	schema := iceberg.NewSchemaWithIdentifiers(0, nil, iceberg.NestedField{
		ID:       1,
		Name:     "body",
		Type:     iceberg.PrimitiveTypes.String,
		Required: true,
	})
	require.NoError(t, cat.CreateTable(ctx, ns, tableName, schema, nil, nil, defaultTableProperties()))
	t.Cleanup(func() { _ = cat.DeleteTable(context.Background(), ns, tableName) })

	// Write one row where body is empty string using the same CsvWriter the
	// connector uses (backtick quoting, no headers).
	csvPath := filepath.Join(t.TempDir(), "data.csv.gz")
	csvFile, err := os.Create(csvPath)
	require.NoError(t, err)
	csvw := writer.NewCsvWriter(csvFile, []string{"body"},
		writer.WithCsvSkipHeaders(), writer.WithCsvQuoteChar('`'))
	require.NoError(t, csvw.Write([]any{""}))
	require.NoError(t, csvw.Close())

	csvBody, err := os.ReadFile(csvPath)
	require.NoError(t, err)

	s3client := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("flow", "flow", ""),
		BaseEndpoint: aws.String("http://localhost:9800"),
		UsePathStyle: true,
	})
	csvKey := "staging/empty-string-" + uuid.New().String() + ".csv.gz"
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("warehouse"),
		Key:    aws.String(csvKey),
		Body:   bytes.NewReader(csvBody),
	})
	require.NoError(t, err)

	mergeInput := python.MergeInput{
		Bindings: []python.MergeBinding{{
			Binding: 0,
			Query:   fmt.Sprintf("INSERT INTO estuary.%s.%s SELECT body FROM merge_view_0", ns, tableName),
			Columns: []python.NestedField{{Name: "body", Type: "string"}},
			Files:   []string{"s3://warehouse/" + csvKey},
		}},
	}
	reqBody, err := json.Marshal(struct {
		Action string            `json:"action"`
		Input  python.MergeInput `json:"input"`
	}{Action: "merge", Input: mergeInput})
	require.NoError(t, err)

	mergeResp, err := http.Post("http://localhost:9806/run", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	defer mergeResp.Body.Close()
	mergeRespBody, err := io.ReadAll(mergeResp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, mergeResp.StatusCode, "daemon response: %s", mergeRespBody)

	var mergeResult struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(mergeRespBody, &mergeResult))
	require.Truef(t, mergeResult.Success, "merge failed: %s", mergeResult.Error)

	// Load the updated table metadata to get the current snapshot's manifest list.
	tbl, err := cat.GetTable(ctx, ns, tableName)
	require.NoError(t, err)
	snap := tbl.Metadata.CurrentSnapshot()
	require.NotNil(t, snap, "expected a current snapshot after merge")

	// Download and parse the manifest list (an Avro file pointed to by the snapshot).
	fio := &testS3FileIO{client: s3client, ctx: ctx}
	snapBucket, snapKey := s3UriToParts(snap.ManifestList)
	snapObj, err := s3client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(snapBucket), Key: aws.String(snapKey)})
	require.NoError(t, err)
	snapBytes, err := io.ReadAll(snapObj.Body)
	snapObj.Body.Close()
	require.NoError(t, err)

	manifests, err := iceberg.ReadManifestList(bytes.NewReader(snapBytes))
	require.NoError(t, err)
	require.NotEmpty(t, manifests, "snapshot has no manifest files")

	// Walk every data-file entry and verify null_value_counts is present and zero
	// for body (ID=1). Absent null_value_counts is distinct from zero: external
	// engines (Bauplan, Snowflake) treat absent stats on a NOT NULL column as
	// "unknown = might have nulls" and reject the table even when no nulls exist.
	const bodyColumnID = 1
	for _, mf := range manifests {
		entries, err := mf.FetchEntries(fio, true)
		require.NoError(t, err)
		for _, entry := range entries {
			nullCounts := entry.DataFile().NullValueCounts()
			require.NotNilf(t, nullCounts,
				"null_value_counts completely absent in %s — metrics not being tracked (metrics mode bug)",
				mf.FilePath())
			n := nullCounts[bodyColumnID]
			require.Zerof(t, n,
				"body column (id=%d) has %d null(s) in %s — empty strings are being written as NULLs (emptyValue bug)",
				bodyColumnID, n, mf.FilePath())
		}
	}
}

// testS3FileIO is an iceio.IO backed by a local MinIO S3 instance. It is used
// to open Iceberg manifest files when verifying null_value_counts in tests.
type testS3FileIO struct {
	client *s3.Client
	ctx    context.Context
}

func (f *testS3FileIO) Open(name string) (iceio.File, error) {
	bucket, key := s3UriToParts(name)
	obj, err := f.client.GetObject(f.ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	defer obj.Body.Close()
	data, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return &s3BufFile{
		Reader: bytes.NewReader(data),
		name:   filepath.Base(name),
		size:   int64(len(data)),
	}, nil
}

func (f *testS3FileIO) Remove(name string) error { return nil }

// s3BufFile implements iceio.File (fs.File + io.ReadSeekCloser + io.ReaderAt)
// over an in-memory buffer downloaded from S3.
type s3BufFile struct {
	*bytes.Reader
	name string
	size int64
}

func (f *s3BufFile) Close() error               { return nil }
func (f *s3BufFile) Stat() (fs.FileInfo, error) { return &s3FileInfo{name: f.name, size: f.size}, nil }

type s3FileInfo struct {
	name string
	size int64
}

func (fi *s3FileInfo) Name() string       { return fi.name }
func (fi *s3FileInfo) Size() int64        { return fi.size }
func (fi *s3FileInfo) Mode() fs.FileMode  { return 0o444 }
func (fi *s3FileInfo) ModTime() time.Time { return time.Time{} }
func (fi *s3FileInfo) IsDir() bool        { return false }
func (fi *s3FileInfo) Sys() any           { return nil }
