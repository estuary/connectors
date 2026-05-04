package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const (
	polarisCatalogURL    = "http://localhost:9700/api/catalog"
	polarisManagementURL = "http://localhost:9700/api/management/v1"
	polarisClientID      = "root"
	polarisClientSecret  = "s3cr3t"
)

// TestMain resolves PYTHON_PATH from the iceberg-ctl Poetry venv if it isn't
// already set, so `go test` works without extra env wrangling. The Docker
// image sets PYTHON_PATH explicitly; this is just for local runs.
func TestMain(m *testing.M) {
	if _, ok := os.LookupEnv("PYTHON_PATH"); !ok {
		cmd := exec.Command("poetry", "env", "info", "--path")
		cmd.Dir = "iceberg-ctl"
		if out, err := cmd.Output(); err == nil {
			venv := strings.TrimSpace(string(out))
			if venv != "" {
				os.Setenv("PYTHON_PATH", venv+"/bin/python")
			}
		}
	}
	os.Exit(m.Run())
}

func TestSpec(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var resp, err = driver{}.
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Start the Iceberg REST catalog (+ PostgreSQL).
	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	cfg := loadTestConfig(t)

	// Create the rustfs bucket and the Polaris catalog backed by it.
	createTestBucket(t, cfg)
	createTestWarehouse(t, cfg)

	d := true
	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table, Delta: &d}
	}

	// Sanitize S3 file paths and table hashes that contain random UUIDs.
	sanitizers := []func(string) string{
		func(s string) string {
			// Replace UUIDs in S3 parquet file paths.
			re := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.parquet`)
			return re.ReplaceAllString(s, "<uuid>.parquet")
		},
		func(s string) string {
			// Replace table name hashes (16 hex chars).
			re := regexp.MustCompile(`_([\dA-F]{16})/data/`)
			return re.ReplaceAllString(s, "_<hash>/data/")
		},
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize.flow.yaml", makeResourceFn, sanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, newMaterialization, "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("ts-overflow-regression", func(t *testing.T) {
		runTimestampOverflowRegression(t, cfg)
	})

	t.Run("date-overflow-regression", func(t *testing.T) {
		runDateOverflowRegression(t, cfg)
	})

	// Migration test is skipped because Iceberg does not support the type
	// migrations exercised by the test (e.g. long→string). pyiceberg rejects
	// schema mismatches when appending files with migrated types.
	//t.Run("migrate", func(t *testing.T) {
	//	boilerplate.RunMigrationTest(t, newMaterialization, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	//})
}

// runTimestampOverflowRegression reproduces the deel/prod/.../profile failure
// where iceberg_ctl `append_files` aborts with `OverflowError: date value out
// of range`: pyiceberg reads the parquet column's min/max stats through
// pyarrow, which materializes a Python `datetime` capped at MAXYEAR=9999.
// Iceberg's INT64-micros encoding admits much wider years; the input value
// here normalizes to UTC year 10000.
func runTimestampOverflowRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	const (
		namespace = "tests"
		table     = "ts_overflow_regression"
	)
	fqn := namespace + "." + table

	// Tolerate leftovers from a prior run.
	_, _ = runIcebergctl(ctx, &cfg, "drop-table", fqn)
	if _, err := runIcebergctl(ctx, &cfg, "create-namespace", namespace); err != nil &&
		!strings.Contains(err.Error(), "already exists") &&
		!strings.Contains(err.Error(), "AlreadyExists") {
		t.Fatalf("create-namespace: %v", err)
	}

	tblLocation := tablePath(cfg.Bucket, cfg.Prefix, namespace, table, NestedLocationStyle)

	tcJson, err := json.Marshal(tableCreate{
		Location: tblLocation,
		Fields: []existingIcebergColumn{
			{Name: "ts", Nullable: true, Type: icebergTypeTimestamptz},
		},
	})
	require.NoError(t, err)
	_, err = runIcebergctl(ctx, &cfg, "create-table", fqn, string(tcJson))
	require.NoError(t, err)

	// "9999-12-31T23:59:59-14:00" normalizes to UTC year 10000.
	parquetPath := filepath.Join(t.TempDir(), "data.parquet")
	parquetFile, err := os.Create(parquetPath)
	require.NoError(t, err)
	pqw := writer.NewParquetWriter(parquetFile, writer.ParquetSchema{
		{Name: "ts", DataType: writer.LogicalTypeTimestamp, Required: false},
	}, writer.WithParquetCompression(writer.Snappy))
	clamped, ok := clampTimestamp("9999-12-31T23:59:59-14:00")
	require.True(t, ok, "test value should require clamping")
	require.NoError(t, pqw.Write([]any{clamped}))
	require.NoError(t, pqw.Close())

	s3Path := strings.TrimSuffix(tblLocation, "/") + "/data/" + uuid.New().String() + ".parquet"
	s3Key := strings.TrimPrefix(s3Path, "s3://"+cfg.Bucket+"/")

	uploadFile, err := os.Open(parquetPath)
	require.NoError(t, err)
	defer uploadFile.Close()

	s3client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, ""),
		BaseEndpoint: aws.String(cfg.S3Endpoint),
		UsePathStyle: true,
	})
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(s3Key),
		Body:   uploadFile,
	})
	require.NoError(t, err)

	_, err = runIcebergctl(ctx, &cfg, "append-files",
		"acmeCo/tests/regression",
		fqn,
		"",
		"deadbeefdeadbeef",
		s3Path,
	)
	require.NoError(t, err, "append-files should accept a timestamp within Iceberg's range")
}

// runDateOverflowRegression is the date analog of
// runTimestampOverflowRegression. Parquet DATE is INT32 days-since-epoch;
// year > 9999 fits the spec but overflows pyarrow's Date32Scalar.as_py.
// getDateVal rejects > 4-digit years at parse time today, so the failure
// surfaces earlier than the pyiceberg call.
func runDateOverflowRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	const (
		namespace = "tests"
		table     = "date_overflow_regression"
	)
	fqn := namespace + "." + table

	_, _ = runIcebergctl(ctx, &cfg, "drop-table", fqn)
	if _, err := runIcebergctl(ctx, &cfg, "create-namespace", namespace); err != nil &&
		!strings.Contains(err.Error(), "already exists") &&
		!strings.Contains(err.Error(), "AlreadyExists") {
		t.Fatalf("create-namespace: %v", err)
	}

	tblLocation := tablePath(cfg.Bucket, cfg.Prefix, namespace, table, NestedLocationStyle)

	tcJson, err := json.Marshal(tableCreate{
		Location: tblLocation,
		Fields: []existingIcebergColumn{
			{Name: "d", Nullable: true, Type: icebergTypeDate},
		},
	})
	require.NoError(t, err)
	_, err = runIcebergctl(ctx, &cfg, "create-table", fqn, string(tcJson))
	require.NoError(t, err)

	parquetPath := filepath.Join(t.TempDir(), "data.parquet")
	parquetFile, err := os.Create(parquetPath)
	require.NoError(t, err)
	pqw := writer.NewParquetWriter(parquetFile, writer.ParquetSchema{
		{Name: "d", DataType: writer.LogicalTypeDate, Required: false},
	}, writer.WithParquetCompression(writer.Snappy))
	clamped, ok := clampDate("10000-01-01")
	require.True(t, ok, "test value should require clamping")
	require.NoError(t, pqw.Write([]any{clamped}))
	require.NoError(t, pqw.Close())

	s3Path := strings.TrimSuffix(tblLocation, "/") + "/data/" + uuid.New().String() + ".parquet"
	s3Key := strings.TrimPrefix(s3Path, "s3://"+cfg.Bucket+"/")

	uploadFile, err := os.Open(parquetPath)
	require.NoError(t, err)
	defer uploadFile.Close()

	s3client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, ""),
		BaseEndpoint: aws.String(cfg.S3Endpoint),
		UsePathStyle: true,
	})
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(s3Key),
		Body:   uploadFile,
	})
	require.NoError(t, err)

	_, err = runIcebergctl(ctx, &cfg, "append-files",
		"acmeCo/tests/regression",
		fqn,
		"",
		"deadbeefdeadbeef",
		s3Path,
	)
	require.NoError(t, err, "append-files should accept a date within Iceberg's range")
}

func loadTestConfig(t *testing.T) config {
	t.Helper()

	bundled := boilerplate.RunFlowctl(t, "raw", "bundle", "--source", "testdata/materialize.flow.yaml")
	taskName := "acmeCo/tests/materialize-s3-iceberg"

	raw := json.RawMessage(gjson.GetBytes(bundled, "materializations."+taskName+".endpoint.local.config").Raw)
	require.NotEmpty(t, raw, "could not find config in bundled spec")

	var cfg config
	require.NoError(t, pf.UnmarshalStrict(raw, &cfg))
	return cfg
}

func createTestBucket(t *testing.T, cfg config) {
	t.Helper()

	ctx := context.Background()
	client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, ""),
		BaseEndpoint: aws.String(cfg.S3Endpoint),
		UsePathStyle: true,
	})

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		// Tolerate already-exists; rustfs bucket persists across container restarts
		// only when /data is a volume. Even so, surface unexpected errors.
		if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
			!strings.Contains(err.Error(), "BucketAlreadyExists") {
			t.Fatalf("creating rustfs bucket %q: %v", cfg.Bucket, err)
		}
	}

	t.Cleanup(func() {
		// Best-effort wipe so reruns start clean.
		paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(cfg.Bucket)})
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return
			}
			for _, obj := range page.Contents {
				_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(cfg.Bucket), Key: obj.Key})
			}
		}
	})
}

func polarisToken(t *testing.T) string {
	t.Helper()

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", polarisClientID)
	form.Set("client_secret", polarisClientSecret)
	form.Set("scope", "PRINCIPAL_ROLE:ALL")

	var resp *http.Response
	var err error
	for i := 0; i < 30; i++ {
		resp, err = http.Post(
			polarisCatalogURL+"/v1/oauth/tokens",
			"application/x-www-form-urlencoded",
			strings.NewReader(form.Encode()),
		)
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "token response: %s", body)

	tok := gjson.GetBytes(body, "access_token").String()
	require.NotEmpty(t, tok, "missing access_token in response: %s", body)
	return tok
}

func createTestWarehouse(t *testing.T, cfg config) {
	t.Helper()

	token := polarisToken(t)

	baseLocation := "s3://" + cfg.Bucket + "/" + cfg.Prefix

	body, err := json.Marshal(map[string]any{
		"catalog": map[string]any{
			"name":     cfg.Catalog.Warehouse,
			"type":     "INTERNAL",
			"readOnly": false,
			"properties": map[string]any{
				"default-base-location": baseLocation,
				"s3.endpoint":           cfg.S3Endpoint,
				"s3.access-key-id":      cfg.AWSAccessKeyID,
				"s3.secret-access-key":  cfg.AWSSecretAccessKey,
				"s3.region":             cfg.Region,
				"s3.path-style-access":  "true",
			},
			"storageConfigInfo": map[string]any{
				"storageType":      "S3",
				"allowedLocations": []string{"s3://" + cfg.Bucket + "/"},
				"roleArn":          "arn:aws:iam::000000000000:role/dummy",
				"region":           cfg.Region,
				// Client-visible endpoint (host port mapped from rustfs).
				"endpoint": cfg.S3Endpoint,
				// Polaris-side endpoint uses the docker-network name.
				"endpointInternal": "http://rustfs:9000",
				"pathStyleAccess":  true,
				// Disable STS-based credential vending: rustfs has no STS.
				// Clients will use the s3.* properties on the catalog instead.
				"stsUnavailable": true,
			},
		},
	})
	require.NoError(t, err)

	// Drop a previous catalog if one exists from an earlier failed run.
	delReq, _ := http.NewRequest(http.MethodDelete, polarisManagementURL+"/catalogs/"+cfg.Catalog.Warehouse, nil)
	delReq.Header.Set("Authorization", "Bearer "+token)
	if delResp, err := http.DefaultClient.Do(delReq); err == nil {
		delResp.Body.Close()
	}

	req, err := http.NewRequest(http.MethodPost, polarisManagementURL+"/catalogs", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("creating polaris catalog failed with status %d: %s", resp.StatusCode, respBody)
	}

	// Grant the bootstrap principal full access on the catalog so the
	// connector (using the same OAuth client) can read/write everything.
	grantPrincipalRole(t, token, "service_admin", cfg.Catalog.Warehouse, "catalog_admin")
}

func grantPrincipalRole(t *testing.T, token, principalRole, catalog, catalogRole string) {
	t.Helper()

	url := polarisManagementURL + "/principal-roles/" + principalRole + "/catalog-roles/" + catalog
	body, err := json.Marshal(map[string]any{
		"catalogRole": map[string]any{"name": catalogRole},
	})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Truef(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent,
		"granting principal role failed: status %d body %s", resp.StatusCode, respBody)
}
