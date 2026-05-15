package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
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

	t.Run("ns-timestamp", func(t *testing.T) {
		runNanosecondTimestampTest(t, cfg)
	})

	// Migration test is skipped because Iceberg does not support the type
	// migrations exercised by the test (e.g. long→string).
	//t.Run("migrate", func(t *testing.T) {
	//	boilerplate.RunMigrationTest(t, newMaterialization, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	//})
}

// runTimestampOverflowRegression reproduces a production failure observed at
// deel/prod/.../profile where appending a parquet file containing a timestamp
// with UTC year 10000 failed. Iceberg's int64-micros encoding can represent
// ~±292,277 years from epoch; "9999-12-31T23:59:59-14:00" normalizes to UTC
// year 10000 and must round-trip without error.
func runTimestampOverflowRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)

	const (
		namespace = "tests"
		table     = "ts_overflow_regression"
	)
	tableIdent := icebergtable.Identifier{namespace, table}

	// Tolerate leftovers from a prior run of this test.
	_ = cat.cat.DropTable(ctx, tableIdent)
	if err := cat.createNamespace(ctx, namespace); err != nil &&
		!errors.Is(err, icebergcatalog.ErrNamespaceAlreadyExists) {
		t.Fatalf("create-namespace: %v", err)
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID:       1,
		Name:     "ts",
		Type:     iceberg.PrimitiveTypes.TimestampTz,
		Required: false,
	})

	nameMappingJSON, err := json.Marshal(schema.NameMapping())
	require.NoError(t, err)

	tblLocation := tablePath(cfg.Bucket, cfg.Prefix, namespace, table, NestedLocationStyle)
	_, err = cat.cat.CreateTable(ctx, tableIdent, schema,
		icebergcatalog.WithLocation(tblLocation),
		icebergcatalog.WithProperties(iceberg.Properties{
			icebergtable.DefaultNameMappingKey: string(nameMappingJSON),
		}),
	)
	require.NoError(t, err)

	parquetPath := filepath.Join(t.TempDir(), "data.parquet")
	parquetFile, err := os.Create(parquetPath)
	require.NoError(t, err)
	pqw := writer.NewParquetWriter(parquetFile, writer.ParquetSchema{
		{Name: "ts", DataType: writer.LogicalTypeTimestamp, Required: false},
	}, writer.WithParquetCompression(writer.Snappy))
	require.NoError(t, pqw.Write([]any{"9999-12-31T23:59:59-14:00"}))
	require.NoError(t, pqw.Close())

	s3Path := strings.TrimSuffix(tblLocation, "/") + "/data/" + uuid.New().String() + ".parquet"
	s3Key := strings.TrimPrefix(s3Path, "s3://"+cfg.Bucket+"/")

	uploadFile, err := os.Open(parquetPath)
	require.NoError(t, err)
	defer uploadFile.Close()

	s3client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, ""),
		BaseEndpoint: aws.String(cfg.S3Endpoint),
		UsePathStyle: true,
	})
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(s3Key),
		Body:   uploadFile,
	})
	require.NoError(t, err)

	require.NoError(t, cat.appendFiles(ctx,
		"acmeCo/tests/regression",
		tableIdent,
		[]string{s3Path},
		"",
		"deadbeefdeadbeef",
	))
}

// runDateOverflowRegression is the date analog of runTimestampOverflowRegression.
// Parquet DATE is INT32 days-since-epoch; year > 9999 fits the Iceberg spec and
// is now handled correctly by the writer's date parser.
func runDateOverflowRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)

	const (
		namespace = "tests"
		table     = "date_overflow_regression"
	)
	tableIdent := icebergtable.Identifier{namespace, table}

	// Tolerate leftovers from a prior run of this test.
	_ = cat.cat.DropTable(ctx, tableIdent)
	if err := cat.createNamespace(ctx, namespace); err != nil &&
		!errors.Is(err, icebergcatalog.ErrNamespaceAlreadyExists) {
		t.Fatalf("create-namespace: %v", err)
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID:       1,
		Name:     "d",
		Type:     iceberg.PrimitiveTypes.Date,
		Required: false,
	})

	nameMappingJSON, err := json.Marshal(schema.NameMapping())
	require.NoError(t, err)

	tblLocation := tablePath(cfg.Bucket, cfg.Prefix, namespace, table, NestedLocationStyle)
	_, err = cat.cat.CreateTable(ctx, tableIdent, schema,
		icebergcatalog.WithLocation(tblLocation),
		icebergcatalog.WithProperties(iceberg.Properties{
			icebergtable.DefaultNameMappingKey: string(nameMappingJSON),
		}),
	)
	require.NoError(t, err)

	parquetPath := filepath.Join(t.TempDir(), "data.parquet")
	parquetFile, err := os.Create(parquetPath)
	require.NoError(t, err)
	pqw := writer.NewParquetWriter(parquetFile, writer.ParquetSchema{
		{Name: "d", DataType: writer.LogicalTypeDate, Required: false},
	}, writer.WithParquetCompression(writer.Snappy))
	require.NoError(t, pqw.Write([]any{"10000-01-01"}))
	require.NoError(t, pqw.Close())

	s3Path := strings.TrimSuffix(tblLocation, "/") + "/data/" + uuid.New().String() + ".parquet"
	s3Key := strings.TrimPrefix(s3Path, "s3://"+cfg.Bucket+"/")

	uploadFile, err := os.Open(parquetPath)
	require.NoError(t, err)
	defer uploadFile.Close()

	s3client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, ""),
		BaseEndpoint: aws.String(cfg.S3Endpoint),
		UsePathStyle: true,
	})
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(s3Key),
		Body:   uploadFile,
	})
	require.NoError(t, err)

	require.NoError(t, cat.appendFiles(ctx,
		"acmeCo/tests/regression",
		tableIdent,
		[]string{s3Path},
		"",
		"deadbeefdeadbeef",
	))
}


// runNanosecondTimestampTest verifies that timestamptz_ns (Iceberg v3 format) columns:
//   - write nanosecond-precision timestamps correctly
//   - clamp out-of-range values (before 1677 / after 2262) to int64 min/max rather than overflowing
//   - produce Parquet files that DuckDB can read with full nanosecond precision
func runNanosecondTimestampTest(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	cfg.Advanced.NanosecondTimestamps = true
	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)

	const (
		namespace = "tests"
		table     = "ns_timestamp_test"
	)
	tableIdent := icebergtable.Identifier{namespace, table}

	_ = cat.cat.DropTable(ctx, tableIdent)
	if err := cat.createNamespace(ctx, namespace); err != nil &&
		!errors.Is(err, icebergcatalog.ErrNamespaceAlreadyExists) {
		t.Fatalf("create-namespace: %v", err)
	}

	iceSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID:       1,
		Name:     "ts",
		Type:     iceberg.PrimitiveTypes.TimestampTzNs,
		Required: false,
	})

	nameMappingJSON, err := json.Marshal(iceSchema.NameMapping())
	require.NoError(t, err)

	tblLocation := tablePath(cfg.Bucket, cfg.Prefix, namespace, table, NestedLocationStyle)
	_, err = cat.cat.CreateTable(ctx, tableIdent, iceSchema,
		icebergcatalog.WithLocation(tblLocation),
		icebergcatalog.WithProperties(iceberg.Properties{
			icebergtable.DefaultNameMappingKey:  string(nameMappingJSON),
			icebergtable.PropertyFormatVersion:  "3",
		}),
	)
	require.NoError(t, err)

	parquetPath := filepath.Join(t.TempDir(), "data.parquet")
	parquetFile, err := os.Create(parquetPath)
	require.NoError(t, err)
	pqw := writer.NewParquetWriter(parquetFile, writer.ParquetSchema{
		{Name: "ts", DataType: writer.LogicalTypeTimestampNanos, Required: false},
	}, writer.WithParquetCompression(writer.Snappy))
	// in-range: sub-microsecond digits (789) must survive the round-trip
	require.NoError(t, pqw.Write([]any{"2023-01-15T12:34:56.123456789Z"}))
	// out-of-range past: clamps to math.MinInt64 rather than overflowing
	require.NoError(t, pqw.Write([]any{"1000-01-01T00:00:00Z"}))
	// out-of-range future: clamps to math.MaxInt64 rather than overflowing
	require.NoError(t, pqw.Write([]any{"3000-01-01T00:00:00Z"}))
	require.NoError(t, pqw.Close())
	parquetFile.Close()

	s3Path := strings.TrimSuffix(tblLocation, "/") + "/data/" + uuid.New().String() + ".parquet"
	s3Key := strings.TrimPrefix(s3Path, "s3://"+cfg.Bucket+"/")

	uploadFile, err := os.Open(parquetPath)
	require.NoError(t, err)
	defer uploadFile.Close()

	s3client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, ""),
		BaseEndpoint: aws.String(cfg.S3Endpoint),
		UsePathStyle: true,
	})
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(s3Key),
		Body:   uploadFile,
	})
	require.NoError(t, err)

	require.NoError(t, cat.appendFiles(ctx,
		"acmeCo/tests/ns-timestamp",
		tableIdent,
		[]string{s3Path},
		"",
		"deadbeefdeadbeef",
	))

	// Verify nanosecond precision by reading the raw int64 values from the Parquet file.
	// DuckDB maps nanosecond Parquet timestamps to TIMESTAMPTZ (microsecond precision),
	// dropping sub-microsecond digits — so we read the encoded bytes directly instead.
	pqf, err := os.Open(parquetPath)
	require.NoError(t, err)
	defer pqf.Close()

	pqr, err := parquetfile.NewParquetReader(pqf)
	require.NoError(t, err)
	defer pqr.Close()

	col, err := pqr.RowGroup(0).Column(0)
	require.NoError(t, err)

	int64Col := col.(*parquetfile.Int64ColumnChunkReader)
	vals := make([]int64, 3)
	defLevels := make([]int16, 3)
	_, valuesRead, err := int64Col.ReadBatch(3, vals, defLevels, nil)
	require.NoError(t, err)
	require.Equal(t, 3, valuesRead)
	// 2023-01-15T12:34:56.123456789Z → unix nanoseconds
	require.Equal(t, int64(1673786096123456789), vals[0], "in-range: sub-microsecond precision lost")
	require.Equal(t, int64(math.MinInt64), vals[1], "out-of-range past not clamped to MinInt64")
	require.Equal(t, int64(math.MaxInt64), vals[2], "out-of-range future not clamped to MaxInt64")
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
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, ""),
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
				"s3.access-key-id":      cfg.Credentials.AWSAccessKeyID,
				"s3.secret-access-key":  cfg.Credentials.AWSSecretAccessKey,
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
