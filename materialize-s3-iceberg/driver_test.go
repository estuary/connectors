package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

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
		r := resource{Table: table, Delta: &d}
		// The harness suffixes the table name with a random run identifier, so
		// match on the configured prefix to attach partition specs.
		switch {
		case strings.HasPrefix(table, "dt_part_bucket"):
			r.PartitionFields = []partitionField{
				{Field: "id", Transform: "bucket[4]"},
			}
		case strings.HasPrefix(table, "dt_part_truncate"):
			r.PartitionFields = []partitionField{
				{Field: "stringField", Transform: "truncate[4]"},
			}
		case strings.HasPrefix(table, "dt_part_date"):
			r.PartitionFields = []partitionField{
				{Field: "stringDateField", Transform: "identity"},
			}
		case strings.HasPrefix(table, "dt_part_multi"):
			r.PartitionFields = []partitionField{
				{Field: "boolField", Transform: "identity"},
				{Field: "stringDateTimeField", Transform: "identity"},
				{Field: "id", Transform: "bucket[2]"},
			}
		}
		return r
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

	t.Run("partition-append-regression", func(t *testing.T) {
		runPartitionAppendRegression(t, cfg)
	})

	t.Run("partition-temporal-regression", func(t *testing.T) {
		runTemporalPartitionRegression(t, cfg)
	})

	t.Run("partition-spec-mismatch-regression", func(t *testing.T) {
		runPartitionSpecMismatchRegression(t, cfg)
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

// runPartitionAppendRegression exercises the partitioned append path end to end:
// it creates a table with a bucket partition spec (a non-linear transform that
// AddFiles cannot infer), appends parquet files with explicit partition values
// via appendDataFiles, and verifies that a re-append with the same checkpoint is
// idempotent (produces no new snapshot).
func runPartitionAppendRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)

	const (
		namespace = "tests"
		table     = "partition_append_regression"
	)
	tableIdent := icebergtable.Identifier{namespace, table}

	_ = cat.cat.DropTable(ctx, tableIdent)
	if err := cat.createNamespace(ctx, namespace); err != nil &&
		!errors.Is(err, icebergcatalog.ErrNamespaceAlreadyExists) {
		t.Fatalf("create-namespace: %v", err)
	}

	pqSchema := writer.ParquetSchema{{Name: "id", DataType: writer.PrimitiveTypeInteger, Required: false}}
	spec, cols, err := buildPartitionSpec([]string{"id"}, pqSchema, []partitionField{
		{Field: "id", Transform: "bucket[4]"},
	})
	require.NoError(t, err)
	require.NotNil(t, spec)

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
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
		icebergcatalog.WithPartitionSpec(spec),
	)
	require.NoError(t, err)

	s3client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, ""),
		BaseEndpoint: aws.String(cfg.S3Endpoint),
		UsePathStyle: true,
	})

	writeFile := func(id int64) fileEntry {
		parquetPath := filepath.Join(t.TempDir(), fmt.Sprintf("data-%d.parquet", id))
		f, err := os.Create(parquetPath)
		require.NoError(t, err)
		pqw := writer.NewParquetWriter(f, pqSchema, writer.WithParquetCompression(writer.Snappy))
		require.NoError(t, pqw.Write([]any{id}))
		require.NoError(t, pqw.Close())

		fi, err := os.Stat(parquetPath)
		require.NoError(t, err)

		s3Path := strings.TrimSuffix(tblLocation, "/") + "/data/" + uuid.New().String() + ".parquet"
		s3Key := strings.TrimPrefix(s3Path, "s3://"+cfg.Bucket+"/")
		up, err := os.Open(parquetPath)
		require.NoError(t, err)
		defer up.Close()
		_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(cfg.Bucket),
			Key:    aws.String(s3Key),
			Body:   up,
		})
		require.NoError(t, err)

		_, fieldData, err := partitionValues([]any{id}, cols)
		require.NoError(t, err)
		part := make(map[string]json.RawMessage, len(fieldData))
		for fid, v := range fieldData {
			raw, err := json.Marshal(v)
			require.NoError(t, err)
			part[strconv.Itoa(fid)] = raw
		}
		return fileEntry{S3Path: s3Path, RecordCount: 1, FileSize: fi.Size(), Partition: part}
	}

	files := []fileEntry{writeFile(1), writeFile(7)}

	require.NoError(t, cat.appendDataFiles(ctx,
		"acmeCo/tests/regression", tableIdent, files, cols, "", "deadbeefdeadbeef"))

	tbl1, err := cat.cat.LoadTable(ctx, tableIdent)
	require.NoError(t, err)
	snap1 := tbl1.CurrentSnapshot()
	require.NotNil(t, snap1)

	// A re-append with the same next-checkpoint must be a no-op.
	require.NoError(t, cat.appendDataFiles(ctx,
		"acmeCo/tests/regression", tableIdent, files, cols, "", "deadbeefdeadbeef"))
	tbl2, err := cat.cat.LoadTable(ctx, tableIdent)
	require.NoError(t, err)
	snap2 := tbl2.CurrentSnapshot()
	require.NotNil(t, snap2)
	require.Equal(t, snap1.SnapshotID, snap2.SnapshotID, "re-append with same checkpoint must not create a new snapshot")
}

// runTemporalPartitionRegression exercises the year/month/day/hour/void
// transforms end to end against the real catalog: it creates a table partitioned
// by every temporal transform (plus void), appends a parquet file with the
// computed partition values via appendDataFiles, and verifies the commit
// succeeds and is idempotent. This covers the int32-result transforms (which
// carry no avro logical type) and void (which always produces a null partition).
func runTemporalPartitionRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)

	const (
		namespace = "tests"
		table     = "partition_temporal_regression"
	)
	tableIdent := icebergtable.Identifier{namespace, table}

	_ = cat.cat.DropTable(ctx, tableIdent)
	if err := cat.createNamespace(ctx, namespace); err != nil &&
		!errors.Is(err, icebergcatalog.ErrNamespaceAlreadyExists) {
		t.Fatalf("create-namespace: %v", err)
	}

	pqSchema := writer.ParquetSchema{
		{Name: "d", DataType: writer.LogicalTypeDate, Required: false},
		{Name: "ts", DataType: writer.LogicalTypeTimestamp, Required: false},
	}
	allFields := []string{"d", "ts"}
	spec, cols, err := buildPartitionSpec(allFields, pqSchema, []partitionField{
		{Field: "d", Transform: "year"},
		{Field: "d", Transform: "month"},
		{Field: "d", Transform: "day"},
		{Field: "ts", Transform: "hour"},
		{Field: "d", Transform: "void"},
	})
	require.NoError(t, err)
	require.NotNil(t, spec)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "d", Type: iceberg.PrimitiveTypes.Date, Required: false},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
	)
	nameMappingJSON, err := json.Marshal(schema.NameMapping())
	require.NoError(t, err)

	tblLocation := tablePath(cfg.Bucket, cfg.Prefix, namespace, table, NestedLocationStyle)
	_, err = cat.cat.CreateTable(ctx, tableIdent, schema,
		icebergcatalog.WithLocation(tblLocation),
		icebergcatalog.WithProperties(iceberg.Properties{
			icebergtable.DefaultNameMappingKey: string(nameMappingJSON),
		}),
		icebergcatalog.WithPartitionSpec(spec),
	)
	require.NoError(t, err)

	parquetPath := filepath.Join(t.TempDir(), "data.parquet")
	f, err := os.Create(parquetPath)
	require.NoError(t, err)
	pqw := writer.NewParquetWriter(f, pqSchema, writer.WithParquetCompression(writer.Snappy))
	row := []any{"2023-07-15", "2023-07-15T13:45:00Z"}
	require.NoError(t, pqw.Write(row))
	require.NoError(t, pqw.Close())
	fi, err := os.Stat(parquetPath)
	require.NoError(t, err)

	s3client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, ""),
		BaseEndpoint: aws.String(cfg.S3Endpoint),
		UsePathStyle: true,
	})
	s3Path := strings.TrimSuffix(tblLocation, "/") + "/data/" + uuid.New().String() + ".parquet"
	s3Key := strings.TrimPrefix(s3Path, "s3://"+cfg.Bucket+"/")
	up, err := os.Open(parquetPath)
	require.NoError(t, err)
	defer up.Close()
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(s3Key),
		Body:   up,
	})
	require.NoError(t, err)

	_, fieldData, err := partitionValues(row, cols)
	require.NoError(t, err)
	part := make(map[string]json.RawMessage, len(fieldData))
	for fid, v := range fieldData {
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		part[strconv.Itoa(fid)] = raw
	}
	files := []fileEntry{{S3Path: s3Path, RecordCount: 1, FileSize: fi.Size(), Partition: part}}

	require.NoError(t, cat.appendDataFiles(ctx,
		"acmeCo/tests/regression", tableIdent, files, cols, "", "deadbeefdeadbeef"))

	tbl1, err := cat.cat.LoadTable(ctx, tableIdent)
	require.NoError(t, err)
	snap1 := tbl1.CurrentSnapshot()
	require.NotNil(t, snap1)

	// Re-append with the same checkpoint must be a no-op.
	require.NoError(t, cat.appendDataFiles(ctx,
		"acmeCo/tests/regression", tableIdent, files, cols, "", "deadbeefdeadbeef"))
	tbl2, err := cat.cat.LoadTable(ctx, tableIdent)
	require.NoError(t, err)
	require.Equal(t, snap1.SnapshotID, tbl2.CurrentSnapshot().SnapshotID, "re-append with same checkpoint must not create a new snapshot")
}

// runPartitionSpecMismatchRegression verifies, against the real catalog, that the
// partition spec written when creating a table round-trips to a spec that
// compares equal to a freshly-built one for the same config (so a matching
// config is not falsely rejected on restart), and that a changed config — a
// different parameter, or dropping partitioning — is detected as a hard error.
func runPartitionSpecMismatchRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)

	const (
		namespace = "tests"
		table     = "partition_mismatch_regression"
	)
	tableIdent := icebergtable.Identifier{namespace, table}

	_ = cat.cat.DropTable(ctx, tableIdent)
	if err := cat.createNamespace(ctx, namespace); err != nil &&
		!errors.Is(err, icebergcatalog.ErrNamespaceAlreadyExists) {
		t.Fatalf("create-namespace: %v", err)
	}

	pqSchema := writer.ParquetSchema{{Name: "id", DataType: writer.PrimitiveTypeInteger, Required: false}}
	allFields := []string{"id"}
	specBucket4, _, err := buildPartitionSpec(allFields, pqSchema, []partitionField{
		{Field: "id", Transform: "bucket[4]"},
	})
	require.NoError(t, err)

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
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
		icebergcatalog.WithPartitionSpec(specBucket4),
	)
	require.NoError(t, err)

	// A matching config must round-trip without being flagged — otherwise every
	// restart of a partitioned task (NewTransactor) would fail.
	require.NoError(t, cat.verifyPartitionSpec(ctx, tableIdent, specBucket4))

	// A changed transform parameter is a hard error.
	specBucket8, _, err := buildPartitionSpec(allFields, pqSchema, []partitionField{
		{Field: "id", Transform: "bucket[8]"},
	})
	require.NoError(t, err)
	require.Error(t, cat.verifyPartitionSpec(ctx, tableIdent, specBucket8))

	// Dropping partitioning entirely is a hard error.
	require.Error(t, cat.verifyPartitionSpec(ctx, tableIdent, nil))

	// A non-existent table is a no-op: CreateResource will create it with the
	// configured spec.
	require.NoError(t, cat.verifyPartitionSpec(ctx, icebergtable.Identifier{namespace, "partition_mismatch_absent"}, specBucket4))
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
