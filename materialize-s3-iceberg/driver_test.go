package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/filesink"
	"github.com/estuary/connectors/go/auth/iam"
	"github.com/estuary/connectors/go/writer"
	mboilerplate "github.com/estuary/connectors/materialize-boilerplate"
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

// regexpReplace returns a sanitizer that replaces every match of pattern with
// repl.
func regexpReplace(pattern, repl string) func(string) string {
	re := regexp.MustCompile(pattern)
	return func(s string) string { return re.ReplaceAllString(s, repl) }
}

// materializeSanitizers normalizes the parts of a materialize snapshot that
// differ between the REST and Glue catalogs — the S3 bucket and prefix, plus
// the random per-run parquet UUIDs and table-name hashes — to stable
// placeholders. The namespace is deliberately NOT sanitized: both tests use the
// same namespace (see the pre-creation in TestIntegration/TestIntegrationGlue),
// and the "Resource:" snapshot header is written by the harness without passing
// through these sanitizers, so the only way it can match is for the value to be
// identical. Applied identically to both tests so their snapshots are
// byte-identical; TestRestGlueSnapshotParity enforces that.
func materializeSanitizers() []func(string) string {
	return []func(string) string{
		// s3://<bucket>/<prefix>/ — the bucket and prefix path segments. The
		// namespace segment that follows is left intact (identical in both).
		regexpReplace(`s3://[^/"]+/[^/"]+/`, `s3://<bucket>/<prefix>/`),
		// Random UUIDs in parquet file names.
		regexpReplace(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.parquet`, `<uuid>.parquet`),
		// Per-run table-name hashes.
		regexpReplace(`_([\dA-F]{16})/data/`, `_<hash>/data/`),
	}
}

// ensureNamespace idempotently pre-creates cfg.Namespace so that the subsequent
// materialization does not emit a "create namespace" apply action. Both the
// REST and Glue tests call this: the REST stack is a fresh Polaris each run (the
// namespace would otherwise be created during the test), while Glue is a shared,
// persistent account where the namespace already exists. Pre-creating in both
// makes the apply actions — and thus the snapshots — identical, and is safe
// under concurrent runs because it neither drops the namespace nor fails when it
// already exists.
func ensureNamespace(t *testing.T, cfg config) {
	t.Helper()

	cat, err := newCatalog(context.Background(), cfg, NestedLocationStyle)
	require.NoError(t, err)
	// Glue's CreateDatabase returns a raw AWS AlreadyExistsException that
	// iceberg-go does not map to ErrNamespaceAlreadyExists (mirroring the
	// EntityNotFoundException quirk handled in populateInfoSchema), so tolerate
	// both the mapped error and the raw string.
	if err := cat.createNamespace(context.Background(), cfg.Namespace); err != nil &&
		!errors.Is(err, icebergcatalog.ErrNamespaceAlreadyExists) &&
		!strings.Contains(err.Error(), "AlreadyExists") {
		t.Fatalf("pre-creating namespace %q: %v", cfg.Namespace, err)
	}
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
	ensureNamespace(t, cfg)

	d := true
	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table, Delta: &d}
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize.flow.yaml", makeResourceFn, materializeSanitizers())
	})

	t.Run("materialize-ns", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize-ns.flow.yaml", makeResourceFn, materializeSanitizers())
	})

	t.Run("materialize-variant", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize-variant.flow.yaml", makeResourceFn, materializeSanitizers())
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

	t.Run("checkpoint-fence-regression", func(t *testing.T) {
		runCheckpointFenceRegression(t, cfg)
	})

	t.Run("ns-timestamp", func(t *testing.T) {
		runNanosecondTimestampTest(t, cfg)
	})

	t.Run("ns-v2-upgrade", func(t *testing.T) {
		runNanosecondV2UpgradeTest(t, cfg)
	})

	t.Run("ns-create-v3", func(t *testing.T) {
		runNanosecondCreateResourceTest(t, cfg)
	})

	t.Run("ns-migrate", func(t *testing.T) {
		runNanosecondMigrateTest(t, cfg)
	})

	t.Run("variant-create-v3", func(t *testing.T) {
		runVariantCreateResourceTest(t, cfg)
	})

	t.Run("variant-duckdb-regression", func(t *testing.T) {
		runVariantDuckDBRegression(t, cfg)
	})

	t.Run("variant-migrate", func(t *testing.T) {
		runVariantMigrateTest(t, cfg)
	})

	t.Run("variant-disable-flip", func(t *testing.T) {
		runVariantDisableFlipTest(t, cfg)
	})

	// Migration test is skipped because Iceberg does not support the type
	// migrations exercised by the test (e.g. long→string).
	//t.Run("migrate", func(t *testing.T) {
	//	boilerplate.RunMigrationTest(t, newMaterialization, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	//})
}

// TestIntegrationGlue runs the same integration subtests as TestIntegration —
// materialize/apply plus the catalog regression tests — against a real AWS
// Glue Data Catalog. The connector advertises Glue support but TestIntegration
// only exercises the Polaris REST catalog, so this covers the otherwise-
// untested Glue code path (including the S3 FileIO credential wiring) and
// guards against drift between the two catalogs. It runs by default
// alongside TestIntegration; the sops-encrypted testdata/config-glue.yaml
// supplies credentials and is decrypted by the test harness (CI and local runs
// need GCP KMS access).
//
// Unlike TestIntegration there is no docker-compose stack; the Glue catalog and
// the S3 bucket named in the config must already exist. The namespace (Glue
// database) is pre-created idempotently and its tables are created and cleaned
// up by the test.
func TestIntegrationGlue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	cfg := loadGlueTestConfig(t)
	ensureNamespace(t, cfg)

	d := true
	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table, Delta: &d}
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize-glue.flow.yaml", makeResourceFn, materializeSanitizers())
	})

	t.Run("materialize-ns", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize-glue-ns.flow.yaml", makeResourceFn, materializeSanitizers())
	})

	t.Run("materialize-variant", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize-glue-variant.flow.yaml", makeResourceFn, materializeSanitizers())
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, newMaterialization, "testdata/apply-glue.flow.yaml", makeResourceFn)
	})

	t.Run("ts-overflow-regression", func(t *testing.T) {
		runTimestampOverflowRegression(t, cfg)
	})

	t.Run("date-overflow-regression", func(t *testing.T) {
		runDateOverflowRegression(t, cfg)
	})

	t.Run("checkpoint-fence-regression", func(t *testing.T) {
		runCheckpointFenceRegression(t, cfg)
	})

	t.Run("ns-timestamp", func(t *testing.T) {
		runNanosecondTimestampTest(t, cfg)
	})

	t.Run("ns-v2-upgrade", func(t *testing.T) {
		runNanosecondV2UpgradeTest(t, cfg)
	})

	t.Run("ns-create-v3", func(t *testing.T) {
		runNanosecondCreateResourceTest(t, cfg)
	})

	t.Run("ns-migrate", func(t *testing.T) {
		runNanosecondMigrateTest(t, cfg)
	})

	t.Run("variant-create-v3", func(t *testing.T) {
		runVariantCreateResourceTest(t, cfg)
	})

	t.Run("variant-migrate", func(t *testing.T) {
		runVariantMigrateTest(t, cfg)
	})

	t.Run("variant-disable-flip", func(t *testing.T) {
		runVariantDisableFlipTest(t, cfg)
	})
}

// TestRestGlueSnapshotParity enforces that the REST and Glue materialize
// snapshots are byte-identical after sanitization. Any divergence means the two
// catalog code paths produced different results — the drift this coverage
// exists to catch.
func TestRestGlueSnapshotParity(t *testing.T) {
	for _, name := range []string{"materialize", "materialize-ns", "materialize-variant"} {
		rest, err := os.ReadFile(".snapshots/TestIntegration-" + name)
		require.NoError(t, err)
		glue, err := os.ReadFile(".snapshots/TestIntegrationGlue-" + name)
		require.NoError(t, err)
		require.Equal(t, string(rest), string(glue),
			"REST and Glue %s snapshots diverged; this indicates catalog-specific drift", name)
	}
}

// runTimestampOverflowRegression reproduces a production failure observed at
// deel/prod/.../profile where appending a parquet file containing a timestamp
// with UTC year 10000 failed. "9999-12-31T23:59:59-14:00" normalizes to UTC
// year 10000; clampTimestamp rewrites it to the year-9999 boundary before the
// writer sees it, so the append round-trips without error.
func runTimestampOverflowRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	rt := newRegressionTable(t, ctx, cfg, "ts_overflow_regression", nil,
		iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false})

	clampedTS, err := clampTimestamp("9999-12-31T23:59:59-14:00")
	require.NoError(t, err)
	s3Path, _ := rt.uploadParquet(t, ctx,
		writer.ParquetSchemaElement{Name: "ts", DataType: writer.LogicalTypeTimestamp, Required: false}, clampedTS)

	require.NoError(t, rt.cat.appendFiles(ctx,
		"acmeCo/tests/regression",
		rt.ident,
		[]string{s3Path},
		"",
		"deadbeefdeadbeef",
	))
}

// regressionTable is the shared scaffolding of the append regression tests: a
// freshly created table plus the machinery to write, upload, and append
// parquet files to it.
type regressionTable struct {
	cfg      config
	cat      *catalog
	ident    icebergtable.Identifier
	location string
	s3client *s3.Client
}

func newRegressionTable(t *testing.T, ctx context.Context, cfg config, table string, extraProps iceberg.Properties, fields ...iceberg.NestedField) *regressionTable {
	t.Helper()

	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)

	ensureNamespace(t, cfg)

	// The suffix follows testutil's testItemIdentifier convention: it keeps
	// concurrent runs against the shared Glue account from dropping each
	// other's tables, and lets the boilerplate subtests' stale-item sweep
	// reclaim tables leaked by crashed runs.
	table = fmt.Sprintf("%s_flow_test_%d", table, time.Now().Unix())
	ident := icebergtable.Identifier{cfg.Namespace, table}

	schema := iceberg.NewSchema(0, fields...)
	nameMappingJSON, err := json.Marshal(schema.NameMapping())
	require.NoError(t, err)

	props := iceberg.Properties{
		icebergtable.DefaultNameMappingKey: string(nameMappingJSON),
	}
	for k, v := range extraProps {
		props[k] = v
	}

	location := tablePath(cfg.Bucket, cfg.Prefix, cfg.Namespace, table, NestedLocationStyle)
	_, err = cat.cat.CreateTable(ctx, ident, schema,
		icebergcatalog.WithLocation(location),
		icebergcatalog.WithProperties(props),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := cat.cat.DropTable(ctx, ident); err != nil {
			t.Log("failed to drop regression table", ident, err)
		}
	})

	s3opts := s3.Options{
		Region:      cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, ""),
	}
	// Path-style addressing only applies to S3-compatible stacks (rustfs in
	// TestIntegration); real AWS (Glue) uses the default endpoint resolution.
	if cfg.S3Endpoint != "" {
		s3opts.BaseEndpoint = aws.String(cfg.S3Endpoint)
		s3opts.UsePathStyle = true
	}

	return &regressionTable{
		cfg:      cfg,
		cat:      cat,
		ident:    ident,
		location: location,
		s3client: s3.New(s3opts),
	}
}

// uploadParquet is the single-column convenience form of uploadParquetRows,
// returning the s3 path and the local path for tests that inspect the encoded
// bytes.
func (rt *regressionTable) uploadParquet(t *testing.T, ctx context.Context, col writer.ParquetSchemaElement, rows ...any) (string, string) {
	t.Helper()
	wide := make([][]any, len(rows))
	for i, row := range rows {
		wide[i] = []any{row}
	}
	return rt.uploadParquetRows(t, ctx, writer.ParquetSchema{col}, wide...)
}

func (rt *regressionTable) uploadParquetRows(t *testing.T, ctx context.Context, cols writer.ParquetSchema, rows ...[]any) (string, string) {
	t.Helper()

	parquetPath := filepath.Join(t.TempDir(), "data.parquet")
	parquetFile, err := os.Create(parquetPath)
	require.NoError(t, err)
	pqw, err := writer.NewParquetWriter(parquetFile, cols,
		writer.WithParquetCompression(writer.Snappy))
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, pqw.Write(row))
	}
	require.NoError(t, pqw.Close())

	s3Path := strings.TrimSuffix(rt.location, "/") + "/data/" + uuid.New().String() + ".parquet"

	uploadFile, err := os.Open(parquetPath)
	require.NoError(t, err)
	defer uploadFile.Close()
	_, err = rt.s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(rt.cfg.Bucket),
		Key:    aws.String(strings.TrimPrefix(s3Path, "s3://"+rt.cfg.Bucket+"/")),
		Body:   uploadFile,
	})
	require.NoError(t, err)

	return s3Path, parquetPath
}

// runDateOverflowRegression is the date analog of runTimestampOverflowRegression.
// clampDate rewrites a year > 9999 (which time.Parse rejects outright) to the
// year-9999 boundary before the writer sees it, so the append round-trips.
func runDateOverflowRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	rt := newRegressionTable(t, ctx, cfg, "date_overflow_regression", nil,
		iceberg.NestedField{ID: 1, Name: "d", Type: iceberg.PrimitiveTypes.Date, Required: false})

	clampedDate, err := clampDate("10000-01-01")
	require.NoError(t, err)
	s3Path, _ := rt.uploadParquet(t, ctx,
		writer.ParquetSchemaElement{Name: "d", DataType: writer.LogicalTypeDate, Required: false}, clampedDate)

	require.NoError(t, rt.cat.appendFiles(ctx,
		"acmeCo/tests/regression",
		rt.ident,
		[]string{s3Path},
		"",
		"deadbeefdeadbeef",
	))
}

// runCheckpointFenceRegression locks in the checkpoint-fence semantics of
// appendFiles: replaying an already-recorded checkpoint is skipped rather
// than re-appending its files. The fence is the sole duplicate guard —
// appendFiles does not scan existing manifests for duplicate paths, matching
// the pyiceberg 0.7.0 behavior this connector originally shipped with.
func runCheckpointFenceRegression(t *testing.T, cfg config) {
	ctx := context.Background()

	const materialization = "acmeCo/tests/regression"

	rt := newRegressionTable(t, ctx, cfg, "checkpoint_fence_regression", nil,
		iceberg.NestedField{ID: 1, Name: "v", Type: iceberg.PrimitiveTypes.String, Required: false})

	vCol := writer.ParquetSchemaElement{Name: "v", DataType: writer.LogicalTypeString, Required: false}
	first, _ := rt.uploadParquet(t, ctx, vCol, "one")
	require.NoError(t, rt.cat.appendFiles(ctx, materialization, rt.ident,
		[]string{first}, "", "checkpoint-1"))

	// Replaying the already-committed checkpoint is skipped by the fence, so
	// the same path is not re-appended.
	require.NoError(t, rt.cat.appendFiles(ctx, materialization, rt.ident,
		[]string{first}, "", "checkpoint-1"))

	second, _ := rt.uploadParquet(t, ctx, vCol, "two")
	require.NoError(t, rt.cat.appendFiles(ctx, materialization, rt.ident,
		[]string{second}, "checkpoint-1", "checkpoint-2"))

	// Two data files total: the replay added nothing.
	tbl, err := rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)
	fs, err := tbl.FS(ctx)
	require.NoError(t, err)
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	var livePaths []string
	for _, m := range manifests {
		for entry, err := range m.Entries(fs, true) {
			require.NoError(t, err)
			livePaths = append(livePaths, entry.DataFile().FilePath())
		}
	}
	require.ElementsMatch(t, []string{first, second}, livePaths)
}

// TestNormalizeLegacyCredentials guards the parity with the removed Python
// `transform_legacy_credentials`: legacy top-level aws_access_key_id/
// aws_secret_access_key must be promoted into cfg.Credentials so the REST
// catalog path applies direct S3 FileIO creds (and strips the delegation
// header). Without promotion, buildRestCatalog's directS3Creds gate is false
// for legacy configs and non-vending catalogs fail with S3 credential errors.
func TestNormalizeLegacyCredentials(t *testing.T) {
	const (
		akid   = "AKIAEXAMPLE"
		secret = "s3cr3tkey"
	)

	t.Run("legacy-only promotes to Credentials and enables direct S3 access", func(t *testing.T) {
		legacyID, legacySecret := akid, secret
		cfg := config{
			Region:             "us-east-1",
			AWSAccessKeyID:     &legacyID,
			AWSSecretAccessKey: &legacySecret,
		}
		cfg.normalizeCredentials()

		require.NotNil(t, cfg.Credentials)
		require.Equal(t, filesink.AWSAccessKey, cfg.Credentials.AuthType)
		require.Equal(t, akid, cfg.Credentials.AWSAccessKeyID)
		require.Equal(t, secret, cfg.Credentials.AWSSecretAccessKey)
		require.Nil(t, cfg.AWSAccessKeyID)
		require.Nil(t, cfg.AWSSecretAccessKey)

		// The REST catalog gates direct S3 FileIO creds and the delegation-header
		// strip on exactly this predicate.
		require.True(t, cfg.Credentials != nil &&
			cfg.Credentials.AuthType == filesink.AWSAccessKey &&
			cfg.Region != "")

		props := s3PropsForDirectCreds(&cfg)
		require.Equal(t, akid, props[icebergio.S3AccessKeyID])
		require.Equal(t, secret, props[icebergio.S3SecretAccessKey])
	})

	t.Run("credentials take precedence over legacy keys when both set", func(t *testing.T) {
		legacyID, legacySecret := akid, secret
		cfg := config{
			Region:             "us-east-1",
			AWSAccessKeyID:     &legacyID,
			AWSSecretAccessKey: &legacySecret,
			Credentials: &filesink.CredentialsConfig{
				AuthType: filesink.AWSAccessKey,
				AccessKeyCredentials: filesink.AccessKeyCredentials{
					AWSAccessKeyID:     "AKIANEW",
					AWSSecretAccessKey: "newsecret",
				},
			},
		}
		cfg.normalizeCredentials()

		require.Equal(t, "AKIANEW", cfg.Credentials.AWSAccessKeyID)
		require.Nil(t, cfg.AWSAccessKeyID)
		require.Nil(t, cfg.AWSSecretAccessKey)
	})

	t.Run("no credentials leaves config unchanged", func(t *testing.T) {
		cfg := config{Region: "us-east-1"}
		cfg.normalizeCredentials()
		require.Nil(t, cfg.Credentials)
	})
}

// TestS3PropsForIAMCredentials is a regression guard for the S3 FileIO
// credentials on the AWSIAM path (used by the Glue catalog). CredentialsConfig
// embeds both AccessKeyCredentials and iam.IAMConfig, so a bare
// cfg.Credentials.AWSAccessKeyID resolves to the shallower AccessKeyCredentials
// field — empty under IAM auth — rather than the runtime-injected STS
// credentials in IAMTokens. The direct-creds props must carry the IAMTokens
// values.
func TestS3PropsForIAMCredentials(t *testing.T) {
	const (
		akid    = "ASIAEXAMPLE"
		secret  = "iamsecret"
		session = "iamsessiontoken"
	)
	cfg := config{
		Region: "us-east-1",
		Credentials: &filesink.CredentialsConfig{
			AuthType: filesink.AWSIAM,
			IAMConfig: iam.IAMConfig{
				IAMTokens: iam.IAMTokens{
					AWSTokens: iam.AWSTokens{
						AWSAccessKeyID:     akid,
						AWSSecretAccessKey: secret,
						AWSSessionToken:    session,
					},
				},
			},
		},
	}

	props := s3PropsForDirectCreds(&cfg)
	require.Equal(t, akid, props[icebergio.S3AccessKeyID])
	require.Equal(t, secret, props[icebergio.S3SecretAccessKey])
	require.Equal(t, session, props[icebergio.S3SessionToken])
}

// fakeCatalog implements only the icebergcatalog.Catalog methods that
// populateInfoSchema exercises; the embedded interface panics if any other
// method is called.
type fakeCatalog struct {
	icebergcatalog.Catalog
	namespaces       []icebergtable.Identifier
	tablesByNS       map[string][]string
	listTablesCalled bool
}

func (f *fakeCatalog) ListNamespaces(ctx context.Context, parent icebergtable.Identifier) ([]icebergtable.Identifier, error) {
	return f.namespaces, nil
}

func (f *fakeCatalog) ListTables(ctx context.Context, namespace icebergtable.Identifier) iter.Seq2[icebergtable.Identifier, error] {
	f.listTablesCalled = true
	return func(yield func(icebergtable.Identifier, error) bool) {
		name := namespace[0]
		tbls, ok := f.tablesByNS[name]
		if !ok {
			// Reproduce Glue's ListTables, which yields a raw AWS
			// EntityNotFoundException for an absent database rather than
			// catalog.ErrNoSuchNamespace.
			yield(icebergtable.Identifier{}, errors.New("failed to list tables in namespace "+name+": EntityNotFoundException"))
			return
		}
		for _, t := range tbls {
			if !yield(icebergtable.Identifier{name, t}, nil) {
				return
			}
		}
	}
}

// TestPopulateInfoSchemaSkipsAbsentNamespace guards parity with the removed
// Python info_schema, which listed tables only in namespaces returned by
// list_namespaces(). A wanted namespace that does not yet exist must be skipped
// silently — on Glue, ListTables surfaces a raw EntityNotFoundException that is
// never catalog.ErrNoSuchNamespace, so error-matching alone fails to bootstrap.
func TestPopulateInfoSchemaSkipsAbsentNamespace(t *testing.T) {
	newIS := func() *mboilerplate.InfoSchema {
		return mboilerplate.NewInfoSchema(
			func(p []string) []string { return p },
			func(ns string) string { return ns },
			func(f string) string { return f },
			false, false,
		)
	}

	t.Run("absent namespace is skipped without error", func(t *testing.T) {
		fake := &fakeCatalog{namespaces: nil}
		c := &catalog{cfg: &config{}, locationStyle: NestedLocationStyle, cat: fake}

		err := c.populateInfoSchema(context.Background(), newIS(), [][]string{{"tests", "mytable"}})
		require.NoError(t, err)
		require.False(t, fake.listTablesCalled, "must not scan a namespace that does not exist")
	})

	t.Run("existing namespace is scanned", func(t *testing.T) {
		fake := &fakeCatalog{
			namespaces: []icebergtable.Identifier{{"tests"}},
			tablesByNS: map[string][]string{"tests": {"other"}},
		}
		c := &catalog{cfg: &config{}, locationStyle: NestedLocationStyle, cat: fake}

		err := c.populateInfoSchema(context.Background(), newIS(), [][]string{{"tests", "mytable"}})
		require.NoError(t, err)
		require.True(t, fake.listTablesCalled)
	})
}

// withAdvanced returns cfg with a cloned Advanced block mutated by set.
// Advanced is a pointer shared with the caller's cfg, so it is cloned rather
// than mutated in place to keep flags from leaking into subtests that run
// later.
func withAdvanced(cfg config, set func(*advancedConfig)) config {
	var adv = advancedConfig{}
	if cfg.Advanced != nil {
		adv = *cfg.Advanced
	}
	set(&adv)
	cfg.Advanced = &adv
	return cfg
}

// runNanosecondTimestampTest verifies that timestamptz_ns (Iceberg v3 format) columns:
//   - write nanosecond-precision timestamps correctly
//   - clamp out-of-range values (before 1677 / after 2262) to int64 min/max rather than overflowing
//   - encode exact int64 nanosecond values in the produced Parquet file
//     (asserted against the raw column bytes)
func runNanosecondTimestampTest(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	cfg = withAdvanced(cfg, func(a *advancedConfig) { a.NanosecondTimestamps = true })
	rt := newRegressionTable(t, ctx, cfg, "ns_timestamp", iceberg.Properties{icebergtable.PropertyFormatVersion: "3"},
		iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTzNs, Required: false})

	// Values pass through clampTimestampNanos as they do in production, where
	// MapType wires it as the binding's ElementConverter.
	var rows []any
	for _, raw := range []string{
		"2023-01-15T12:34:56.123456789Z", // in-range: sub-microsecond digits (789) must survive the round-trip
		"1000-01-01T00:00:00Z",           // out-of-range past: clamps to math.MinInt64 rather than overflowing
		"3000-01-01T00:00:00Z",           // out-of-range future: clamps to math.MaxInt64 rather than overflowing
	} {
		v, err := clampTimestampNanos(raw)
		require.NoError(t, err)
		rows = append(rows, v)
	}
	s3Path, parquetPath := rt.uploadParquet(t, ctx,
		writer.ParquetSchemaElement{Name: "ts", DataType: writer.LogicalTypeTimestampNanos, Required: false}, rows...)

	require.NoError(t, rt.cat.appendFiles(ctx,
		"acmeCo/tests/ns-timestamp",
		rt.ident,
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

// runNanosecondV2UpgradeTest reproduces adding a timestamptz_ns column to a
// table that predates nanosecond_timestamps being enabled: the table is still
// format v2 (and had no date-time columns, so flipping the flag forces no
// backfill), and UpdateResource must upgrade it to v3 for the AddColumn to be
// valid.
func runNanosecondV2UpgradeTest(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	cfg = withAdvanced(cfg, func(a *advancedConfig) { a.NanosecondTimestamps = true })
	rt := newRegressionTable(t, ctx, cfg, "ns_v2_upgrade", iceberg.Properties{icebergtable.PropertyFormatVersion: "2"},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false})
	cat := rt.cat
	tableIdent := rt.ident

	tbl, err := cat.cat.LoadTable(ctx, tableIdent)
	require.NoError(t, err)
	require.Equal(t, 2, tbl.Metadata().Version())

	update := mboilerplate.BindingUpdate[config, resource, mappedType]{
		Binding: mboilerplate.MappedBinding[config, resource, mappedType]{
			MaterializationSpec_Binding: pf.MaterializationSpec_Binding{
				ResourcePath: tableIdent,
			},
		},
		NewProjections: []mboilerplate.MappedProjection[mappedType]{{
			Projection: mboilerplate.Projection{
				Projection: pf.Projection{
					Field: "ts",
					Inference: pf.Inference{
						Types:   []string{"string"},
						String_: &pf.Inference_String{Format: "date-time"},
					},
				},
			},
		}},
	}

	_, apply, err := cat.UpdateResource(ctx, update)
	require.NoError(t, err)
	require.NoError(t, apply(ctx))

	tbl, err = cat.cat.LoadTable(ctx, tableIdent)
	require.NoError(t, err)
	require.Equal(t, 3, tbl.Metadata().Version())
	f, ok := tbl.Schema().FindFieldByName("ts")
	require.True(t, ok)
	require.True(t, f.Type.Equals(iceberg.PrimitiveTypes.TimestampTzNs))
}

// runNanosecondCreateResourceTest exercises the connector's own CreateResource
// path with nanosecond_timestamps enabled: a user-supplied format-version
// table property that conflicts with the required v3 is rejected with an
// explicit error, and a clean create yields a format v3 table with a
// timestamptz_ns column.
func runNanosecondCreateResourceTest(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	cfg = withAdvanced(cfg, func(a *advancedConfig) { a.NanosecondTimestamps = true })

	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)
	ensureNamespace(t, cfg)

	// Same per-run naming convention as newRegressionTable, so concurrent runs
	// can't collide and leaked tables are swept by the boilerplate cleanup.
	table := fmt.Sprintf("ns_create_flow_test_%d", time.Now().Unix())
	ident := icebergtable.Identifier{cfg.Namespace, table}

	binding := &pf.MaterializationSpec_Binding{
		ResourcePath: ident,
		Collection: pf.CollectionSpec{
			Name: "acmeCo/tests/ns-create",
			Projections: []pf.Projection{{
				Field: "ts",
				Inference: pf.Inference{
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "date-time"},
				},
			}},
		},
		FieldSelection: pf.FieldSelection{Values: []string{"ts"}},
	}

	_, _, err = cat.CreateResource(ctx, binding, resource{
		AdditionalTableProperties: map[string]string{
			icebergtable.PropertyFormatVersion: "2",
		},
	})
	require.ErrorContains(t, err, "requires format version 3")

	_, apply, err := cat.CreateResource(ctx, binding, resource{})
	require.NoError(t, err)
	require.NoError(t, apply(ctx))
	t.Cleanup(func() {
		if err := cat.cat.DropTable(ctx, ident); err != nil {
			t.Log("failed to drop table", ident, err)
		}
	})

	tbl, err := cat.cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	require.Equal(t, 3, tbl.Metadata().Version())
	f, ok := tbl.Schema().FindFieldByName("ts")
	require.True(t, ok)
	require.True(t, f.Type.Equals(iceberg.PrimitiveTypes.TimestampTzNs))
}

// runNanosecondMigrateTest exercises the timestamp-precision migration that
// UpdateResource performs when nanosecond_timestamps is toggled on an existing
// table: the column is dropped and re-added under a new field ID (Iceberg has
// no in-place type change), the table upgrades to format v3, and the column's
// name-mapping entry is removed. The decisive assertions read the table back
// through iceberg-go's spec-compliant scanner: pre-migration rows must be null
// for the migrated column — never a misread of their old-encoding values —
// while post-migration rows (written with embedded field IDs) carry full
// nanosecond precision. A second migration back to microseconds proves the
// reverse direction.
func runNanosecondMigrateTest(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	// The table keeps a non-timestamp column alongside ts: iceberg-go's
	// scanner refuses data files that contribute zero projected columns, so a
	// realistic multi-column shape is required for reading back the
	// pre-migration file whose only timestamp column was migrated away.
	rt := newRegressionTable(t, ctx, cfg, "ns_migrate", nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false})

	// A microsecond row lands before the migration in a file without embedded
	// field IDs, as the connector wrote before field-ID embedding.
	oldPath, _ := rt.uploadParquetRows(t, ctx, writer.ParquetSchema{
		{Name: "id", DataType: writer.LogicalTypeString, Required: false},
		{Name: "ts", DataType: writer.LogicalTypeTimestamp, Required: false},
	}, []any{"old", "2023-01-15T12:34:56.123456Z"})
	require.NoError(t, rt.cat.appendFiles(ctx, "acmeCo/tests/ns-migrate", rt.ident,
		[]string{oldPath}, "", "checkpoint-1"))

	migrate := func(fromType string, to iceberg.Type) {
		t.Helper()
		update := mboilerplate.BindingUpdate[config, resource, mappedType]{
			Binding: mboilerplate.MappedBinding[config, resource, mappedType]{
				MaterializationSpec_Binding: pf.MaterializationSpec_Binding{
					ResourcePath: rt.ident,
				},
			},
			FieldsToMigrate: []mboilerplate.MigrateField[mappedType]{{
				From: mboilerplate.ExistingField{Name: "ts", Type: fromType},
				To: mboilerplate.MappedProjection[mappedType]{
					Projection: mboilerplate.Projection{Projection: pf.Projection{Field: "ts"}},
					Mapped:     mappedType{icebergType: to},
				},
			}},
		}
		_, apply, err := rt.cat.UpdateResource(ctx, update)
		require.NoError(t, err)
		require.NoError(t, apply(ctx))
	}

	migrate("timestamptz", iceberg.PrimitiveTypes.TimestampTzNs)

	tbl, err := rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)
	require.Equal(t, 3, tbl.Metadata().Version())
	f, ok := tbl.Schema().FindFieldByName("ts")
	require.True(t, ok)
	require.True(t, f.Type.Equals(iceberg.PrimitiveTypes.TimestampTzNs))
	require.NotEqual(t, 2, f.ID, "migrated column must get a new field ID")

	var mapping iceberg.NameMapping
	require.NoError(t, json.Unmarshal([]byte(tbl.Properties()[icebergtable.DefaultNameMappingKey]), &mapping))
	for _, mf := range mapping {
		require.NotContains(t, mf.Names, "ts", "migrated column must be removed from the name mapping")
	}

	// The pre-migration row survives the migration with its migrated column
	// reading null — never a misread of its microsecond value as nanoseconds.
	require.Equal(t, []any{"old"}, scanColumn(t, ctx, rt, "id"))
	require.Equal(t, []any{nil}, scanColumn(t, ctx, rt, "ts"))

	// A nanosecond row lands after the migration with the table's field IDs
	// embedded, as the connector now writes.
	idID, tsID := int32(1), int32(f.ID)
	newPath, _ := rt.uploadParquetRows(t, ctx, writer.ParquetSchema{
		{Name: "id", DataType: writer.LogicalTypeString, Required: false, FieldId: &idID},
		{Name: "ts", DataType: writer.LogicalTypeTimestampNanos, Required: false, FieldId: &tsID},
	}, []any{"new", "2025-06-15T12:00:00.123456789Z"})
	require.NoError(t, rt.cat.appendFiles(ctx, "acmeCo/tests/ns-migrate", rt.ident,
		[]string{newPath}, "checkpoint-1", "checkpoint-2"))

	require.ElementsMatch(t, []any{"old", "new"}, scanColumn(t, ctx, rt, "id"))
	require.ElementsMatch(t, []any{nil, int64(1749988800123456789)}, scanColumn(t, ctx, rt, "ts"))

	// Reverse direction: back to microseconds.
	migrate("timestamptz_ns", iceberg.PrimitiveTypes.TimestampTz)

	tbl, err = rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)
	f2, ok := tbl.Schema().FindFieldByName("ts")
	require.True(t, ok)
	require.True(t, f2.Type.Equals(iceberg.PrimitiveTypes.TimestampTz))
	require.NotEqual(t, f.ID, f2.ID)

	// Both prior rows are now null: the ns row's file embeds the retired ns
	// field ID, and the µs row's file remains unmapped.
	require.ElementsMatch(t, []any{nil, nil}, scanColumn(t, ctx, rt, "ts"))
}

// scanColumn reads the named column of every row through iceberg-go's scanner,
// which applies the table's current schema and name mapping the same way
// external query engines do. Values are returned as int64 epoch offsets in the
// column's unit (timestamps), Go strings (strings), or compact JSON strings
// (variants), with nil for nulls. Scan options allow time travel to a prior
// snapshot, which reads under that snapshot's schema.
func scanColumn(t *testing.T, ctx context.Context, rt *regressionTable, name string, opts ...icebergtable.ScanOption) []any {
	t.Helper()

	tbl, err := rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)
	arrowTbl, err := tbl.Scan(opts...).ToArrowTable(ctx)
	require.NoError(t, err)
	defer arrowTbl.Release()

	idx := arrowTbl.Schema().FieldIndices(name)
	require.Len(t, idx, 1)

	var out []any
	for _, chunk := range arrowTbl.Column(idx[0]).Data().Chunks() {
		for i := 0; i < chunk.Len(); i++ {
			if chunk.IsNull(i) {
				out = append(out, nil)
				continue
			}
			switch arr := chunk.(type) {
			case *array.Timestamp:
				out = append(out, int64(arr.Value(i)))
			case *array.String:
				out = append(out, arr.Value(i))
			case *extensions.VariantArray:
				v, err := arr.Value(i)
				require.NoError(t, err)
				j, err := json.Marshal(v)
				require.NoError(t, err)
				out = append(out, string(j))
			default:
				t.Fatalf("column %q has unhandled arrow type %T", name, chunk)
			}
		}
	}
	return out
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

// loadGlueTestConfig bundles the Glue task spec and returns its decrypted
// endpoint config. testdata/config-glue.yaml is sops-encrypted, so unlike
// loadTestConfig this decrypts the bundled config before unmarshaling.
func loadGlueTestConfig(t *testing.T) config {
	t.Helper()

	bundled := boilerplate.RunFlowctl(t, "raw", "bundle", "--source", "testdata/materialize-glue.flow.yaml")
	taskName := "acmeCo/tests/materialize-s3-iceberg"

	raw := json.RawMessage(gjson.GetBytes(bundled, "materializations."+taskName+".endpoint.local.config").Raw)
	require.NotEmpty(t, raw, "could not find config in bundled spec")

	if gjson.GetBytes(raw, "sops").Exists() {
		raw = sopsDecrypt(t, raw)
	}

	var cfg config
	require.NoError(t, pf.UnmarshalStrict(raw, &cfg))
	return cfg
}

// sopsDecrypt decrypts a sops-encrypted JSON config and strips the _sops key
// suffixes, mirroring the boilerplate test harness's own decryption so the
// result is the plaintext config the connector expects.
func sopsDecrypt(t *testing.T, raw json.RawMessage) json.RawMessage {
	t.Helper()

	sopsCmd := exec.Command("sops", "--decrypt", "--input-type", "json", "--output-type", "json", "/dev/stdin")
	jqCmd := exec.Command("jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
	sopsCmd.Stdin = bytes.NewReader(raw)

	var err error
	jqCmd.Stdin, err = sopsCmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, sopsCmd.Start())
	out, err := jqCmd.Output()
	require.NoError(t, err)
	require.NoError(t, sopsCmd.Wait())

	return out
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

// TestMapTypeIgnoreStringFormatOnNonStringField guards against a regression
// where MapType returned a zero mappedType (nil iceberg.Type) when
// projectionToParquetSchemaElement errored, panicking in String/Compatible.
// ignoreStringFormat is a no-op on a non-string field, so the field maps by
// its native type instead.
func TestMapTypeIgnoreStringFormatOnNonStringField(t *testing.T) {
	proj := mboilerplate.Projection{
		Projection: pf.Projection{
			Field: "n",
			Inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"integer"},
			},
		},
	}

	var d = &materialization{}
	mt, conv := d.MapType(proj, fieldConfig{IgnoreStringFormat: true})

	require.Equal(t, "long", mt.String())
	require.Nil(t, conv)
}

// TestMapTypeNanosecondWiring guards the MapType switch arm that installs
// clampTimestampNanos for date-time fields when nanosecond_timestamps is
// enabled. The integration tests invoke the clamp directly, so a silent
// fallback to the microsecond clamp would pass every other test while
// overflowing int64 nanoseconds in production.
func TestMapTypeNanosecondWiring(t *testing.T) {
	proj := mboilerplate.Projection{
		Projection: pf.Projection{
			Field: "ts",
			Inference: pf.Inference{
				Exists:  pf.Inference_MUST,
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
			},
		},
	}

	// Year 3000 distinguishes the two clamps: in-range for the microsecond
	// clamp (years 1-9999), beyond the int64-nanoseconds band for the
	// nanosecond clamp.
	const probe = "3000-01-01T00:00:00Z"

	var micro = &materialization{}
	mt, conv := micro.MapType(proj, fieldConfig{})
	require.Equal(t, "timestamptz", mt.String())
	v, err := conv(probe)
	require.NoError(t, err)
	require.Equal(t, probe, v)

	var nanos = &materialization{cfg: config{Advanced: &advancedConfig{NanosecondTimestamps: true}}}
	mt, conv = nanos.MapType(proj, fieldConfig{})
	require.Equal(t, "timestamptz_ns", mt.String())
	v, err = conv(probe)
	require.NoError(t, err)
	require.Equal(t, "2262-04-11T23:47:16.854775807Z", v)
}

// A user-supplied format-version conflict must name every enabled option that
// requires format v3: with both nanosecond_timestamps and variant_columns on,
// blaming only one of them misleads the user about what to reconfigure. The
// conflict check runs before any catalog access, so a bare catalog struct is
// enough here.
func TestCreateResourceFormatVersionConflictNamesOptions(t *testing.T) {
	ctx := context.Background()

	binding := &pf.MaterializationSpec_Binding{
		ResourcePath: []string{"test_namespace", "format_version_conflict"},
		Collection: pf.CollectionSpec{
			Name: "acmeCo/tests/format-version-conflict",
			Projections: []pf.Projection{{
				Field: "f",
				Ptr:   "/f",
				Inference: pf.Inference{
					Types:   []string{"string"},
					String_: &pf.Inference_String{},
				},
			}},
		},
		FieldSelection: pf.FieldSelection{Values: []string{"f"}},
	}

	for _, tt := range []struct {
		name   string
		adv    advancedConfig
		expect []string
	}{
		{"nanosecond timestamps only", advancedConfig{NanosecondTimestamps: true}, []string{"nanosecond_timestamps"}},
		{"variant columns only", advancedConfig{VariantColumns: true}, []string{"variant_columns"}},
		{"both options", advancedConfig{NanosecondTimestamps: true, VariantColumns: true}, []string{"nanosecond_timestamps", "variant_columns"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var adv = tt.adv
			var cat = &catalog{cfg: &config{Advanced: &adv}, locationStyle: NestedLocationStyle}
			_, _, err := cat.CreateResource(ctx, binding, resource{
				AdditionalTableProperties: map[string]string{
					icebergtable.PropertyFormatVersion: "2",
				},
			})
			for _, want := range tt.expect {
				require.ErrorContains(t, err, want)
			}
		})
	}
}

// runVariantCreateResourceTest exercises the connector's CreateResource path
// with variant_columns enabled: a user-supplied format-version table property
// that conflicts with the required v3 is rejected with an explicit error, and
// a clean create yields a format v3 table where the JSON-shaped fields —
// object, array, multi-type, and the root document — are variant columns,
// while the (multi-type) collection key and an ignoreStringFormat-overridden
// field keep their string mapping.
func runVariantCreateResourceTest(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	cfg = withAdvanced(cfg, func(a *advancedConfig) { a.VariantColumns = true })

	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)
	ensureNamespace(t, cfg)

	// Same per-run naming convention as newRegressionTable, so concurrent runs
	// can't collide and leaked tables are swept by the boilerplate cleanup.
	table := fmt.Sprintf("variant_create_flow_test_%d", time.Now().Unix())
	ident := icebergtable.Identifier{cfg.Namespace, table}

	// A multi-type collection key is not expressible in a Flow catalog today
	// (keys must be a single scalar type), but legacy or synthesized specs can
	// still present one, so the binding is built directly to pin that a key
	// never maps to variant.
	binding := &pf.MaterializationSpec_Binding{
		ResourcePath: ident,
		Collection: pf.CollectionSpec{
			Name: "acmeCo/tests/variant-create",
			Projections: []pf.Projection{
				{Field: "arr", Ptr: "/arr", Inference: pf.Inference{Types: []string{"array"}}},
				{Field: "cast", Ptr: "/cast", Inference: pf.Inference{
					Types:   []string{"integer", "string"},
					String_: &pf.Inference_String{},
				}},
				{Field: "flow_document", Ptr: "", Inference: pf.Inference{
					Types:  []string{"object"},
					Exists: pf.Inference_MUST,
				}},
				{Field: "multi", Ptr: "/multi", Inference: pf.Inference{
					Types:   []string{"integer", "string"},
					String_: &pf.Inference_String{},
				}},
				{Field: "multiKey", Ptr: "/multiKey", IsPrimaryKey: true, Inference: pf.Inference{
					Types:   []string{"integer", "string"},
					String_: &pf.Inference_String{},
					Exists:  pf.Inference_MUST,
				}},
				{Field: "obj", Ptr: "/obj", Inference: pf.Inference{Types: []string{"object"}}},
			},
		},
		FieldSelection: pf.FieldSelection{
			Keys:     []string{"multiKey"},
			Values:   []string{"arr", "cast", "multi", "obj"},
			Document: "flow_document",
			FieldConfigJsonMap: map[string]json.RawMessage{
				"cast": json.RawMessage(`{"ignoreStringFormat": true}`),
			},
		},
	}

	_, _, err = cat.CreateResource(ctx, binding, resource{
		AdditionalTableProperties: map[string]string{
			icebergtable.PropertyFormatVersion: "2",
		},
	})
	require.ErrorContains(t, err, "requires format version 3")

	_, apply, err := cat.CreateResource(ctx, binding, resource{})
	require.NoError(t, err)
	require.NoError(t, apply(ctx))
	t.Cleanup(func() {
		if err := cat.cat.DropTable(ctx, ident); err != nil {
			t.Log("failed to drop table", ident, err)
		}
	})

	tbl, err := cat.cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	require.Equal(t, 3, tbl.Metadata().Version())

	for name, want := range map[string]iceberg.Type{
		"arr":           iceberg.VariantType{},
		"cast":          iceberg.PrimitiveTypes.String,
		"flow_document": iceberg.VariantType{},
		"multi":         iceberg.VariantType{},
		"multiKey":      iceberg.PrimitiveTypes.String,
		"obj":           iceberg.VariantType{},
	} {
		f, ok := tbl.Schema().FindFieldByName(name)
		require.True(t, ok, "column %q missing", name)
		require.True(t, f.Type.Equals(want), "column %q: got %s, want %s", name, f.Type, want)
	}
}

// duckdbAttachQuery runs query against the test REST catalog using a pinned
// dockerized DuckDB: an independent Iceberg v3 implementation whose version
// isn't tied to anything in go.mod. The catalog is attached as `ice`, with
// direct S3 credentials (the local rustfs has no STS, so credential vending is
// disabled via ACCESS_DELEGATION_MODE none). The container uses host
// networking so the catalog and S3 endpoints resolve at localhost.
func duckdbAttachQuery(t *testing.T, cfg config, query string) string {
	t.Helper()

	clientID, clientSecret, ok := strings.Cut(cfg.Catalog.Credential, ":")
	require.True(t, ok, "catalog credential must be client_id:client_secret")

	script := fmt.Sprintf(`
INSTALL iceberg; LOAD iceberg;
CREATE SECRET s3sec (TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s', ENDPOINT '%s', URL_STYLE 'path', USE_SSL false);
CREATE SECRET icesec (TYPE ICEBERG, CLIENT_ID '%s', CLIENT_SECRET '%s', OAUTH2_SERVER_URI '%s/v1/oauth/tokens', OAUTH2_SCOPE '%s');
ATTACH '%s' AS ice (TYPE iceberg, SECRET icesec, ENDPOINT '%s', ACCESS_DELEGATION_MODE 'none');
%s`,
		cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, cfg.Region,
		strings.TrimPrefix(cfg.S3Endpoint, "http://"),
		clientID, clientSecret, cfg.Catalog.URI, cfg.Catalog.Scope,
		cfg.Catalog.Warehouse, cfg.Catalog.URI,
		query,
	)

	// Capture stdout only: docker writes image-pull progress to stderr when
	// the pinned image isn't cached (as on a fresh CI runner), which must not
	// pollute the query result that callers parse.
	cmd := exec.Command("docker", "run", "--rm", "--network", "host",
		writer.DuckDBDockerImage, "duckdb", "-json", "-c", script,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(t, err, "dockerized duckdb query failed: %s", stderr.String())

	return string(out)
}

// runVariantDuckDBRegression proves that variant data written by the connector
// is readable by an independent Iceberg v3 reader: a pinned dockerized DuckDB
// attaches the test REST catalog and round-trips nested objects, arrays,
// mixed-type fields, and nulls from the variant column. It runs only against
// the REST catalog: DuckDB cannot attach the Glue catalog with this mechanism,
// and the variant encoding under test is identical for both.
func runVariantDuckDBRegression(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	cfg = withAdvanced(cfg, func(a *advancedConfig) { a.VariantColumns = true })

	cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
	require.NoError(t, err)
	ensureNamespace(t, cfg)

	table := fmt.Sprintf("variant_duckdb_flow_test_%d", time.Now().Unix())
	ident := icebergtable.Identifier{cfg.Namespace, table}

	binding := &pf.MaterializationSpec_Binding{
		ResourcePath: ident,
		Collection: pf.CollectionSpec{
			Name: "acmeCo/tests/variant-duckdb",
			Projections: []pf.Projection{
				{Field: "id", Ptr: "/id", IsPrimaryKey: true, Inference: pf.Inference{
					Types:  []string{"integer"},
					Exists: pf.Inference_MUST,
				}},
				{Field: "v", Ptr: "/v", Inference: pf.Inference{
					Types:   []string{"array", "boolean", "number", "object", "string"},
					String_: &pf.Inference_String{},
				}},
			},
		},
		FieldSelection: pf.FieldSelection{
			Keys:   []string{"id"},
			Values: []string{"v"},
		},
	}

	_, apply, err := cat.CreateResource(ctx, binding, resource{})
	require.NoError(t, err)
	require.NoError(t, apply(ctx))
	t.Cleanup(func() {
		if err := cat.cat.DropTable(ctx, ident); err != nil {
			t.Log("failed to drop table", ident, err)
		}
	})

	tbl, err := cat.cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	vField, ok := tbl.Schema().FindFieldByName("v")
	require.True(t, ok)
	require.True(t, vField.Type.Equals(iceberg.VariantType{}),
		"column v: got %s, want variant", vField.Type)
	idField, ok := tbl.Schema().FindFieldByName("id")
	require.True(t, ok)

	rt := &regressionTable{
		cfg:      cfg,
		cat:      cat,
		ident:    ident,
		location: tbl.Location(),
		s3client: newTestS3Client(cfg),
	}

	// The JSON round-tripped through the variant encoding, in `id` order. A
	// JSON null value and an absent (SQL NULL) value both read back as NULL.
	vals := []string{
		`{"a": 1, "b": {"c": [1, 2, "three"], "d": null}}`,
		`[1, "two", 3.5, false, null, {"nested": true}]`,
		`"plain string"`,
		`3.5`,
		`true`,
		`null`,
		"",
	}

	idID, vID := int32(idField.ID), int32(vField.ID)
	rows := make([][]any, len(vals))
	for i, v := range vals {
		row := []any{i + 1, any(nil)}
		if v != "" {
			row[1] = []byte(v)
		}
		rows[i] = row
	}
	s3Path, _ := rt.uploadParquetRows(t, ctx, writer.ParquetSchema{
		{Name: "id", DataType: writer.PrimitiveTypeInteger, Required: true, FieldId: &idID},
		{Name: "v", DataType: writer.LogicalTypeVariant, Required: false, FieldId: &vID},
	}, rows...)
	require.NoError(t, rt.cat.appendFiles(ctx, "acmeCo/tests/variant-duckdb", rt.ident,
		[]string{s3Path}, "", "deadbeefdeadbeef"))

	out := duckdbAttachQuery(t, cfg, fmt.Sprintf(
		`SELECT id, v::JSON AS v FROM ice.%s."%s" ORDER BY id;`, cfg.Namespace, table))

	// The final -json result set is the last JSON array in the output (the
	// preceding statements each emit a small success array).
	start := strings.LastIndex(out, "[{")
	require.NotEqual(t, -1, start, "no result rows in duckdb output: %s", out)
	var got []struct {
		ID int64           `json:"id"`
		V  json.RawMessage `json:"v"`
	}
	require.NoError(t, json.Unmarshal([]byte(out[start:]), &got))
	require.Len(t, got, len(vals))

	for i, want := range vals {
		require.Equal(t, int64(i+1), got[i].ID)
		if want == "" || want == "null" {
			require.True(t, got[i].V == nil || string(got[i].V) == "null",
				"row %d: want NULL, got %s", i+1, got[i].V)
			continue
		}
		require.JSONEq(t, want, string(got[i].V), "row %d", i+1)
	}
}

// newTestS3Client builds an S3 client from the test config, using path-style
// addressing only for S3-compatible stacks with a custom endpoint (rustfs in
// TestIntegration); real AWS (Glue) uses the default endpoint resolution.
func newTestS3Client(cfg config) *s3.Client {
	s3opts := s3.Options{
		Region:      cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, ""),
	}
	if cfg.S3Endpoint != "" {
		s3opts.BaseEndpoint = aws.String(cfg.S3Endpoint)
		s3opts.UsePathStyle = true
	}
	return s3.New(s3opts)
}

// migrateProbe pairs a projection with its field config for computing a
// migration set through the connector's real MapType/Compatible/CanMigrate
// seam.
type migrateProbe struct {
	proj pf.Projection
	fc   fieldConfig
}

// computeMigrateSet mirrors the boilerplate apply path's migration computation
// over the given value fields: a selected field whose mapped type is
// incompatible with its existing column either migrates or forces a
// recreation — a variant_columns flip in either direction must always take the
// former.
func computeMigrateSet(t *testing.T, ctx context.Context, rt *regressionTable, d *materialization, fields []migrateProbe) []mboilerplate.MigrateField[mappedType] {
	t.Helper()
	tbl, err := rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)

	var out []mboilerplate.MigrateField[mappedType]
	for _, field := range fields {
		existing, ok := tbl.Schema().FindFieldByName(field.proj.Field)
		require.True(t, ok)
		ef := mboilerplate.ExistingField{
			Name:     existing.Name,
			Type:     existing.Type.Type(),
			Nullable: !existing.Required,
		}
		mt, _ := d.MapType(mboilerplate.Projection{Projection: field.proj}, field.fc)
		if !mt.Compatible(ef) && mt.CanMigrate(ef) {
			out = append(out, mboilerplate.MigrateField[mappedType]{
				From: ef,
				To: mboilerplate.MappedProjection[mappedType]{
					Projection: mboilerplate.Projection{Projection: field.proj},
					Mapped:     mt,
				},
			})
		} else {
			require.True(t, mt.Compatible(ef),
				"field %q must either migrate or remain compatible: the flip must never force recreation", field.proj.Field)
		}
	}
	return out
}

// nameMappingNames returns the set of column names present in the table's
// default name mapping.
func nameMappingNames(t *testing.T, tbl *icebergtable.Table) map[string]bool {
	t.Helper()
	var mapping iceberg.NameMapping
	require.NoError(t, json.Unmarshal([]byte(tbl.Properties()[icebergtable.DefaultNameMappingKey]), &mapping))
	mapped := make(map[string]bool)
	for _, mf := range mapping {
		for _, n := range mf.Names {
			mapped[n] = true
		}
	}
	return mapped
}

// runVariantMigrateTest exercises the column conversion performed when
// variant_columns is toggled on an existing materialization: the JSON-shaped
// column's conversion must route through the migration path (never table
// recreation), dropping and re-adding the column under the same name with a
// new field ID, upgrading the table to format v3 in the same transaction, and
// scrubbing the replaced name from the default name mapping. The decisive
// reads go through iceberg-go's spec-compliant scanner: pre-flip rows read
// null for the converted column — never a misread of their JSON-text values —
// post-flip rows read as variant, time travel to the pre-flip snapshot reads
// the old schema fully populated for field-ID-embedded files, and the key and
// castToString columns are untouched. The migration set is computed through the connector's real
// MapType/Compatible/CanMigrate seam exactly as the boilerplate apply path
// does, and recomputing it after the flip yields nothing, pinning that
// repeated applies cause no schema churn.
func runVariantMigrateTest(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	cfg = withAdvanced(cfg, func(a *advancedConfig) { a.VariantColumns = true })

	// The table as the connector created it before the flip: the JSON-shaped
	// field v is a JSON-text string column, alongside a string key and a
	// castToString-overridden field.
	rt := newRegressionTable(t, ctx, cfg, "variant_migrate", nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "cast", Type: iceberg.PrimitiveTypes.String, Required: false})

	// Two pre-flip rows land before the flip: a legacy row in a file without
	// embedded field IDs, as the connector wrote before field-ID embedding, and
	// a recent row in a file with the table's field IDs embedded, as the
	// connector writes today.
	legacyPath, _ := rt.uploadParquetRows(t, ctx, writer.ParquetSchema{
		{Name: "id", DataType: writer.LogicalTypeString, Required: false},
		{Name: "v", DataType: writer.LogicalTypeString, Required: false},
		{Name: "cast", DataType: writer.LogicalTypeString, Required: false},
	}, []any{"legacy", `{"l":true}`, "kept-legacy"})
	preIDID, preVID, preCastID := int32(1), int32(2), int32(3)
	oldPath, _ := rt.uploadParquetRows(t, ctx, writer.ParquetSchema{
		{Name: "id", DataType: writer.LogicalTypeString, Required: false, FieldId: &preIDID},
		{Name: "v", DataType: writer.LogicalTypeString, Required: false, FieldId: &preVID},
		{Name: "cast", DataType: writer.LogicalTypeString, Required: false, FieldId: &preCastID},
	}, []any{"old", `{"a":1}`, "kept"})
	require.NoError(t, rt.cat.appendFiles(ctx, "acmeCo/tests/variant-migrate", rt.ident,
		[]string{legacyPath, oldPath}, "", "checkpoint-1"))

	preFlip, err := rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)
	preFlipSnapshot := preFlip.CurrentSnapshot().SnapshotID

	keyProj := pf.Projection{Field: "id", Ptr: "/id", IsPrimaryKey: true, Inference: pf.Inference{
		Types:   []string{"string"},
		String_: &pf.Inference_String{},
		Exists:  pf.Inference_MUST,
	}}
	vProj := pf.Projection{Field: "v", Ptr: "/v", Inference: pf.Inference{Types: []string{"object"}}}
	castProj := pf.Projection{Field: "cast", Ptr: "/cast", Inference: pf.Inference{
		Types:   []string{"integer", "string"},
		String_: &pf.Inference_String{},
	}}

	d := &materialization{cfg: cfg}

	computeMigrates := func() []mboilerplate.MigrateField[mappedType] {
		return computeMigrateSet(t, ctx, rt, d, []migrateProbe{
			{vProj, fieldConfig{}},
			{castProj, fieldConfig{IgnoreStringFormat: true}},
		})
	}

	// The key column keeps its string mapping with the option on, so the flip
	// leaves it compatible (the boilerplate additionally never migrates keys).
	keyMapped, _ := d.MapType(mboilerplate.Projection{Projection: keyProj}, fieldConfig{})
	require.True(t, keyMapped.Compatible(mboilerplate.ExistingField{Name: "id", Type: "string"}))

	migrates := computeMigrates()
	require.Len(t, migrates, 1, "the variant_columns flip must route the JSON-shaped column through migration")
	require.Equal(t, "v", migrates[0].From.Name)

	_, apply, err := rt.cat.UpdateResource(ctx, mboilerplate.BindingUpdate[config, resource, mappedType]{
		Binding: mboilerplate.MappedBinding[config, resource, mappedType]{
			MaterializationSpec_Binding: pf.MaterializationSpec_Binding{
				ResourcePath: rt.ident,
			},
		},
		FieldsToMigrate: migrates,
	})
	require.NoError(t, err)
	require.NoError(t, apply(ctx))

	tbl, err := rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)
	require.Equal(t, 3, tbl.Metadata().Version(), "the flip must upgrade the table to format v3")
	f, ok := tbl.Schema().FindFieldByName("v")
	require.True(t, ok, "the converted column must keep its name")
	require.True(t, f.Type.Equals(iceberg.VariantType{}))
	require.NotEqual(t, 2, f.ID, "the converted column must get a new field ID")

	// The key and castToString columns are untouched by the flip.
	for name, wantID := range map[string]int{"id": 1, "cast": 3} {
		uf, ok := tbl.Schema().FindFieldByName(name)
		require.True(t, ok)
		require.Equal(t, wantID, uf.ID)
		require.True(t, uf.Type.Equals(iceberg.PrimitiveTypes.String))
	}

	// The replaced name is scrubbed from the default name mapping so the
	// pre-flip file's JSON-text column cannot mis-bind to the variant column;
	// the untouched columns keep their entries.
	mapped := nameMappingNames(t, tbl)
	require.False(t, mapped["v"], "converted column must be removed from the name mapping")
	require.True(t, mapped["id"] && mapped["cast"], "untouched columns must keep their name-mapping entries")

	// The pre-flip rows survive the flip with their converted column reading
	// null — never a misread of their JSON-text values — under the current
	// schema, while their untouched columns stay fully readable (the legacy
	// file's, via the surgically-scrubbed name mapping).
	require.ElementsMatch(t, []any{"legacy", "old"}, scanColumn(t, ctx, rt, "id"))
	require.ElementsMatch(t, []any{nil, nil}, scanColumn(t, ctx, rt, "v"))
	require.ElementsMatch(t, []any{"kept-legacy", "kept"}, scanColumn(t, ctx, rt, "cast"))

	// Time travel to the pre-flip snapshot reads the old schema: the field-ID-
	// embedded row is fully populated, while the legacy row's converted column
	// is null — the name mapping is a table property, not versioned per
	// snapshot, so its scrubbed entry is gone for historical reads too, the
	// same trade-off the nanosecond-timestamps migration shipped with.
	require.ElementsMatch(t, []any{"legacy", "old"}, scanColumn(t, ctx, rt, "id", icebergtable.WithSnapshotID(preFlipSnapshot)))
	require.ElementsMatch(t, []any{nil, `{"a":1}`}, scanColumn(t, ctx, rt, "v", icebergtable.WithSnapshotID(preFlipSnapshot)))
	require.ElementsMatch(t, []any{"kept-legacy", "kept"}, scanColumn(t, ctx, rt, "cast", icebergtable.WithSnapshotID(preFlipSnapshot)))

	// A post-flip row lands with the table's field IDs embedded and the
	// converted column written as variant, as the connector now writes.
	idID, castID, vID := int32(1), int32(3), int32(f.ID)
	newPath, _ := rt.uploadParquetRows(t, ctx, writer.ParquetSchema{
		{Name: "id", DataType: writer.LogicalTypeString, Required: false, FieldId: &idID},
		{Name: "v", DataType: writer.LogicalTypeVariant, Required: false, FieldId: &vID},
		{Name: "cast", DataType: writer.LogicalTypeString, Required: false, FieldId: &castID},
	}, []any{"new", []byte(`{"b":2}`), "kept2"})
	require.NoError(t, rt.cat.appendFiles(ctx, "acmeCo/tests/variant-migrate", rt.ident,
		[]string{newPath}, "checkpoint-1", "checkpoint-2"))

	require.ElementsMatch(t, []any{"legacy", "old", "new"}, scanColumn(t, ctx, rt, "id"))
	require.ElementsMatch(t, []any{nil, nil, `{"b":2}`}, scanColumn(t, ctx, rt, "v"))

	// Recomputing the migration set after the flip yields nothing: repeated
	// applies are idempotent and cause no schema churn.
	require.Empty(t, computeMigrates())
}

// runVariantDisableFlipTest is the mirror of runVariantMigrateTest: disabling
// variant_columns on a materialization with existing variant columns must also
// route through the migration path (never table recreation), dropping and
// re-adding the column as a JSON-text string under the same name with a new
// field ID, scrubbing the replaced name from the default name mapping, and
// leaving the table at format v3. Pre-flip variant rows read null for the
// converted column under the current schema — the same going-forward semantics
// as the enable flip — while time travel to the pre-flip snapshot reads the
// variant data fully populated (the connector always embeds field IDs when
// writing variant, so no pre-flip file depends on the scrubbed mapping). The
// key and castToString columns are untouched, and recomputing the migration
// set after the flip yields nothing, pinning that repeated applies cause no
// schema churn.
func runVariantDisableFlipTest(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	cfg = withAdvanced(cfg, func(a *advancedConfig) { a.VariantColumns = false })

	// The table as the connector created it while variant_columns was on: the
	// JSON-shaped field v is a variant column in a format v3 table, alongside
	// a string key and a castToString-overridden field.
	rt := newRegressionTable(t, ctx, cfg, "variant_disable",
		iceberg.Properties{icebergtable.PropertyFormatVersion: "3"},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.VariantType{}, Required: false},
		iceberg.NestedField{ID: 3, Name: "cast", Type: iceberg.PrimitiveTypes.String, Required: false})

	// A pre-flip row lands as the connector wrote it with the option on:
	// field IDs embedded and v encoded as variant.
	idID, vID, castID := int32(1), int32(2), int32(3)
	prePath, _ := rt.uploadParquetRows(t, ctx, writer.ParquetSchema{
		{Name: "id", DataType: writer.LogicalTypeString, Required: false, FieldId: &idID},
		{Name: "v", DataType: writer.LogicalTypeVariant, Required: false, FieldId: &vID},
		{Name: "cast", DataType: writer.LogicalTypeString, Required: false, FieldId: &castID},
	}, []any{"pre", []byte(`{"a":1}`), "kept"})
	require.NoError(t, rt.cat.appendFiles(ctx, "acmeCo/tests/variant-disable",
		rt.ident, []string{prePath}, "", "checkpoint-1"))

	preFlip, err := rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)
	preFlipSnapshot := preFlip.CurrentSnapshot().SnapshotID

	keyProj := pf.Projection{Field: "id", Ptr: "/id", IsPrimaryKey: true, Inference: pf.Inference{
		Types:   []string{"string"},
		String_: &pf.Inference_String{},
		Exists:  pf.Inference_MUST,
	}}
	vProj := pf.Projection{Field: "v", Ptr: "/v", Inference: pf.Inference{Types: []string{"object"}}}
	castProj := pf.Projection{Field: "cast", Ptr: "/cast", Inference: pf.Inference{
		Types:   []string{"integer", "string"},
		String_: &pf.Inference_String{},
	}}

	d := &materialization{cfg: cfg}

	computeMigrates := func() []mboilerplate.MigrateField[mappedType] {
		return computeMigrateSet(t, ctx, rt, d, []migrateProbe{
			{vProj, fieldConfig{}},
			{castProj, fieldConfig{IgnoreStringFormat: true}},
		})
	}

	// The key column's string mapping is unaffected by the option, so the flip
	// leaves it compatible (the boilerplate additionally never migrates keys).
	keyMapped, _ := d.MapType(mboilerplate.Projection{Projection: keyProj}, fieldConfig{})
	require.True(t, keyMapped.Compatible(mboilerplate.ExistingField{Name: "id", Type: "string"}))

	migrates := computeMigrates()
	require.Len(t, migrates, 1, "the disable flip must route the variant column through migration")
	require.Equal(t, "v", migrates[0].From.Name)

	_, apply, err := rt.cat.UpdateResource(ctx, mboilerplate.BindingUpdate[config, resource, mappedType]{
		Binding: mboilerplate.MappedBinding[config, resource, mappedType]{
			MaterializationSpec_Binding: pf.MaterializationSpec_Binding{
				ResourcePath: rt.ident,
			},
		},
		FieldsToMigrate: migrates,
	})
	require.NoError(t, err)
	require.NoError(t, apply(ctx))

	tbl, err := rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)
	require.Equal(t, 3, tbl.Metadata().Version(), "the disable flip must leave the table at format v3")
	f, ok := tbl.Schema().FindFieldByName("v")
	require.True(t, ok, "the converted column must keep its name")
	require.True(t, f.Type.Equals(iceberg.PrimitiveTypes.String))
	require.NotEqual(t, 2, f.ID, "the converted column must get a new field ID")

	// The key and castToString columns are untouched by the flip.
	for name, wantID := range map[string]int{"id": 1, "cast": 3} {
		uf, ok := tbl.Schema().FindFieldByName(name)
		require.True(t, ok)
		require.Equal(t, wantID, uf.ID)
		require.True(t, uf.Type.Equals(iceberg.PrimitiveTypes.String))
	}

	// The replaced name is scrubbed from the default name mapping, matching
	// the enable flip's guard against mis-binding field-ID-less files; the
	// untouched columns keep their entries.
	mapped := nameMappingNames(t, tbl)
	require.False(t, mapped["v"], "converted column must be removed from the name mapping")
	require.True(t, mapped["id"] && mapped["cast"], "untouched columns must keep their name-mapping entries")

	// The pre-flip row survives the flip with its converted column reading
	// null — never a misread of its variant bytes — under the current schema,
	// while its untouched columns stay fully readable.
	require.ElementsMatch(t, []any{"pre"}, scanColumn(t, ctx, rt, "id"))
	require.ElementsMatch(t, []any{nil}, scanColumn(t, ctx, rt, "v"))
	require.ElementsMatch(t, []any{"kept"}, scanColumn(t, ctx, rt, "cast"))

	// Time travel to the pre-flip snapshot reads the old schema fully
	// populated: the pre-flip file embeds field IDs, so the name-mapping scrub
	// cannot affect it.
	require.ElementsMatch(t, []any{"pre"}, scanColumn(t, ctx, rt, "id", icebergtable.WithSnapshotID(preFlipSnapshot)))
	require.ElementsMatch(t, []any{`{"a":1}`}, scanColumn(t, ctx, rt, "v", icebergtable.WithSnapshotID(preFlipSnapshot)))

	// A post-flip row lands with the converted column written as JSON text
	// under its new field ID, as the connector now writes.
	newVID := int32(f.ID)
	newPath, _ := rt.uploadParquetRows(t, ctx, writer.ParquetSchema{
		{Name: "id", DataType: writer.LogicalTypeString, Required: false, FieldId: &idID},
		{Name: "v", DataType: writer.LogicalTypeString, Required: false, FieldId: &newVID},
		{Name: "cast", DataType: writer.LogicalTypeString, Required: false, FieldId: &castID},
	}, []any{"post", `{"b":2}`, "kept2"})
	require.NoError(t, rt.cat.appendFiles(ctx, "acmeCo/tests/variant-disable",
		rt.ident, []string{newPath}, "checkpoint-1", "checkpoint-2"))

	require.ElementsMatch(t, []any{"pre", "post"}, scanColumn(t, ctx, rt, "id"))
	require.ElementsMatch(t, []any{nil, `{"b":2}`}, scanColumn(t, ctx, rt, "v"))

	// Recomputing the migration set after the flip yields nothing: repeated
	// applies are idempotent and cause no schema churn.
	require.Empty(t, computeMigrates())
}

// TestMapTypeVariantWiring pins the variant_columns mapping seam: with the
// option enabled, object, array, multi-type, and root document projections map
// to variant, while collection keys and ignoreStringFormat-overridden fields
// keep their string mapping. With the option disabled, everything keeps the
// current JSON-string mapping.
func TestMapTypeVariantWiring(t *testing.T) {
	mkProj := func(field string, isKey bool, withString bool, types ...string) mboilerplate.Projection {
		inference := pf.Inference{Exists: pf.Inference_MUST, Types: types}
		if withString {
			inference.String_ = &pf.Inference_String{}
		}
		ptr := "/" + field
		if field == "flow_document" {
			ptr = ""
		}
		return mboilerplate.Projection{
			Projection: pf.Projection{
				Field:        field,
				Ptr:          ptr,
				IsPrimaryKey: isKey,
				Inference:    inference,
			},
		}
	}

	var plain = &materialization{}
	var variant = &materialization{cfg: config{Advanced: &advancedConfig{VariantColumns: true}}}

	for _, tc := range []struct {
		name        string
		p           mboilerplate.Projection
		fc          fieldConfig
		wantPlain   string
		wantVariant string
	}{
		{"object", mkProj("obj", false, false, "object"), fieldConfig{}, "string", "variant"},
		{"array", mkProj("arr", false, false, "array"), fieldConfig{}, "string", "variant"},
		{"multi-type", mkProj("multi", false, true, "integer", "string"), fieldConfig{}, "string", "variant"},
		{"root document", mkProj("flow_document", false, false, "object"), fieldConfig{}, "string", "variant"},
		{"multi-type key", mkProj("multiKey", true, true, "integer", "string"), fieldConfig{}, "string", "string"},
		{"castToString override", mkProj("cast", false, true, "integer", "string"), fieldConfig{IgnoreStringFormat: true}, "string", "string"},
		{"single scalar", mkProj("num", false, false, "integer"), fieldConfig{}, "long", "long"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mt, conv := plain.MapType(tc.p, tc.fc)
			require.Equal(t, tc.wantPlain, mt.String())
			require.Nil(t, conv)

			mt, conv = variant.MapType(tc.p, tc.fc)
			require.Equal(t, tc.wantVariant, mt.String())
			require.Nil(t, conv)
		})
	}
}

// TestCanMigrate pins the migratable set: the microsecond↔nanosecond
// timestamp pair in both directions, any non-variant type to variant (the
// variant_columns flip, including column types that had previously evolved),
// and variant back to string (disabling variant_columns or applying
// castToString). Anything else must remain INCOMPATIBLE (backfill) rather
// than silently migrating.
func TestCanMigrate(t *testing.T) {
	us := mappedType{icebergType: iceberg.PrimitiveTypes.TimestampTz}
	ns := mappedType{icebergType: iceberg.PrimitiveTypes.TimestampTzNs}
	str := mappedType{icebergType: iceberg.PrimitiveTypes.String}
	vnt := mappedType{icebergType: iceberg.VariantType{}}
	lng := mappedType{icebergType: iceberg.PrimitiveTypes.Int64}

	require.True(t, ns.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz"}))
	require.True(t, us.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz_ns"}))
	require.False(t, us.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz"}))
	require.False(t, ns.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz_ns"}))
	require.False(t, ns.CanMigrate(mboilerplate.ExistingField{Type: "string"}))
	require.False(t, str.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz"}))
	require.False(t, str.CanMigrate(mboilerplate.ExistingField{Type: "long"}))

	// Enabling variant_columns converts existing JSON-string columns, and any
	// column type a JSON-shaped field had previously evolved to, into variant.
	require.True(t, vnt.CanMigrate(mboilerplate.ExistingField{Type: "string"}))
	require.True(t, vnt.CanMigrate(mboilerplate.ExistingField{Type: "long"}))
	require.True(t, vnt.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz"}))
	require.False(t, vnt.CanMigrate(mboilerplate.ExistingField{Type: "variant"}))
	// Disabling variant_columns (or applying castToString) migrates variant
	// back to a JSON string, but never to any other type.
	require.True(t, str.CanMigrate(mboilerplate.ExistingField{Type: "variant"}))
	require.False(t, lng.CanMigrate(mboilerplate.ExistingField{Type: "variant"}))
}
