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

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, newMaterialization, "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("apply-drain", func(t *testing.T) {
		ctx := context.Background()
		tableName := fmt.Sprintf("applydrain%s_flow_test_%d", uuid.NewString()[:8], time.Now().Unix())
		res := makeResourceFn(tableName, true).WithDefaults(cfg)

		seedPending := func(t *testing.T, appliedSpec *pf.MaterializationSpec) json.RawMessage {
			// A staged transaction is a set of parquet data files recorded in
			// the connector state, appended to the Iceberg table during
			// Acknowledge under a per-materialization checkpoint fence. The
			// file must carry the table's field IDs, exactly as the connector
			// writes them.
			b := appliedSpec.Bindings[0]

			cat, err := newCatalog(ctx, cfg, NestedLocationStyle)
			require.NoError(t, err)
			infos, err := cat.tableInfos(ctx, [][]string{b.ResourcePath})
			require.NoError(t, err)
			require.NotEmpty(t, infos[0].location, "the base Apply must have created the table")

			pqSchema, err := parquetSchema(b.FieldSelection.AllFields(), b.Collection, b.FieldSelection.FieldConfigJsonMap, cfg.nanosecondTimestamps())
			require.NoError(t, err)
			for i := range pqSchema {
				id, ok := infos[0].fieldIDs[pqSchema[i].Name]
				require.True(t, ok, "no field ID for column %q", pqSchema[i].Name)
				id32 := int32(id)
				pqSchema[i].FieldId = &id32
			}

			// Values for the fields of the drain fixture specs.
			literals := map[string]any{
				"key":                  "k1",
				"flow_published_at":    "2024-01-01T00:00:00Z",
				"_meta/flow_truncated": false,
				"optionalBoolean":      true,
				"requiredBoolean":      true,
				"optionalInteger":      int64(2),
				"requiredInteger":      int64(1),
				"optionalString":       "opt",
				"requiredString":       "req",
				"optionalObject":       json.RawMessage(`{}`),
				"requiredObject":       json.RawMessage(`{}`),
				"second_root":          json.RawMessage(`{}`),
				"flow_document":        json.RawMessage(`{}`),
			}
			var row []any
			for _, f := range b.FieldSelection.AllFields() {
				v, ok := literals[f]
				require.True(t, ok, "no seed value for selected field %q", f)
				row = append(row, v)
			}

			s3opts := s3.Options{
				Region:      cfg.Region,
				Credentials: credentials.NewStaticCredentialsProvider(cfg.Credentials.AWSAccessKeyID, cfg.Credentials.AWSSecretAccessKey, ""),
			}
			if cfg.S3Endpoint != "" {
				s3opts.BaseEndpoint = aws.String(cfg.S3Endpoint)
				s3opts.UsePathStyle = true
			}
			rt := &regressionTable{
				cfg:      cfg,
				cat:      cat,
				ident:    icebergtable.Identifier(b.ResourcePath),
				location: infos[0].location,
				s3client: s3.New(s3opts),
			}
			s3Path, _ := rt.uploadParquetRows(t, ctx, pqSchema, row)

			state, err := json.Marshal(map[string]any{
				"bindingStates": map[string]any{
					b.StateKey: map[string]any{
						"fileKeys":          []string{s3Path},
						"currentCheckpoint": "applydrain-" + uuid.NewString()[:8],
					},
				},
			})
			require.NoError(t, err)
			return state
		}

		verifyDrained := func(t *testing.T, _ *pf.MaterializationSpec, _ []string, rows [][]any) {
			require.Len(t, rows, 1, "the staged transaction's files must have been appended")
		}

		boilerplate.RunApplyDrainTest(t, &driver{}, newMaterialization, cfg, res, seedPending, verifyDrained)
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
}

// TestRestGlueSnapshotParity enforces that the REST and Glue materialize
// snapshots are byte-identical after sanitization. Any divergence means the two
// catalog code paths produced different results — the drift this coverage
// exists to catch.
func TestRestGlueSnapshotParity(t *testing.T) {
	for _, name := range []string{"materialize", "materialize-ns"} {
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

// runNanosecondTimestampTest verifies that timestamptz_ns (Iceberg v3 format) columns:
//   - write nanosecond-precision timestamps correctly
//   - clamp out-of-range values (before 1677 / after 2262) to int64 min/max rather than overflowing
//   - encode exact int64 nanosecond values in the produced Parquet file
//     (asserted against the raw column bytes)
func runNanosecondTimestampTest(t *testing.T, cfg config) {
	t.Helper()
	ctx := context.Background()

	// Advanced is a pointer shared with the caller's cfg: clone it before
	// mutating so the flag doesn't leak into subtests that run after this one.
	adv := advancedConfig{}
	if cfg.Advanced != nil {
		adv = *cfg.Advanced
	}
	adv.NanosecondTimestamps = true
	cfg.Advanced = &adv
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

	// Advanced is a pointer shared with the caller's cfg: clone it before
	// mutating so the flag doesn't leak into subtests that run after this one.
	adv := advancedConfig{}
	if cfg.Advanced != nil {
		adv = *cfg.Advanced
	}
	adv.NanosecondTimestamps = true
	cfg.Advanced = &adv
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

	// Advanced is a pointer shared with the caller's cfg: clone it before
	// mutating so the flag doesn't leak into subtests that run after this one.
	adv := advancedConfig{}
	if cfg.Advanced != nil {
		adv = *cfg.Advanced
	}
	adv.NanosecondTimestamps = true
	cfg.Advanced = &adv

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
// column's unit, with nil for nulls.
func scanColumn(t *testing.T, ctx context.Context, rt *regressionTable, name string) []any {
	t.Helper()

	tbl, err := rt.cat.cat.LoadTable(ctx, rt.ident)
	require.NoError(t, err)
	arrowTbl, err := tbl.Scan().ToArrowTable(ctx)
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

// TestCanMigrate pins the migratable set to exactly the microsecond↔nanosecond
// timestamp pair: anything else must remain INCOMPATIBLE (backfill) rather
// than silently migrating.
func TestCanMigrate(t *testing.T) {
	us := mappedType{icebergType: iceberg.PrimitiveTypes.TimestampTz}
	ns := mappedType{icebergType: iceberg.PrimitiveTypes.TimestampTzNs}
	str := mappedType{icebergType: iceberg.PrimitiveTypes.String}

	require.True(t, ns.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz"}))
	require.True(t, us.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz_ns"}))
	require.False(t, us.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz"}))
	require.False(t, ns.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz_ns"}))
	require.False(t, ns.CanMigrate(mboilerplate.ExistingField{Type: "string"}))
	require.False(t, str.CanMigrate(mboilerplate.ExistingField{Type: "timestamptz"}))
	require.False(t, str.CanMigrate(mboilerplate.ExistingField{Type: "long"}))
}
