package connector

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/estuary/connectors/go/writer"
	"github.com/estuary/connectors/materialize-iceberg/catalog"
	"github.com/estuary/connectors/materialize-iceberg/python"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pf "github.com/estuary/flow/go/protocols/flow"
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

	t.Run("apply-drain", func(t *testing.T) {
		boilerplate.RunTestAllTasks(t, applySpec, func(t *testing.T, bundled []byte, taskName string, cfg config) {
			if cfg.Compute.ComputeType != computeTypeSparkStandalone {
				// Row verification runs eager queries through the local spark
				// daemon, which the cloud compute variants don't expose.
				t.Skipf("apply-drain only runs against the local spark stack, not %q", cfg.Compute.ComputeType)
			}

			ctx := context.Background()
			tableName := fmt.Sprintf("applydrain%s_flow_test_%d", uuid.NewString()[:8], time.Now().Unix())
			res := makeResourceFn(tableName, false).WithDefaults(cfg)

			// sparkExec runs an eagerly-evaluated statement through the local
			// spark daemon, returning its error, since this connector has no
			// direct query access to table data.
			sparkExec := func(t *testing.T, query string) error {
				t.Helper()
				body, err := json.Marshal(struct {
					Action string           `json:"action"`
					Input  python.ExecInput `json:"input"`
				}{Action: "exec", Input: python.ExecInput{Query: query}})
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
				if !result.Success {
					return fmt.Errorf("%s", result.Error)
				}
				return nil
			}

			seedPending := func(t *testing.T, appliedSpec *pf.MaterializationSpec) json.RawMessage {
				// A staged transaction is a Spark SQL query persisted in the
				// connector state along with the staged CSV files it consumes
				// through a temporary view, keyed by the binding's state key.
				// The file must exist since recovery prunes missing files and
				// Acknowledge deletes them after the merge job succeeds.
				bucket, err := cfg.toBucket(ctx)
				require.NoError(t, err)
				fileKey := stagedFileClient{}.NewKey([]string{cfg.Compute.BucketPath, fmt.Sprintf("applydrain-%s", uuid.NewString()), "seed"})
				fileWriter := stagedFileClient{}.NewWriter(bucket.NewWriter(ctx, fileKey), []string{"key"})
				require.NoError(t, fileWriter.Write([]any{"k1"}))
				require.NoError(t, fileWriter.Close())

				b := appliedSpec.Bindings[0]
				fqnParts := []string{"`estuary`"}
				for _, part := range b.ResourcePath {
					fqnParts = append(fqnParts, quoteIdentifier(part))
				}

				// Literals for the fields of the drain fixture specs; the key
				// is read from the staged file's temporary view.
				literals := map[string]string{
					"key":                  "r.`key`",
					"flow_published_at":    "current_timestamp()",
					"_meta/flow_truncated": "false",
					"optionalBoolean":      "true",
					"requiredBoolean":      "true",
					"optionalInteger":      "2",
					"requiredInteger":      "1",
					"optionalString":       "'opt'",
					"requiredString":       "'req'",
					"optionalObject":       "'{}'",
					"requiredObject":       "'{}'",
					"second_root":          "'{}'",
					"flow_document":        "'{}'",
				}
				fields := append(append([]string{}, b.FieldSelection.Keys...), b.FieldSelection.Values...)
				if b.FieldSelection.Document != "" {
					fields = append(fields, b.FieldSelection.Document)
				}
				var cols, vals []string
				for _, f := range fields {
					lit, ok := literals[f]
					require.True(t, ok, "no seed literal for selected field %q", f)
					cols = append(cols, quoteIdentifier(f))
					vals = append(vals, lit)
				}
				query := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM merge_view_0 AS r",
					strings.Join(fqnParts, "."), strings.Join(cols, ", "), strings.Join(vals, ", "))

				state, err := json.Marshal(map[string]*python.MergeBinding{
					b.StateKey: {
						Binding: 0,
						Query:   query,
						Columns: []python.NestedField{{Name: "key", Type: "string"}},
						Files:   []string{bucket.URI(fileKey)},
					},
				})
				require.NoError(t, err)
				return state
			}

			verifyDrained := func(t *testing.T, appliedSpec *pf.MaterializationSpec, _ []string, _ [][]any) {
				// SnapshotTestResource is unimplemented for this connector, so
				// assert on the committed row with an eager CTAS: assert_true
				// fails the statement unless the target holds exactly the
				// seeded row.
				b := appliedSpec.Bindings[0]
				fqnParts := []string{"`estuary`"}
				for _, part := range b.ResourcePath {
					fqnParts = append(fqnParts, quoteIdentifier(part))
				}
				fqn := strings.Join(fqnParts, ".")
				verifyFQN := fmt.Sprintf("`estuary`.%s.%s", quoteIdentifier(b.ResourcePath[0]), quoteIdentifier(tableName+"_verify"))

				require.NoError(t, sparkExec(t, fmt.Sprintf("DROP TABLE IF EXISTS %s", verifyFQN)))
				require.NoError(t, sparkExec(t,
					fmt.Sprintf("CREATE TABLE %s AS SELECT CAST(assert_true((SELECT count(*) FROM %s) = 1, 'expected exactly one committed row') AS STRING) AS ok", verifyFQN, fqn)),
					"the staged transaction's row must have been committed")
				require.NoError(t, sparkExec(t, fmt.Sprintf("DROP TABLE IF EXISTS %s", verifyFQN)))
			}

			boilerplate.RunApplyDrainTest(t, &Driver{}, newMaterialization, cfg, res, seedPending, verifyDrained)
		})
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
