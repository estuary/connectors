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
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	boilertest "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/estuary/connectors/materialize-iceberg/catalog"
	"github.com/estuary/connectors/materialize-iceberg/python"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/assert"
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
		boilertest.RunMaterializationTestParallel(t, newMaterialization, materializeSpec, makeResourceFn, actionDescSanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		boilertest.RunApplyTestParallel(t, &Driver{}, newMaterialization, applySpec, makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		boilertest.RunMigrationTestParallel(t, newMaterialization, migrateSpec, makeResourceFn, nil)
	})

	t.Run("ts-overflow-regression", func(t *testing.T) {
		runTimestampOverflowRegression(t)
	})

	t.Run("date-overflow-regression", func(t *testing.T) {
		runDateOverflowRegression(t)
	})

	t.Run("add-required-column-regression", func(t *testing.T) {
		runAddRequiredColumnRegression(t)
	})

	t.Run("demote-required-column-regression", func(t *testing.T) {
		runDemoteRequiredColumnRegression(t)
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

// runAddRequiredColumnRegression reproduces the Snowflake-rejection failure
// mode reported for the `html_body` column: errno 100478, "Invalid parquet
// file: non-nullable column without default missing data".
//
// The trigger is schema evolution, not value size. When the connector adds a
// projection whose inference says MUST-exist (or it becomes a primary key) to
// a table that already has data files, `appendProjectionsAsFields` in
// type_mapping.go writes the new field with `required: true`. Pre-existing
// data files have no data for the new field id, and the resulting manifest
// entries have no `null_value_counts` entry for it. Snowflake (and any other
// reader that refuses to assume "absent ⇒ 0" for a required column) then
// rejects the table.
//
// The test creates a table with `id` only, inserts a row, then evolves the
// schema by adding `payload` as a required column - the same kind of update
// `UpdateResource` issues via `catalog.AddSchemaUpdate` + `SetCurrentSchemaUpdate`
// - and asserts that the manifest entry carries `null_value_counts` for the
// new field id. Pre-fix the assertion fails because the metric maps for the
// pre-existing data file have no entry for field id 2.
func runAddRequiredColumnRegression(t *testing.T) {
	ctx := context.Background()

	creds, err := readPolarisCreds()
	require.NoError(t, err)
	credential := creds.ClientID + ":" + creds.ClientSecret
	scope := "PRINCIPAL_ROLE:flow_user_role"

	cat, err := catalog.New(ctx, "http://localhost:9802/api/catalog", "quickstart_catalog",
		catalog.WithClientCredential(credential, "v1/oauth/tokens", &scope))
	require.NoError(t, err)

	s3client := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("flow", "flow", ""),
		BaseEndpoint: aws.String("http://localhost:9800"),
		UsePathStyle: true,
	})

	const (
		ns      = "add_required_column_regression"
		tblName = "payload"
	)

	// Start with id only.
	schemaV0 := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	_ = cat.DeleteTable(ctx, ns, tblName)
	// CreateNamespace is idempotent for our purposes - an "already exists"
	// just means leftover state from a prior run.
	_ = cat.CreateNamespace(ctx, ns)
	require.NoError(t, cat.CreateTable(ctx, ns, tblName, schemaV0, nil, nil, nil))
	t.Cleanup(func() { _ = cat.DeleteTable(context.Background(), ns, tblName) })

	// Insert a row through the production CSV → Spark merge daemon path, so
	// the table has a real data file written by Spark (matching what
	// happens in production before the schema evolves).
	csvPath := filepath.Join(t.TempDir(), "data.csv.gz")
	csvFile, err := os.Create(csvPath)
	require.NoError(t, err)
	csvw := writer.NewCsvWriter(csvFile, []string{"id"},
		writer.WithCsvSkipHeaders(), writer.WithCsvQuoteChar('`'))
	require.NoError(t, csvw.Write([]any{"row1"}))
	require.NoError(t, csvw.Close())
	csvBody, err := os.ReadFile(csvPath)
	require.NoError(t, err)
	csvKey := "staging/add-required-col-" + uuid.New().String() + ".csv.gz"
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("warehouse"),
		Key:    aws.String(csvKey),
		Body:   bytes.NewReader(csvBody),
	})
	require.NoError(t, err)

	mergeInput := python.MergeInput{
		Bindings: []python.MergeBinding{{
			Binding: 0,
			Query:   fmt.Sprintf("INSERT INTO estuary.%s.%s SELECT id FROM merge_view_0", ns, tblName),
			Columns: []python.NestedField{{Name: "id", Type: "string"}},
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
	require.Truef(t, result.Success, "initial merge failed: %s", result.Error)

	// Evolve the schema through the connector's own code path. The
	// boilerplate hands UpdateResource a BindingUpdate with NewProjections;
	// the connector calls computeSchemaForUpdatedTable to derive the next
	// schema and submits it via cat.UpdateTable. Exercising that function
	// here means the test reflects exactly what the connector emits.
	tbl, err := cat.GetTable(ctx, ns, tblName)
	require.NoError(t, err)
	current := tbl.Metadata.CurrentSchema()

	newPayload := boilerplate.MappedProjection[mapped]{
		Projection: boilerplate.Projection{
			Projection: pf.Projection{Field: "payload"},
			// MustExist=true is the production trigger - the source
			// collection's required array includes the new field, and its
			// types doesn't include "null", so MustExist comes back true.
			MustExist: true,
		},
		Mapped: mapped{type_: iceberg.PrimitiveTypes.String, Name: "payload"},
	}
	bindingUpdate := boilerplate.BindingUpdate[config, resource, mapped]{
		NewProjections: []boilerplate.MappedProjection[mapped]{newPayload},
	}
	schemaV1 := computeSchemaForUpdatedTable(current.Fields()[len(current.Fields())-1].ID, current, bindingUpdate)

	require.NoError(t, cat.UpdateTable(ctx, ns, tblName,
		[]catalog.TableRequirement{catalog.AssertCurrentSchemaID(current.ID)},
		[]catalog.TableUpdate{
			catalog.AddSchemaUpdate(schemaV1),
			catalog.SetCurrentSchemaUpdate(schemaV1.ID),
		},
	))

	// Locate the `payload` field in the schema the connector produced -
	// the field id is whatever computeSchemaForUpdatedTable assigned, not
	// a hard-coded 2.
	var payloadField *iceberg.NestedField
	for _, f := range schemaV1.Fields() {
		if f.Name == "payload" {
			f := f
			payloadField = &f
			break
		}
	}
	require.NotNil(t, payloadField, "computeSchemaForUpdatedTable did not produce a payload field")
	payloadFieldID := payloadField.ID

	// Walk the current snapshot's manifests via the iceberg-go avro
	// readers, fed directly from the same MinIO that holds the data files.
	tblAfter, err := cat.GetTable(ctx, ns, tblName)
	require.NoError(t, err)
	snap := tblAfter.Metadata.CurrentSnapshot()
	require.NotNilf(t, snap, "no current snapshot for %s.%s", ns, tblName)

	openS3 := func(s3URI string) io.ReadCloser {
		require.True(t, strings.HasPrefix(s3URI, "s3://warehouse/"),
			"unexpected s3 uri: %s", s3URI)
		key := strings.TrimPrefix(s3URI, "s3://warehouse/")
		out, err := s3client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("warehouse"),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		return out.Body
	}

	mlBody := openS3(snap.ManifestList)
	defer mlBody.Close()
	manifests, err := iceberg.ReadManifestList(mlBody)
	require.NoError(t, err)
	require.NotEmptyf(t, manifests, "no manifests for %s.%s", ns, tblName)

	hasValueCount, hasNullCount, hasLowerBound, hasUpperBound := true, true, true, true
	entriesSeen := 0
	for _, mf := range manifests {
		mfBody := openS3(mf.FilePath())
		entries, err := iceberg.ReadManifest(mf, mfBody, true)
		mfBody.Close()
		require.NoError(t, err)
		for _, e := range entries {
			df := e.DataFile()
			if _, ok := df.ValueCounts()[payloadFieldID]; !ok {
				hasValueCount = false
			}
			if _, ok := df.NullValueCounts()[payloadFieldID]; !ok {
				hasNullCount = false
			}
			if _, ok := df.LowerBoundValues()[payloadFieldID]; !ok {
				hasLowerBound = false
			}
			if _, ok := df.UpperBoundValues()[payloadFieldID]; !ok {
				hasUpperBound = false
			}
			entriesSeen++
		}
	}
	require.NotZerof(t, entriesSeen, "no manifest entries for %s.%s", ns, tblName)

	t.Logf("computeSchemaForUpdatedTable produced payload: Required=%v (fieldID=%d)", payloadField.Required, payloadFieldID)
	t.Logf("manifest metric maps for payload column added after data: ValueCounts=%v NullValueCounts=%v LowerBounds=%v UpperBounds=%v",
		hasValueCount, hasNullCount, hasLowerBound, hasUpperBound)

	// Snowflake's errno 100478 fires when a `required` column lacks
	// null_value_counts in the manifest. If the connector emits the new
	// field as Optional - the post-fix state - Snowflake doesn't care that
	// the metric maps are empty for pre-existing data files, because the
	// column is allowed to be null. The test passes either way provided
	// the connector never produces the (Required, no-null_value_counts)
	// combination.
	if payloadField.Required {
		assert.True(t, hasNullCount,
			"a Required column added to an existing table requires null_value_counts in the manifest; the connector should have emitted Optional instead")
	}
}

// runDemoteRequiredColumnRegression covers the inverse direction of
// runAddRequiredColumnRegression: a column that was MUST-exist when the table
// was created later has its inference relax (a document arrives without it,
// or the inferred-schema engine demotes it). This flows through the
// boilerplate's `NewlyNullableFields` path, and `computeSchemaForUpdatedTable`
// in type_mapping.go flips the existing field's `required` to false before
// issuing `AddSchemaUpdate` + `SetCurrentSchemaUpdate`. The expected outcome
// is benign: the parquet for the pre-demotion data file is unchanged, so its
// manifest entry still carries every metric map for `payload`. This test
// pins that expectation so a future change to the demotion path can't
// regress the per-column metric maps for pre-existing data.
func runDemoteRequiredColumnRegression(t *testing.T) {
	ctx := context.Background()

	creds, err := readPolarisCreds()
	require.NoError(t, err)
	credential := creds.ClientID + ":" + creds.ClientSecret
	scope := "PRINCIPAL_ROLE:flow_user_role"

	cat, err := catalog.New(ctx, "http://localhost:9802/api/catalog", "quickstart_catalog",
		catalog.WithClientCredential(credential, "v1/oauth/tokens", &scope))
	require.NoError(t, err)

	s3client := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("flow", "flow", ""),
		BaseEndpoint: aws.String("http://localhost:9800"),
		UsePathStyle: true,
	})

	const (
		ns      = "demote_required_column_regression"
		tblName = "payload"
	)

	// Start with `payload` required - mirrors a table created when
	// inference said the field is MUST-exist.
	schemaV0 := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	_ = cat.DeleteTable(ctx, ns, tblName)
	_ = cat.CreateNamespace(ctx, ns)
	require.NoError(t, cat.CreateTable(ctx, ns, tblName, schemaV0, nil, nil, nil))
	t.Cleanup(func() { _ = cat.DeleteTable(context.Background(), ns, tblName) })

	// Insert one row with both columns populated through the production
	// CSV → Spark merge daemon path.
	csvPath := filepath.Join(t.TempDir(), "data.csv.gz")
	csvFile, err := os.Create(csvPath)
	require.NoError(t, err)
	csvw := writer.NewCsvWriter(csvFile, []string{"id", "payload"},
		writer.WithCsvSkipHeaders(), writer.WithCsvQuoteChar('`'))
	require.NoError(t, csvw.Write([]any{"row1", "hello"}))
	require.NoError(t, csvw.Close())
	csvBody, err := os.ReadFile(csvPath)
	require.NoError(t, err)
	csvKey := "staging/demote-required-col-" + uuid.New().String() + ".csv.gz"
	_, err = s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("warehouse"),
		Key:    aws.String(csvKey),
		Body:   bytes.NewReader(csvBody),
	})
	require.NoError(t, err)

	mergeInput := python.MergeInput{
		Bindings: []python.MergeBinding{{
			Binding: 0,
			Query:   fmt.Sprintf("INSERT INTO estuary.%s.%s SELECT id, payload FROM merge_view_0", ns, tblName),
			Columns: []python.NestedField{{Name: "id", Type: "string"}, {Name: "payload", Type: "string"}},
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
	require.Truef(t, result.Success, "initial merge failed: %s", result.Error)

	// Evolve the schema by demoting `payload` to optional. Mirrors what
	// `computeSchemaForUpdatedTable` does when boilerplate hands it a
	// `NewlyNullableFields` entry: keep every existing field as-is but flip
	// the matching field's `required` to false.
	tbl, err := cat.GetTable(ctx, ns, tblName)
	require.NoError(t, err)
	current := tbl.Metadata.CurrentSchema()
	schemaV1 := iceberg.NewSchemaWithIdentifiers(current.ID+1, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	require.NoError(t, cat.UpdateTable(ctx, ns, tblName,
		[]catalog.TableRequirement{catalog.AssertCurrentSchemaID(current.ID)},
		[]catalog.TableUpdate{
			catalog.AddSchemaUpdate(schemaV1),
			catalog.SetCurrentSchemaUpdate(schemaV1.ID),
		},
	))

	const payloadFieldID = 2

	tblAfter, err := cat.GetTable(ctx, ns, tblName)
	require.NoError(t, err)
	snap := tblAfter.Metadata.CurrentSnapshot()
	require.NotNilf(t, snap, "no current snapshot for %s.%s", ns, tblName)

	openS3 := func(s3URI string) io.ReadCloser {
		require.True(t, strings.HasPrefix(s3URI, "s3://warehouse/"),
			"unexpected s3 uri: %s", s3URI)
		key := strings.TrimPrefix(s3URI, "s3://warehouse/")
		out, err := s3client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("warehouse"),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		return out.Body
	}

	mlBody := openS3(snap.ManifestList)
	defer mlBody.Close()
	manifests, err := iceberg.ReadManifestList(mlBody)
	require.NoError(t, err)
	require.NotEmptyf(t, manifests, "no manifests for %s.%s", ns, tblName)

	hasValueCount, hasNullCount, hasLowerBound, hasUpperBound := true, true, true, true
	entriesSeen := 0
	for _, mf := range manifests {
		mfBody := openS3(mf.FilePath())
		entries, err := iceberg.ReadManifest(mf, mfBody, true)
		mfBody.Close()
		require.NoError(t, err)
		for _, e := range entries {
			df := e.DataFile()
			if _, ok := df.ValueCounts()[payloadFieldID]; !ok {
				hasValueCount = false
			}
			if _, ok := df.NullValueCounts()[payloadFieldID]; !ok {
				hasNullCount = false
			}
			if _, ok := df.LowerBoundValues()[payloadFieldID]; !ok {
				hasLowerBound = false
			}
			if _, ok := df.UpperBoundValues()[payloadFieldID]; !ok {
				hasUpperBound = false
			}
			entriesSeen++
		}
	}
	require.NotZerof(t, entriesSeen, "no manifest entries for %s.%s", ns, tblName)

	t.Logf("manifest metric maps for payload column demoted to optional after data: ValueCounts=%v NullValueCounts=%v LowerBounds=%v UpperBounds=%v",
		hasValueCount, hasNullCount, hasLowerBound, hasUpperBound)

	// Demotion mustn't strip per-column metric maps for pre-existing data
	// files - those data files actually have the values, so the manifest
	// entries should retain every metric map. If any of these go missing
	// it implies the demotion path is mutating manifest metadata in
	// addition to the schema, which would be a regression in its own
	// right.
	assert.True(t, hasValueCount, "manifest should retain value_counts for payload after demotion to optional")
	assert.True(t, hasNullCount, "manifest should retain null_value_counts for payload after demotion to optional")
	assert.True(t, hasLowerBound, "manifest should retain lower_bounds for payload after demotion to optional")
	assert.True(t, hasUpperBound, "manifest should retain upper_bounds for payload after demotion to optional")
}
