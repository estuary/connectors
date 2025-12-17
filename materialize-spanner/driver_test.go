//go:build !nodb

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"slices"
	"testing"

	"cloud.google.com/go/spanner"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var testDialect = createSpannerDialect(featureFlagDefaults)
var testTemplates = renderTemplates(testDialect, false)

func testConfig(t testing.TB) config {
	t.Helper()

	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var ctx = context.Background()
	var sops = exec.CommandContext(ctx, "sops", "--decrypt", "--output-type", "json", "config.yaml")
	var configRaw, err = sops.Output()
	require.NoError(t, err, "failed to decrypt config.yaml with sops")

	var jq = exec.CommandContext(ctx, "jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
	jq.Stdin = bytes.NewReader(configRaw)
	cleanedConfig, err := jq.Output()
	require.NoError(t, err, "failed to clean config with jq")

	var cfg config
	err = json.Unmarshal(cleanedConfig, &cfg)
	require.NoError(t, err, "failed to unmarshal config")

	// Set test-specific overrides
	cfg.Advanced.NoFlowDocument = true
	cfg.Advanced.DisableKeyDistributionOptimization = true
	cfg.Advanced.FeatureFlags = "allow_existing_tables_for_new_bindings"

	return cfg
}

// createTestSpannerClient creates a Spanner client using credentials from the config
func createTestSpannerClient(ctx context.Context, t testing.TB, cfg config) *spanner.Client {
	t.Helper()

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		cfg.ProjectID, cfg.InstanceID, cfg.Database)

	var opts []option.ClientOption
	opts = append(opts, option.WithCredentialsJSON([]byte(cfg.Credentials.ServiceAccountJSON)))

	dataClient, err := spanner.NewClient(ctx, dbPath, opts...)
	require.NoError(t, err)

	return dataClient
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig(t)

	resourceConfig := tableConfig{
		Table: "target",
	}

	dataClient := createTestSpannerClient(ctx, t, cfg)
	defer dataClient.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		&driver{},
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := getSpannerTableSchema(ctx, dataClient, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()
			dropTableIfExists(ctx, t, cfg, resourceConfig.Table)
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig(t)

	resourceConfig := tableConfig{
		Table: "target",
	}

	dataClient := createTestSpannerClient(ctx, t, cfg)
	defer dataClient.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		&driver{},
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := getSpannerTableSchema(ctx, dataClient, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			var keys = make([]string, len(cols))
			numericKey := slices.Index(cols, "int64ToNumber")
			values[numericKey] = fmt.Sprintf("NUMERIC '%s'", values[numericKey])
			for i, col := range cols {
				keys[i] = testDialect.Identifier(col)
			}
			keys = append(keys, testDialect.Identifier("_meta/flow_truncated"))
			values = append(values, "FALSE")
			keys = append(keys, testDialect.Identifier("flow_published_at"))
			values = append(values, "TIMESTAMP '2024-09-13 01:01:01+00'")
			keys = append(keys, testDialect.Identifier("flow_document"))
			values = append(values, "JSON '{}'")

			q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				testDialect.Identifier(resourceConfig.Table),
				strings.Join(keys, ","),
				strings.Join(values, ","))

			_, err := dataClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				_, err := txn.Update(ctx, spanner.Statement{SQL: q})
				return err
			})
			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := dumpSpannerTable(ctx, dataClient, resourceConfig.Table)
			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			dropTableIfExists(ctx, t, cfg, resourceConfig.Table)
		},
	)
}

func TestFencingCases(t *testing.T) {
	ctx := context.Background()

	c, err := newClient(ctx, &sql.Endpoint[config]{Config: testConfig(t), Dialect: testDialect})
	require.NoError(t, err)
	defer c.Close()

	spannerClient := c.(*client).dataClient

	sql.RunFenceTestCases(t,
		c,
		[]string{"temp_test_fencing_checkpoints"},
		testDialect,
		testTemplates.createTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := testTemplates.updateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			// Use a read-write transaction for DML (UPDATE) instead of DDL API
			_, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				_, err := txn.Update(ctx, spanner.Statement{SQL: fenceUpdate.String()})
				return err
			})
			return err
		},
		func(table sql.Table) (out string, err error) {
			return dumpSpannerTableWithKeys(ctx, spannerClient, table)
		},
	)
}

// getSpannerTableSchema returns a string representation of the table schema for Spanner
func getSpannerTableSchema(ctx context.Context, client *spanner.Client, tableName string) (string, error) {
	stmt := spanner.Statement{
		SQL: `SELECT column_name, spanner_type, is_nullable
			FROM information_schema.columns
			WHERE table_name = @table
			ORDER BY ordinal_position`,
		Params: map[string]interface{}{
			"table": tableName,
		},
	}

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var result strings.Builder
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", err
		}

		var columnName, spannerType, isNullable string
		if err := row.Columns(&columnName, &spannerType, &isNullable); err != nil {
			return "", err
		}

		result.WriteString(fmt.Sprintf("%s %s %s\n", columnName, spannerType, isNullable))
	}

	return result.String(), nil
}

// dumpSpannerTable dumps all rows from a Spanner table, similar to sql.DumpTestTable.
// It outputs column names in the header, followed by data rows.
func dumpSpannerTable(ctx context.Context, client *spanner.Client, tableName string) (string, error) {
	// First get the column names
	stmt := spanner.Statement{
		SQL: `SELECT column_name
			FROM information_schema.columns
			WHERE table_name = @table
			ORDER BY ordinal_position`,
		Params: map[string]interface{}{
			"table": tableName,
		},
	}

	iter := client.Single().Query(ctx, stmt)

	var columns []string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter.Stop()
			return "", err
		}

		var columnName string
		if err := row.Columns(&columnName); err != nil {
			iter.Stop()
			return "", err
		}
		columns = append(columns, columnName)
	}
	iter.Stop()

	if len(columns) == 0 {
		return "", nil
	}

	var result strings.Builder

	// Write header with column names only (matching expected snapshot format)
	for i, col := range columns {
		if i > 0 {
			result.WriteString(", ")
		}
		result.WriteString(col)
	}

	// Now query the table data - quote column names using dialect identifier
	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, testDialect.Identifier(col))
	}
	selectSQL := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s",
		strings.Join(quotedColumns, ", "),
		testDialect.Identifier(tableName),
		quotedColumns[0])

	iter = client.Single().Query(ctx, spanner.Statement{SQL: selectSQL})
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", err
		}

		// Use spanner.GenericColumnValue to handle arbitrary column types
		values := make([]spanner.GenericColumnValue, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := row.Columns(ptrs...); err != nil {
			return "", err
		}

		result.WriteString("\n")
		for i, v := range values {
			if i > 0 {
				result.WriteString(", ")
			}
			result.WriteString(formatSpannerGenericValue(v))
		}
	}

	return result.String(), nil
}

// formatSpannerGenericValue formats a spanner.GenericColumnValue for consistent output in tests.
func formatSpannerGenericValue(v spanner.GenericColumnValue) string {
	// Handle NULL values
	if v.Value == nil {
		return "<nil>"
	}

	// Try to decode the value based on the type
	switch v.Type.Code.String() {
	case "STRING":
		var s string
		if err := v.Decode(&s); err == nil {
			return s
		}
	case "INT64":
		var i int64
		if err := v.Decode(&i); err == nil {
			return fmt.Sprintf("%d", i)
		}
	case "BYTES":
		var b []byte
		if err := v.Decode(&b); err == nil {
			return string(b)
		}
	case "BOOL":
		var b bool
		if err := v.Decode(&b); err == nil {
			return fmt.Sprintf("%v", b)
		}
	case "FLOAT64":
		var f float64
		if err := v.Decode(&f); err == nil {
			return fmt.Sprintf("%v", f)
		}
	}

	// Fallback: return the string representation of the value
	return fmt.Sprintf("%v", v.Value)
}

// dumpSpannerTableWithKeys dumps a Spanner table using the sql.Table structure.
// It extracts the table name from the Table identifier and delegates to dumpSpannerTable.
func dumpSpannerTableWithKeys(ctx context.Context, client *spanner.Client, table sql.Table) (string, error) {
	// Extract the actual table name from the quoted identifier (e.g., `table_name` -> table_name)
	tableName := strings.Trim(table.Identifier, "`")
	return dumpSpannerTable(ctx, client, tableName)
}

// dropTableIfExists drops a table if it exists
func dropTableIfExists(ctx context.Context, t *testing.T, cfg config, tableName string) {
	c, err := newClient(ctx, &sql.Endpoint[config]{Config: cfg, Dialect: testDialect})
	if err != nil {
		return
	}
	defer c.Close()

	_ = c.ExecStatements(ctx, []string{fmt.Sprintf("DROP TABLE IF EXISTS %s", testDialect.Identifier(tableName))})
}

func TestComputeKeyHash_Float64(t *testing.T) {
	tests := []struct {
		name   string
		values []interface{}
	}{
		{"positive float", []interface{}{3.14}},
		{"negative float", []interface{}{-2.5}},
		{"zero", []interface{}{0.0}},
		{"multiple floats", []interface{}{1.1, 2.2, 3.3}},
		{"NaN", []interface{}{math.NaN()}},
		{"positive infinity", []interface{}{math.Inf(1)}},
		{"negative infinity", []interface{}{math.Inf(-1)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1, err := computeKeyHash(tt.values)
			require.NoError(t, err)

			hash2, err := computeKeyHash(tt.values)
			require.NoError(t, err)

			// Verify deterministic hashing
			assert.Equal(t, hash1, hash2, "Hash should be deterministic")

			// Verify hash is non-zero (except for special cases)
			if tt.name != "zero" {
				assert.NotEqual(t, int64(0), hash1, "Hash should be non-zero")
			}
		})
	}
}

func TestComputeKeyHash_DifferentFloatsProduceDifferentHashes(t *testing.T) {
	// Test that different float values produce different hashes
	hash1, err := computeKeyHash([]interface{}{3.14})
	require.NoError(t, err)

	hash2, err := computeKeyHash([]interface{}{3.15})
	require.NoError(t, err)

	hash3, err := computeKeyHash([]interface{}{3.1})
	require.NoError(t, err)

	// All hashes should be different
	assert.NotEqual(t, hash1, hash2, "3.14 and 3.15 should have different hashes")
	assert.NotEqual(t, hash1, hash3, "3.14 and 3.1 should have different hashes")
	assert.NotEqual(t, hash2, hash3, "3.15 and 3.1 should have different hashes")
}

func TestComputeKeyHash_JsonMarshalError(t *testing.T) {
	// Channels cannot be marshaled to JSON
	_, err := computeKeyHash([]interface{}{make(chan int)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "marshaling key value")
	assert.Contains(t, err.Error(), "chan int")
}

func TestComputeKeyHash_VariousTypes(t *testing.T) {
	tests := []struct {
		name   string
		values []interface{}
	}{
		{"int64", []interface{}{int64(123)}},
		{"string", []interface{}{"test"}},
		{"bytes", []interface{}{[]byte("test")}},
		{"bool true", []interface{}{true}},
		{"bool false", []interface{}{false}},
		{"nil", []interface{}{nil}},
		{"mixed types", []interface{}{int64(1), "test", 3.14, true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1, err := computeKeyHash(tt.values)
			require.NoError(t, err)

			hash2, err := computeKeyHash(tt.values)
			require.NoError(t, err)

			// Verify deterministic hashing
			assert.Equal(t, hash1, hash2, "Hash should be deterministic for "+tt.name)
		})
	}
}

func TestComputeKeyHash_OrderMatters(t *testing.T) {
	// Test that order of values affects hash
	hash1, err := computeKeyHash([]interface{}{int64(1), int64(2), int64(3)})
	require.NoError(t, err)

	hash2, err := computeKeyHash([]interface{}{int64(3), int64(2), int64(1)})
	require.NoError(t, err)

	assert.NotEqual(t, hash1, hash2, "Different order should produce different hash")
}
