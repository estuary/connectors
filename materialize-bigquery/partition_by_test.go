package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// loadBaseSpec returns the shared boilerplate test spec, whose single binding
// materializes the key/value collection with a full field selection.
func loadBaseSpec(t *testing.T) *pf.MaterializationSpec {
	t.Helper()

	specBytes, err := os.ReadFile("../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))
	return &spec
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return raw
}

// specWithPartitionBy clones the base spec's binding into a spec bound to the
// given table of the endpoint's dataset with the given partition_by and
// backfill counter.
func specWithPartitionBy(t *testing.T, cfg config, table string, partitionBy string, backfill uint32) *pf.MaterializationSpec {
	return specWithResource(t, cfg, tableConfig{Table: table, PartitionBy: partitionBy}, backfill)
}

func specWithResource(t *testing.T, cfg config, rc tableConfig, backfill uint32) *pf.MaterializationSpec {
	t.Helper()

	var spec = loadBaseSpec(t)
	var table = rc.Table
	spec.ConfigJson = mustMarshal(t, cfg)
	spec.Bindings[0].ResourceConfigJson = mustMarshal(t, rc)
	spec.Bindings[0].DeltaUpdates = rc.Delta
	spec.Bindings[0].ResourcePath = []string{cfg.ProjectID, cfg.Dataset, table}
	spec.Bindings[0].Backfill = backfill
	return spec
}

func validatePartitionByReq(t *testing.T, cfg config, table string, partitionBy string, backfill uint32, lastSpec *pf.MaterializationSpec) *pm.Request_Validate {
	return validateResourceReq(t, cfg, tableConfig{Table: table, PartitionBy: partitionBy}, backfill, lastSpec)
}

func validateResourceReq(t *testing.T, cfg config, rc tableConfig, backfill uint32, lastSpec *pf.MaterializationSpec) *pm.Request_Validate {
	t.Helper()

	var spec = loadBaseSpec(t)
	return &pm.Request_Validate{
		Name:                spec.Name,
		ConnectorType:       pf.MaterializationSpec_IMAGE,
		ConfigJson:          mustMarshal(t, cfg),
		LastMaterialization: lastSpec,
		Bindings: []*pm.Request_Validate_Binding{{
			ResourceConfigJson: mustMarshal(t, rc),
			Collection:         spec.Bindings[0].Collection,
			Backfill:           backfill,
		}},
	}
}

// incompatibleFields returns the fields of the response's single binding that
// carry an INCOMPATIBLE constraint mentioning partition_by.
func incompatibleFields(t *testing.T, resp *pm.Response_Validated) []string {
	t.Helper()
	var out []string
	for _, pc := range resp.Bindings[0].ProjectionConstraints {
		if pc.Constraint.Type == pm.Response_Validated_Constraint_INCOMPATIBLE {
			require.Contains(t, pc.Constraint.Reason, "partition_by")
			out = append(out, pc.Field)
		}
	}
	return out
}

func applyReq(spec, last *pf.MaterializationSpec) *pm.Request_Apply {
	var req = &pm.Request_Apply{Materialization: spec, Version: "test"}
	if last != nil {
		req.LastMaterialization = last
		req.LastVersion = "test"
	}
	return req
}

// bqTestClient opens a raw BigQuery client for inspecting and cleaning up the
// tables a test touches. Every named table is dropped now and at cleanup.
func bqTestClient(t *testing.T, cfg config, tables ...string) *bigquery.Client {
	t.Helper()

	credOption, err := cfg.CredentialsClientOption()
	require.NoError(t, err)
	bq, err := bigquery.NewClient(t.Context(), cfg.ProjectID, credOption)
	require.NoError(t, err)

	drop := func(ctx context.Context) {
		for _, table := range tables {
			_ = bq.DatasetInProject(cfg.ProjectID, cfg.Dataset).Table(table).Delete(ctx)
		}
	}
	drop(t.Context())
	t.Cleanup(func() {
		drop(context.Background())
		bq.Close()
	})
	return bq
}

func tableMetadata(t *testing.T, bq *bigquery.Client, cfg config, table string) *bigquery.TableMetadata {
	t.Helper()
	md, err := tableMetadataWithRetry(t.Context(), bq.DatasetInProject(cfg.ProjectID, cfg.Dataset).Table(table))
	require.NoError(t, err)
	return md
}

func requireNoIncompatible(t *testing.T, resp *pm.Response_Validated) {
	t.Helper()
	for _, pc := range resp.Bindings[0].ProjectionConstraints {
		require.NotEqual(t, pm.Response_Validated_Constraint_INCOMPATIBLE, pc.Constraint.Type, pc.Constraint.Reason)
	}
}

func TestValidatePartitionBy(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	testutil.RunTestAllTasks(t, "testdata/apply.flow.yaml", func(t *testing.T, _ []byte, _ string, cfg config) {
		var ctx = t.Context()
		var tableName = "partition_by_test_validate"

		t.Run("dry-run accepts a valid expression", func(t *testing.T) {
			resp, err := NewDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "DATE(flow_published_at)", 0, nil))
			require.NoError(t, err)
			requireNoIncompatible(t, resp)
		})

		t.Run("dry-run accepts ingestion-time and range expressions", func(t *testing.T) {
			for _, expr := range []string{"_PARTITIONDATE", "RANGE_BUCKET(requiredInteger, GENERATE_ARRAY(0, 1000, 10))"} {
				resp, err := NewDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, expr, 0, nil))
				require.NoError(t, err, expr)
				requireNoIncompatible(t, resp)
			}
		})

		t.Run("dry-run rejects a bad expression", func(t *testing.T) {
			_, err := NewDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "DATE(no_such_column)", 0, nil))
			require.ErrorContains(t, err, "no_such_column")
			require.ErrorContains(t, err, "partition_by")
		})

		t.Run("change without backfill is INCOMPATIBLE", func(t *testing.T) {
			var lastSpec = specWithPartitionBy(t, cfg, tableName, "", 0)
			resp, err := NewDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "DATE(flow_published_at)", 0, lastSpec))
			require.NoError(t, err)

			// Every projection carries the constraint, so that at least one
			// lands on a selected field of any binding, including
			// delta-updates bindings that deselect their keys.
			var incompatible = incompatibleFields(t, resp)
			require.Contains(t, incompatible, "key")
			require.Contains(t, incompatible, "requiredString")
			require.Contains(t, incompatible, "flow_document")
		})

		t.Run("change without backfill is INCOMPATIBLE for a delta-updates binding", func(t *testing.T) {
			var lastRC = tableConfig{Table: tableName, Delta: true}
			var newRC = tableConfig{Table: tableName, Delta: true, PartitionBy: "DATE(flow_published_at)"}
			resp, err := NewDriver().Validate(ctx, validateResourceReq(t, cfg, newRC, 0, specWithResource(t, cfg, lastRC, 0)))
			require.NoError(t, err)
			var incompatible = incompatibleFields(t, resp)
			require.Contains(t, incompatible, "key")
			require.Contains(t, incompatible, "requiredString")
			require.Contains(t, incompatible, "flow_published_at")
		})

		t.Run("change with a backfill bump is allowed", func(t *testing.T) {
			var lastSpec = specWithPartitionBy(t, cfg, tableName, "", 0)
			resp, err := NewDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "DATE(flow_published_at)", 1, lastSpec))
			require.NoError(t, err)
			requireNoIncompatible(t, resp)
		})

		t.Run("unchanged expression is not re-verified", func(t *testing.T) {
			// Schema evolution can remove the projection a long-established
			// table's partition expression references. The existing table
			// keeps its column and keeps working, so Validate must not re-run
			// the dry-run (which would fail on the missing column) for an
			// expression identical to the last applied spec's.
			var lastSpec = specWithPartitionBy(t, cfg, tableName, "DATE(no_such_column)", 0)
			resp, err := NewDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, " DATE(no_such_column) ", 0, lastSpec))
			require.NoError(t, err)
			requireNoIncompatible(t, resp)
		})
	})
}

func TestApplyPartitionBy(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	testutil.RunTestAllTasks(t, "testdata/apply.flow.yaml", func(t *testing.T, _ []byte, _ string, cfg config) {
		var ctx = t.Context()

		t.Run("backfill re-creates for a changed expression and truncates for an unchanged one", func(t *testing.T) {
			var tableName = "partition_by_test_backfill"
			var bq = bqTestClient(t, cfg, tableName)

			var specA = specWithPartitionBy(t, cfg, tableName, "DATE(flow_published_at)", 0)
			resp, err := NewDriver().Apply(ctx, applyReq(specA, nil))
			require.NoError(t, err)
			require.Contains(t, resp.ActionDescription, "PARTITION BY DATE(flow_published_at)")
			md := tableMetadata(t, bq, cfg, tableName)
			require.NotNil(t, md.TimePartitioning)
			require.Equal(t, bigquery.DayPartitioningType, md.TimePartitioning.Type)
			require.Equal(t, "flow_published_at", md.TimePartitioning.Field)

			var specB = specWithPartitionBy(t, cfg, tableName, "TIMESTAMP_TRUNC(flow_published_at, MONTH)", 1)
			resp, err = NewDriver().Apply(ctx, applyReq(specB, specA))
			require.NoError(t, err)
			require.Contains(t, resp.ActionDescription, "DROP TABLE")
			require.Contains(t, resp.ActionDescription, "PARTITION BY TIMESTAMP_TRUNC(flow_published_at, MONTH)")
			md = tableMetadata(t, bq, cfg, tableName)
			require.NotNil(t, md.TimePartitioning)
			require.Equal(t, bigquery.MonthPartitioningType, md.TimePartitioning.Type)
			var created = md.CreationTime

			var specC = specWithPartitionBy(t, cfg, tableName, "TIMESTAMP_TRUNC(flow_published_at, MONTH)", 2)
			resp, err = NewDriver().Apply(ctx, applyReq(specC, specB))
			require.NoError(t, err)
			require.Contains(t, resp.ActionDescription, "TRUNCATE TABLE")
			require.NotContains(t, resp.ActionDescription, "DROP TABLE")
			md = tableMetadata(t, bq, cfg, tableName)
			require.Equal(t, created, md.CreationTime, "an unchanged expression must keep the existing table")
			require.Equal(t, bigquery.MonthPartitioningType, md.TimePartitioning.Type)

			// A truncating backfill against a table whose partitioning has
			// drifted from the unchanged expression cannot be fixed by
			// another backfill, so the remedy names the feature flag that
			// forces a drop instead.
			var driftedLast = specWithPartitionBy(t, cfg, tableName, "DATE(flow_published_at)", 2)
			var driftedNext = specWithPartitionBy(t, cfg, tableName, "DATE(flow_published_at)", 3)
			_, err = NewDriver().Apply(ctx, applyReq(driftedNext, driftedLast))
			require.ErrorContains(t, err, `is partitioned by "TIMESTAMP_TRUNC(flow_published_at, MONTH)"`)
			require.ErrorContains(t, err, "always_drop_tables_on_backfill")
		})

		t.Run("delta-updates binding is created partitioned", func(t *testing.T) {
			var tableName = "partition_by_test_delta"
			var bq = bqTestClient(t, cfg, tableName)

			var spec = specWithResource(t, cfg, tableConfig{Table: tableName, Delta: true, PartitionBy: "DATE(flow_published_at)"}, 0)
			resp, err := NewDriver().Apply(ctx, applyReq(spec, nil))
			require.NoError(t, err)
			require.Contains(t, resp.ActionDescription, "PARTITION BY DATE(flow_published_at)")
			md := tableMetadata(t, bq, cfg, tableName)
			require.NotNil(t, md.TimePartitioning)
			require.Equal(t, "flow_published_at", md.TimePartitioning.Field)
		})

		t.Run("existing table is checked against the configured expression", func(t *testing.T) {
			var tableName = "partition_by_test_existing"
			bqTestClient(t, cfg, tableName)

			// Adopting a pre-created table: the binding is new to the
			// materialization but its table already exists.
			_, err := NewDriver().Apply(ctx, applyReq(specWithPartitionBy(t, cfg, tableName, "DATE(flow_published_at)", 0), nil))
			require.NoError(t, err)

			t.Run("matching expression", func(t *testing.T) {
				_, err := NewDriver().Apply(ctx, applyReq(specWithPartitionBy(t, cfg, tableName, " date( flow_published_at ) ", 0), nil))
				require.NoError(t, err)
			})

			t.Run("different expression", func(t *testing.T) {
				_, err := NewDriver().Apply(ctx, applyReq(specWithPartitionBy(t, cfg, tableName, "TIMESTAMP_TRUNC(flow_published_at, MONTH)", 0), nil))
				require.ErrorContains(t, err, `is partitioned by "DATE(flow_published_at)"`)
				require.ErrorContains(t, err, `'partition_by' is "TIMESTAMP_TRUNC(flow_published_at, MONTH)"`)
				require.ErrorContains(t, err, "Backfill the binding")
			})

			t.Run("partitioned table without partition_by warns", func(t *testing.T) {
				var hook = logtest.NewGlobal()
				defer hook.Reset()
				_, err := NewDriver().Apply(ctx, applyReq(specWithPartitionBy(t, cfg, tableName, "", 0), nil))
				require.NoError(t, err)

				var warned bool
				for _, e := range hook.AllEntries() {
					if strings.Contains(e.Message, "partition_by") && e.Data["field"] == "flow_published_at" {
						warned = true
					}
				}
				require.True(t, warned, "expected a warning naming the partitioning field")
			})
		})

		t.Run("unpartitioned table with partition_by is rejected", func(t *testing.T) {
			var tableName = "partition_by_test_unpartitioned"
			bqTestClient(t, cfg, tableName)

			_, err := NewDriver().Apply(ctx, applyReq(specWithPartitionBy(t, cfg, tableName, "", 0), nil))
			require.NoError(t, err)

			_, err = NewDriver().Apply(ctx, applyReq(specWithPartitionBy(t, cfg, tableName, "DATE(flow_published_at)", 0), nil))
			require.ErrorContains(t, err, "is not partitioned")
			require.ErrorContains(t, err, "Backfill the binding")
		})

		// BigQuery reports each of these canonically in INFORMATION_SCHEMA
		// DDL; a re-Apply of the same expression must compare equal to it.
		for _, expr := range []string{
			"_PARTITIONDATE",
			"DATE(_PARTITIONTIME)",
			"RANGE_BUCKET(requiredInteger, GENERATE_ARRAY(0, 1000, 10))",
			"DATETIME_TRUNC(DATETIME(flow_published_at), DAY)",
		} {
			t.Run(fmt.Sprintf("round-trips %s", expr), func(t *testing.T) {
				var tableName = "partition_by_test_roundtrip"
				bqTestClient(t, cfg, tableName)

				_, err := NewDriver().Apply(ctx, applyReq(specWithPartitionBy(t, cfg, tableName, expr, 0), nil))
				if strings.HasPrefix(expr, "DATETIME_TRUNC") {
					// Not a valid BigQuery partitioning expression: only a
					// bare column or one of the documented functions of a
					// column is accepted. It is here to pin that BigQuery's
					// own error reaches the user.
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				_, err = NewDriver().Apply(ctx, applyReq(specWithPartitionBy(t, cfg, tableName, expr, 0), nil))
				require.NoError(t, err)
			})
		}
	})
}
