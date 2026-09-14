package sql

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestTruncationBoundary(t *testing.T) {
	var got = TruncationBoundary(time.Date(2026, 9, 14, 21, 0, 5, 123456789, time.UTC))
	require.True(t, got.Equal(time.Date(2026, 9, 14, 21, 0, 5, 0, time.UTC)))
	require.Equal(t, time.UTC, got.Location())

	var pdt = time.FixedZone("PDT", -7*3600)
	got = TruncationBoundary(time.Date(2026, 9, 14, 14, 0, 5, 999999999, pdt))
	require.True(t, got.Equal(time.Date(2026, 9, 14, 21, 0, 5, 0, time.UTC)))
	require.Equal(t, time.UTC, got.Location())
}

func loadTruncateTestSpec(t *testing.T) *pf.MaterializationSpec {
	t.Helper()
	raw, err := os.ReadFile("../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
	require.NoError(t, err)

	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(raw))
	return &spec
}

// buildTruncateTestTable resolves a Table from the shared testdata fixture's
// sole binding, optionally mutated to exercise a missing or unusable
// flow_published_at column.
func buildTruncateTestTable(t *testing.T, dialect Dialect, mutate func(*pf.MaterializationSpec_Binding)) Table {
	t.Helper()
	var spec = loadTruncateTestSpec(t)
	var binding = *spec.Bindings[0]
	if mutate != nil {
		mutate(&binding)
	}

	var shape = BuildTableShape(string(spec.Name), &binding, 0, []string{"one", "schema", "target"}, false)
	table, err := ResolveTable(shape, dialect)
	require.NoError(t, err)
	return table
}

func TestTruncateStatement(t *testing.T) {
	var (
		dialect = newTestDialect()
		before  = time.Date(2026, 9, 14, 21, 0, 5, 123456789, time.UTC)
	)

	t.Run("renders delete statement", func(t *testing.T) {
		var table = buildTruncateTestTable(t, dialect, nil)

		out, err := TruncateStatement(dialect, table, before)
		require.NoError(t, err)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("field selection omits flow_published_at", func(t *testing.T) {
		var table = buildTruncateTestTable(t, dialect, func(b *pf.MaterializationSpec_Binding) {
			var values []string
			for _, f := range b.FieldSelection.Values {
				if f != "flow_published_at" {
					values = append(values, f)
				}
			}
			b.FieldSelection.Values = values
		})

		_, err := TruncateStatement(dialect, table, before)
		require.EqualError(t, err, "table has no flow_published_at column")
	})

	t.Run("flow_published_at is cast to a string", func(t *testing.T) {
		var table = buildTruncateTestTable(t, dialect, func(b *pf.MaterializationSpec_Binding) {
			b.FieldSelection.FieldConfigJsonMap = map[string]json.RawMessage{
				"flow_published_at": json.RawMessage(`{"castToString":true}`),
			}
		})

		_, err := TruncateStatement(dialect, table, before)
		require.EqualError(t, err, "flow_published_at column is not a timestamp")
	})
}
