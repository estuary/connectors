package sql

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"math"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

//go:generate ../materialize-boilerplate/testdata/generate-spec-proto.sh testdata/flow.yaml

// snapshotPath is a common set of test snapshots that may be used by SQL materialization connectors
// that produce standard snapshots.
var snapshotPath = "../materialize-sql/.snapshots"

// RunFenceTestCases is a generalized form of test cases over fencing behavior,
// which ought to function with any Client implementation.
func RunFenceTestCases(
	t *testing.T,
	client Client,
	checkpointsPath []string,
	dialect Dialect,
	createTableTpl *template.Template,
	updateFence func(Table, Fence) error,
	dumpTable func(Table) (string, error),
) {

	// runTest takes zero or more key range fixtures, followed by a final pair
	// which is the key range under test.
	var runTest = func(t *testing.T, ranges ...uint32) {
		var ctx = context.Background()

		var metaShape = FlowCheckpointsTable(checkpointsPath...)
		var metaTable, err = ResolveTable(metaShape, dialect)
		require.NoError(t, err)

		createSQL, err := RenderTableTemplate(metaTable, createTableTpl)
		require.NoError(t, err)
		err = client.ExecStatements(ctx, []string{createSQL})
		require.NoError(t, err)

		defer func() {
			err = client.ExecStatements(ctx, []string{fmt.Sprintf("DROP TABLE %s;", metaTable.Identifier)})
			require.NoError(t, err)
		}()

		var fixtures = ranges[:len(ranges)-2]
		var testCase = ranges[len(ranges)-2:]

		for i := 0; i*2 < len(fixtures); i++ {
			var _, err = client.InstallFence(ctx, metaTable, Fence{
				TablePath:       metaShape.Path,
				Materialization: "the/materialization",
				KeyBegin:        ranges[i*2],
				KeyEnd:          ranges[i*2+1],
				Fence:           5,
				Checkpoint:      bytes.Repeat([]byte{byte(i + 1)}, 10),
			})
			require.NoError(t, err)
		}

		// Add an extra fixture from a different materialization.
		_, err = client.InstallFence(ctx, metaTable, Fence{
			TablePath:       metaShape.Path,
			Materialization: "other/one",
			KeyBegin:        0,
			KeyEnd:          math.MaxUint32,
			Fence:           99,
			Checkpoint:      []byte("other-checkpoint"),
		})
		require.NoError(t, err)

		dump1, err := dumpTable(metaTable)
		require.NoError(t, err)

		// Install the fence under test
		fence, err := client.InstallFence(ctx, metaTable, Fence{
			TablePath:       metaShape.Path,
			Materialization: "the/materialization",
			KeyBegin:        testCase[0],
			KeyEnd:          testCase[1],
			Fence:           0,
			Checkpoint:      nil,
		})
		require.NoError(t, err)

		dump2, err := dumpTable(metaTable)
		require.NoError(t, err)

		// Update it once.
		fence.Checkpoint = append(fence.Checkpoint, []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
		require.NoError(t, updateFence(metaTable, fence))

		dump3, err := dumpTable(metaTable)
		require.NoError(t, err)

		snapshotter := cupaloy.New(cupaloy.SnapshotSubdirectory(snapshotPath))

		snapshotter.SnapshotT(t,
			"After installing fixtures:\n"+dump1+
				"\nAfter install fence under test:\n"+dump2+
				"\nAfter update:\n"+dump3)
	}

	// If a fence exactly matches a checkpoint, we'll fence that checkpoint and its parent
	// but not siblings. The used checkpoint is that of the exact match.
	t.Run("exact match", func(t *testing.T) {
		runTest(t,
			0, 99, // Unrelated sibling.
			100, 199, // Exactly matched.
			200, 299, // Unrelated sibling.
			0, 1000, // Old parent.
			100, 199)
	})
	// If a fence sub-divides a parent, we'll fence the parent and grand parent
	// but not siblings of the parent. The checkpoint is the younger parent.
	t.Run("split from parent", func(t *testing.T) {
		runTest(t,
			0, 499, // Younger uncle.
			500, 799, // Younger parent.
			800, 1000, // Other uncle.
			0, 1000, // Grand parent.
			500, 599)
	})
	// If a new range straddles existing ranges (this shouldn't ever happen),
	// we'll fence the straddled ranges while taking the checkpoint of the parent.
	t.Run("straddle", func(t *testing.T) {
		runTest(t,
			0, 499,
			500, 1000,
			0, 1000,
			400, 599)
	})
	// If a new range covers another (this also shouldn't ever happen),
	// it takes the checkpoint of its parent while also fencing the covered sub-range.
	t.Run("covered child", func(t *testing.T) {
		runTest(t,
			100, 199,
			0, 1000,
			100, 800)
	})
}

//go:embed testdata/generated_specs
var testFS embed.FS

// TestTemplates is the set of templates that may be rendered for testing.
// TableTemplates is required but the rest are optional.
type TestTemplates struct {
	// TableTemplates are all templates that take a Table as input for
	// rendering.
	TableTemplates []*template.Template
	// TplAddColumns is a template that adds one or more columns to a table.
	TplAddColumns *template.Template
	// TplDropNotNulls is a template that drops one or more NOT NULL
	// constraints.
	TplDropNotNulls *template.Template
	// TplCombinedAlter is a template that combines adding columns and dropping
	// NOT NULL constraints.
	TplCombinedAlter *template.Template
	// TplInstallFence is a template that installs a fence.
	TplInstallFence *template.Template
	// TplUpdateFence is a template that updates a fence.
	TplUpdateFence *template.Template
}

// RunSqlGenTests runs a standardized materialization spec against the provided
// templates. The snapshot builder and generated tables are returned so that
// callers can run additional tests if needed.
func RunSqlGenTests(
	t *testing.T,
	dialect Dialect,
	newResource func(table string, deltaUpdates bool) Resource,
	templates TestTemplates,
) (*strings.Builder, []Table) {
	specBytes, err := testFS.ReadFile("testdata/generated_specs/flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	tables := []Table{}
	for idx, delta := range []bool{false, true} {
		shape := BuildTableShape(&spec, idx, newResource(spec.Bindings[idx].ResourcePath[0], delta))

		if idx == 1 {
			// The delta updates case.
			shape.Document = nil
		}

		table, err := ResolveTable(shape, dialect)
		require.NoError(t, err)
		tables = append(tables, table)
	}

	var snap strings.Builder

	for _, tpl := range templates.TableTemplates {
		for _, tbl := range tables {
			var testcase = tbl.Identifier + " " + tpl.Name()
			snap.WriteString("--- Begin " + testcase + " ---\n")
			require.NoError(t, tpl.Execute(&snap, &tbl))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	addCols := []Column{
		{Identifier: "first_new_column", MappedType: MappedType{NullableDDL: "STRING"}},
		{Identifier: "second_new_column", MappedType: MappedType{NullableDDL: "BOOL"}},
	}
	dropNotNulls := []boilerplate.EndpointField{
		{
			Name:               "first_required_column",
			Nullable:           true,
			Type:               "string",
			CharacterMaxLength: 0,
		},
		{
			Name:               "second_required_column",
			Nullable:           true,
			Type:               "bool",
			CharacterMaxLength: 0,
		},
	}

	for _, testcase := range []struct {
		name         string
		addColumns   []Column
		dropNotNulls []boilerplate.EndpointField
		tpl          *template.Template
	}{
		{
			name:         "alter table add columns and drop not nulls",
			addColumns:   addCols,
			dropNotNulls: dropNotNulls,
			tpl:          templates.TplCombinedAlter,
		},
		{
			name:       "alter table add columns",
			addColumns: addCols,
			tpl:        templates.TplAddColumns,
		},
		{
			name:         "alter table drop not nulls",
			dropNotNulls: dropNotNulls,
			tpl:          templates.TplDropNotNulls,
		},
	} {
		if testcase.tpl == nil {
			continue
		}

		snap.WriteString("--- Begin " + testcase.name + " ---\n")
		require.NoError(t, testcase.tpl.Execute(&snap, TableAlter{
			Table:        tables[0],
			AddColumns:   testcase.addColumns,
			DropNotNulls: testcase.dropNotNulls,
		}))
		snap.WriteString("--- End " + testcase.name + " ---\n\n")
	}

	var fence = Fence{
		TablePath:       TablePath{"path", "to", "checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}

	if tpl := templates.TplInstallFence; tpl != nil {
		snap.WriteString("--- Begin Fence Install ---\n")
		require.NoError(t, tpl.Execute(&snap, fence))
		snap.WriteString("--- End Fence Install ---\n\n")
	}

	if tpl := templates.TplUpdateFence; tpl != nil {
		snap.WriteString("--- Begin Fence Update ---")
		require.NoError(t, tpl.Execute(&snap, fence))
		snap.WriteString("--- End Fence Update ---\n\n")
	}

	return &snap, tables
}
