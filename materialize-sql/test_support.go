package sql

import (
	"bytes"
	"cmp"
	"context"
	stdsql "database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/common"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

//go:generate ../materialize-boilerplate/testdata/generate-spec-proto.sh testdata/flow.yaml

// snapshotPath is a common set of test snapshots that may be used by SQL materialization connectors
// that produce standard snapshots.
var snapshotPath = "../materialize-sql/.snapshots"

func RunMaterializationTest[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]](
	t *testing.T,
	driver *Driver[EC, RC],
	source string,
	makeResourceFn func(string, bool) RC,
	actionDescSanitizers []func(string) string,
) {
	boilerplate.RunMaterializationTest(t, driver.newMaterialization, source, makeResourceFn, actionDescSanitizers)
}

func RunApplyTest[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]](
	t *testing.T,
	driver *Driver[EC, RC],
	sourcePath string,
	makeResourceFn func(string, bool) RC,
) {
	boilerplate.RunApplyTest(t, driver, driver.newMaterialization, sourcePath, makeResourceFn)
}

func RunMigrationTest[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]](
	t *testing.T,
	driver *Driver[EC, RC],
	sourcePath string,
	makeResourceFn func(string, bool) RC,
) {
	boilerplate.RunMigrationTest(t, driver.newMaterialization, sourcePath, makeResourceFn)
}

// RunFencingTest is a generalized form of test cases over fencing behavior,
// which ought to function with any Client implementation.
func RunFencingTest[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]](
	t *testing.T,
	driver *Driver[EC, RC],
	sourcePath string,
	makeResourceFn func(string, bool) RC,
	createTableTpl *template.Template,
	updateFence func(context.Context, Client, Fence) error,
) {
	dumpTable := func(
		ctx context.Context,
		m boilerplate.Materializer[EC, FieldConfig, RC, MappedType],
		path []string,
	) (string, error) {
		columnNames, rows, err := m.SnapshotTestResource(ctx, path)
		if err != nil {
			return "", err
		}

		var out strings.Builder
		enc := json.NewEncoder(&out)
		for _, r := range rows {
			doc := make(map[string]any, len(columnNames))
			for i, col := range columnNames {
				doc[col] = r[i]
			}
			require.NoError(t, enc.Encode(doc))
		}

		return out.String(), nil
	}

	// runTest takes zero or more key range fixtures, followed by a final pair
	// which is the key range under test.
	var runTest = func(t *testing.T, taskName string, cfg EC, ranges ...uint32) {
		var ctx = context.Background()
		tsSuffix := fmt.Sprintf("_flow_test_%d", time.Now().Unix())
		rngSuffix := "_" + uuid.NewString()[:8] + tsSuffix

		checkpointsTable := "temp_test_fencing_checkpoints" + rngSuffix

		rawFlags, defaultFlags := cfg.FeatureFlags()
		parsedFlags := common.ParseFeatureFlags(rawFlags, defaultFlags)

		ep, err := driver.NewEndpoint(ctx, cfg, parsedFlags)
		require.NoError(t, err)

		m, err := driver.newMaterialization(ctx, taskName, cfg, parsedFlags)
		require.NoError(t, err)

		dialect := ep.Dialect
		checkpointsRes := makeResourceFn(checkpointsTable, false).WithDefaults(cfg)
		checkpointsPath, _, err := checkpointsRes.Parameters()
		require.NoError(t, err)

		client, err := ep.NewClient(ctx, ep)
		require.NoError(t, err)

		var metaShape = FlowCheckpointsTable(checkpointsPath)
		metaShape.Path = checkpointsPath
		metaTable, err := ResolveTable(*metaShape, dialect)
		require.NoError(t, err)

		defer func() {
			boilerplate.CleanupTestResources(t, ctx, m, [][]string{checkpointsPath}, tsSuffix)
		}()

		createSQL, err := RenderTableTemplate(metaTable, createTableTpl)
		require.NoError(t, err)
		err = client.ExecStatements(ctx, []string{createSQL})
		require.NoError(t, err)

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

		dump1, err := dumpTable(ctx, m, checkpointsPath)
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

		dump2, err := dumpTable(ctx, m, checkpointsPath)
		require.NoError(t, err)

		// Update it once.
		fence.Checkpoint = append(fence.Checkpoint, []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
		require.NoError(t, updateFence(ctx, client, fence))

		dump3, err := dumpTable(ctx, m, checkpointsPath)
		require.NoError(t, err)

		snapshotter := cupaloy.New(cupaloy.SnapshotSubdirectory(snapshotPath))

		snapshotter.SnapshotT(t,
			"After installing fixtures:\n"+dump1+
				"\nAfter install fence under test:\n"+dump2+
				"\nAfter update:\n"+dump3)
	}

	boilerplate.RunTestAllTasks(t, sourcePath, func(t *testing.T, _ []byte, taskName string, cfg EC) {
		// If a fence exactly matches a checkpoint, we'll fence that checkpoint and its parent
		// but not siblings. The used checkpoint is that of the exact match.
		t.Run("exact match", func(t *testing.T) {
			runTest(t, taskName, cfg,
				0, 99, // Unrelated sibling.
				100, 199, // Exactly matched.
				200, 299, // Unrelated sibling.
				0, 1000, // Old parent.
				100, 199)
		})
		// If a fence sub-divides a parent, we'll fence the parent and grand parent
		// but not siblings of the parent. The checkpoint is the younger parent.
		t.Run("split from parent", func(t *testing.T) {
			runTest(t, taskName, cfg,
				0, 499, // Younger uncle.
				500, 799, // Younger parent.
				800, 1000, // Other uncle.
				0, 1000, // Grand parent.
				500, 599)
		})
		// If a new range straddles existing ranges (this shouldn't ever happen),
		// we'll fence the straddled ranges while taking the checkpoint of the parent.
		t.Run("straddle", func(t *testing.T) {
			runTest(t, taskName, cfg,
				0, 499,
				500, 1000,
				0, 1000,
				400, 599)
		})
		// If a new range covers another (this also shouldn't ever happen),
		// it takes the checkpoint of its parent while also fencing the covered sub-range.
		t.Run("covered child", func(t *testing.T) {
			runTest(t, taskName, cfg,
				100, 199,
				0, 1000,
				100, 800)
		})
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
	newPath func(table string) []string,
	templates TestTemplates,
) (*strings.Builder, []Table) {
	specBytes, err := testFS.ReadFile("testdata/generated_specs/flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	tables := []Table{}
	for idx, delta := range []bool{false, true} {
		shape := BuildTableShape(
			spec.Name.String(),
			spec.Bindings[idx],
			idx,
			newPath(spec.Bindings[idx].ResourcePath[0]),
			delta,
		)
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
	dropNotNulls := []boilerplate.ExistingField{
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
		dropNotNulls []boilerplate.ExistingField
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

func ListCheckpointsEntries(ctx context.Context, db *stdsql.DB, tableIdentifier string) ([]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("select materialization from %s;", tableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("querying materializations from checkpoints table: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var taskName string
		if err := rows.Scan(&taskName); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		out = append(out, taskName)
	}

	return out, nil
}

func DeleteCheckpointsEntry(ctx context.Context, db *stdsql.DB, tableIdentifier, taskName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"delete from %s where materialization='%s';",
		tableIdentifier,
		taskName,
	))

	return err
}

func SnapshotTestTable(ctx context.Context, db *stdsql.DB, tableIdentifier string) ([]string, [][]any, error) {
	sql := fmt.Sprintf("select * from %s;", tableIdentifier)

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query table %s: %w", tableIdentifier, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get columns for table %s: %w", tableIdentifier, err)
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get column types for table %s: %w", tableIdentifier, err)
	}

	var out [][]any
	for rows.Next() {
		var data = make([]any, len(cols))
		var ptrs = make([]any, len(cols))
		for i := range data {
			ptrs[i] = &data[i]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}

		for i, v := range data {
			switch b := v.(type) {
			case []byte:
				if json.Valid(b) {
					// Better test snapshots, so we can see the actual JSON
					// rather the base64 encoded bytes.
					data[i] = json.RawMessage(b)
				} else if strings.EqualFold("UUID", types[i].DatabaseTypeName()) {
					// If the database has a specific UUID column type, format
					// it as a string. Currently this only applies to
					// MotherDuck.
					u, err := uuid.FromBytes(b)
					if err != nil {
						return nil, nil, fmt.Errorf("parsing UUID from bytes in column %q: %w", cols[i], err)
					}

					data[i] = u.String()
				} else {
					data[i] = string(b)
				}
			case string:
				if json.Valid([]byte(b)) {
					data[i] = json.RawMessage(b)
				}
			}
		}

		out = append(out, data)
	}
	SortRows(out)

	return cols, out, nil
}

func SortRows(rows [][]any) {
	slices.SortFunc(rows, func(a, b []any) int {
		// Compare each column in order
		for col := 0; col < len(a) && col < len(b); col++ {
			cmp := compareValues(a[col], b[col])
			if cmp == 0 {
				continue
			}

			return cmp
		}

		return 0 // all columns equal
	})
}

func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	} else if a == nil {
		return -1
	} else if b == nil {
		return 1
	}

	normalizeValue := func(v any) any {
		switch val := v.(type) {
		case int:
			return int64(val)
		case int32:
			return int64(val)
		case int64:
			return val
		case float32:
			return float64(val)
		case float64:
			return val
		case string:
			return val
		case []byte:
			return string(val)
		default:
			return fmt.Sprintf("%v", val)
		}
	}

	aVal := normalizeValue(a)
	bVal := normalizeValue(b)

	switch av := aVal.(type) {
	case int64:
		if bv, ok := bVal.(int64); ok {
			return cmp.Compare(av, bv)
		}
	case float64:
		if bv, ok := bVal.(float64); ok {
			return cmp.Compare(av, bv)
		}
	}

	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)

	return cmp.Compare(sa, sb)
}
