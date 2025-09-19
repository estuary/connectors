package boilerplate

import (
	"bufio"
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

//go:generate ./testdata/generate-spec-proto.sh testdata/validate_apply_test_cases/add-and-remove-many.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_apply_test_cases/add-single-optional.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_apply_test_cases/base.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_apply_test_cases/big-schema-changed.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_apply_test_cases/big-schema-nullable.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_apply_test_cases/big-schema.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_apply_test_cases/challenging-fields.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_apply_test_cases/remove-single-optional.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_apply_test_cases/remove-single-required.flow.yaml

//go:embed testdata/validate_apply_test_cases/generated_specs
var applyValidateFs embed.FS

func loadSpec(t *testing.T, path string) *pf.MaterializationSpec {
	t.Helper()

	specBytes, err := applyValidateFs.ReadFile(filepath.Join("testdata/validate_apply_test_cases/generated_specs", path))
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	return &spec
}

const testItemIdentifier = "_flow_test_"

func relativePath(t *testing.T, file string) string {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	dir := filepath.Dir(filename)

	return filepath.Join(dir, file)
}

// RunTestAllTasks calls testFn for each materialization task found in the spec
// at sourcePath. The endpoint configuration for the task is decrypted and
// unmarshalled into EC.
func RunTestAllTasks[EC EndpointConfiger](
	t *testing.T,
	sourcePath string,
	testFn func(t *testing.T, bundled []byte, taskName string, cfg EC),
) {
	bundled := RunFlowctl(t, "raw", "bundle", "--source", sourcePath)
	gjson.GetBytes(bundled, "materializations").ForEach(func(task, _ gjson.Result) bool {
		t.Log("running test for", task.String())

		taskName := task.String()
		cfg := decryptConfig[EC](t, bundled, taskName)
		testFn(t, bundled, taskName, cfg)

		return true
	})

}

// RunMaterializationTest tests the transactions part of a materialization,
// where data is actually written to and read from the destination system.
//
// The flow spec located at `sourcePath` must have one or more materialization
// tasks, each with bindings that reference the collections in the
// `testdata/integration/collections.materialize.flow.yaml` file. The
// configuration of the bindings will be used in the tests, notably: Delta
// updates can be set, and field selection can be applied, depending on the
// capabilities of the specific connector.
func RunMaterializationTest[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
) {
	ctx := context.Background()
	var snap strings.Builder
	tsSuffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	RunTestAllTasks(t, sourcePath, func(t *testing.T, bundled []byte, taskName string, cfg EC) {
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", taskName))
		snap.WriteString(runMaterializationTestForTask(t, ctx, newMaterializer, taskName, bundled, tsSuffix, makeResourceFn, actionDescSanitizers))
	})

	cupaloy.SnapshotT(t, snap.String())
}

// RunApplyTest tests a variety of scenarios involving changes to materialized
// resources, such as adding and removing fields from the field selection, and
// changing them in various ways.
//
// The flow spec located at `sourcePath` must have one or more materialization
// tasks with a zero-length array for the bindings. The actual binding specs for
// the tests are inserted dynamically. Currently, these specs have been
// synthesized and are located in
// `testdata/validate_apply_test_cases/generated_specs`. Ideally we'd figure out
// a way to use `flowctl` commands instead of generating the binary spec files -
// the main blocker for this is that there is not a way to simulate a previously
// applied materialization spec via `flowctl preview` etc.
func RunApplyTest[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	driver Connector,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) {
	ctx := context.Background()
	var snap strings.Builder

	bundled := RunFlowctl(t, "raw", "bundle", "--source", sourcePath)
	tsSuffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	gjson.GetBytes(bundled, "materializations").ForEach(func(key, value gjson.Result) bool {
		cfg := decryptConfig[EC](t, bundled, key.String())
		materializer, err := newMaterializer(ctx, key.String(), cfg, parseFlags(cfg))
		require.NoError(t, err)

		var testResourcePaths [][]string
		t.Cleanup(func() { CleanupTestResources(t, ctx, materializer, testResourcePaths, tsSuffix) })

		snap.WriteString("Task: " + key.String() + "\n\n")

		for _, tc := range []struct {
			tableStartsWith string
			do              func(RC)
		}{
			{
				tableStartsWith: "bigschema_",
				do: func(res RC) {
					runBigSchemaApplyTests(t, ctx, &snap, driver, materializer, cfg, res)
				},
			},
			{
				tableStartsWith: "addandremovefields_",
				do: func(res RC) {
					runAddAndRemoveFieldsApplyTests(t, ctx, &snap, driver, materializer, cfg, res)
				},
			},
			{
				tableStartsWith: "challengingnames_",
				do: func(res RC) {
					runChallengingNamesApplyTests(t, ctx, &snap, driver, materializer, cfg, res)
				},
			},
		} {
			tableName := tc.tableStartsWith + uuid.NewString()[:8] + tsSuffix
			res := makeResourceFn(tableName, false).WithDefaults(cfg)
			resourcePath, _, err := res.Parameters()
			require.NoError(t, err)
			testResourcePaths = append(testResourcePaths, resourcePath)
			tc.do(res)
		}

		return true
	})

	cupaloy.SnapshotT(t, snap.String())
}

// RunMigrationTest tests migrations of all known schema widening scenarios.
// Data from the pre-migration types is materialized first, and then data from
// the migrated types is materialized in a second run.
//
// The flow spec located at `sourcePath` must have one or more materialization
// tasks with a single binding that references the collection in
// `testdata/integration/collections.migrate-base.flow.yaml`. There's a
// collection with identical field names but with widened types in
// `testdata/integration/collections.migrate-migrated.flow.yaml` that is used to
// simulate the migration.
//
// The binding in the materialization task(s) can use field configuration to
// exclude fields where migration is not supported. Materializations should
// strive to support all of these migrations though.
func RunMigrationTest[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
) {
	ctx := context.Background()
	var snap strings.Builder

	bundled := RunFlowctl(t, "raw", "bundle", "--source", sourcePath)
	suffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	gjson.GetBytes(bundled, "materializations").ForEach(func(task, _ gjson.Result) bool {
		taskName := task.String()
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", taskName))
		snap.WriteString(runMigrationTestForTask(t, ctx, newMaterializer, taskName, bundled, suffix, makeResourceFn, actionDescSanitizers))

		return true
	})

	cupaloy.SnapshotT(t, snap.String())
}

func runMaterializationTestForTask[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	taskName string,
	bundled []byte,
	tsSuffix string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
) string {
	var snap strings.Builder

	rndSuffix := "_" + uuid.NewString()[:8] + tsSuffix
	workingTaskName := taskName + rndSuffix
	cfg := decryptConfig[EC](t, bundled, taskName)

	materializer, err := newMaterializer(ctx, taskName, cfg, parseFlags(cfg))
	require.NoError(t, err)

	var snapshotResources []RC
	var testResourcePaths [][]string
	gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s.bindings", taskName)).ForEach(func(bindingIdx, binding gjson.Result) bool {
		// Replace the final resource path part with a unique name for this test
		// run, to prevent concurrent runs of the test from interfering with
		// each other.
		var res RC
		require.NoError(t, unmarshalStrict(json.RawMessage(gjson.Get(binding.Raw, "resource").Raw), &res))
		path, deltaUpdates, err := res.WithDefaults(cfg).Parameters()
		require.NoError(t, err)
		lastPathPart := path[len(path)-1] + rndSuffix

		res = makeResourceFn(lastPathPart, deltaUpdates).WithDefaults(cfg)
		path, _, err = res.WithDefaults(cfg).Parameters()
		require.NoError(t, err)
		resCfgRaw, err := json.Marshal(res)
		require.NoError(t, err)
		snapshotResources = append(snapshotResources, res)
		testResourcePaths = append(testResourcePaths, path)

		bundled, err = sjson.SetBytes(
			bundled,
			fmt.Sprintf("materializations.%s.bindings.%d.resource", taskName, bindingIdx.Int()),
			json.RawMessage(resCfgRaw),
		)

		return true
	})

	// Also replace the name of the materialization itself with a unique name,
	// again to prevent concurrent tasks from clobbering each other. This is
	// mostly relevant for materializations that use a "checkpoints" table keyed
	// on the task name.
	bundled, err = sjson.SetBytes(
		bundled,
		"materializations."+workingTaskName,
		json.RawMessage(gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s", taskName)).Raw),
	)
	require.NoError(t, err)

	source := filepath.Join(t.TempDir(), "test.flow.yaml")
	require.NoError(t, os.WriteFile(source, bundled, 0o600))

	t.Cleanup(func() {
		CleanupTestResources(t, ctx, materializer, testResourcePaths, tsSuffix)
		cleanupTestTasks(t, ctx, materializer, tsSuffix)
	})

	// Drive task with the data from the fixture.
	actionDescription := RunFlowctl(
		t,
		"preview",
		"--name", workingTaskName,
		"--source", source,
		"--fixture", relativePath(t, "testdata/integration/fixture.materialize.json"),
		"--network", "flow-test",
		"--output-apply",
		"--output-state",
	)
	for _, sanitize := range actionDescSanitizers {
		actionDescription = []byte(sanitize(string(actionDescription)))
	}

	for _, res := range snapshotResources {
		snap.WriteString(snapshotTestTable(t, ctx, materializer, res, actionDescription, rndSuffix, true))
	}

	return snap.String()
}

func snapshotTestTable[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	m Materializer[EC, FC, RC, MT],
	res RC,
	actionDescription []byte,
	rndSuffix string,
	withTableData bool,
) string {
	t.Helper()

	var snap strings.Builder

	path, _, err := res.Parameters()
	require.NoError(t, err)

	schema := dumpSchema(t, ctx, m, res)

	action := strings.ReplaceAll(string(actionDescription), rndSuffix, "")
	action = strings.ReplaceAll(string(action), strings.ToUpper(rndSuffix), "") // a convenience for Snowflake, which uppercases table identifiers

	snap.WriteString("Resource: " + strings.TrimSuffix(strings.Join(path, "."), rndSuffix))
	snap.WriteString("\n")
	snap.WriteString(action)
	snap.WriteString("\n")
	snap.WriteString(schema)
	snap.WriteString("\n")
	if withTableData {
		columnNames, rows, err := m.SnapshotTestResource(ctx, path)
		require.NoError(t, err)
		snap.WriteString("Table Data:\n")
		snap.WriteString(renderTestTableData(t, columnNames, rows))
		snap.WriteString("\n")
	}

	return snap.String()
}

func renderTestTableData(t *testing.T, columnNames []string, rows [][]any) string {
	var data strings.Builder
	enc := json.NewEncoder(&data)
	for _, r := range rows {
		doc := make(map[string]any, len(columnNames))
		for i, col := range columnNames {
			doc[col] = r[i]
		}
		require.NoError(t, enc.Encode(doc))
	}

	return data.String()
}

func decryptConfig[EC EndpointConfiger](t *testing.T, bundled []byte, taskName string) EC {
	t.Helper()

	raw := json.RawMessage(gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s.endpoint.local.config", taskName)).Raw)
	if gjson.GetBytes(raw, "sops").Exists() {
		// Decrypt with sops and strip _sops suffixes.
		sopsCmd := exec.Command("sops", "--decrypt", "--input-type", "json", "--output-type", "json", "/dev/stdin")
		jqCmd := exec.Command("jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
		sopsCmd.Stdin = bytes.NewReader(raw)
		var err error
		jqCmd.Stdin, err = sopsCmd.StdoutPipe()
		require.NoError(t, err)
		require.NoError(t, sopsCmd.Start())
		raw, err = jqCmd.Output()
		require.NoError(t, err)
		require.NoError(t, sopsCmd.Wait())
	}

	var out EC
	require.NoError(t, unmarshalStrict(raw, &out))

	return out
}

func runBigSchemaApplyTests[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	snap *strings.Builder,
	driver Connector,
	m Materializer[EC, FC, RC, MT],
	cfg EC,
	res RC,
) {
	configJson, resourceConfigJson := rawJson(t, cfg), rawJson(t, res)
	fixture := loadSpec(t, "big-schema.flow.proto")

	// Initial validation with no previously existing table.
	validateRes, err := driver.Validate(ctx, validateReq(fixture, nil, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("Big Schema Initial Constraints:\n")
	snap.WriteString(snapshotConstraints(t, validateRes.Bindings[0].Constraints))

	// Initial apply with no previously existing table.
	_, err = driver.Apply(ctx, applyReq(fixture, nil, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)

	sch := dumpSchema(t, ctx, m, res)

	// Validate again.
	validateRes, err = driver.Validate(ctx, validateReq(fixture, fixture, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("\nBig Schema Re-validated Constraints:\n")
	snap.WriteString(snapshotConstraints(t, validateRes.Bindings[0].Constraints))

	// Apply again - this should be a no-op.
	_, err = driver.Apply(ctx, applyReq(fixture, fixture, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)
	require.Equal(t, sch, dumpSchema(t, ctx, m, res))

	// Validate with most of the field types changed somewhat randomly.
	changed := loadSpec(t, "big-schema-changed.flow.proto")
	validateRes, err = driver.Validate(ctx, validateReq(changed, fixture, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("\nBig Schema Changed Types Constraints:\n")
	snap.WriteString(snapshotConstraints(t, validateRes.Bindings[0].Constraints))

	snap.WriteString("\nBig Schema Materialized Resource Schema With All Fields Required:\n")
	snap.WriteString(sch)

	// Validate and apply the schema with all fields removed from required and snapshot the
	// table output.
	nullable := loadSpec(t, "big-schema-nullable.flow.proto")
	validateRes, err = driver.Validate(ctx, validateReq(nullable, fixture, configJson, resourceConfigJson))
	require.NoError(t, err)

	_, err = driver.Apply(ctx, applyReq(nullable, fixture, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)

	// A second apply of the nullable schema should be a no-op.
	sch = dumpSchema(t, ctx, m, res)
	_, err = driver.Apply(ctx, applyReq(nullable, nullable, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)
	require.Equal(t, sch, dumpSchema(t, ctx, m, res))

	snap.WriteString("\nBig Schema Materialized Resource Schema With No Fields Required:\n")
	snap.WriteString(sch)

	// Apply the spec with the randomly changed types, but this time with a backfill.
	changed.Bindings[0].Backfill = 1
	validateRes, err = driver.Validate(ctx, validateReq(changed, nullable, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("\nBig Schema Changed Types With Backfill Constraints:\n")
	snap.WriteString(snapshotConstraints(t, validateRes.Bindings[0].Constraints))

	_, err = driver.Apply(ctx, applyReq(changed, nullable, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)
	snap.WriteString("\nBig Schema Materialized Resource Schema Changed Types With Backfill:\n")
	snap.WriteString(dumpSchema(t, ctx, m, res) + "\n")
}

func runAddAndRemoveFieldsApplyTests[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	snap *strings.Builder,
	driver Connector,
	m Materializer[EC, FC, RC, MT],
	cfg EC,
	res RC,
) {
	tests := []struct {
		name    string
		newSpec *pf.MaterializationSpec
	}{
		{
			name:    "add a single field",
			newSpec: loadSpec(t, "add-single-optional.flow.proto"),
		},
		{
			name:    "remove a single optional field",
			newSpec: loadSpec(t, "remove-single-optional.flow.proto"),
		},
		{
			name:    "remove a single required field",
			newSpec: loadSpec(t, "remove-single-required.flow.proto"),
		},
		{
			name:    "add and remove many fields",
			newSpec: loadSpec(t, "add-and-remove-many.flow.proto"),
		},
	}

	resourcePath, _, err := res.Parameters()
	require.NoError(t, err)
	configJson, resourceConfigJson := rawJson(t, cfg), rawJson(t, res)

	for _, tt := range tests {
		initial := loadSpec(t, "base.flow.proto")

		// Validate and Apply the base spec.
		validateRes, err := driver.Validate(ctx, validateReq(initial, nil, configJson, resourceConfigJson))
		require.NoError(t, err)
		_, err = driver.Apply(ctx, applyReq(initial, nil, configJson, resourceConfigJson, validateRes, true))
		require.NoError(t, err)

		// Validate and Apply the updated spec.
		validateRes, err = driver.Validate(ctx, validateReq(tt.newSpec, initial, configJson, resourceConfigJson))
		require.NoError(t, err)
		_, err = driver.Apply(ctx, applyReq(tt.newSpec, initial, configJson, resourceConfigJson, validateRes, true))
		require.NoError(t, err)

		snap.WriteString(tt.name + ":\n")
		snap.WriteString(dumpSchema(t, ctx, m, res) + "\n")
		_, fn, err := m.DeleteResource(ctx, resourcePath)
		require.NoError(t, err)
		require.NoError(t, fn(ctx))
	}
}

func runChallengingNamesApplyTests[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	snap *strings.Builder,
	driver Connector,
	m Materializer[EC, FC, RC, MT],
	cfg EC,
	res RC,
) {
	configJson, resourceConfigJson := rawJson(t, cfg), rawJson(t, res)
	fixture := loadSpec(t, "challenging-fields.flow.proto")

	// Validate and apply twice to make sure that a re-application does not attempt to re-create
	// any columns. This makes sure we are able to read back the schema we have created
	// correctly.
	for range 2 {
		validateRes, err := driver.Validate(ctx, validateReq(fixture, nil, configJson, resourceConfigJson))
		require.NoError(t, err)
		_, err = driver.Apply(ctx, applyReq(fixture, nil, configJson, resourceConfigJson, validateRes, false))
		require.NoError(t, err)
	}

	snap.WriteString("Challenging Field Names Materialized Columns:\n")
	snap.WriteString(dumpSchema(t, ctx, m, res))
}

func runMigrationTestForTask[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	taskName string,
	bundled []byte,
	suffix string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
) string {
	var snap strings.Builder

	rndSuffix := "_" + uuid.NewString()[:8] + suffix
	workingTableName := "migration_test" + rndSuffix
	workingTaskName := taskName + rndSuffix

	bundledMigratedCollection := RunFlowctl(t, "raw", "bundle", "--source", relativePath(t, "testdata/integration/collections.migrate-migrated.flow.yaml"))

	cfg := decryptConfig[EC](t, bundled, taskName)
	materializer, err := newMaterializer(ctx, taskName, cfg, parseFlags(cfg))
	require.NoError(t, err)

	bindings := gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s.bindings", taskName)).Array()
	require.Equal(t, 1, len(bindings))

	res := makeResourceFn(workingTableName, false).WithDefaults(cfg)
	resCfgRaw, err := json.Marshal(res)
	require.NoError(t, err)

	bundled, err = sjson.SetBytes(
		bundled,
		fmt.Sprintf("materializations.%s.bindings.0.resource", taskName),
		json.RawMessage(resCfgRaw),
	)

	bundled, err = sjson.SetBytes(
		bundled,
		"materializations."+workingTaskName,
		json.RawMessage(gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s", taskName)).Raw),
	)
	require.NoError(t, err)

	initialSource := filepath.Join(t.TempDir(), "test-migration-initial.flow.yaml")
	require.NoError(t, os.WriteFile(initialSource, bundled, 0o600))

	collections := gjson.GetBytes(bundledMigratedCollection, "collections")
	require.Len(t, collections.Map(), 1)
	collections.ForEach(func(collectionName, spec gjson.Result) bool {
		bundled, err = sjson.SetBytes(bundled, fmt.Sprintf("collections.%s", collectionName.String()), json.RawMessage(spec.Raw))
		require.NoError(t, err)
		return false
	})

	migratedSource := filepath.Join(t.TempDir(), "test-migration-migrated.flow.yaml")
	require.NoError(t, os.WriteFile(migratedSource, bundled, 0o600))

	path, _, err := res.Parameters()
	require.NoError(t, err)

	t.Cleanup(func() {
		CleanupTestResources(t, ctx, materializer, [][]string{path}, suffix)
		cleanupTestTasks(t, ctx, materializer, suffix)
	})

	// Drive the migrations in 2 parts, first with the initial fixture data
	// pre-migration, and then with fixture data representing the migrated
	// state. The second one will both cause the columns to be migrated, as well
	// as put some more data in the new form into the table to make sure it
	// works.
	for _, tc := range []struct{ source, fixture string }{
		{source: initialSource, fixture: relativePath(t, "testdata/integration/fixture.migrate-base.json")},
		{source: migratedSource, fixture: relativePath(t, "testdata/integration/fixture.migrate-migrated.json")},
	} {

		actionDescription := RunFlowctl(
			t,
			"preview",
			"--name", workingTaskName,
			"--source", tc.source,
			"--fixture", tc.fixture,
			"--network", "flow-test",
			"--output-apply",
		)
		for _, sanitize := range actionDescSanitizers {
			actionDescription = []byte(sanitize(string(actionDescription)))
		}

		snap.WriteString(snapshotTestTable(t, ctx, materializer, res, actionDescription, rndSuffix, true))
	}

	return snap.String()
}

// validateReq makes a mock Validate request object from a built spec fixture. It only works with a
// single binding.
func validateReq(spec *pf.MaterializationSpec, lastSpec *pf.MaterializationSpec, config json.RawMessage, resourceConfig json.RawMessage) *pm.Request_Validate {
	req := &pm.Request_Validate{
		Name:          spec.Name,
		ConnectorType: spec.ConnectorType,
		ConfigJson:    config,
		Bindings: []*pm.Request_Validate_Binding{{
			ResourceConfigJson: resourceConfig,
			Collection:         spec.Bindings[0].Collection,
			FieldConfigJsonMap: spec.Bindings[0].FieldSelection.FieldConfigJsonMap,
			Backfill:           spec.Bindings[0].Backfill,
		}},
		LastMaterialization: lastSpec,
	}

	return req
}

// applyReq conjures a pm.Request_Apply from a spec and validate response.
func applyReq(spec *pf.MaterializationSpec, lastSpec *pf.MaterializationSpec, config json.RawMessage, resourceConfig json.RawMessage, validateRes *pm.Response_Validated, includeOptional bool) *pm.Request_Apply {
	spec.ConfigJson = config
	spec.Bindings[0].ResourceConfigJson = resourceConfig
	spec.Bindings[0].ResourcePath = validateRes.Bindings[0].ResourcePath
	spec.Bindings[0].DeltaUpdates = validateRes.Bindings[0].DeltaUpdates
	spec.Bindings[0].FieldSelection = selectedFields(validateRes.Bindings[0], spec.Bindings[0].Collection, includeOptional)

	req := &pm.Request_Apply{
		Materialization:     spec,
		Version:             "someVersion",
		LastMaterialization: lastSpec,
	}

	return req
}

// selectedFields creates a field selection that includes all possible fields.
func selectedFields(binding *pm.Response_Validated_Binding, collection pf.CollectionSpec, includeOptional bool) pf.FieldSelection {
	out := pf.FieldSelection{}

	for field, constraint := range binding.Constraints {
		if constraint.Type.IsForbidden() || !includeOptional && constraint.Type == pm.Response_Validated_Constraint_FIELD_OPTIONAL {
			continue
		}

		proj := collection.GetProjection(field)
		if proj.IsPrimaryKey {
			out.Keys = append(out.Keys, field)
		} else if proj.IsRootDocumentProjection() {
			if out.Document == "" {
				out.Document = field
			} else {
				// Handle cases with more than one root document projection selected - the "first"
				// one is the document, and the rest are materialized as values.
				out.Values = append(out.Values, field)
			}
		} else {
			out.Values = append(out.Values, field)
		}
	}

	slices.Sort(out.Keys)
	slices.Sort(out.Values)

	return out
}

// snapshotConstraints makes a compact string representation of a set of constraints, with one
// constraint printed per line.
func snapshotConstraints(t *testing.T, cs map[string]*pm.Response_Validated_Constraint) string {
	t.Helper()

	type constraintRow struct {
		Field      string
		Type       int
		TypeString string
		Reason     string
	}

	rows := make([]constraintRow, 0, len(cs))
	for f, c := range cs {
		rows = append(rows, constraintRow{
			Field:      f,
			Type:       int(c.Type),
			TypeString: c.Type.String(),
			Reason:     c.Reason,
		})
	}

	slices.SortFunc(rows, func(i, j constraintRow) int {
		return strings.Compare(i.Field, j.Field)
	})

	var out strings.Builder
	enc := json.NewEncoder(&out)
	for _, r := range rows {
		require.NoError(t, enc.Encode(r))
	}

	return out.String()
}

func cleanupTestTasks[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	m Materializer[EC, FC, RC, MT],
	tsSuffix string,
) {
	t.Helper()

	tasks, err := m.ListTestTasks(ctx)
	require.NoError(t, err)
	now := time.Now()
	for _, task := range tasks {
		if shouldCleanup(t, now, task, tsSuffix) {
			if err := m.CleanupTestTask(ctx, task); err != nil {
				t.Log("failed to clean up task", err)
			} else {
				t.Log("cleaned up task", task)
			}
		}
	}
}

func CleanupTestResources[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	m Materializer[EC, FC, RC, MT],
	paths [][]string,
	tsSuffix string,
) {
	t.Helper()

	is := initInfoSchema(m.Config())
	require.NoError(t, m.PopulateInfoSchema(ctx, is, paths, true))
	now := time.Now()

	for _, r := range is.resources {
		if shouldCleanup(t, now, r.location[len(r.location)-1], tsSuffix) {
			_, fn, err := m.DeleteResource(ctx, r.location)
			require.NoError(t, err)

			if err := fn(ctx); err != nil {
				t.Log("failed to clean up resource", err)
			} else {
				t.Log("cleaned up resource", r.location)
			}
		}
	}
}

func shouldCleanup(t *testing.T, now time.Time, item string, suffix string) bool {
	if !strings.Contains(item, testItemIdentifier) {
		// Not created for testing.
		return false
	} else if strings.HasSuffix(item, suffix) {
		// Created specifically by this test run.
		return true
	} else if parts := strings.Split(item, "_"); len(parts) < 2 {
		t.Log("malformed test item name", item)
	} else if seconds, err := strconv.Atoi(parts[len(parts)-1]); err != nil {
		t.Log("failed to parse timestamp from test item name", item, err)
	} else if timestamp := time.Unix(int64(seconds), 0); now.Sub(timestamp) > 5*time.Minute {
		t.Log("will cleanup old test item", item)
		return true
	}

	t.Log("will not cleanup recent test item", item)
	return false
}

// RunFlowctl runs the flowctl command with the given arguments, and returns its
// stdout output. The command is expected to succeed, and any stderr output is
// logged to the test log.
func RunFlowctl(t *testing.T, args ...string) []byte {
	t.Helper()

	cmd := exec.Command("flowctl", args...)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			t.Log(scanner.Text())
		}
	}()

	var stdoutBuf bytes.Buffer
	_, err = io.Copy(&stdoutBuf, stdout)
	require.NoError(t, err)

	require.NoError(t, cmd.Wait())
	wg.Wait()

	return stdoutBuf.Bytes()
}

func dumpSchema[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	m Materializer[EC, FC, RC, MT],
	res RC,
) string {
	t.Helper()

	path, _, err := res.Parameters()
	require.NoError(t, err)

	is := initInfoSchema(m.Config())
	require.NoError(t, m.PopulateInfoSchema(ctx, is, [][]string{path}, false))

	type field struct {
		Name     string
		Nullable bool
		Type     string
	}

	var out strings.Builder
	enc := json.NewEncoder(&out)
	fields := slices.Clone(is.GetResource(path).AllFields())
	slices.SortFunc(fields, func(a, b ExistingField) int {
		return strings.Compare(a.Name, b.Name)
	})

	for _, f := range fields {
		require.NoError(t, enc.Encode(field{
			Name:     f.Name,
			Nullable: f.Nullable,
			Type:     f.Type,
		}))
	}

	return out.String()
}

func rawJson(t *testing.T, v any) json.RawMessage {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	return b
}
