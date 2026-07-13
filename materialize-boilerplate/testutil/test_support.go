package testutil

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
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
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func loadSpec(t *testing.T, path string) *pf.MaterializationSpec {
	t.Helper()

	specBytes, err := os.ReadFile(testdataPath("validate_apply_test_cases", "generated_specs", path))
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	return &spec
}

const testItemIdentifier = "_flow_test_"

// flowCheckpointsTableName is the name of the meta checkpoints table created
// by SQL materialization connectors. It is intentionally never cleaned up
// between test runs: keeping it around means subsequent Apply RPCs don't
// re-create it (so it stays out of the action description and snapshots), and
// avoids parallel test runs racing to delete a table they all share.
const flowCheckpointsTableName = "flow_checkpoints_v1"

func testdataPath(parts ...string) string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	elems := append([]string{dir, "..", "testdata"}, parts...)
	return filepath.Join(elems...)
}

func relativePath(t *testing.T, file string) string {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	dir := filepath.Dir(filename)

	return filepath.Join(dir, "..", file)
}

// taskNames returns the ordered list of materialization task names from a bundled spec.
func taskNames(bundled []byte) []string {
	var names []string
	gjson.GetBytes(bundled, "materializations").ForEach(func(task, _ gjson.Result) bool {
		names = append(names, task.String())
		return true
	})
	return names
}

// RunTestAllTasks calls testFn for each materialization task found in the spec
// at sourcePath. The endpoint configuration for the task is decrypted and
// unmarshalled into EC.
func RunTestAllTasks[EC boilerplate.EndpointConfiger](
	t *testing.T,
	sourcePath string,
	testFn func(t *testing.T, bundled []byte, taskName string, cfg EC),
) {
	bundled := RunFlowctl(t, "raw", "bundle", "--source", sourcePath)
	for _, taskName := range taskNames(bundled) {
		t.Log("running test for", taskName)
		cfg := decryptConfig[EC](t, bundled, taskName)
		testFn(t, bundled, taskName, cfg)
	}
}

// maxParallelTasks limits how many materialization tasks run concurrently to
// avoid excessive memory usage.
const maxParallelTasks = 3

// RunTestAllTasksParallel calls testFn for each materialization task
// concurrently (up to maxParallelTasks at a time) using t.Run subtests. It
// returns the ordered task names and a map from task name to the string result
// of testFn, preserving the ability to reassemble results in deterministic
// order for snapshots.
func RunTestAllTasksParallel[EC boilerplate.EndpointConfiger](
	t *testing.T,
	sourcePath string,
	testFn func(t *testing.T, bundled []byte, taskName string, cfg EC) string,
) ([]string, map[string]string) {
	bundled := RunFlowctl(t, "raw", "bundle", "--source", sourcePath)
	names := taskNames(bundled)

	results := make(map[string]string, len(names))
	var mu sync.Mutex
	sem := make(chan struct{}, maxParallelTasks)

	// Use a group subtest so that t.Run blocks until all parallel subtests complete.
	t.Run("tasks", func(t *testing.T) {
		for _, taskName := range names {
			t.Run(taskName, func(t *testing.T) {
				t.Parallel()
				sem <- struct{}{}
				defer func() { <-sem }()
				t.Log("running test for", taskName)
				cfg := decryptConfig[EC](t, bundled, taskName)
				result := testFn(t, bundled, taskName, cfg)
				mu.Lock()
				results[taskName] = result
				mu.Unlock()
			})
		}
	})

	return names, results
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
func RunMaterializationTest[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
	v2 ...RuntimeV2Config,
) {
	if len(v2) > 0 {
		skipUnlessRuntimeV2Flowctl(t)
	}

	ctx := context.Background()
	var snap strings.Builder
	tsSuffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	RunTestAllTasks(t, sourcePath, func(t *testing.T, bundled []byte, taskName string, cfg EC) {
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", taskName))
		if len(v2) > 0 {
			snap.WriteString(runMaterializationTestForTaskV2(t, ctx, newMaterializer, taskName, bundled, tsSuffix, makeResourceFn, actionDescSanitizers, v2[0]))
		} else {
			snap.WriteString(runMaterializationTestForTask(t, ctx, newMaterializer, taskName, bundled, tsSuffix, makeResourceFn, actionDescSanitizers))
		}
	})

	cupaloy.SnapshotT(t, snap.String())
}

// RunMaterializationTestParallel is like RunMaterializationTest but runs tasks
// concurrently (up to maxParallelTasks at a time). Use this for connectors
// where parallel task execution is safe and beneficial.
func RunMaterializationTestParallel[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
	v2 ...RuntimeV2Config,
) {
	if len(v2) > 0 {
		skipUnlessRuntimeV2Flowctl(t)
	}

	ctx := context.Background()
	tsSuffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	names, results := RunTestAllTasksParallel(t, sourcePath, func(t *testing.T, bundled []byte, taskName string, cfg EC) string {
		if len(v2) > 0 {
			return runMaterializationTestForTaskV2(t, ctx, newMaterializer, taskName, bundled, tsSuffix, makeResourceFn, actionDescSanitizers, v2[0])
		}
		return runMaterializationTestForTask(t, ctx, newMaterializer, taskName, bundled, tsSuffix, makeResourceFn, actionDescSanitizers)
	})

	var snap strings.Builder
	for _, name := range names {
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", name))
		snap.WriteString(results[name])
	}

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
func RunApplyTest[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	driver boilerplate.Connector,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) {
	ctx := context.Background()
	var snap strings.Builder

	bundled := RunFlowctl(t, "raw", "bundle", "--source", sourcePath)
	tsSuffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	for _, taskName := range taskNames(bundled) {
		cfg := decryptConfig[EC](t, bundled, taskName)
		materializer, err := newMaterializer(ctx, taskName, cfg, boilerplate.ParseFlags(cfg))
		require.NoError(t, err)

		var testResourcePaths [][]string
		t.Cleanup(func() { CleanupTestResources(t, ctx, materializer, testResourcePaths, tsSuffix) })

		snap.WriteString("Task: " + taskName + "\n\n")

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
	}

	cupaloy.SnapshotT(t, snap.String())
}

// RunApplyTestParallel is like RunApplyTest but runs tasks concurrently (up to
// maxParallelTasks at a time). Use this for connectors where parallel task
// execution is safe and beneficial.
func RunApplyTestParallel[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	driver boilerplate.Connector,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) {
	ctx := context.Background()
	tsSuffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	names, results := RunTestAllTasksParallel(t, sourcePath, func(t *testing.T, bundled []byte, taskName string, cfg EC) string {
		materializer, err := newMaterializer(ctx, taskName, cfg, boilerplate.ParseFlags(cfg))
		require.NoError(t, err)

		var testResourcePaths [][]string
		t.Cleanup(func() { CleanupTestResources(t, ctx, materializer, testResourcePaths, tsSuffix) })

		var taskSnap strings.Builder

		for _, tc := range []struct {
			tableStartsWith string
			do              func(RC)
		}{
			{
				tableStartsWith: "bigschema_",
				do: func(res RC) {
					runBigSchemaApplyTests(t, ctx, &taskSnap, driver, materializer, cfg, res)
				},
			},
			{
				tableStartsWith: "addandremovefields_",
				do: func(res RC) {
					runAddAndRemoveFieldsApplyTests(t, ctx, &taskSnap, driver, materializer, cfg, res)
				},
			},
			{
				tableStartsWith: "challengingnames_",
				do: func(res RC) {
					runChallengingNamesApplyTests(t, ctx, &taskSnap, driver, materializer, cfg, res)
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

		return taskSnap.String()
	})

	var snap strings.Builder
	for _, name := range names {
		snap.WriteString("Task: " + name + "\n\n")
		snap.WriteString(results[name])
	}

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
func RunMigrationTest[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
) {
	ctx := context.Background()
	var snap strings.Builder

	bundled := RunFlowctl(t, "raw", "bundle", "--source", sourcePath)
	suffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	for _, taskName := range taskNames(bundled) {
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", taskName))
		snap.WriteString(runMigrationTestForTask(t, ctx, newMaterializer, taskName, bundled, suffix, makeResourceFn, actionDescSanitizers))
	}

	cupaloy.SnapshotT(t, snap.String())
}

// FeatureFlagMigrationPhase is a single apply within a
// RunFeatureFlagMigrationTest. The binding is materialized with the given
// feature flags and fixture data. Running consecutive phases against the same
// resource exercises column type migrations that are driven by a feature flag
// change rather than a collection schema change.
type FeatureFlagMigrationPhase struct {
	// FeatureFlags is the value of the endpoint's advanced.feature_flags for
	// this phase, e.g. "objects_and_arrays_as_json=false".
	FeatureFlags string
	// Fixture is the path to the fixture document data applied in this phase.
	Fixture string
}

// RunFeatureFlagMigrationTest applies a single binding repeatedly against the
// same resource, changing the endpoint's feature flags between each phase. It
// verifies column type migrations that are triggered by a feature flag change
// rather than a collection schema change - notably that the flow_document column
// migrates in place between JSON and text when objects_and_arrays_as_json is
// toggled, without requiring a backfill.
//
// The source spec must have one or more materialization tasks with a single
// binding. Each phase snapshots the apply action (migration DDL) and the
// resulting table so that both the schema and data can be verified.
func RunFeatureFlagMigrationTest[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	phases []FeatureFlagMigrationPhase,
	actionDescSanitizers []func(string) string,
) {
	ctx := context.Background()
	var snap strings.Builder

	bundled := RunFlowctl(t, "raw", "bundle", "--source", sourcePath)
	suffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	for _, taskName := range taskNames(bundled) {
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", taskName))
		snap.WriteString(runFeatureFlagMigrationForTask(t, ctx, newMaterializer, taskName, bundled, suffix, makeResourceFn, phases, actionDescSanitizers))
	}

	cupaloy.SnapshotT(t, snap.String())
}

func runFeatureFlagMigrationForTask[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	taskName string,
	bundled []byte,
	suffix string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	phases []FeatureFlagMigrationPhase,
	actionDescSanitizers []func(string) string,
) string {
	var snap strings.Builder

	rndSuffix := "_" + uuid.NewString()[:8] + suffix
	workingTableName := "migration_test" + rndSuffix
	workingTaskName := taskName + rndSuffix

	cfg := decryptConfig[EC](t, bundled, taskName)
	materializer, err := newMaterializer(ctx, taskName, cfg, boilerplate.ParseFlags(cfg))
	require.NoError(t, err)

	res := makeResourceFn(workingTableName, false).WithDefaults(cfg)
	resCfgRaw, err := json.Marshal(res)
	require.NoError(t, err)

	bundled, err = sjson.SetBytes(
		bundled,
		fmt.Sprintf("materializations.%s.bindings.0.resource", taskName),
		json.RawMessage(resCfgRaw),
	)
	require.NoError(t, err)

	bundled, err = sjson.SetBytes(
		bundled,
		"materializations."+workingTaskName,
		json.RawMessage(gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s", taskName)).Raw),
	)
	require.NoError(t, err)

	// The config is decrypted once so that each phase can override its feature
	// flags. flowctl passes a plaintext (non-sops) config to the connector
	// unchanged, so writing the decrypted config back inline is sufficient.
	rawCfg := decryptConfigRaw(t, bundled, workingTaskName)

	// The phase's flags are appended to whatever the source config already sets
	// (later entries win in ParseFeatureFlags), so that flags the test relies on
	// - notably allow_existing_tables_for_new_bindings, which lets each phase
	// re-apply to the table created by the previous phase - are preserved.
	baseFlags := gjson.GetBytes(rawCfg, "advanced.feature_flags").String()

	path, _, err := res.Parameters()
	require.NoError(t, err)

	t.Cleanup(func() {
		CleanupTestResources(t, ctx, materializer, [][]string{path}, suffix)
		cleanupTestTasks(t, ctx, materializer, suffix)
	})

	for _, phase := range phases {
		phaseFlags := phase.FeatureFlags
		if baseFlags != "" {
			phaseFlags = baseFlags + "," + phaseFlags
		}
		phaseCfg, err := sjson.SetBytes(rawCfg, "advanced.feature_flags", phaseFlags)
		require.NoError(t, err)

		phaseBundled, err := sjson.SetRawBytes(
			bundled,
			"materializations."+workingTaskName+".endpoint.local.config",
			phaseCfg,
		)
		require.NoError(t, err)

		source := filepath.Join(t.TempDir(), "source.flow.yaml")
		require.NoError(t, os.WriteFile(source, phaseBundled, 0o600))

		actionDescription := RunFlowctl(
			t,
			"preview",
			"--name", workingTaskName,
			"--source", source,
			"--fixture", phase.Fixture,
			"--network", "flow-test",
			"--output-apply",
		)
		for _, sanitize := range actionDescSanitizers {
			actionDescription = []byte(sanitize(string(actionDescription)))
		}

		snap.WriteString(fmt.Sprintf("Feature flags: %q\n", phase.FeatureFlags))
		snap.WriteString(snapshotTestTable(t, ctx, materializer, res, actionDescription, rndSuffix, true))
	}

	return snap.String()
}

// RunMigrationTestParallel is like RunMigrationTest but runs tasks concurrently
// (up to maxParallelTasks at a time). Use this for connectors where parallel
// task execution is safe and beneficial.
func RunMigrationTestParallel[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
) {
	ctx := context.Background()
	suffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	names, results := RunTestAllTasksParallel(t, sourcePath, func(t *testing.T, bundled []byte, taskName string, cfg EC) string {
		return runMigrationTestForTask(t, ctx, newMaterializer, taskName, bundled, suffix, makeResourceFn, actionDescSanitizers)
	})

	var snap strings.Builder
	for _, name := range names {
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", name))
		snap.WriteString(results[name])
	}

	cupaloy.SnapshotT(t, snap.String())
}

func runMaterializationTestForTask[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	taskName string,
	bundled []byte,
	tsSuffix string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
) string {
	var snap strings.Builder

	cfg := decryptConfig[EC](t, bundled, taskName)
	rt := rewriteTaskForTest[EC, RC](t, bundled, taskName, tsSuffix, cfg, makeResourceFn)

	materializer, err := newMaterializer(ctx, taskName, cfg, boilerplate.ParseFlags(cfg))
	require.NoError(t, err)

	t.Cleanup(func() {
		CleanupTestResources(t, ctx, materializer, rt.resourcePaths, tsSuffix)
		cleanupTestTasks(t, ctx, materializer, tsSuffix)
	})

	// Drive task with the data from the fixture.
	actionDescription := RunFlowctl(
		t,
		"preview",
		"--name", rt.workingTaskName,
		"--source", rt.sourcePath,
		"--fixture", relativePath(t, "testdata/integration/fixture.materialize.json"),
		"--network", "flow-test",
		"--output-apply",
		"--output-state",
	)
	for _, sanitize := range actionDescSanitizers {
		actionDescription = []byte(sanitize(string(actionDescription)))
	}

	for _, res := range rt.resources {
		snap.WriteString(snapshotTestTable(t, ctx, materializer, res, actionDescription, rt.rndSuffix, true))
	}

	return snap.String()
}

func snapshotTestTable[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	m boilerplate.Materializer[EC, FC, RC, MT],
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

// decryptConfigRaw returns a task's endpoint config as raw JSON. If the config
// is sops-encrypted it is decrypted and the _sops suffixes are stripped, so the
// result is the plaintext config the connector expects. A plaintext (non-sops)
// config is passed through to flowctl and the connector unchanged, which allows
// callers to edit it (e.g. overriding feature flags) between applies.
func decryptConfigRaw(t *testing.T, bundled []byte, taskName string) json.RawMessage {
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

	return raw
}

func decryptConfig[EC boilerplate.EndpointConfiger](t *testing.T, bundled []byte, taskName string) EC {
	t.Helper()

	var out EC
	require.NoError(t, boilerplate.UnmarshalStrict(decryptConfigRaw(t, bundled, taskName), &out))

	return out
}

func runBigSchemaApplyTests[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	snap *strings.Builder,
	driver boilerplate.Connector,
	m boilerplate.Materializer[EC, FC, RC, MT],
	cfg EC,
	res RC,
) {
	configJson, resourceConfigJson := rawJson(t, cfg), rawJson(t, res)
	fixture := loadSpec(t, "big-schema.flow.proto")

	// Initial validation with no previously existing table.
	validateRes, err := driver.Validate(ctx, validateReq(fixture, nil, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("Big Schema Initial Constraints:\n")
	snap.WriteString(snapshotConstraints(t, validateRes.Bindings[0].ProjectionConstraints))

	// Initial apply with no previously existing table.
	_, err = driver.Apply(ctx, applyReq(fixture, nil, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)

	sch := dumpSchema(t, ctx, m, res)

	// Validate again.
	validateRes, err = driver.Validate(ctx, validateReq(fixture, fixture, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("\nBig Schema Re-validated Constraints:\n")
	snap.WriteString(snapshotConstraints(t, validateRes.Bindings[0].ProjectionConstraints))

	// Apply again - this should be a no-op.
	_, err = driver.Apply(ctx, applyReq(fixture, fixture, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)
	require.Equal(t, sch, dumpSchema(t, ctx, m, res))

	// Validate with most of the field types changed somewhat randomly.
	changed := loadSpec(t, "big-schema-changed.flow.proto")
	validateRes, err = driver.Validate(ctx, validateReq(changed, fixture, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("\nBig Schema Changed Types Constraints:\n")
	snap.WriteString(snapshotConstraints(t, validateRes.Bindings[0].ProjectionConstraints))

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
	snap.WriteString(snapshotConstraints(t, validateRes.Bindings[0].ProjectionConstraints))

	_, err = driver.Apply(ctx, applyReq(changed, nullable, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)
	snap.WriteString("\nBig Schema Materialized Resource Schema Changed Types With Backfill:\n")
	snap.WriteString(dumpSchema(t, ctx, m, res) + "\n")
}

func runAddAndRemoveFieldsApplyTests[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	snap *strings.Builder,
	driver boilerplate.Connector,
	m boilerplate.Materializer[EC, FC, RC, MT],
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

func runChallengingNamesApplyTests[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	snap *strings.Builder,
	driver boilerplate.Connector,
	m boilerplate.Materializer[EC, FC, RC, MT],
	cfg EC,
	res RC,
) {
	configJson, resourceConfigJson := rawJson(t, cfg), rawJson(t, res)
	fixture := loadSpec(t, "challenging-fields.flow.proto")

	// Validate and apply twice to make sure that a re-application does not attempt to re-create
	// any columns. This makes sure we are able to read back the schema we have created
	// correctly. Optionals are included so the challenging-named fields are
	// materialized: recommended/optional fields are not selected by name alone.
	for range 2 {
		validateRes, err := driver.Validate(ctx, validateReq(fixture, nil, configJson, resourceConfigJson))
		require.NoError(t, err)
		_, err = driver.Apply(ctx, applyReq(fixture, nil, configJson, resourceConfigJson, validateRes, true))
		require.NoError(t, err)
	}

	snap.WriteString("Challenging Field Names Materialized Columns:\n")
	snap.WriteString(dumpSchema(t, ctx, m, res))
}

func runMigrationTestForTask[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
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
	materializer, err := newMaterializer(ctx, taskName, cfg, boilerplate.ParseFlags(cfg))
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

	// The live (prior) field selection keeps previously materialized fields
	// selected, just as the control plane does.
	var live pf.FieldSelection
	if lastSpec != nil && len(lastSpec.Bindings) > 0 {
		live = lastSpec.Bindings[0].FieldSelection
	}
	spec.Bindings[0].FieldSelection = selectedFields(validateRes.Bindings[0], spec.Bindings[0].Collection, live, includeOptional)

	req := &pm.Request_Apply{
		Materialization:     spec,
		Version:             "someVersion",
		LastMaterialization: lastSpec,
	}

	return req
}

// selectedFields emulates the control plane's field selection from a binding's
// projection_constraints, mirroring extract_constraints and build_selection in
// flow's field_selection.rs. A field is selected when something wants it and
// nothing fatally rejects it.
//
// Wants that are independent of the connector's "optional" opinion ("hard"
// wants) are: group-by keys and the root document (always selected); the live
// (prior) field selection, which keeps previously materialized fields stable;
// and a connector FIELD_REQUIRED or LOCATION_REQUIRED constraint. When
// includeOptional is set, every other non-rejected field is wanted too,
// emulating a `recommended` selection depth.
//
// Per flow, LOCATION_RECOMMENDED and FIELD_OPTIONAL are equivalent and carry no
// selection signal on their own. FIELD_FORBIDDEN always rejects. INCOMPATIBLE
// rejects a field unless it is independently (hard-)wanted, in which case the
// field is selected presuming a backfill.
func selectedFields(binding *pm.Response_Validated_Binding, collection pf.CollectionSpec, live pf.FieldSelection, includeOptional bool) pf.FieldSelection {
	out := pf.FieldSelection{}

	// Group the binding's projection_constraints by field, preserving order so
	// the folded field is taken from the first constraint, as the control plane
	// does.
	byField := make(map[string][]*pm.Response_Validated_Constraint)
	var fieldOrder []string
	for _, pc := range binding.ProjectionConstraints {
		if _, ok := byField[pc.Field]; !ok {
			fieldOrder = append(fieldOrder, pc.Field)
		}
		byField[pc.Field] = append(byField[pc.Field], pc.Constraint)
	}

	liveFields := make(map[string]struct{})
	for _, f := range live.AllFields() {
		liveFields[f] = struct{}{}
	}

	var foldedFieldMap = make(map[string]struct{})

	for _, field := range fieldOrder {
		constraints := byField[field]

		var forbidden, incompatible, required bool
		for _, c := range constraints {
			switch c.Type {
			case pm.Response_Validated_Constraint_FIELD_FORBIDDEN:
				forbidden = true
			case pm.Response_Validated_Constraint_INCOMPATIBLE,
				pm.Response_Validated_Constraint_UNSATISFIABLE:
				incompatible = true
			case pm.Response_Validated_Constraint_FIELD_REQUIRED,
				pm.Response_Validated_Constraint_LOCATION_REQUIRED:
				required = true
			case pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
				pm.Response_Validated_Constraint_FIELD_OPTIONAL:
				// Neither selected nor rejected by the connector; selection
				// depends on includeOptional below.
			}
		}

		proj := collection.GetProjection(field)
		_, inLive := liveFields[field]
		isKey := proj != nil && proj.IsPrimaryKey
		isDoc := proj != nil && proj.IsRootDocumentProjection()
		hardWanted := required || isKey || isDoc || inLive

		switch {
		case forbidden:
			continue
		case hardWanted:
			// Selected even if also INCOMPATIBLE: presumes a backfill.
		case incompatible:
			// A bare incompatible field with no independent want is dropped.
			continue
		case includeOptional:
			// Recommended/optional field selected at this depth.
		default:
			continue
		}

		var foldedField = field
		if constraints[0].FoldedField != "" {
			foldedField = constraints[0].FoldedField
		}

		// The runtime only keeps one of the fields which have equal FoldedFields. We replicate
		// the same logic here:
		if _, ok := foldedFieldMap[foldedField]; ok {
			continue
		}
		foldedFieldMap[foldedField] = struct{}{}

		switch {
		case isKey:
			out.Keys = append(out.Keys, field)
		case isDoc:
			if out.Document == "" {
				out.Document = field
			} else {
				// Handle cases with more than one root document projection selected - the "first"
				// one is the document, and the rest are materialized as values.
				out.Values = append(out.Values, field)
			}
		default:
			out.Values = append(out.Values, field)
		}
	}

	slices.Sort(out.Keys)
	slices.Sort(out.Values)

	return out
}

// snapshotConstraints makes a compact string representation of a binding's
// projection_constraints, with one constraint printed per line. A field may
// carry multiple constraints, each of which is printed.
func snapshotConstraints(t *testing.T, pcs []*pm.Response_Validated_ProjectionConstraint) string {
	t.Helper()

	type constraintRow struct {
		Field      string
		Type       int
		TypeString string
		Reason     string
	}

	rows := make([]constraintRow, 0, len(pcs))
	for _, pc := range pcs {
		rows = append(rows, constraintRow{
			Field:      pc.Field,
			Type:       int(pc.Constraint.Type),
			TypeString: pc.Constraint.Type.String(),
			Reason:     pc.Constraint.Reason,
		})
	}

	slices.SortFunc(rows, func(i, j constraintRow) int {
		return cmp.Or(strings.Compare(i.Field, j.Field), cmp.Compare(i.Type, j.Type))
	})

	var out strings.Builder
	enc := json.NewEncoder(&out)
	for _, r := range rows {
		require.NoError(t, enc.Encode(r))
	}

	return out.String()
}

func cleanupTestTasks[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	m boilerplate.Materializer[EC, FC, RC, MT],
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

func CleanupTestResources[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	m boilerplate.Materializer[EC, FC, RC, MT],
	paths [][]string,
	tsSuffix string,
) {
	t.Helper()

	is := boilerplate.InitInfoSchema(m.Config())
	require.NoError(t, m.PopulateInfoSchema(ctx, is, paths))
	now := time.Now()

	for _, r := range is.Resources() {
		if shouldCleanup(t, now, r.Location()[len(r.Location())-1], tsSuffix) {
			_, fn, err := m.DeleteResource(ctx, r.Location())
			require.NoError(t, err)

			if err := fn(ctx); err != nil {
				t.Log("failed to clean up resource", err)
			} else {
				t.Log("cleaned up resource", r.Location())
			}
		}
	}
}

func shouldCleanup(t *testing.T, now time.Time, item string, suffix string) bool {
	if item == flowCheckpointsTableName {
		// Never cleanup the meta checkpoints table: leaving it in place keeps
		// it out of the next run's Apply action description (so snapshots stay
		// stable), and prevents parallel test runs sharing a database from
		// racing to delete each other's checkpoints table.
		return false
	}
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
	} else if timestamp := time.Unix(int64(seconds), 0); now.Sub(timestamp) > 60*time.Minute {
		// The threshold must comfortably exceed the longest single test run,
		// since concurrent CI jobs sharing a database will otherwise drop each
		// other's in-use tables mid-test.
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

	// Set TZ=UTC to make tests always run as if the machine
	// has UTC timezone, consisent across contributors' machines
	// and the CI
	os.Setenv("TZ", "UTC")
	cmd := exec.Command("flowctl", args...)
	cmd.Env = append(cmd.Environ(),
		// Set the LOG_FORMAT to text instead of color for better to avoid
		// escaping of ansi control characters and better compatibility with
		// dumb terminals.
		"LOG_FORMAT=text",
	)

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

func dumpSchema[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	m boilerplate.Materializer[EC, FC, RC, MT],
	res RC,
) string {
	t.Helper()

	path, _, err := res.Parameters()
	require.NoError(t, err)

	is := boilerplate.InitInfoSchema(m.Config())
	require.NoError(t, m.PopulateInfoSchema(ctx, is, [][]string{path}))

	type field struct {
		Name     string
		Nullable bool
		Type     string
	}

	var out strings.Builder
	enc := json.NewEncoder(&out)
	fields := slices.Clone(is.GetResource(path).AllFields())
	slices.SortFunc(fields, func(a, b boilerplate.ExistingField) int {
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
