package boilerplate

import (
	"bufio"
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/sync/errgroup"
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

func RunMaterializationTest[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) {
	ctx := context.Background()
	var snap strings.Builder

	bundledSource, err := exec.Command("flowctl", "raw", "bundle", "--source", sourcePath).CombinedOutput()
	require.NoError(t, err)

	suffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	gjson.GetBytes(bundledSource, "materializations").ForEach(func(task, _ gjson.Result) bool {
		taskName := task.String()
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", taskName))
		snap.WriteString(runMaterializationTestForTask(t, ctx, newMaterializer, taskName, bundledSource, suffix, makeResourceFn))

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
	suffix string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) string {
	var snap strings.Builder

	nameTrailer := "_" + uuid.NewString()[:8] + suffix
	workingTaskName := taskName + nameTrailer

	var cfg EC
	rawCfg := json.RawMessage(gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s.endpoint.local.config", taskName)).Raw)
	decrypted := decryptConfig(t, rawCfg)
	require.NoError(t, unmarshalStrict(decrypted, &cfg))

	materializer, err := newMaterializer(ctx, taskName, cfg, parseFlags(cfg))
	require.NoError(t, err)

	var snapshotResources []RC
	var testResourcePaths [][]string
	gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s.bindings", taskName)).ForEach(func(bindingIdx, binding gjson.Result) bool {
		var res RC
		require.NoError(t, unmarshalStrict(json.RawMessage(gjson.Get(binding.Raw, "resource").Raw), &res))
		path, deltaUpdates, err := res.WithDefaults(cfg).Parameters()
		require.NoError(t, err)
		lastPathPart := path[len(path)-1] + nameTrailer

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

	bundled, err = sjson.SetBytes(
		bundled,
		"materializations."+workingTaskName,
		json.RawMessage(gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s", taskName)).Raw),
	)
	require.NoError(t, err)

	source := filepath.Join(t.TempDir(), "test.flow.yaml")
	require.NoError(t, os.WriteFile(source, bundled, 0o600))

	t.Cleanup(func() {
		cleanupTestResources(t, ctx, materializer, testResourcePaths, suffix)
		cleanupTestTasks(t, ctx, materializer, suffix)
	})

	actionDescription := driveTask(t, ctx, true, source, workingTaskName, "../materialize-boilerplate/testdata/integration/fixture.json")
	actionDescription = strings.ReplaceAll(actionDescription, nameTrailer, "")
	for _, res := range snapshotResources {
		path, _, err := res.Parameters()
		require.NoError(t, err)
		columnNames, rows, err := materializer.SnapshotTestResource(ctx, path)
		require.NoError(t, err)
		schema := dumpSchema(t, ctx, materializer, res)

		snap.WriteString("Resource: " + strings.TrimSuffix(strings.Join(path, "."), nameTrailer))
		snap.WriteString("\n")
		snap.WriteString(actionDescription)
		snap.WriteString("\n")
		snap.WriteString(schema)
		snap.WriteString("\n")
		snap.WriteString(renderTestTableData(t, columnNames, rows))
		snap.WriteString("\n")
	}

	return snap.String()
}

func RunApplyTest[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	driver Connector,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) {
	ctx := context.Background()
	var snap strings.Builder

	bundled, err := exec.Command("flowctl", "raw", "bundle", "--source", sourcePath).CombinedOutput()
	require.NoError(t, err)

	suffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	gjson.GetBytes(bundled, "materializations").ForEach(func(key, value gjson.Result) bool {
		rawCfg := json.RawMessage(gjson.Get(value.Raw, "endpoint.local.config").Raw)
		decrypted := decryptConfig(t, rawCfg)

		var cfg EC
		require.NoError(t, unmarshalStrict(decrypted, &cfg))

		materializer, err := newMaterializer(ctx, key.String(), cfg, parseFlags(cfg))
		require.NoError(t, err)

		var testResourcePaths [][]string
		t.Cleanup(func() { cleanupTestResources(t, ctx, materializer, testResourcePaths, suffix) })

		snap.WriteString("\n--- Materialization: " + key.String() + " ---\n\n")

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
			tableName := tc.tableStartsWith + uuid.NewString()[:8] + suffix
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

func RunMigrationTest[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	sourcePath string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) {
	ctx := context.Background()
	var snap strings.Builder

	bundledSource, err := exec.Command("flowctl", "raw", "bundle", "--source", sourcePath).CombinedOutput()
	require.NoError(t, err, string(bundledSource))

	suffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	gjson.GetBytes(bundledSource, "materializations").ForEach(func(task, _ gjson.Result) bool {
		taskName := task.String()
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", taskName))
		snap.WriteString(runMigrationTestForTask(t, ctx, newMaterializer, taskName, bundledSource, suffix, makeResourceFn))

		return true
	})

	cupaloy.SnapshotT(t, snap.String())
}

func runMigrationTestForTask[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	taskName string,
	bundled []byte,
	suffix string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) string {
	var snap strings.Builder

	nameTrailer := "_" + uuid.NewString()[:8] + suffix
	workingTableName := "migration_test" + nameTrailer
	workingTaskName := taskName + nameTrailer

	bundledMigratedCollection, err := exec.Command("flowctl", "raw", "bundle", "--source", "../materialize-boilerplate/testdata/integration/migration-migrated-collection.flow.yaml").CombinedOutput()
	require.NoError(t, err)

	var cfg EC
	rawCfg := json.RawMessage(gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s.endpoint.local.config", taskName)).Raw)
	decrypted := decryptConfig(t, rawCfg)
	require.NoError(t, unmarshalStrict(decrypted, &cfg))

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

	// TODO: Clean up, verify length is one and do it that way
	gjson.GetBytes(bundledMigratedCollection, "collections").ForEach(func(collectionName, spec gjson.Result) bool {
		bundled, err = sjson.SetBytes(bundled, fmt.Sprintf("collections.%s", collectionName.String()), spec.Value())
		require.NoError(t, err)
		return true
	})

	migratedSource := filepath.Join(t.TempDir(), "test-migration-migrated.flow.yaml")
	require.NoError(t, os.WriteFile(migratedSource, bundled, 0o600))

	path, _, err := res.Parameters()
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupTestResources(t, ctx, materializer, [][]string{path}, suffix)
		cleanupTestTasks(t, ctx, materializer, suffix)
	})

	{
		actionDescription := driveTask(t, ctx, false, initialSource, workingTaskName, "../materialize-boilerplate/testdata/integration/fixture.migration-initial.json")
		actionDescription = strings.ReplaceAll(actionDescription, nameTrailer, "")

		columnNames, rows, err := materializer.SnapshotTestResource(ctx, path)
		require.NoError(t, err)
		schema := dumpSchema(t, ctx, materializer, res)

		snap.WriteString(actionDescription)
		snap.WriteString("\n")
		snap.WriteString(schema)
		snap.WriteString("\n")
		snap.WriteString(renderTestTableData(t, columnNames, rows))
		snap.WriteString("\n")
	}

	{
		actionDescription := driveTask(t, ctx, false, migratedSource, workingTaskName, "../materialize-boilerplate/testdata/integration/fixture.migration-migrated.json")
		actionDescription = strings.ReplaceAll(actionDescription, nameTrailer, "")

		columnNames, rows, err := materializer.SnapshotTestResource(ctx, path)
		require.NoError(t, err)
		schema := dumpSchema(t, ctx, materializer, res)

		snap.WriteString(actionDescription)
		snap.WriteString("\n")
		snap.WriteString(schema)
		snap.WriteString("\n")
		snap.WriteString(renderTestTableData(t, columnNames, rows))
		snap.WriteString("\n")
	}

	return snap.String()
}

func renderTestTableData(t *testing.T, columnNames []string, rows [][]any) string {
	sorted, err := sortRows(columnNames, rows)
	require.NoError(t, err)

	var data strings.Builder
	enc := json.NewEncoder(&data)
	for _, r := range sorted {
		doc := make(map[string]any, len(columnNames))
		for i, col := range columnNames {
			doc[col] = r[i]
		}
		require.NoError(t, enc.Encode(doc))
	}

	return data.String()
}

func decryptConfig(t *testing.T, cfg json.RawMessage) json.RawMessage {
	t.Helper()

	// Check for sops metadata. If none, return the config as-is, assuming it is
	// not encrypted, as would be the case for local test configs.
	type skimCfg struct {
		Sops json.RawMessage `json:"sops"`
	}
	var skim skimCfg
	require.NoError(t, json.Unmarshal(cfg, &skim))
	if len(skim.Sops) == 0 {
		return cfg
	}

	sopsCmd := exec.Command("sops", "--decrypt", "--input-type", "json", "--output-type", "json", "/dev/stdin")
	jqCmd := exec.Command("jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
	sopsCmd.Stdin = bytes.NewReader(cfg)
	var err error
	jqCmd.Stdin, err = sopsCmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, sopsCmd.Start())
	decryptedConfig, err := jqCmd.Output()
	require.NoError(t, err)
	require.NoError(t, sopsCmd.Wait())

	return decryptedConfig
}

func driveTask(t *testing.T, ctx context.Context, outputState bool, source, name, fixturePath string) string {
	t.Helper()

	args := []string{
		"preview",
		"--name", name,
		"--source", source,
		"--fixture", fixturePath,
		"--network", "flow-test",
		"--output-apply",
	}

	if outputState {
		args = append(args, "--output-state")
	}

	cmd := exec.Command("flowctl", args...)
	cmd.Env = append(cmd.Environ(), "RUST_LOG=info")

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
	stdoutOp := m.RunAsyncOperation(func() error {
		_, err = io.Copy(&stdoutBuf, stdout)
		return err
	})

	select {
	case <-ctx.Done():
		// Early exit on context cancellation, which would arise from a failure
		// of one of the other configured tasks.
		t.Fatal(ctx.Err())
	case <-stdoutOp.Done():
	}

	require.NoError(t, stdoutOp.Err())
	require.NoError(t, cmd.Wait(), stdoutBuf.String())
	wg.Wait()

	return stdoutBuf.String()
}

// sortRows sorts rows by the "flow_published_at" column, which is expected to
// be present in all materialized tables. This is more generally applicable than
// sorting by a specific key column, since a test task might have a different
// key name, or more than one key. But it means "flow_published_at" must be
// selected.
func sortRows(columnNames []string, rows [][]any) ([][]any, error) {
	publishedAtIdx := slices.IndexFunc(columnNames, func(s string) bool {
		return strings.EqualFold(s, "flow_published_at")
	})
	if publishedAtIdx == -1 {
		return nil, errors.New("no flow_published_at column found in columnNames")
	}

	slices.SortFunc(rows, func(a, b []any) int {
		l, r := a[publishedAtIdx], b[publishedAtIdx]

		switch l := l.(type) {
		case string:
			return strings.Compare(l, r.(string))
		case time.Time:
			return l.Compare(r.(time.Time))
		default:
			panic(fmt.Sprintf("unsupported flow_published_at column type %T", l))
		}
	})

	return rows, nil
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
	validateRes, err := driver.Validate(ctx, ValidateReq(fixture, nil, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("Big Schema Initial Constraints:\n")
	snap.WriteString(SnapshotConstraints(t, validateRes.Bindings[0].Constraints))

	// Initial apply with no previously existing table.
	_, err = driver.Apply(ctx, ApplyReq(fixture, nil, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)

	sch := dumpSchema(t, ctx, m, res)

	// Validate again.
	validateRes, err = driver.Validate(ctx, ValidateReq(fixture, fixture, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("\nBig Schema Re-validated Constraints:\n")
	snap.WriteString(SnapshotConstraints(t, validateRes.Bindings[0].Constraints))

	// Apply again - this should be a no-op.
	_, err = driver.Apply(ctx, ApplyReq(fixture, fixture, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)
	require.Equal(t, sch, dumpSchema(t, ctx, m, res))

	// Validate with most of the field types changed somewhat randomly.
	changed := loadSpec(t, "big-schema-changed.flow.proto")
	validateRes, err = driver.Validate(ctx, ValidateReq(changed, fixture, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("\nBig Schema Changed Types Constraints:\n")
	snap.WriteString(SnapshotConstraints(t, validateRes.Bindings[0].Constraints))

	snap.WriteString("\nBig Schema Materialized Resource Schema With All Fields Required:\n")
	snap.WriteString(sch)

	// Validate and apply the schema with all fields removed from required and snapshot the
	// table output.
	nullable := loadSpec(t, "big-schema-nullable.flow.proto")
	validateRes, err = driver.Validate(ctx, ValidateReq(nullable, fixture, configJson, resourceConfigJson))
	require.NoError(t, err)

	_, err = driver.Apply(ctx, ApplyReq(nullable, fixture, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)

	// A second apply of the nullable schema should be a no-op.
	sch = dumpSchema(t, ctx, m, res)
	_, err = driver.Apply(ctx, ApplyReq(nullable, nullable, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)
	require.Equal(t, sch, dumpSchema(t, ctx, m, res))

	snap.WriteString("\nBig Schema Materialized Resource Schema With No Fields Required:\n")
	snap.WriteString(sch)

	// Apply the spec with the randomly changed types, but this time with a backfill.
	changed.Bindings[0].Backfill = 1
	validateRes, err = driver.Validate(ctx, ValidateReq(changed, nullable, configJson, resourceConfigJson))
	require.NoError(t, err)

	snap.WriteString("\nBig Schema Changed Types With Backfill Constraints:\n")
	snap.WriteString(SnapshotConstraints(t, validateRes.Bindings[0].Constraints))

	_, err = driver.Apply(ctx, ApplyReq(changed, nullable, configJson, resourceConfigJson, validateRes, true))
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
		validateRes, err := driver.Validate(ctx, ValidateReq(initial, nil, configJson, resourceConfigJson))
		require.NoError(t, err)
		_, err = driver.Apply(ctx, ApplyReq(initial, nil, configJson, resourceConfigJson, validateRes, true))
		require.NoError(t, err)

		// Validate and Apply the updated spec.
		validateRes, err = driver.Validate(ctx, ValidateReq(tt.newSpec, initial, configJson, resourceConfigJson))
		require.NoError(t, err)
		_, err = driver.Apply(ctx, ApplyReq(tt.newSpec, initial, configJson, resourceConfigJson, validateRes, true))
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
		validateRes, err := driver.Validate(ctx, ValidateReq(fixture, nil, configJson, resourceConfigJson))
		require.NoError(t, err)
		_, err = driver.Apply(ctx, ApplyReq(fixture, nil, configJson, resourceConfigJson, validateRes, false))
		require.NoError(t, err)
	}

	snap.WriteString("Challenging Field Names Materialized Columns:\n")
	snap.WriteString(dumpSchema(t, ctx, m, res))
}

// validateReq makes a mock Validate request object from a built spec fixture. It only works with a
// single binding.
func ValidateReq(spec *pf.MaterializationSpec, lastSpec *pf.MaterializationSpec, config json.RawMessage, resourceConfig json.RawMessage) *pm.Request_Validate {
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
func ApplyReq(spec *pf.MaterializationSpec, lastSpec *pf.MaterializationSpec, config json.RawMessage, resourceConfig json.RawMessage, validateRes *pm.Response_Validated, includeOptional bool) *pm.Request_Apply {
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
func SnapshotConstraints(t *testing.T, cs map[string]*pm.Response_Validated_Constraint) string {
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
	suffix string,
) {
	t.Helper()

	tasks, err := m.ListTestTasks(ctx)
	require.NoError(t, err)
	now := time.Now()
	for _, task := range tasks {
		if !strings.Contains(task, testItemIdentifier) {
			// Not a test table.
		} else if strings.HasSuffix(task, suffix) {
			// This resource was created by this test run.
			if err := m.CleanupTestTask(ctx, task); err != nil {
				t.Log("failed to clean up test item", t, err)
			} else {
				t.Log("cleaned up test item", task)
			}
		} else if parts := strings.Split(task, "_"); len(parts) < 2 {
			t.Log("malformed test item name", t)
		} else if seconds, err := strconv.Atoi(parts[len(parts)-1]); err != nil {
			t.Log("failed to parse timestamp from test item name", t, err)
		} else if timestamp := time.Unix(int64(seconds), 0); now.Sub(timestamp) > 5*time.Minute {
			t.Log("will cleanup old test item", task)
			if err := m.CleanupTestTask(ctx, task); err != nil {
				t.Log("failed to clean up test task", t, err)
			} else {
				t.Log("cleaned up test item", task)
			}
		}

	}

}

func cleanupTestResources[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	ctx context.Context,
	m Materializer[EC, FC, RC, MT],
	paths [][]string,
	suffix string,
) {
	t.Helper()

	now := time.Now()
	var toCleanup [][]string

	is := initInfoSchema(m.Config())
	if err := m.PopulateInfoSchema(ctx, is, paths, true); err != nil {
		// Couldn't list existing resources for some reason. At least try to
		// clean up the paths that were passed in.
		toCleanup = paths
		t.Log("failed to populate info schema for cleanup", err)
	} else {
		for _, r := range is.resources {
			last := r.location[len(r.location)-1]
			if !strings.Contains(last, testItemIdentifier) {
				// Not a test table.
			} else if strings.HasSuffix(last, suffix) {
				// This resource was created by this test run.
				toCleanup = append(toCleanup, r.location)
			} else if parts := strings.Split(last, "_"); len(parts) < 2 {
				t.Log("malformed test table name", last)
			} else if seconds, err := strconv.Atoi(parts[len(parts)-1]); err != nil {
				t.Log("failed to parse timestamp from test table name", last, err)
			} else if timestamp := time.Unix(int64(seconds), 0); now.Sub(timestamp) > 6*time.Hour {
				t.Log("will cleanup old test table", r.location)
				toCleanup = append(toCleanup, r.location)
			}
		}
	}

	var group errgroup.Group
	for _, path := range toCleanup {
		group.Go(func() error {
			if _, fn, err := m.DeleteResource(ctx, path); err != nil {
				return err
			} else if err := fn(ctx); err != nil {
				t.Log("failed to clean up resource", path, err)
				return nil
			}

			t.Log("cleaned up resource", path)
			return nil
		})
	}

	require.NoError(t, group.Wait())
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
