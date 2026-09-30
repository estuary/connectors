package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// RunKeyChangeMigrationTest verifies that a connector with KeyChangeInPlace
// keeps its resource and data through a backfill that changes the binding's
// group-by, when retain_existing_data_on_backfill is enabled:
//
//  1. Documents are materialized under the collection key [id].
//  2. The group-by is changed to [tenant, id] with a backfill, which must
//     succeed without dropping the resource.
//  3. Documents are materialized under the new group-by. Those sharing a
//     retained row's (tenant, id) must merge into it rather than duplicate it.
//
// The flow spec located at `sourcePath` must have one or more materialization
// tasks with a single binding of the collection in
// `testdata/integration/collections.key-change.flow.yaml`.
func RunKeyChangeMigrationTest[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	driver boilerplate.Connector,
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
		snap.WriteString(runKeyChangeMigrationForTask(t, ctx, driver, newMaterializer, taskName, bundled, suffix, makeResourceFn, actionDescSanitizers))
	}

	snapshotT(t, snap.String())
}

func runKeyChangeMigrationForTask[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	driver boilerplate.Connector,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	taskName string,
	bundled []byte,
	suffix string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
) string {
	var snap strings.Builder

	rndSuffix := "_" + uuid.NewString()[:8] + suffix
	workingTableName := "key_change_test" + rndSuffix
	workingTaskName := taskName + rndSuffix

	cfg := decryptConfig[EC](t, bundled, taskName)
	materializer, err := newMaterializer(ctx, taskName, cfg, harnessFlags(t, cfg))
	require.NoError(t, err)

	res := makeResourceFn(workingTableName, false).WithDefaults(cfg)
	resCfgRaw := rawJson(t, res)

	bundled, err = sjson.SetRawBytes(bundled, fmt.Sprintf("materializations.%s.bindings.0.resource", taskName), resCfgRaw)
	require.NoError(t, err)
	bundled, err = sjson.SetRawBytes(
		bundled,
		"materializations."+workingTaskName,
		[]byte(gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s", taskName)).Raw),
	)
	require.NoError(t, err)

	rawCfg := decryptConfigRaw(t, bundled, workingTaskName)
	flags := "retain_existing_data_on_backfill"
	if base := gjson.GetBytes(rawCfg, "advanced.feature_flags").String(); base != "" {
		flags = base + "," + flags
	}
	rawCfg, err = sjson.SetBytes(rawCfg, "advanced.feature_flags", flags)
	require.NoError(t, err)
	bundled, err = sjson.SetRawBytes(bundled, "materializations."+workingTaskName+".endpoint.local.config", rawCfg)
	require.NoError(t, err)

	path, _, err := res.Parameters()
	require.NoError(t, err)

	t.Cleanup(func() {
		CleanupTestResources(t, ctx, materializer, [][]string{path}, suffix)
		cleanupTestTasks(t, ctx, materializer, suffix)
	})

	preview := func(source []byte, fixture string) []byte {
		sourcePath := filepath.Join(t.TempDir(), "source.flow.yaml")
		require.NoError(t, os.WriteFile(sourcePath, source, 0o600))

		return RunFlowctl(
			t,
			"raw", "preview-next",
			"--name", workingTaskName,
			"--source", sourcePath,
			"--fixture", relativePath(t, fixture),
			"--network", "flow-test",
			"--output-apply",
		)
	}
	sanitize := func(desc []byte) []byte {
		for _, s := range actionDescSanitizers {
			desc = []byte(s(string(desc)))
		}
		return desc
	}

	snap.WriteString("Base:\n")
	snap.WriteString(snapshotTestTable(t, ctx, materializer, res, sanitize(preview(bundled, "testdata/integration/fixture.key-change-base.json")), rndSuffix, true))

	// flowctl can't present a previously applied spec, so the backfill is
	// applied directly against the live base spec.
	initial := loadSpec(t, "key-change-base.flow.proto")
	updated := loadSpec(t, "key-change-group-by.flow.proto")

	// Both selections are limited to the fields the base phase materialized,
	// so that the backfill differs from it only by its group-by.
	is := boilerplate.InitInfoSchema(materializer.Config())
	require.NoError(t, materializer.PopulateInfoSchema(ctx, is, [][]string{path}))
	existing := is.GetResource(path)
	materialized := func(fields []string) []string {
		return slices.DeleteFunc(fields, func(f string) bool { return existing.GetField(f) == nil })
	}
	limitSelection := func(sel *pf.FieldSelection) {
		sel.Keys, sel.Values = materialized(sel.Keys), materialized(sel.Values)
		if existing.GetField(sel.Document) == nil {
			sel.Document = ""
		}
	}

	validateRes, err := driver.Validate(ctx, validateReq(initial, nil, rawCfg, resCfgRaw))
	require.NoError(t, err)
	applyReq(initial, nil, rawCfg, resCfgRaw, validateRes, true)
	limitSelection(&initial.Bindings[0].FieldSelection)

	validateRes, err = driver.Validate(ctx, validateReq(updated, initial, rawCfg, resCfgRaw))
	require.NoError(t, err)
	req := applyReq(updated, initial, rawCfg, resCfgRaw, validateRes, true)
	limitSelection(&req.Materialization.Bindings[0].FieldSelection)
	applied, err := driver.Apply(ctx, req)
	require.NoError(t, err)

	snap.WriteString("Group-by changed with backfill:\n")
	snap.WriteString(snapshotTestTable(t, ctx, materializer, res, sanitize([]byte(applied.ActionDescription)), rndSuffix, true))

	clearTaskCheckpointBetweenPhases(t, ctx, materializer, workingTaskName)

	bundled, err = sjson.SetBytes(bundled, "materializations."+workingTaskName+".bindings.0.backfill", 1)
	require.NoError(t, err)
	bundled, err = sjson.SetBytes(bundled, "materializations."+workingTaskName+".bindings.0.fields.groupBy", []string{"tenant", "id"})
	require.NoError(t, err)

	snap.WriteString("Materialized under the new group-by:\n")
	snap.WriteString(snapshotTestTable(t, ctx, materializer, res, sanitize(preview(bundled, "testdata/integration/fixture.key-change-group-by.json")), rndSuffix, true))

	return snap.String()
}
