package testutil

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// rewrittenTask is a bundled task specification rewritten for a test run:
// the task and its bindings' resources carry unique names so that concurrent
// runs don't interfere, and the rewritten bundle is written to a temp file.
type rewrittenTask[RC any] struct {
	sourcePath      string
	workingTaskName string
	rndSuffix       string
	resources       []RC
	resourcePaths   [][]string
}

func rewriteTaskForTest[EC boilerplate.EndpointConfiger, RC boilerplate.Resourcer[RC, EC]](
	t *testing.T,
	bundled []byte,
	taskName string,
	tsSuffix string,
	cfg EC,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) rewrittenTask[RC] {
	t.Helper()

	rndSuffix := "_" + uuid.NewString()[:8] + tsSuffix
	workingTaskName := taskName + rndSuffix

	var snapshotResources []RC
	var testResourcePaths [][]string
	gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s.bindings", taskName)).ForEach(func(bindingIdx, binding gjson.Result) bool {
		// Replace the final resource path part with a unique name for this test
		// run, to prevent concurrent runs of the test from interfering with
		// each other.
		var res RC
		require.NoError(t, boilerplate.UnmarshalStrict(json.RawMessage(gjson.Get(binding.Raw, "resource").Raw), &res))
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
		require.NoError(t, err)

		return true
	})

	// Also replace the name of the materialization itself with a unique name,
	// again to prevent concurrent tasks from clobbering each other. This is
	// mostly relevant for materializations that use a "checkpoints" table keyed
	// on the task name.
	bundled, err := sjson.SetBytes(
		bundled,
		"materializations."+workingTaskName,
		json.RawMessage(gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s", taskName)).Raw),
	)
	require.NoError(t, err)

	source := filepath.Join(t.TempDir(), "test.flow.yaml")
	require.NoError(t, os.WriteFile(source, bundled, 0o600))

	return rewrittenTask[RC]{
		sourcePath:      source,
		workingTaskName: workingTaskName,
		rndSuffix:       rndSuffix,
		resources:       snapshotResources,
		resourcePaths:   testResourcePaths,
	}
}

// RuntimeV2Config opts a materialization integration test into running on
// the v2 runtime, via `flowctl raw preview-next --fixture --shards N`,
// feeding the same fixture as the legacy runner. Multi-shard runs hash-route
// fixture documents across shards exactly as live shuffled reads would, and
// require the connector to implement the scale-out contract (range-scoped
// state, shard-zero-executes).
//
// The run spans two sessions: the v2 runtime halts a session after its final
// commit without running its post-commit Acknowledge, so a second session
// recovers the consolidated connector state and applies the outstanding
// staged transaction before destination tables are snapshotted. The second
// session's budget is one trailing empty transaction appended to a copy of
// the fixture, so both sessions end deterministically by transaction count.
//
// Single-shard runs also capture `--output-apply` / `--output-state` lines
// into the snapshot, exactly as the legacy runner does; those flags don't
// yet support multiple shards, so sharded snapshots omit them.
type RuntimeV2Config struct {
	// Shards to run the task with. Zero defaults to one.
	Shards int
	// ExtraFeatureFlags are appended to the endpoint config's existing
	// feature flags — e.g. the connector's scale-out flag, which multi-shard
	// runs require.
	ExtraFeatureFlags []string
	// Timeout is a backstop against a hung run; a healthy run ends by
	// transaction count. Zero defaults to ten minutes.
	Timeout time.Duration
}

func runMaterializationTestForTaskV2[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	taskName string,
	bundled []byte,
	tsSuffix string,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
	actionDescSanitizers []func(string) string,
	v2 RuntimeV2Config,
) string {
	var snap strings.Builder

	shards := max(v2.Shards, 1)
	timeout := v2.Timeout
	if timeout == 0 {
		timeout = 10 * time.Minute
	}
	// Keep flowctl's own timeout inside Go's `-test.timeout`, leaving a buffer so
	// flowctl can stop gracefully (finishing its drain session and tearing down
	// connector containers) before the test binary is force-killed on timeout.
	if f := flag.Lookup("test.timeout"); f != nil {
		if testTimeout, err := time.ParseDuration(f.Value.String()); err == nil && testTimeout > time.Minute {
			if capped := testTimeout - time.Minute; capped < timeout {
				timeout = capped
			}
		}
	}

	// Extra feature flags are appended to whatever the source config already
	// sets (later entries win in ParseFeatureFlags). flowctl passes a
	// plaintext (non-sops) config to the connector unchanged, so writing the
	// decrypted config back inline is sufficient.
	if len(v2.ExtraFeatureFlags) > 0 {
		rawCfg := decryptConfigRaw(t, bundled, taskName)
		flags := strings.Join(v2.ExtraFeatureFlags, ",")
		if baseFlags := gjson.GetBytes(rawCfg, "advanced.feature_flags").String(); baseFlags != "" {
			flags = baseFlags + "," + flags
		}
		rawCfg, err := sjson.SetBytes(rawCfg, "advanced.feature_flags", flags)
		require.NoError(t, err)
		bundled, err = sjson.SetRawBytes(bundled, fmt.Sprintf("materializations.%s.endpoint.local.config", taskName), rawCfg)
		require.NoError(t, err)
	}

	cfg := decryptConfig[EC](t, bundled, taskName)
	rt := rewriteTaskForTest[EC, RC](t, bundled, taskName, tsSuffix, cfg, makeResourceFn)

	materializer, err := newMaterializer(ctx, taskName, cfg, boilerplate.ParseFlags(cfg))
	require.NoError(t, err)

	t.Cleanup(func() {
		CleanupTestResources(t, ctx, materializer, rt.resourcePaths, tsSuffix)
		cleanupTestTasks(t, ctx, materializer, tsSuffix)
	})

	// Drive the task on the v2 runtime: one session runs every fixture
	// transaction, and flowctl's final drain session recovery-applies the
	// last (committed but not yet acknowledged) one.
	args := []string{
		"raw", "preview-next",
		"--name", rt.workingTaskName,
		"--source", rt.sourcePath,
		"--fixture", relativePath(t, "testdata/integration/fixture.materialize.json"),
		"--shards", strconv.Itoa(shards),
		"--timeout", timeout.String(),
		"--network", "flow-test",
	}
	if shards == 1 {
		args = append(args, "--output-apply", "--output-state")
	}

	actionDescription := RunFlowctl(t, args...)
	for _, sanitize := range actionDescSanitizers {
		actionDescription = []byte(sanitize(string(actionDescription)))
	}

	for _, res := range rt.resources {
		snap.WriteString(snapshotTestTable(t, ctx, materializer, res, actionDescription, rt.rndSuffix, true))
	}

	return snap.String()
}
