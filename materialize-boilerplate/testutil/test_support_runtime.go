package testutil

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
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
	// originalNames are the bindings' final resource path parts before
	// rewriting, parallel to resourcePaths.
	originalNames []string
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
	var originalNames []string
	gjson.GetBytes(bundled, fmt.Sprintf("materializations.%s.bindings", taskName)).ForEach(func(bindingIdx, binding gjson.Result) bool {
		// Replace the final resource path part with a unique name for this test
		// run, to prevent concurrent runs of the test from interfering with
		// each other.
		var res RC
		require.NoError(t, boilerplate.UnmarshalStrict(json.RawMessage(gjson.Get(binding.Raw, "resource").Raw), &res))
		path, deltaUpdates, err := res.WithDefaults(cfg).Parameters()
		require.NoError(t, err)
		originalNames = append(originalNames, path[len(path)-1])
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
		originalNames:   originalNames,
	}
}

// TestTask is a task of a bundled spec renamed for one test run, for tests that
// drive flowctl themselves. Its task and resources are removed when the test
// ends.
type TestTask struct {
	// SourcePath is the rewritten spec, for flowctl's --source.
	SourcePath string
	// Name is the task's per-run name, for flowctl's --name.
	Name string
	// paths maps each binding's final resource path part in the original spec
	// to its per-run resource path.
	paths map[string][]string
}

// Path returns the per-run resource path of the binding named name in the
// original spec.
func (tt TestTask) Path(t *testing.T, name string) []string {
	t.Helper()
	path, ok := tt.paths[name]
	require.True(t, ok, "no binding named %q", name)
	return path
}

// Resource returns the per-run final path part of the binding named name in
// the original spec.
func (tt TestTask) Resource(t *testing.T, name string) string {
	t.Helper()
	path := tt.Path(t, name)
	return path[len(path)-1]
}

// PrepareTestTask gives taskName and its bindings' resources unique per-run
// names, so that concurrent runs of a test sharing a destination don't
// interfere, and removes them when the test ends.
func PrepareTestTask[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	bundled []byte,
	taskName string,
	cfg EC,
	makeResourceFn func(finalResourcePathPart string, deltaUpdates bool) RC,
) TestTask {
	t.Helper()

	ctx := context.Background()
	tsSuffix := testItemIdentifier + fmt.Sprintf("%d", time.Now().Unix())

	rt := rewriteTaskForTest[EC, RC](t, bundled, taskName, tsSuffix, cfg, makeResourceFn)
	paths := make(map[string][]string)
	for i, name := range rt.originalNames {
		paths[name] = rt.resourcePaths[i]
	}

	materializer, err := newMaterializer(ctx, taskName, cfg, harnessFlags(t, cfg))
	require.NoError(t, err)
	t.Cleanup(func() {
		CleanupTestResources(t, ctx, materializer, rt.resourcePaths, tsSuffix)
		cleanupTestTasks(t, ctx, materializer, tsSuffix)
	})

	return TestTask{SourcePath: rt.sourcePath, Name: rt.workingTaskName, paths: paths}
}

// RuntimeConfig configures how a materialization integration test drives the
// runtime, via `flowctl raw preview-next --fixture --shards N`. With the
// FLOW_TEST_RUNTIME environment variable set to "v1" the test instead drives
// legacy `flowctl preview` (runtime v1): a liveness check (runtimeV1Liveness)
// and then a drained run snapshotted against a table-only `-runtime-v1`
// golden; CI runs both. Multi-shard
// runs hash-route fixture documents across shards exactly as live shuffled
// reads would, and require the connector to implement the scale-out contract
// (range-scoped state, shard-zero-executes).
//
// Single-shard runs also capture `--output-apply` / `--output-state` lines into
// the snapshot; those flags don't yet support multiple shards, so sharded
// snapshots omit them.
type RuntimeConfig struct {
	// Shards to run the task with. Zero defaults to one.
	Shards int
	// ExtraFeatureFlags are appended to the endpoint config's existing
	// feature flags — e.g. the connector's scale-out flag, which multi-shard
	// runs require.
	ExtraFeatureFlags []string
	// ConfigOverrides sets endpoint config values by sjson path, e.g.
	// "advanced.no_flow_document", to re-run a suite in another mode.
	ConfigOverrides map[string]any
	// Timeout is a backstop against a hung run; a healthy run ends by
	// transaction count. Zero defaults to ten minutes.
	Timeout time.Duration
	// Fidelity is the transaction health fidelity the connector is expected to
	// report: the run's "transaction health" log lines must all be healthy and
	// carry exactly this fidelity, so a connector that stops reporting fails
	// its own suite. Zero means the connector reports nothing (FidelityNone).
	Fidelity m.Fidelity
}

// applyConfigOverrides returns bundled with runtime's ExtraFeatureFlags and
// ConfigOverrides applied to the endpoint config of taskName. Extra feature
// flags are appended to whatever the source config already sets (later
// entries win in ParseFeatureFlags). flowctl passes a plaintext (non-sops)
// config to the connector unchanged, so writing the decrypted config back
// inline is sufficient.
func applyConfigOverrides(t *testing.T, bundled []byte, taskName string, runtime RuntimeConfig) []byte {
	t.Helper()

	if len(runtime.ExtraFeatureFlags) == 0 && len(runtime.ConfigOverrides) == 0 {
		return bundled
	}

	rawCfg := decryptConfigRaw(t, bundled, taskName)
	var err error
	if len(runtime.ExtraFeatureFlags) > 0 {
		flags := strings.Join(runtime.ExtraFeatureFlags, ",")
		if baseFlags := gjson.GetBytes(rawCfg, "advanced.feature_flags").String(); baseFlags != "" {
			flags = baseFlags + "," + flags
		}
		rawCfg, err = sjson.SetBytes(rawCfg, "advanced.feature_flags", flags)
		require.NoError(t, err)
	}
	for path, value := range runtime.ConfigOverrides {
		rawCfg, err = sjson.SetBytes(rawCfg, path, value)
		require.NoError(t, err)
	}
	bundled, err = sjson.SetRawBytes(bundled, fmt.Sprintf("materializations.%s.endpoint.local.config", taskName), rawCfg)
	require.NoError(t, err)

	return bundled
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
	runtime RuntimeConfig,
) string {
	var snap strings.Builder

	shards := max(runtime.Shards, 1)
	timeout := runtime.Timeout
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

	bundled = applyConfigOverrides(t, bundled, taskName, runtime)

	cfg := decryptConfig[EC](t, bundled, taskName)
	rt := rewriteTaskForTest[EC, RC](t, bundled, taskName, tsSuffix, cfg, makeResourceFn)

	materializer, err := newMaterializer(ctx, taskName, cfg, harnessFlags(t, cfg))
	require.NoError(t, err)

	// Bound now: the runtime v1 pass reassigns rt and materializer below, and
	// its liveness tables must still be dropped.
	firstRT, firstMaterializer := rt, materializer
	t.Cleanup(func() {
		CleanupTestResources(t, ctx, firstMaterializer, firstRT.resourcePaths, tsSuffix)
		cleanupTestTasks(t, ctx, firstMaterializer, tsSuffix)
	})

	// One session runs every fixture transaction. A session halts after its
	// final commit without running the post-commit Acknowledge, so flowctl
	// appends an empty drain session which recovery-applies that last
	// transaction before the destination tables are snapshotted.
	var args []string
	if RuntimeV1() {
		runtimeV1Liveness(t, ctx, materializer, rt)
		// Then the whole fixture, drained, for the runtime v1 golden: what a
		// correct connector leaves in the destination on either runtime. Legacy
		// preview does not auto-append the drain session that preview-next
		// does, and the apply/state output is omitted because it embeds
		// per-run values and differs by runtime. It runs as a fresh task with
		// fresh tables: a connector that keeps its runtime checkpoint in the
		// destination would otherwise resume from the liveness run's and apply
		// only the fixture's tail, and a table that survived a name-based sweep
		// would take these rows on top of the liveness run's.
		rt = rewriteTaskForTest[EC, RC](t, bundled, taskName, tsSuffix, cfg, makeResourceFn)
		// And a fresh materializer to read it back with: a test materializer may
		// cache what it read for the liveness run (eventbridge drains its queue
		// once), or hold connections the liveness run's cleanup closes.
		materializer, err = newMaterializer(ctx, taskName, cfg, harnessFlags(t, cfg))
		require.NoError(t, err)
		t.Cleanup(func() {
			CleanupTestResources(t, ctx, materializer, rt.resourcePaths, tsSuffix)
		})
		args = []string{
			"preview",
			"--name", rt.workingTaskName,
			"--source", rt.sourcePath,
			"--fixture", relativePath(t, "testdata/integration/fixture.materialize.json"),
			"--sessions=-1,0", // one argument: clap reads a bare "-1" as a flag
			"--timeout", timeout.String(),
			"--network", "flow-test",
		}
	} else {
		args = []string{
			"raw", "preview-next",
			"--name", rt.workingTaskName,
			"--source", rt.sourcePath,
			"--fixture", relativePath(t, "testdata/integration/fixture.materialize.json"),
			"--shards", strconv.Itoa(shards),
			"--timeout", timeout.String(),
			"--network", "flow-test",
			"--log-json",
		}
		if shards == 1 {
			args = append(args, "--output-apply", "--output-state")
		}
	}

	// The transaction-health assertion reads the JSON task logs of the
	// preview-next (runtime v2) run; the legacy v1 golden path does not carry
	// --log-json and is not health-checked here.
	var actionDescription []byte
	if RuntimeV1() {
		actionDescription = RunFlowctl(t, args...)
	} else {
		var stderr []byte
		actionDescription, stderr = runFlowctl(t, args...)
		AssertTransactionHealth(t, ParseHealthLines(stderr), shards, runtime.Fidelity)
	}
	for _, sanitize := range actionDescSanitizers {
		actionDescription = []byte(sanitize(string(actionDescription)))
	}

	for _, res := range rt.resources {
		snap.WriteString(snapshotTestTable(t, ctx, materializer, res, actionDescription, rt.rndSuffix, true))
	}

	return snap.String()
}

// SanitizeCheckpointHashes returns an actionDescription sanitizer which rewrites
// each distinct value of pattern's first capture group to `<placeholder-N>`,
// numbered by order of first appearance.
//
// Some connectors derive a token by hashing the runtime checkpoint -- see
// materialize-s3-iceberg's binding checkpoints and materialize-snowflake's
// stream offset tokens. The checkpoint advances per transaction and covers the
// randomized per-run task name, so these tokens are unique to both the
// transaction and the run and cannot be snapshotted verbatim. Numbering by first
// appearance keeps what they are there to demonstrate -- each transaction's
// token is distinct, and a "previous" token equals the prior transaction's
// "current" -- without the run-specific bytes.
func SanitizeCheckpointHashes(pattern, placeholder string) func(string) string {
	re := regexp.MustCompile(pattern)

	return func(s string) string {
		var (
			seen = make(map[string]string)
			out  strings.Builder
			last int
		)

		for _, m := range re.FindAllStringSubmatchIndex(s, -1) {
			if len(m) < 4 || m[2] < 0 {
				continue // pattern matched, but its capture group did not
			}

			hash := s[m[2]:m[3]]
			repl, ok := seen[hash]
			if !ok {
				repl = fmt.Sprintf("<%s-%d>", placeholder, len(seen)+1)
				seen[hash] = repl
			}

			out.WriteString(s[last:m[2]])
			out.WriteString(repl)
			last = m[3]
		}
		out.WriteString(s[last:])

		return out.String()
	}
}

// RuntimeV1 reports whether FLOW_TEST_RUNTIME selects the runtime v1 pass.
func RuntimeV1() bool { return os.Getenv("FLOW_TEST_RUNTIME") == "v1" }

// SkipRuntimeV1 skips the runtime v1 pass of a test that never drives legacy
// `flowctl preview`: it drives the connector directly or through preview-next,
// so its outcome is the runtime-next pass's.
func SkipRuntimeV1(t *testing.T) {
	t.Helper()
	if RuntimeV1() {
		t.Skip("does not drive runtime v1; covered by the runtime-next pass")
	}
}

// requireRuntimeV1Selected fails a materialization test that CI's runtime v1
// pass, which runs only `-run '^TestIntegration'`, would never select.
func requireRuntimeV1Selected(t *testing.T) {
	t.Helper()
	if !strings.HasPrefix(t.Name(), "TestIntegration") {
		t.Fatalf("%s drives runtime v1 but CI's runtime v1 pass only runs TestIntegration* tests; rename it", t.Name())
	}
}

// runtimeV1Liveness drives the task through legacy `flowctl preview` -- the
// runtime v1 protocol, under which Acknowledge never carries statePatches -- as
// a single session with no trailing drain session, and requires that every
// binding's resource then holds data.
//
// The fixture writes each collection in two of its four transactions, and a
// session's final Acknowledge never runs, so a connector that applies what it
// acknowledges lands most of the fixture within the session. One that only
// learns its own staged work from the statePatches echo runtime-next sends
// lands nothing until the session rotates -- which a drain session, and every
// runtime-next test, would hide. No snapshot: this asserts liveness, not
// content, so it holds for every connector without a runtime v1 golden.
func runtimeV1Liveness[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	ctx context.Context,
	materializer boilerplate.Materializer[EC, FC, RC, MT],
	rt rewrittenTask[RC],
) {
	t.Helper()

	RunFlowctl(t,
		"preview",
		"--name", rt.workingTaskName,
		"--source", rt.sourcePath,
		"--fixture", relativePath(t, "testdata/integration/fixture.materialize.json"),
		"--sessions=-1", // one argument: clap reads a bare "-1" as a flag
		"--network", "flow-test",
	)

	for _, path := range rt.resourcePaths {
		columns, rows, err := materializer.SnapshotTestResource(ctx, path)
		require.NoError(t, err)
		if columns == nil && rows == nil {
			t.Logf("runtime v1: %v cannot be read back (SnapshotTestResource is not implemented); liveness not checked", path)
			continue
		}
		require.NotEmptyf(t, rows,
			"runtime v1: %v is empty after a single session; the connector applied nothing it acknowledged within the session (does it only learn its own staged work from Acknowledge's statePatches, which runtime v1 never sends?)", path)
	}
}
