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

// RuntimeConfig configures how a materialization integration test drives the
// runtime, via `flowctl raw preview-next --fixture --shards N`. Multi-shard
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
	// Timeout is a backstop against a hung run; a healthy run ends by
	// transaction count. Zero defaults to ten minutes.
	Timeout time.Duration
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

	// Extra feature flags are appended to whatever the source config already
	// sets (later entries win in ParseFeatureFlags). flowctl passes a
	// plaintext (non-sops) config to the connector unchanged, so writing the
	// decrypted config back inline is sufficient.
	if len(runtime.ExtraFeatureFlags) > 0 {
		rawCfg := decryptConfigRaw(t, bundled, taskName)
		flags := strings.Join(runtime.ExtraFeatureFlags, ",")
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

	// One session runs every fixture transaction. A session halts after its
	// final commit without running the post-commit Acknowledge, so flowctl
	// appends an empty drain session which recovery-applies that last
	// transaction before the destination tables are snapshotted.
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
