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

func RunValidateAndApplyTestCases(
	t *testing.T,
	driver Connector,
	config any,
	resourceConfig any,
	dumpSchema func(t *testing.T) string,
	cleanup func(t *testing.T),
) {
	ctx := context.Background()
	var snap strings.Builder

	configJson, err := json.Marshal(config)
	require.NoError(t, err)

	resourceConfigJson, err := json.Marshal(resourceConfig)
	require.NoError(t, err)

	t.Run("validate and apply many different types of fields", func(t *testing.T) {
		defer cleanup(t)

		fixture := loadSpec(t, "big-schema.flow.proto")

		// Initial validation with no previously existing table.
		validateRes, err := driver.Validate(ctx, ValidateReq(fixture, nil, configJson, resourceConfigJson))
		require.NoError(t, err)

		snap.WriteString("Big Schema Initial Constraints:\n")
		snap.WriteString(SnapshotConstraints(t, validateRes.Bindings[0].Constraints))

		// Initial apply with no previously existing table.
		_, err = driver.Apply(ctx, ApplyReq(fixture, nil, configJson, resourceConfigJson, validateRes, true))
		require.NoError(t, err)

		sch := dumpSchema(t)

		// Validate again.
		validateRes, err = driver.Validate(ctx, ValidateReq(fixture, fixture, configJson, resourceConfigJson))
		require.NoError(t, err)

		snap.WriteString("\nBig Schema Re-validated Constraints:\n")
		snap.WriteString(SnapshotConstraints(t, validateRes.Bindings[0].Constraints))

		// Apply again - this should be a no-op.
		_, err = driver.Apply(ctx, ApplyReq(fixture, fixture, configJson, resourceConfigJson, validateRes, true))
		require.NoError(t, err)
		require.Equal(t, sch, dumpSchema(t))

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
		sch = dumpSchema(t)
		_, err = driver.Apply(ctx, ApplyReq(nullable, nullable, configJson, resourceConfigJson, validateRes, true))
		require.NoError(t, err)
		require.Equal(t, sch, dumpSchema(t))

		snap.WriteString("\nBig Schema Materialized Resource Schema With No Fields Required:\n")
		snap.WriteString(sch)

		// Apply the spec with the randomly changed types, but this time with a backfill that will
		// cause the table to be replaced.
		changed.Bindings[0].Backfill = 1
		validateRes, err = driver.Validate(ctx, ValidateReq(changed, nullable, configJson, resourceConfigJson))
		require.NoError(t, err)

		snap.WriteString("\nBig Schema Changed Types With Table Replacement Constraints:\n")
		snap.WriteString(SnapshotConstraints(t, validateRes.Bindings[0].Constraints))

		_, err = driver.Apply(ctx, ApplyReq(changed, nullable, configJson, resourceConfigJson, validateRes, true))
		require.NoError(t, err)
		snap.WriteString("\nBig Schema Materialized Resource Schema Changed Types With Table Replacement:\n")
		snap.WriteString(dumpSchema(t) + "\n")
	})

	t.Run("add and remove fields", func(t *testing.T) {
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

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				defer cleanup(t)

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
				snap.WriteString(string(dumpSchema(t)) + "\n")
			})
		}
	})

	t.Run("validate and apply fields with challenging names", func(t *testing.T) {
		defer cleanup(t)

		fixture := loadSpec(t, "challenging-fields.flow.proto")

		// Validate and apply twice to make sure that a re-application does not attempt to re-create
		// any columns. This makes sure we are able to read back the schema we have created
		// correctly.
		for idx := 0; idx < 2; idx++ {
			validateRes, err := driver.Validate(ctx, ValidateReq(fixture, nil, configJson, resourceConfigJson))
			require.NoError(t, err)
			_, err = driver.Apply(ctx, ApplyReq(fixture, nil, configJson, resourceConfigJson, validateRes, false))
			require.NoError(t, err)
		}

		snap.WriteString("Challenging Field Names Materialized Columns:\n")
		snap.WriteString(dumpSchema(t))
	})

	cupaloy.SnapshotT(t, snap.String())
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
	spec.Bindings[0].FieldSelection = SelectedFields(validateRes.Bindings[0], spec.Bindings[0].Collection, includeOptional)

	req := &pm.Request_Apply{
		Materialization:     spec,
		Version:             "someVersion",
		LastMaterialization: lastSpec,
	}

	return req
}

// selectedFields creates a field selection that includes all possible fields.
func SelectedFields(binding *pm.Response_Validated_Binding, collection pf.CollectionSpec, includeOptional bool) pf.FieldSelection {
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

type testTask[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper] struct {
	materializer Materializer[EC, FC, RC, MT]
	bindings     []testBinding
	suffix       string
}

type testBinding struct {
	source string
	path   []string
}

type taskResult struct {
	name           string
	flowctlOutput  string
	bindingResults []bindingResult
}

type bindingResult struct {
	source string
	path   []string
	data   string
}

type parsedSpec struct {
	Materializations map[string]struct {
		Endpoint struct {
			Local struct {
				Config json.RawMessage
			}
		}
		Bindings []struct {
			Resource json.RawMessage
			Source   string
		}
	}
}

const testTableIdentifer = "_flow_test_"

func RunIntegrationTest[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	newMaterializer NewMaterializerFn[EC, FC, RC, MT],
	source string,
	finalResourchPathKey string,
) {
	ctx := context.Background()

	// Parse the bundled spec to extract the tasks and their bindings.
	var parsed parsedSpec
	bundled, err := exec.Command("flowctl", "raw", "bundle", "--source", source).CombinedOutput()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(bundled, &parsed))

	// Extract each task's config and bindings.
	ts := time.Now().Unix()
	tasks := make(map[string]testTask[EC, FC, RC, MT])
	for taskName, spec := range parsed.Materializations {
		var cfg EC
		require.NoError(t, unmarshalStrict(decryptConfig(t, spec.Endpoint.Local.Config), &cfg))
		materializer, err := newMaterializer(ctx, taskName, cfg, parseFlags(cfg))
		require.NoError(t, err)

		// A suffix is added to the resource final path component. It includes a
		// random part to ensure that concurrent runs of the test don't
		// interfere with each other, as well as a timestamp to facilitate
		// cleaning up leftover resources that weren't cleanup by a prior run
		// for some reason.
		suffix := testTableIdentifer + fmt.Sprintf("%s_%d", uuid.NewString()[:8], ts)

		var bindings []testBinding
		for bIdx, binding := range spec.Bindings {
			var resourceCfg RC
			require.NoError(t, unmarshalStrict(binding.Resource, &resourceCfg))
			path, _, err := resourceCfg.WithDefaults(cfg).Parameters()
			require.NoError(t, err)

			// Swap out the reported final path component from the spec with one
			// that has the suffix. This also needs to be added to the spec file
			// read by `flowctl preview`, which will be written out from the
			// working bundled spec.
			path[len(path)-1] = path[len(path)-1] + suffix
			bundled, err = sjson.SetBytes(
				bundled,
				fmt.Sprintf("materializations.%s.bindings.%d.resource.%s", taskName, bIdx, finalResourchPathKey),
				path[len(path)-1],
			)

			bindings = append(bindings, testBinding{
				source: binding.Source,
				path:   path,
			})
		}

		tasks[taskName] = testTask[EC, FC, RC, MT]{
			materializer: materializer,
			bindings:     bindings,
			suffix:       suffix,
		}
	}

	t.Cleanup(func() { cleanupTasks(t, tasks) })

	// Write out the modified bundled spec to a temp file for `flowctl preview`.
	// This bundle now has the resource suffixes added to it. `flowctl preview`
	// will drive the materialization using those suffixed paths.
	dir := t.TempDir()
	suffixedSource := filepath.Join(dir, "test.flow.yaml")
	require.NoError(t, os.WriteFile(suffixedSource, bundled, 0o600))

	// Drive the materialization and collect the results.
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	var results []taskResult
	for name, task := range tasks {
		group.Go(func() error {
			actionDescription := driveTask(t, ctx, suffixedSource, name)
			res := taskResult{name: name, flowctlOutput: actionDescription}

			for _, binding := range task.bindings {
				columnNames, rows, err := task.materializer.SnapshotTestResource(groupCtx, binding.path)
				require.NoError(t, err)

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

				res.bindingResults = append(res.bindingResults, bindingResult{
					source: binding.source,
					path:   binding.path,
					data:   data.String(),
				})
			}

			mu.Lock()
			results = append(results, res)
			mu.Unlock()

			return nil
		})
	}

	require.NoError(t, group.Wait())

	slices.SortFunc(results, func(a, b taskResult) int {
		return strings.Compare(a.name, b.name)
	})

	var snap strings.Builder
	for _, res := range results {
		snap.WriteString(fmt.Sprintf("Task: %s\n\n", res.name))
		snap.WriteString(res.flowctlOutput)
		snap.WriteString("\n")

		for _, binding := range res.bindingResults {
			snap.WriteString(fmt.Sprintf("Resource: %s\n", strings.Join(binding.path, ".")))
			snap.WriteString(fmt.Sprintf("Source: %s\n", binding.source))
			snap.WriteString(binding.data)
			snap.WriteString("\n")
		}
	}

	// Strip the generated suffixes from the snapshot output.
	output := snap.String()
	for _, task := range tasks {
		output = strings.ReplaceAll(output, task.suffix, "")
	}

	cupaloy.SnapshotT(t, output)
}

func cleanupTasks[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	t *testing.T,
	tasks map[string]testTask[EC, FC, RC, MT],
) {
	t.Helper()

	ctx := context.Background()
	now := time.Now()
	var group errgroup.Group

	for taskName, task := range tasks {
		// Cleanup actions for the task itself.
		group.Go(func() error {
			if err := task.materializer.CleanupTestTask(ctx, taskName); err != nil {
				t.Log("failed to clean up task", taskName)
			}

			t.Log("cleaned up task", taskName)
			return nil
		})

		// Cleanup actions for the task's resources.
		group.Go(func() error {
			var paths [][]string
			for _, b := range task.bindings {
				paths = append(paths, b.path)
			}

			var toCleanup [][]string

			is := initInfoSchema(task.materializer.Config())
			if err := task.materializer.PopulateInfoSchema(ctx, is, paths, true); err != nil {
				// Couldn't list existing resources for some reason. At least
				// try to clean up the tables that are part of this task.
				toCleanup = paths
				t.Log("failed to populate info schema for cleanup", err)
			} else {
				for _, r := range is.resources {
					last := r.location[len(r.location)-1]
					if i := strings.LastIndex(last, testTableIdentifer); i == -1 {
						// Not a test table.
					} else if s := last[i:]; s == task.suffix {
						// This resource was created by this test run.
						toCleanup = append(toCleanup, r.location)
					} else if parts := strings.Split(s, "_"); len(parts) != 2 {
						// Not a test table.
					} else if seconds, err := strconv.Atoi(parts[1]); err != nil {
						// Not a test table.
					} else if timestamp := time.Unix(int64(seconds), 0); now.Sub(timestamp) > 6*time.Hour {
						t.Log("will cleanup old test table", r.location)
						toCleanup = append(toCleanup, r.location)
					}
				}
			}

			for _, path := range toCleanup {
				group.Go(func() error {
					if _, fn, err := task.materializer.DeleteResource(ctx, path); err != nil {
						return err
					} else if err := fn(ctx); err != nil {
						return err
					}

					t.Log("cleaned up resource", path)
					return nil
				})
			}

			return nil
		})
	}

	require.NoError(t, group.Wait())
}

func decryptConfig(t *testing.T, cfg json.RawMessage) json.RawMessage {
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

func driveTask(t *testing.T, ctx context.Context, source, name string) string {
	t.Helper()

	cmd := exec.Command(
		"flowctl",
		"preview",
		"--name", name,
		"--source", source,
		"--fixture", "../materialize-boilerplate/testdata/integration/fixture.json",
		"--network", "flow-test",
		"--output-state",
		"--output-apply",
	)
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
	require.NoError(t, cmd.Wait())
	wg.Wait()

	return stdoutBuf.String()
}
