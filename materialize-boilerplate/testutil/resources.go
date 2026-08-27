package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"gopkg.in/yaml.v3"
)

// The helpers in this file work without a *testing.T, so that a program outside
// the connector can use them: flow's materialize-consistency suite runs a
// materialization against a real runtime, then needs to read the destination
// back to check what landed and to remove the resources it created. Doing that
// through the same functions the integration tests use means there is only one
// account of what a resource holds and one way to drop it.

// ResourceSnapshotter is the part of boilerplate.Materializer that reads a
// materialized resource back. It is a narrow, non-generic view of that
// interface, so callers which only want the data need not name the connector's
// type parameters.
type ResourceSnapshotter interface {
	SnapshotTestResource(ctx context.Context, path []string) (columnNames []string, rows [][]any, _ error)
}

// ResourceDeleter is the part of boilerplate.Materializer that removes a
// materialized resource.
type ResourceDeleter interface {
	DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error)
}

// TestTaskCleaner is the part of boilerplate.Materializer that enumerates and
// removes the task-level metadata a materialization leaves behind, which for a
// SQL connector is its entries in the checkpoints table.
type TestTaskCleaner interface {
	ListTestTasks(ctx context.Context) ([]string, error)
	CleanupTestTask(ctx context.Context, taskName string) error
}

// ResourceSweeper is the part of boilerplate.Materializer that enumerates the
// resources present in the endpoint, in addition to removing them.
type ResourceSweeper interface {
	ResourceDeleter

	Config() boilerplate.MaterializeCfg
	PopulateInfoSchema(context.Context, *boilerplate.InfoSchema, [][]string) error
}

// SnapshotResource renders every row of a materialized resource as
// newline-delimited JSON, one object per row keyed by column name.
//
// Rows are objects of columns rather than the collection documents that produced
// them, which is the honest shape: a materialized resource is columns, and a
// standard binding need not carry a root document at all. The order is whatever
// the destination returns them in, so a caller that needs a stable ordering must
// sort.
func SnapshotResource(ctx context.Context, m ResourceSnapshotter, path []string) (string, error) {
	columnNames, rows, err := m.SnapshotTestResource(ctx, path)
	if err != nil {
		return "", fmt.Errorf("snapshotting %s: %w", strings.Join(path, "."), err)
	}

	return RenderRows(columnNames, rows)
}

// RenderRows encodes rows as newline-delimited JSON objects keyed by column
// name, being the representation of materialized data that both the connector
// snapshots and an external harness compare against.
func RenderRows(columnNames []string, rows [][]any) (string, error) {
	var out strings.Builder
	enc := json.NewEncoder(&out)
	for _, r := range rows {
		doc := make(map[string]any, len(columnNames))
		for i, col := range columnNames {
			doc[col] = r[i]
		}
		if err := enc.Encode(doc); err != nil {
			return "", fmt.Errorf("encoding row: %w", err)
		}
	}

	return out.String(), nil
}

// DropResource removes a materialized resource, running the action that
// DeleteResource returns and reporting the connector's description of what it
// did.
//
// Nothing in the materialization protocol drops a resource, and nothing should:
// removing a binding leaves its table in place, since destroying a user's data
// as a side effect of a catalog edit would be indefensible, and DeleteResource
// is reached only to replace a resource whose backfill counter incremented. A
// test harness has the opposite problem — it creates uniquely named resources on
// every run, and without a way to remove them accumulates them forever in a
// warehouse someone else is paying for.
func DropResource(ctx context.Context, m ResourceDeleter, path []string) (string, error) {
	desc, action, err := m.DeleteResource(ctx, path)
	if err != nil {
		return "", fmt.Errorf("preparing to delete %s: %w", strings.Join(path, "."), err)
	} else if action != nil {
		if err := action(ctx); err != nil {
			return "", fmt.Errorf("deleting %s: %w", strings.Join(path, "."), err)
		}
	}

	return desc, nil
}

// OpenResource parses an endpoint and resource configuration, opens a
// Materializer for them, and returns the resource's path.
//
// newMaterializer is the connector's exported entry point
// (sql.Driver.NewMaterializer, for a SQL connector), which is what establishes
// the connector's initialization order; the type parameters are inferred from
// it, so a caller never has to name the connector's unexported config types.
// Configurations are parsed exactly as the connector parses them when the
// runtime hands them over, so a configuration this accepts is one the connector
// accepts.
//
// The caller is responsible for Close-ing the returned Materializer.
func OpenResource[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	ctx context.Context,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	taskName string,
	endpointConfig json.RawMessage,
	resourceConfig json.RawMessage,
) (boilerplate.Materializer[EC, FC, RC, MT], []string, error) {
	var cfg EC
	if err := boilerplate.UnmarshalStrict(endpointConfig, &cfg); err != nil {
		return nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var resCfg RC
	if err := boilerplate.UnmarshalStrict(resourceConfig, &resCfg); err != nil {
		return nil, nil, fmt.Errorf("parsing resource config: %w", err)
	}
	path, _, err := resCfg.WithDefaults(cfg).Parameters()
	if err != nil {
		return nil, nil, fmt.Errorf("getting resource parameters: %w", err)
	}

	// These helpers read a destination back rather than run a task, so there is
	// no runtime spec to take a creation date from and date-gated flags resolve
	// as they do for a brand-new task.
	flags, err := boilerplate.ResolveFlags(cfg, &pf.MaterializationSpec{})
	if err != nil {
		return nil, nil, err
	}

	m, err := newMaterializer(ctx, taskName, cfg, flags)
	if err != nil {
		return nil, nil, fmt.Errorf("creating materializer: %w", err)
	}

	return m, path, nil
}

// SweepOutcome is what a sweep did, or declined to do, with one test resource or
// task.
type SweepOutcome struct {
	Item string
	// Selected is whether the item was matched for removal, and Reason says why
	// it was or was not. A dry run selects without removing, so Selected is set
	// where Swept is not.
	Selected bool
	Swept    bool
	Reason   string
	// Err is set if the item was selected for removal but removing it failed.
	// A sweep continues past a failure, so that one unremovable item does not
	// strand everything after it.
	Err error
}

// SweepTestResources removes the resources left behind by integration tests in
// the namespaces of the given paths: those whose names carry the test identifier
// and whose embedded timestamp is old enough that no in-flight run could still
// be using them.
//
// Enumerating is the point. Test resources are named per-run, so a caller
// dropping them by name can only remove the ones it knows it created, and the
// leftovers of runs that crashed or were cancelled stay forever. The info schema
// lists everything actually present, which is where those become visible.
//
// tsSuffix identifies the caller's own run, whose resources are removed
// regardless of age; pass an empty string when the caller has no run of its own
// and only leftovers should go. A dry run reports what it would remove without
// removing anything, which is worth offering to whoever points this at a
// warehouse they share with other people.
//
// The returned error reports a failure to enumerate; per-item failures are
// carried in the outcomes.
func SweepTestResources(
	ctx context.Context,
	m ResourceSweeper,
	paths [][]string,
	tsSuffix string,
	dryRun bool,
) ([]SweepOutcome, error) {
	is := boilerplate.InitInfoSchema(m.Config())
	if err := m.PopulateInfoSchema(ctx, is, paths); err != nil {
		return nil, fmt.Errorf("populating info schema: %w", err)
	}

	now := time.Now()
	var outcomes []SweepOutcome
	for _, r := range is.Resources() {
		location := r.Location()
		selected, reason := shouldCleanup(now, location[len(location)-1], tsSuffix)
		outcome := SweepOutcome{Item: strings.Join(location, "."), Selected: selected, Reason: reason}
		if selected && !dryRun {
			if _, err := DropResource(ctx, m, location); err != nil {
				outcome.Err = err
			} else {
				outcome.Swept = true
			}
		}
		outcomes = append(outcomes, outcome)
	}

	return outcomes, nil
}

// SweepTestTasks removes the task metadata left behind by integration tests,
// selecting tasks on the same basis as SweepTestResources selects resources,
// tsSuffix and dryRun included.
func SweepTestTasks(ctx context.Context, m TestTaskCleaner, tsSuffix string, dryRun bool) ([]SweepOutcome, error) {
	tasks, err := m.ListTestTasks(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing test tasks: %w", err)
	}

	now := time.Now()
	var outcomes []SweepOutcome
	for _, task := range tasks {
		selected, reason := shouldCleanup(now, task, tsSuffix)
		outcome := SweepOutcome{Item: task, Selected: selected, Reason: reason}
		if selected && !dryRun {
			if err := m.CleanupTestTask(ctx, task); err != nil {
				outcome.Err = err
			} else {
				outcome.Swept = true
			}
		}
		outcomes = append(outcomes, outcome)
	}

	return outcomes, nil
}

// ReadConfigFile reads an endpoint or resource configuration from a file as
// JSON.
//
// A path rather than an inline value, because an endpoint configuration carries
// credentials and an argument vector is readable by anyone who can list
// processes. Parsed as YAML, which is how these configurations are written in
// this repository, and which subsumes JSON.
func ReadConfigFile(path string) (json.RawMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	if encrypted, err := isSopsEncrypted(raw); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	} else if encrypted {
		out, err := exec.Command("sops", "--decrypt", "--output-type", "json", path).Output()
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				return nil, fmt.Errorf("decrypting %s with sops: %s", path, strings.TrimSpace(string(exitErr.Stderr)))
			}
			return nil, fmt.Errorf("decrypting %s with sops (is it installed?): %w", path, err)
		}
		return json.RawMessage(out), nil
	}

	var intermediate any
	if err := yaml.Unmarshal(raw, &intermediate); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	out, err := json.Marshal(intermediate)
	if err != nil {
		return nil, fmt.Errorf("converting %s to JSON: %w", path, err)
	}

	return out, nil
}

func isSopsEncrypted(raw []byte) (bool, error) {
	var doc struct {
		Sops any `yaml:"sops"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return false, err
	}
	return doc.Sops != nil, nil
}
