// Command materialize-testctl drives a materialization connector's test
// capabilities from outside the connector: it snapshots what a materialized
// resource holds, drops a resource, or sweeps away the resources and task
// metadata that earlier test runs left behind.
//
// It exists for flow's materialize-consistency suite, which runs a
// materialization against a real Flow runtime and then has to check what
// actually landed. Verification that asks the subject under test to report on
// itself proves very little, and the materialization protocol offers no way to
// read a destination back — Load answers only the keys the runtime asks about,
// and only for bindings that are not delta-updates. Nor does the protocol drop a
// resource, and it should not: removing a binding leaves its table in place,
// because destroying a user's data as a side effect of a catalog edit would be
// indefensible. A harness has the opposite problem, since it names resources
// per-run and otherwise accumulates them forever in a shared warehouse.
//
// The capabilities themselves are the ones the connector's own integration tests
// use, reached through the same helpers. This program only supplies the
// out-of-process entry point, so the connector's production interface grows
// nothing for the harness's benefit.
//
// Usage:
//
//	go run ./tests/materialize/testctl -connector materialize-databricks \
//	  -config config.yaml -resource resource.yaml -mode snapshot
//
// Snapshot output is one JSON object per line, keyed by column name.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	databricks "github.com/estuary/connectors/materialize-databricks"
	log "github.com/sirupsen/logrus"
)

// materializer is the type-erased view of boilerplate.Materializer that this
// program works with. The connector's endpoint and resource configuration types
// are its own, and unexported; they are resolved inside the opener below and
// never named here.
type materializer interface {
	testutil.ResourceSnapshotter
	testutil.ResourceSweeper
	testutil.TestTaskCleaner

	Close(context.Context)
}

// opener parses an endpoint and resource configuration and connects to the
// endpoint, returning the resource's path along with the materializer.
type opener func(ctx context.Context, taskName string, endpointConfig, resourceConfig json.RawMessage) (materializer, []string, error)

// connectors are the materialization connectors this program can drive. A
// connector can be added here once its package is importable, which means
// `package connector` with func main under cmd/connector.
var connectors = map[string]opener{
	"materialize-databricks": func(ctx context.Context, taskName string, endpointConfig, resourceConfig json.RawMessage) (materializer, []string, error) {
		return open(ctx, databricks.NewDriver().NewMaterializer, taskName, endpointConfig, resourceConfig)
	},
}

// open adapts testutil.OpenResource, whose result is parameterized by the
// connector's configuration types, to the type-erased view above. The type
// parameters are inferred from the connector's entry point.
func open[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	ctx context.Context,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	taskName string,
	endpointConfig, resourceConfig json.RawMessage,
) (materializer, []string, error) {
	return testutil.OpenResource(ctx, newMaterializer, taskName, endpointConfig, resourceConfig)
}

func main() {
	var (
		connector = flag.String("connector", "", "connector to drive, e.g. materialize-databricks")
		config    = flag.String("config", "", "path to the endpoint configuration, as JSON or YAML")
		resource  = flag.String("resource", "", "path to the resource configuration, as JSON or YAML")
		mode      = flag.String("mode", "", "snapshot the resource's rows, drop the resource, or sweep away what earlier test runs left behind")
		taskName  = flag.String("task", "materialize-testctl", "materialization name to connect as, which only labels the connection")
		logLevel  = flag.String("log-level", "info", "logging verbosity; debug reports the items a sweep left in place")
	)
	flag.Parse()

	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.WithFields(log.Fields{"level": *logLevel, "error": err}).Fatal("unrecognized log level")
	}
	log.SetLevel(lvl)
	// Logging goes to stderr, leaving stdout to carry snapshot output alone.
	log.SetOutput(os.Stderr)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	if err := run(ctx, *connector, *config, *resource, *mode, *taskName); err != nil {
		log.WithFields(log.Fields{"mode": *mode, "error": err}).Fatal("materialize-testctl failed")
	}
}

func run(ctx context.Context, connector, configPath, resourcePath, mode, taskName string) error {
	openConnector, ok := connectors[connector]
	if !ok {
		return fmt.Errorf("unknown connector %q: expected one of %s", connector, strings.Join(slices.Sorted(maps.Keys(connectors)), ", "))
	} else if configPath == "" || resourcePath == "" {
		return errors.New("-config and -resource are both required")
	} else if !slices.Contains([]string{"snapshot", "drop", "sweep"}, mode) {
		return fmt.Errorf("unknown -mode %q: expected snapshot, drop, or sweep", mode)
	}

	endpointConfig, err := testutil.ReadConfigFile(configPath)
	if err != nil {
		return err
	}
	resourceConfig, err := testutil.ReadConfigFile(resourcePath)
	if err != nil {
		return err
	}

	m, path, err := openConnector(ctx, taskName, endpointConfig, resourceConfig)
	if err != nil {
		return err
	}
	defer m.Close(ctx)

	switch mode {
	case "snapshot":
		rows, err := testutil.SnapshotResource(ctx, m, path)
		if err != nil {
			return err
		}
		_, err = os.Stdout.WriteString(rows)
		return err
	case "drop":
		description, err := testutil.DropResource(ctx, m, path)
		if err != nil {
			return err
		}
		log.WithField("action", description).Info("dropped the resource")
		return nil
	default:
		return sweep(ctx, m, path)
	}
}

// sweep removes the test resources in the namespace of the given resource path,
// along with the task metadata of test materializations, leaving in place
// anything recent enough that a run still in flight could be using it.
//
// The resource path only locates the namespace to sweep: what goes is decided by
// what is actually present, which is the point of sweeping rather than dropping
// by name. An empty run suffix says this program has no resources of its own to
// clean up, so only leftovers are considered.
func sweep(ctx context.Context, m materializer, path []string) error {
	resources, resourcesErr := testutil.SweepTestResources(ctx, m, [][]string{path}, "")
	tasks, tasksErr := testutil.SweepTestTasks(ctx, m, "")

	var errs = []error{resourcesErr, tasksErr}
	for _, swept := range []struct {
		kind     string
		outcomes []testutil.SweepOutcome
	}{{"resource", resources}, {"task", tasks}} {
		var kind = swept.kind
		for _, o := range swept.outcomes {
			entry := log.WithFields(log.Fields{kind: o.Item, "reason": o.Reason})
			if o.Err != nil {
				errs = append(errs, fmt.Errorf("sweeping %s %s: %w", kind, o.Item, o.Err))
			} else if o.Swept {
				entry.Info("swept")
			} else {
				entry.Debug("left in place")
			}
		}
	}

	return errors.Join(errs...)
}
