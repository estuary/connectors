package main

import (
	"context"
	"encoding/json"
	"fmt"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// gatedDriver refuses the RPCs that carry a materialization spec when the spec
// asks for a write path the task's runtime cannot support, then delegates to the
// wrapped driver.
//
// The gate lives here rather than in a Materializer method because the runtime
// is a property of the task, and Apply and Open are the only RPCs that carry the
// task's spec.
type gatedDriver struct {
	*sql.Driver[config, tableConfig]
}

var _ boilerplate.Connector = gatedDriver{}

// Apply refuses at publication time, which is where an operator sees the error
// while they are still making the change.
func (d gatedDriver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := requireStreamingV2Runtime(req.Materialization); err != nil {
		return nil, err
	}

	return d.Driver.Apply(ctx, req)
}

// NewTransactor refuses at startup, covering the task whose runtime flag is
// turned off without its endpoint configuration being touched. Such a task must
// fail rather than fall back to a write path whose recovery assumptions no
// longer hold.
func (d gatedDriver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	if err := requireStreamingV2Runtime(req.Materialization); err != nil {
		return nil, nil, nil, err
	}

	return d.Driver.NewTransactor(ctx, req, be)
}

// requireStreamingV2Runtime reports whether the given task spec may run the
// snowpipe_streaming_v2 write path. That path appends rows to a channel as it
// stores them and resumes from Snowflake's committed offset token, which is only
// correct if an interrupted transaction is replayed identically — a guarantee
// the v2 runtime makes and the v1 runtime does not.
func requireStreamingV2Runtime(spec *pf.MaterializationSpec) error {
	// The gate reads the spec before anything else validates it, so an absent one
	// is reported here rather than dereferenced.
	if spec == nil {
		return fmt.Errorf("request carries no materialization spec")
	}

	// The full configuration is validated by the boilerplate, which reports a
	// better error than this gate could. Only the feature flags are read here.
	var cfg config
	if err := json.Unmarshal(spec.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	// A configuration asking for both write paths is reported as such even when
	// the runtime is also wrong, so that the operator is told about the mistake
	// they made rather than the one it implies.
	if err := cfg.validateStreamingFlags(); err != nil {
		return err
	}

	if !boilerplate.ParseFlags(cfg)[flagSnowpipeStreamingV2] {
		return nil
	} else if boilerplate.IsMaterializationSpecRuntimeV2(spec) {
		return nil
	}

	return fmt.Errorf(
		"the %q feature flag requires the v2 materialization runtime, which this task is not running: add %q to the task's shards.flags, or remove %q from the endpoint configuration's feature_flags",
		flagSnowpipeStreamingV2, boilerplate.RuntimeV2FlagName, flagSnowpipeStreamingV2,
	)
}
