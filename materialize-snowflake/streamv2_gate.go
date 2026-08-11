package connector

import (
	"context"
	"encoding/json"
	"fmt"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// gatedDriver refuses the RPCs that carry a materialization spec when the spec
// asks for a write path the task's runtime cannot support, then delegates to the
// wrapped driver. Validate, which carries no spec of the task being published,
// warns instead.
//
// The gate lives here rather than in a Materializer method because the runtime
// is a property of the task, and Apply and Open are the only RPCs that carry the
// task's spec.
type gatedDriver struct {
	*sql.Driver[config, tableConfig]
}

var _ boilerplate.Connector = gatedDriver{}

// NewConnector assembles the connector as it runs: the driver NewDriver builds,
// behind the runtime gate.
//
// The gate is applied here rather than in cmd/connector so that it cannot be lost
// by a caller which builds the connector for itself, and NewDriver goes on
// returning the driver rather than this wrapper because the test helpers of
// materialize-sql take the concrete driver.
func NewConnector() boilerplate.Connector {
	return gatedDriver{NewDriver()}
}

// Validate warns while the operator is still making the change, which is the most
// a publication can do about the runtime: see missingRuntimeV2Warning for what the
// request does and does not carry. The warning is logged rather than returned
// because it cannot be told apart from a publish which is correct.
func (d gatedDriver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	if warning := missingRuntimeV2Warning(req.ConfigJson, req.LastMaterialization); warning != "" {
		log.Warn(warning)
	}

	return d.Driver.Validate(ctx, req)
}

// Apply refuses ahead of the first session of a new specification, so a task
// whose runtime cannot support the configured write path fails before anything in
// Snowflake is created or altered for it.
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

// missingRuntimeV2Warning reports what to tell an operator who is publishing the
// snowpipe_streaming_v2 write path onto a task which has not been running the v2
// runtime, and "" where there is nothing to tell them.
//
// Publication invokes Validate, whose request carries the endpoint configuration
// and the last published spec but no shard template of its own: the runtime the
// task is *about* to run under is not in it, and neither is anything else which
// implies it. So the only shard template a publish can be read against is the
// last one, which says what the task has been running rather than what it will
// run — enough to recognise a task being moved onto this write path, and not
// enough to refuse one. A publish which adds the runtime flag and the feature
// flag together is indistinguishable here from one which forgets the runtime
// flag, and refusing would break the first.
func missingRuntimeV2Warning(configJson json.RawMessage, last *pf.MaterializationSpec) string {
	var cfg config
	if err := json.Unmarshal(configJson, &cfg); err != nil {
		return ""
	}

	if !boilerplate.ParseFlags(cfg)[flagSnowpipeStreamingV2] {
		return ""
	} else if last == nil || boilerplate.IsMaterializationSpecRuntimeV2(last) {
		return ""
	}

	return fmt.Sprintf(
		"this task's last published specification did not run the v2 materialization runtime, which the %q feature flag requires: unless this publication also adds %q to the task's shards.flags, the task will refuse to start",
		flagSnowpipeStreamingV2, boilerplate.RuntimeV2FlagName,
	)
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
