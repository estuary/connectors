package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// runtimePrereqDriver wraps the connector's driver so that Apply and Open are
// refused when the spec they carry needs the v2 materialization runtime and its
// shard template does not select it. Those two requests are the only ones whose
// spec carries the shard template, so the check can run nowhere earlier.
type runtimePrereqDriver struct {
	*sql.Driver[config, tableConfig]
}

var _ boilerplate.Connector = runtimePrereqDriver{}

func NewRuntimePrereqDriver() boilerplate.Connector {
	return runtimePrereqDriver{NewDriver()}
}

func (d runtimePrereqDriver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return d.Driver.Validate(ctx, req)
}

func (d runtimePrereqDriver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := requireStreamingV2Runtime(req.Materialization, req.StateJson); err != nil {
		return nil, err
	}

	return d.Driver.Apply(ctx, req)
}

func (d runtimePrereqDriver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	if err := requireStreamingV2Runtime(req.Materialization, req.StateJson); err != nil {
		return nil, nil, nil, err
	}

	return d.Driver.NewTransactor(ctx, req, be)
}

// requireStreamingV2Runtime rejects a task spec that needs the v2 materialization
// runtime but is not running it. The snowpipe_streaming_v2 write path needs that
// runtime, so a spec selecting the path is rejected.
func requireStreamingV2Runtime(spec *pf.MaterializationSpec, stateJson json.RawMessage) error {
	if spec == nil {
		return fmt.Errorf("request carries no materialization spec")
	}
	if boilerplate.IsMaterializationSpecRuntimeV2(spec) {
		return nil
	} // else runtime is v1

	var cfg config
	if err := json.Unmarshal(spec.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	flags, err := boilerplate.ResolveFlags(cfg, spec)
	if err != nil {
		return err
	}
	if flags[flagSnowpipeStreaming] && flags[flagSnowpipeStreamingV2] {
		return fmt.Errorf(
			"the %q feature flag requires the v2 materialization runtime, which this task is not running: add %q to the task's shards.flags, or remove %q from the endpoint configuration's feature_flags",
			flagSnowpipeStreamingV2, boilerplate.RuntimeV2FlagName, flagSnowpipeStreamingV2,
		)
	}

	if len(stateJson) == 0 {
		return nil
	}
	var cp checkpoint
	if err := json.Unmarshal(stateJson, &cp); err != nil {
		return nil
	}

	for _, binding := range spec.Bindings {
		var item = cp[binding.StateKey]
		if item == nil {
			continue
		}
		var channels = item.StreamV2.channelNames()
		if len(channels) == 0 {
			continue
		}
		return fmt.Errorf(
			"the task's checkpoint still records the snowpipe_streaming_v2 channel(s) %s for %s, and this task is not running the v2 materialization runtime: dropping them requires that runtime, so add %q to the task's shards.flags, or restore %q to the endpoint configuration's feature_flags",
			strings.Join(channels, ", "), strings.Join(binding.ResourcePath, "."), boilerplate.RuntimeV2FlagName, flagSnowpipeStreamingV2,
		)
	}
	return nil
}
