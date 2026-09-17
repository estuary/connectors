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
	log "github.com/sirupsen/logrus"
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
	} else if err := rejectOrphanedStreamV2Bindings(req.Materialization, req.StateJson); err != nil {
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

// rejectOrphanedStreamV2Bindings rejects a publication which moves a binding
// off the snowpipe_streaming_v2 write path, other than by the one exit this
// connector supports — see streamV2Checkpoint.validateNotOrphaned for what an unsupported
// departure costs the binding, and streamV2Downgrade for the one it allows.
//
// The transactor rejects the same thing, and has to, because the connector state
// a task runs on is its own. But a task which is rejected is a task an
// operator has to notice, while a publication which is rejected is one they are already
// watching. Apply is the earliest RPC that can tell: it is the first to carry
// both the specification being published and the connector state the task has
// accumulated, which Validate does not.
//
// A state document this connector cannot read reports nothing rather than
// rejecting. The runtime owns that document's shape, and a binding whose write
// path cannot be established from it is one the transactor will establish for
// itself.
func rejectOrphanedStreamV2Bindings(spec *pf.MaterializationSpec, stateJson json.RawMessage) error {
	if spec == nil || len(stateJson) == 0 {
		return nil
	}

	var cfg config
	if err := json.Unmarshal(spec.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	} else if cfg.Credentials == nil {
		// Which the boilerplate's own validation reports, and better.
		return nil
	}

	var cp checkpoint
	if err := json.Unmarshal(stateJson, &cp); err != nil {
		return nil
	}

	for _, binding := range spec.Bindings {
		var item = cp[binding.StateKey]
		if item == nil || cfg.isStreamsV2(binding.DeltaUpdates) {
			continue
		}
		var table = strings.Join(binding.ResourcePath, ".")
		if cfg.isStreamsDowngradeV2ToV1(binding.DeltaUpdates) {
			if channelNames := item.StreamV2.channelNames(); len(channelNames) > 0 {
				log.Warnf(
					"binding %s is leaving the snowpipe_streaming_v2 write path for snowpipe_streaming. Every document that its channel(s) %s committed beyond the offset the checkpoint records for them will be materialized again by the snowpipe_streaming path, and this binding uses delta updates, so those duplicates are permanent. The channels are dropped when the task next opens on the new path",
					table, strings.Join(channelNames, ", "),
				)
			}
			continue
		}
		if err := item.StreamV2.validateNotOrphaned(table); err != nil {
			return err
		}
	}
	return nil
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

	flags := boilerplate.ParseFlags(cfg)
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
