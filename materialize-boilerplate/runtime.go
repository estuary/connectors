package boilerplate

import (
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// RuntimeV2FlagName is the name of the task shard flag that puts a task on the
// v2 runtime.
const RuntimeV2FlagName = "enable-runtime-v2"

const runtimeV2Flag = labels.FlagPrefix + RuntimeV2FlagName

// IsMaterializationSpecRuntimeV2 reports whether the task's shard template
// carries the runtime-v2 feature flag, meaning the connector is being driven by
// the v2 runtime.
func IsMaterializationSpecRuntimeV2(spec *pf.MaterializationSpec) bool {
	return spec != nil &&
		spec.ShardTemplate != nil &&
		spec.ShardTemplate.LabelSet.ValueOf(runtimeV2Flag) == "true"
}
