package boilerplate

import (
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// RuntimeV2FlagName is the name of the task shard flag that puts a task on the
// v2 runtime. Connectors that refuse to run outside the v2 runtime name it in
// their error messages, since setting it is the operator's remedy.
const RuntimeV2FlagName = "enable-runtime-v2"

const runtimeV2Flag = labels.FlagPrefix + RuntimeV2FlagName

// IsMaterializationSpecRuntimeV2 reports whether the task's shard template
// carries the runtime-v2 feature flag, meaning the connector is being driven by
// the v2 runtime. Pass either pm.Request_Open.Materialization or
// pm.Request_Apply.Materialization, which are the specs the runtime hands to a
// connector.
//
// The template is authoritative for the individual shard the connector is
// running under: each shard spec is a clone of the template with only runtime
// labels (key range, r-clock range, split source/target, cordon) layered on, so
// a shard's feature flags are always exactly the template's. The reactor
// selects its v1 or v2 code path from this same label.
func IsMaterializationSpecRuntimeV2(spec *pf.MaterializationSpec) bool {
	return spec != nil &&
		spec.ShardTemplate != nil &&
		spec.ShardTemplate.LabelSet.ValueOf(runtimeV2Flag) == "true"
}
