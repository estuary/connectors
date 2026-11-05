package boilerplate

import (
	"fmt"
	"math"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
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

// RequireSingleShard returns an error when the opened shard covers less than
// the full key range, which means the task has been split into multiple
// shards. It exists for connectors whose transactors are correct only when a
// single shard writes each binding.
func RequireSingleShard(open pm.Request_Open) error {
	if r := open.Range; r != nil && (r.KeyBegin != 0 || r.KeyEnd != math.MaxUint32) {
		var err = fmt.Errorf("shard covers key range [%08x, %08x] of a task split into multiple shards", r.KeyBegin, r.KeyEnd)
		return cerrors.NewUserError(err, "This connector does not support tasks split into multiple shards. Merge the task's shards back into one.")
	}
	return nil
}
