package connector

import (
	"fmt"
	"math"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// requireMultiShardCapable refuses a shard of a multi-shard task when any
// binding writes through a path other than snowpipe_streaming_v2, because only
// that path keeps its state per shard. A shard is one of several when its range
// spans less than the full key or r-clock space.
func requireMultiShardCapable(r *pf.RangeSpec, bindings []*binding) error {
	if r.KeyBegin == 0 && r.KeyEnd == math.MaxUint32 && r.RClockBegin == 0 && r.RClockEnd == math.MaxUint32 {
		return nil
	}

	var tables []string
	for _, b := range bindings {
		if !b.streamingV2 {
			tables = append(tables, b.target.Identifier)
		}
	}
	if len(tables) == 0 {
		return nil
	}

	var err = fmt.Errorf("bindings %s do not write through %s, which is the only write path that supports more than one shard", strings.Join(tables, ", "), flagSnowpipeStreamingV2)
	return cerrors.NewUserError(err, fmt.Sprintf(
		"This task runs on more than one shard, but its bindings for tables %s do not write through Snowpipe Streaming v2, which is the only write path that supports more than one shard. Run the task on one shard, or make every binding a delta updates binding with JWT authentication and the %q and %q feature flags enabled.",
		strings.Join(tables, ", "), flagSnowpipeStreaming, flagSnowpipeStreamingV2))
}
