package connector

import (
	"fmt"
	"math"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// requireMultiShardCapable refuses a shard of a multi-shard task when any
// binding writes through snowpipe_streaming or a Snowpipe pipe, because only
// staged files and snowpipe_streaming_v2 keep their state per shard. A shard
// is one of several when its range spans less than the full key or r-clock
// space.
func requireMultiShardCapable(r *pf.RangeSpec, bindings []*binding) error {
	if r.KeyBegin == 0 && r.KeyEnd == math.MaxUint32 && r.RClockBegin == 0 && r.RClockEnd == math.MaxUint32 {
		return nil
	}

	var tables []string
	for _, b := range bindings {
		if !b.streamingV2 && (b.streaming || b.pipeName != "") {
			tables = append(tables, b.target.Identifier)
		}
	}
	if len(tables) == 0 {
		return nil
	}

	var err = fmt.Errorf("bindings %s write through Snowpipe, which supports only one shard", strings.Join(tables, ", "))
	return cerrors.NewUserError(err, fmt.Sprintf(
		"This task runs on more than one shard, but its delta updates bindings for tables %s write through Snowpipe, which supports only one shard. Run the task on one shard, disable delta updates on those bindings, or use JWT authentication with the %q and %q feature flags enabled.",
		strings.Join(tables, ", "), flagSnowpipeStreaming, flagSnowpipeStreamingV2))
}
