package connector

import (
	"math"
	"testing"

	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// The runtime decodes each connector log line into a document whose top-level
// keys shard, fields, spans, ts and message have fixed shapes. A logrus field
// carrying one of those names lands at the top level of the line, and a line the
// runtime cannot decode is re-logged as a raw string at warn level. The fake
// sidecar suffices: the field names do not depend on Snowflake.
func TestStreamV2LogFieldsAvoidRuntimeKeys(t *testing.T) {
	var hook = logtest.NewGlobal()
	defer hook.Reset()

	var m = newTopologyManager(t, "test/logfields", 0, math.MaxUint32, nil)
	var full = streamV2Range{keyBegin: 0, keyEnd: math.MaxUint32}
	storeKeys(t, m, keysHashingTo(full, 4, "log"))

	var reserved = []string{"shard", "fields", "spans", "ts", "message"}
	var seen = false
	for _, e := range hook.AllEntries() {
		if e.Message == "opened snowpipe streaming v2 channels" {
			seen = true
		}
		for _, key := range reserved {
			_, clash := e.Data[key]
			require.False(t, clash, "log %q carries field %q, which the runtime reserves", e.Message, key)
		}
	}
	require.True(t, seen, "the channel-open log line was not emitted")
}
