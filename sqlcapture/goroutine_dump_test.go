package sqlcapture

import (
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// blockedForStackDumpTest parks a goroutine on a channel receive so that
// dumpGoroutineStacks has a recognizable, deliberately-blocked goroutine to
// capture.
func blockedForStackDumpTest(release <-chan struct{}) {
	<-release
}

func TestDumpGoroutineStacks(t *testing.T) {
	// Capture logrus output via a test hook rather than writing to stderr.
	var hook = logtest.NewGlobal()
	defer hook.Reset()

	// Park a goroutine on a channel receive so the dump has a known blocked
	// stack to find.
	var release = make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		blockedForStackDumpTest(release)
	}()
	<-started
	defer close(release)

	dumpGoroutineStacks()

	var entry = hook.LastEntry()
	require.NotNil(t, entry, "expected a log entry from dumpGoroutineStacks")
	require.Equal(t, log.WarnLevel, entry.Level)
	require.Equal(t, "goroutine stack dump for long-running replication streaming", entry.Message)

	var stacks, ok = entry.Data["stacks"].(string)
	require.True(t, ok, "expected a 'stacks' string field")

	require.Contains(t, stacks, "goroutine ")
	require.Contains(t, stacks, "sqlcapture.dumpGoroutineStacks")
	require.Contains(t, stacks, "sqlcapture.blockedForStackDumpTest")
	require.Contains(t, stacks, "chan receive")

	require.GreaterOrEqual(t, strings.Count(stacks, "goroutine "), 2)
}
