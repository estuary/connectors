package main

import (
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
)

// fence is an installed barrier in a shared checkpoints table which prevents
// other sessions from committing transactions under the fenced ID --
// and prevents this Fence from committing where another session has in turn
// fenced this instance off.
type fence struct {
	// checkpoint associated with this Fence.
	checkpoint []byte
	// fence is the current value of the monotonically increasing integer used to identify unique
	// instances of transactions rpcs.
	fence int64
	// Full name of the fenced materialization.
	materialization pf.Materialization
	// [keyBegin, keyEnd) identify the range of keys covered by this Fence.
	keyBegin uint32
	keyEnd   uint32
}

// LogEntry returns a log.Entry with pre-set fields that identify the Shard ID and Fence that is useful
// for logging when multiple shards are working and determining the difference between thread logs.
func (f *fence) LogEntry() *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"materialization": f.materialization,
		"keyBegin":        f.keyBegin,
		"keyEnd":          f.keyEnd,
		"fence":           f.fence,
	})
}

// Checkpoint returns the current checkpoint for the fence.
func (f *fence) Checkpoint() []byte {
	return f.checkpoint
}

// SetCheckpoint sets the current checkpoint.
func (f *fence) SetCheckpoint(checkpoint []byte) {
	f.checkpoint = checkpoint
}
