package connector

import (
	"time"

	"github.com/estuary/connectors/go/retry"
)

// appendRetry retries appending files to a table. Each attempt reloads the
// table and does nothing if the checkpoint, which commits atomically with the
// files, is already recorded, so an attempt that may have committed is safe to
// run again.
var appendRetry = retry.Policy{
	Classify:       classifyAppend,
	RetryAmbiguous: true,
	MaxAttempts:    3,
	InitialBackoff: 2 * time.Second,
	Jitter:         0.5,
}

// classifyAppend treats every error as possibly transient, since loading,
// staging and committing all go to the catalog or object storage. Errors that
// another attempt would repeat are marked retry.Permanent.
func classifyAppend(error) retry.Outcome { return retry.Ambiguous }
