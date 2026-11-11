package connector

import (
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/go/retry"
)

// commitQueryRetry re-runs a commit query that conflicted with concurrent
// activity, waiting long enough in all (about 8 minutes) for that activity to
// finish. Only failures that took no effect are retried.
var commitQueryRetry = retry.Policy{
	Classify:       classifyCommitQuery,
	MaxAttempts:    6,
	InitialBackoff: 15 * time.Second,
	Jitter:         0.5,
}

// putRetry repeats a staged file upload, which fails intermittently even
// under low load. PUT ... OVERWRITE leaves the same file however often it
// runs, so every failure is retried.
var putRetry = retry.Policy{
	Classify:       func(error) retry.Outcome { return retry.Ambiguous },
	RetryAmbiguous: true,
	MaxAttempts:    4,
	InitialBackoff: time.Second,
	Jitter:         0.5,
}

// retriableErrorClasses are the Databricks error classes of a commit query
// which succeeds when re-run once the conflicting activity has finished.
//
// COPY_INTO_DUPLICATED_FILES_COPY_NOT_ALLOWED: another COPY INTO into the same
// table committed some of the same files, which usually means a previous
// primary shard was killed mid-Acknowledge. We retry until that orphaned query
// has finished, at which point our query skips the already-copied files.
var retriableErrorClasses = []string{
	"COPY_INTO_DUPLICATED_FILES_COPY_NOT_ALLOWED",
}

func classifyCommitQuery(err error) retry.Outcome {
	var text = err.Error()
	if slices.ContainsFunc(retriableErrorClasses, func(class string) bool {
		return strings.Contains(text, class)
	}) {
		return retry.Retryable
	}
	return retry.Fatal
}
