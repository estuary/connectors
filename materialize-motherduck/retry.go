package connector

import (
	"errors"
	"time"

	"github.com/estuary/connectors/go/retry"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// ddlRetry retries Apply's statements and the fence installation. Retries
// wait a jittered, exponentially growing delay, so that concurrent writers
// which conflicted once are unlikely to conflict again.
//
// A statement run again after an ambiguous failure it had in fact applied
// either succeeds again (TRUNCATE, the fence install) or fails without
// changing anything (creating, altering or dropping what it already did), so
// ambiguous failures are retried too.
var ddlRetry = retry.Policy{
	Classify:       classify,
	RetryAmbiguous: true,
	MaxAttempts:    10,
	InitialBackoff: 250 * time.Millisecond,
	Jitter:         0.5,
}

// commitRetry retries the store transaction, with ddlRetry's delays. COMMIT
// failing with a connection error may have committed the transaction, and
// running it again would append its inserted rows a second time, so that
// failure is not retried: the connector restarts and recovers from whichever
// checkpoint the destination holds.
var commitRetry = retry.Policy{
	Classify:       classifyCommit,
	MaxAttempts:    ddlRetry.MaxAttempts,
	InitialBackoff: ddlRetry.InitialBackoff,
	Jitter:         ddlRetry.Jitter,
}

// classify sorts the duckdb errors MotherDuck returns intermittently and that
// resolve on a retry. A transaction error, which includes DuckLake rejecting a
// commit because a concurrent transaction changed the same catalog state
// first, refused the statement and is retryable. A connection error may come
// after MotherDuck applied the statement, so it is ambiguous.
func classify(err error) retry.Outcome {
	if duckdbErr, ok := errors.AsType[*duckdb.Error](err); !ok {
		return retry.Fatal
	} else if duckdbErr.Type == duckdb.ErrorTypeTransaction {
		return retry.Retryable
	} else if duckdbErr.Type == duckdb.ErrorTypeConnection {
		return retry.Ambiguous
	}
	return retry.Fatal
}

// classifyCommit classifies errors from the store transaction. Only an
// error raised by COMMIT can leave it committed; any earlier error rolled it
// back.
func classifyCommit(err error) retry.Outcome {
	var outcome = classify(err)
	if _, atCommit := errors.AsType[*commitError](err); outcome == retry.Ambiguous && !atCommit {
		return retry.Retryable
	}
	return outcome
}

// commitError is an error returned by COMMIT.
type commitError struct{ err error }

func (e *commitError) Error() string { return e.err.Error() }
func (e *commitError) Unwrap() error { return e.err }
