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
// A statement run again after a failure it had in fact applied either
// succeeds again (TRUNCATE, the fence install) or fails without changing
// anything (creating, altering or dropping what it already did), so
// connection errors are retried too.
var ddlRetry = retry.Policy{
	Classify:       classify,
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

// classify retries the duckdb errors MotherDuck returns intermittently and
// that resolve on a retry. A transaction error, which includes DuckLake
// rejecting a commit because a concurrent transaction changed the same
// catalog state first, refused the statement. A connection error may come
// after MotherDuck applied the statement.
func classify(err error) retry.Outcome {
	if duckdbErr, ok := errors.AsType[*duckdb.Error](err); !ok {
		return retry.Fatal
	} else if duckdbErr.Type == duckdb.ErrorTypeTransaction || duckdbErr.Type == duckdb.ErrorTypeConnection {
		return retry.Retryable
	}
	return retry.Fatal
}

// classifyCommit classifies errors from the store transaction. Only an
// error raised by COMMIT can leave it committed, and only a connection error
// leaves it unknown whether it did; any earlier error rolled it back.
func classifyCommit(err error) retry.Outcome {
	var duckdbErr, isDuckdb = errors.AsType[*duckdb.Error](err)
	if _, atCommit := errors.AsType[*commitError](err); atCommit && isDuckdb && duckdbErr.Type == duckdb.ErrorTypeConnection {
		return retry.Fatal
	}
	return classify(err)
}

// commitError is an error returned by COMMIT.
type commitError struct{ err error }

func (e *commitError) Error() string { return e.err.Error() }
func (e *commitError) Unwrap() error { return e.err }
