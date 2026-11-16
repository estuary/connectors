package connector

import (
	"errors"
	"time"

	"github.com/estuary/connectors/go/retry"
	"github.com/jackc/pgx/v5/pgconn"
)

// commitRetry retries the commit transaction after Redshift rolled it back
// on a conflict with a concurrent transaction. Jitter keeps the conflicting
// writers from retrying in lockstep.
var commitRetry = retry.Policy{
	Classify:       classify,
	MaxAttempts:    5,
	InitialBackoff: 5 * time.Second,
	MaxBackoff:     20 * time.Second,
	Jitter:         0.5,
}

type pgErrorMatch struct {
	code, message string
}

// retryableErrors roll back the transaction, so nothing was applied.
var retryableErrors = []pgErrorMatch{
	{code: "XX000", message: "1023"}, // serializable isolation violation
	{code: "40P01"},                  // deadlock detected
}

func classify(err error) retry.Outcome {
	var pgErr, ok = errors.AsType[*pgconn.PgError](err)
	if !ok {
		return retry.Fatal
	}
	for _, m := range retryableErrors {
		if pgErr.Code == m.code && (m.message == "" || pgErr.Message == m.message) {
			return retry.Retryable
		}
	}
	return retry.Fatal
}
