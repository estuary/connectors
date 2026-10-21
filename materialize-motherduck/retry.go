package connector

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	log "github.com/sirupsen/logrus"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// maxAttempts is how many times a retried operation is run before its
// last error is returned.
const maxAttempts = 10

// retryBaseDelay is the mean delay before the first retry. Each later retry
// doubles it.
var retryBaseDelay = 250 * time.Millisecond

// isRetryable reports whether err is a duckdb error of a type that MotherDuck
// returns intermittently and that resolves on a retry. Transaction errors
// include DuckLake rejecting a commit because a concurrent transaction changed
// the same catalog state first.
func isRetryable(err error) bool {
	var duckdbErr *duckdb.Error
	return errors.As(err, &duckdbErr) &&
		(duckdbErr.Type == duckdb.ErrorTypeConnection || duckdbErr.Type == duckdb.ErrorTypeTransaction)
}

// withRetries runs fn until it succeeds, returns an error that is not
// retryable, or has been attempted maxAttempts times. Retries wait a
// jittered, exponentially growing delay, so that concurrent writers which
// conflicted once are unlikely to conflict again.
func withRetries(ctx context.Context, operation string, fn func() error) error {
	for attempt := 1; ; attempt++ {
		var err = fn()
		if err == nil || attempt == maxAttempts || !isRetryable(err) {
			return err
		}

		var delay = retryBaseDelay << (attempt - 1)
		delay = delay/2 + rand.N(delay)

		log.WithFields(log.Fields{
			"operation": operation,
			"attempt":   attempt,
			"delay":     delay.String(),
		}).WithError(err).Warn("retrying after retryable error")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}
