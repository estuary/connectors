// Package retry runs operations against a destination again when they fail
// with errors that a retry can resolve.
//
// A Classifier decides whether each error is Retryable or Fatal. Whether a
// retry is safe depends on the operation as well as the error: a timeout or
// dropped connection after a request was sent, or a 5xx response, says
// nothing about whether the request took effect. Running an idempotent
// operation again after such an error is safe, but running a non-idempotent
// write again can apply it twice, which for an append is a permanent
// duplicate. So a classifier is written for the operations that use it, and
// retries errors that may follow the operation taking effect only when those
// operations are idempotent, or when their failure would repeat them anyway:
// a destination that keeps no checkpoint of its own has a failed transaction
// replayed in full, so retrying the one request re-sends less.
//
// Classifiers live with their connector. This package provides the retry loop
// and two predicates for connection-level failures, which are the same
// everywhere: FailedToConnect and FailedAfterConnect.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	log "github.com/sirupsen/logrus"
)

// Outcome is a Classifier's verdict on an error.
type Outcome int

const (
	// Fatal errors are returned without retrying.
	Fatal Outcome = iota
	// Retryable errors are retried.
	Retryable
)

// Classifier reports the Outcome of a non-nil error.
type Classifier func(error) Outcome

// Delayer is implemented by errors that carry the minimum delay the server
// asked for before the next attempt, such as an HTTP Retry-After header.
type Delayer interface {
	error
	RetryDelay() time.Duration
}

// WithDelay returns err annotated with the minimum delay before a retry.
func WithDelay(err error, delay time.Duration) error {
	return &delayError{err: err, delay: delay}
}

type delayError struct {
	err   error
	delay time.Duration
}

func (e *delayError) Error() string             { return e.err.Error() }
func (e *delayError) Unwrap() error             { return e.err }
func (e *delayError) RetryDelay() time.Duration { return e.delay }

// Permanent marks err Fatal, whatever the Classifier would say of it. It
// returns nil if err is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Policy decides which failed attempts are retried, and how long to wait
// before each retry.
//
// At least one of MaxAttempts and MaxElapsed must be set.
type Policy struct {
	// Classify sorts errors into outcomes. Required.
	Classify Classifier

	// MaxAttempts bounds the number of attempts, including the first. Zero
	// means no bound on attempts.
	MaxAttempts int
	// MaxElapsed bounds the time since the first attempt started: a retry
	// whose delay would end past it is not made. Zero means no bound.
	MaxElapsed time.Duration

	// InitialBackoff is the delay before the first retry. It doubles for
	// each later retry, up to MaxBackoff.
	InitialBackoff time.Duration
	// MaxBackoff caps each delay, jitter included. Zero means no cap. A
	// longer delay asked for by the server (Delayer) is still honoured.
	MaxBackoff time.Duration
	// Jitter, between 0 and 1, spreads each delay uniformly over that
	// fraction either side of its nominal value, so that writers which
	// failed together don't retry together.
	Jitter float64

	// QuietRetries is how many retries, from the first, are logged at debug
	// level rather than as warnings, for policies whose first retries are
	// routine, such as re-sending throttled writes.
	QuietRetries int

	// AttemptTimeout, if set, bounds each attempt with its own deadline
	// rather than leaving one deadline to span every attempt. Classify sees
	// the error of an attempt that ran out of time like any other. The
	// context passed to the operation is cancelled when it returns, so the
	// operation must not return anything bound to it, such as *sql.Rows.
	AttemptTimeout time.Duration
}

// randFloat is replaced in tests.
var randFloat = rand.Float64

// Do runs fn until it succeeds, fails with an error the policy doesn't retry,
// or runs out of attempts or time. It never retries once ctx is done, nor
// waits for a retry that ctx's deadline would cut short. Each retry logs one
// warning, or a debug line within QuietRetries, naming the operation, the
// attempt and its error.
//
// When Do gives up after retrying, the returned error wraps fn's last error
// and says how many attempts were made.
func (p Policy) Do(ctx context.Context, operation string, fn func(ctx context.Context) error) error {
	if p.Classify == nil {
		panic("retry: Policy.Classify is required")
	}
	if p.MaxAttempts <= 0 && p.MaxElapsed <= 0 {
		panic("retry: Policy needs MaxAttempts or MaxElapsed")
	}

	var start = time.Now()
	var backoff = p.InitialBackoff

	for attempt := 1; ; attempt++ {
		var err = p.attempt(ctx, fn)
		if err == nil {
			return nil
		} else if ctx.Err() != nil {
			return err
		}

		var outcome = Fatal
		if _, ok := errors.AsType[*permanentError](err); !ok {
			outcome = p.Classify(err)
		}
		var giveUp = func() error {
			if attempt == 1 {
				return err
			}
			return fmt.Errorf("after %d attempts: %w", attempt, err)
		}

		if outcome == Fatal {
			return giveUp()
		} else if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
			return giveUp()
		}

		var delay = p.delay(backoff, err)
		var resumeAt = time.Now().Add(delay)
		if p.MaxElapsed > 0 && !resumeAt.Before(start.Add(p.MaxElapsed)) {
			return giveUp()
		} else if deadline, ok := ctx.Deadline(); ok && !resumeAt.Before(deadline) {
			return giveUp()
		}

		var level = log.WarnLevel
		if attempt <= p.QuietRetries {
			level = log.DebugLevel
		}
		log.WithFields(log.Fields{
			"operation": operation,
			"attempt":   attempt,
			"delay":     delay.String(),
		}).WithError(err).Log(level, "retrying failed operation")

		var timer = time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			// The last error is only described, so that a cancelled Do isn't
			// mistaken for one that failed with it.
			return fmt.Errorf("%w while waiting to retry %s after: %v", ctx.Err(), operation, err)
		case <-timer.C:
		}

		if backoff < math.MaxInt64/2 {
			backoff *= 2
		}
		if p.MaxBackoff > 0 && backoff > p.MaxBackoff {
			backoff = p.MaxBackoff
		}
	}
}

// attempt runs fn once, within its own AttemptTimeout if set.
func (p Policy) attempt(ctx context.Context, fn func(context.Context) error) error {
	if p.AttemptTimeout <= 0 {
		return fn(ctx)
	}
	var attemptCtx, cancel = context.WithTimeout(ctx, p.AttemptTimeout)
	defer cancel()

	return fn(attemptCtx)
}

func (p Policy) delay(backoff time.Duration, err error) time.Duration {
	var delay = backoff
	if jitter := min(max(p.Jitter, 0), 1); jitter > 0 {
		delay = time.Duration(float64(delay) * (1 - jitter + 2*jitter*randFloat()))
	}
	if p.MaxBackoff > 0 && delay > p.MaxBackoff {
		delay = p.MaxBackoff
	}
	if d, ok := errors.AsType[Delayer](err); ok && d.RetryDelay() > delay {
		delay = d.RetryDelay()
	}
	return delay
}
