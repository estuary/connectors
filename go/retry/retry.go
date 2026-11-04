// Package retry runs operations against a destination again when they fail
// with errors that a retry can resolve.
//
// A Classifier sorts each error into one of three outcomes:
//
//   - Retryable: the operation failed without taking effect, so running it
//     again is safe. Examples are a refused connection, a rate-limit
//     rejection, or a transaction the destination aborted on a conflict.
//   - Ambiguous: the operation may have taken effect before it failed. A
//     timeout or dropped connection after the request was sent, or a 5xx
//     response to a write, says nothing about whether the write happened.
//   - Fatal: anything else, returned to the caller without retrying.
//
// A Policy retries Ambiguous errors only when its RetryAmbiguous field is set,
// which a caller should do only for idempotent operations. Retrying a
// non-idempotent write after an ambiguous failure can apply it twice, which
// for an append is a permanent duplicate.
//
// Classifiers for a destination's own errors live with its connector. This
// package provides the retry loop and classifiers for errors that are the
// same everywhere: Network for connection-level failures and HTTPStatus for
// HTTP responses.
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
	// Retryable errors failed without taking effect.
	Retryable
	// Ambiguous errors may have been raised after the operation took effect.
	Ambiguous
)

func (o Outcome) String() string {
	switch o {
	case Retryable:
		return "retryable"
	case Ambiguous:
		return "ambiguous"
	default:
		return "fatal"
	}
}

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

// Policy decides which failed attempts are retried, and how long to wait
// before each retry.
//
// At least one of MaxAttempts and MaxElapsed must be set.
type Policy struct {
	// Classify sorts errors into outcomes. Required.
	Classify Classifier
	// RetryAmbiguous retries errors classified Ambiguous. Set it only for
	// operations that leave the same result when run again after they
	// already took effect.
	RetryAmbiguous bool

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

	// AttemptTimeout, if set, bounds each attempt with its own deadline
	// rather than leaving one deadline to span every attempt. An attempt
	// that runs out of time is Ambiguous, whatever Classify says. The
	// context passed to the operation is cancelled when it returns, so the
	// operation must not return anything bound to it, such as *sql.Rows.
	AttemptTimeout time.Duration
}

// randFloat is replaced in tests.
var randFloat = rand.Float64

// Do runs fn until it succeeds, fails with an error the policy doesn't retry,
// or runs out of attempts or time. It never retries once ctx is done, nor
// waits for a retry that ctx's deadline would cut short. Each retry logs one
// warning naming the operation, the attempt and its error.
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
		var err, timedOut = p.attempt(ctx, fn)
		if err == nil {
			return nil
		} else if ctx.Err() != nil {
			return err
		}

		var outcome = Ambiguous
		if !timedOut {
			outcome = p.Classify(err)
		}
		var giveUp = func() error {
			if attempt == 1 {
				return err
			}
			return fmt.Errorf("after %d attempts: %w", attempt, err)
		}

		if outcome == Fatal || (outcome == Ambiguous && !p.RetryAmbiguous) {
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

		log.WithFields(log.Fields{
			"operation": operation,
			"attempt":   attempt,
			"outcome":   outcome.String(),
			"delay":     delay.String(),
		}).WithError(err).Warn("retrying failed operation")

		var timer = time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w while waiting to retry %s after: %w", ctx.Err(), operation, err)
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

// attempt runs fn once and reports whether its own AttemptTimeout ended it.
func (p Policy) attempt(ctx context.Context, fn func(context.Context) error) (error, bool) {
	if p.AttemptTimeout <= 0 {
		return fn(ctx), false
	}
	var attemptCtx, cancel = context.WithTimeout(ctx, p.AttemptTimeout)
	defer cancel()

	var err = fn(attemptCtx)
	return err, err != nil && attemptCtx.Err() != nil && ctx.Err() == nil
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
