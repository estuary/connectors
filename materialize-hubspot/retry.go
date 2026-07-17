package hubspot

import (
	"context"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"
)

// TemporaryError is an error for an operation that is expected to succeed if
// retried.
type TemporaryError struct {
	err        error
	extraDelay time.Duration
}

func (e *TemporaryError) Error() string {
	return e.err.Error()
}

func (e *TemporaryError) Unwrap() error {
	return e.err
}

func (e *TemporaryError) ExtraDelay() time.Duration {
	return e.extraDelay
}

// If additional delay needs to be applied on top of the backoff, perhaps due
// to rate limiting.
type DelayError interface {
	ExtraDelay() time.Duration
}

var DefaultBackoff = []float64{0.0, 4.0, 8.0, 15.0, 30.0, 60.0}

// Run a function and if it fails, retry it according to the second delays in
// backoffSeconds.  Only errors of the type E are retried.  If too many failures
// occur, return the last error.
func Retry[E error](ctx context.Context, backoffSeconds []float64, fn func(attempt int) error) error {
	var err error
	var attempt = 0
	for i, backoff := range append(backoffSeconds, 0.0) {
		attempt++

		err = fn(attempt)

		// No need to backoff if this was the last attempt.
		if i != len(backoffSeconds) {
			var extraDelay time.Duration

			var delayError DelayError
			if errors.As(err, &delayError) {
				extraDelay = delayError.ExtraDelay()
			}

			var retryError E
			if errors.As(err, &retryError) {
				delay := time.Duration(backoff*float64(time.Second)) + extraDelay

				log.WithFields(log.Fields{
					"attempt":       attempt,
					"backoff_delay": delay.String(),
				}).WithError(retryError).Info("retryable error occurred")

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
					continue
				}
			}
		}
		return err
	}
	return err
}
