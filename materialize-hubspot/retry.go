package hubspot

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// TemporaryError is an error for an operation that is expected to succeed if
// retried.
type TemporaryError struct {
	err error
}

func NewTemporaryError(format string, a ...any) *TemporaryError {
	return &TemporaryError{err: fmt.Errorf(format, a...)}
}

func (e *TemporaryError) Error() string {
	return e.err.Error()
}

func (e *TemporaryError) Unwrap() error {
	return e.err
}

var DefaultBackoff = []float64{0.0, 4.0, 8.0, 15.0, 30.0, 60.0}

// Run a function and if it fails, retry it according to the second delays in
// backoffSeconds.  Only errors of the type E are retried.  If too many failures
// occur, return the last error.
func Retry[E error](ctx context.Context, backoffSeconds []float64, fn func(attempt int) error) error {
	var err error
	var attempt = 0
	for _, backoff := range backoffSeconds {
		attempt++

		err = fn(attempt)

		var retryError E
		if errors.As(err, &retryError) {
			delay := time.Duration(backoff * float64(time.Second))
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
		return err
	}
	return err
}
