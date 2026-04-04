package main

import (
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

var RetryDelays = []float64{0.0, 4.0, 8.0, 15.0, 30.0, 60.0}

// Run a function and if it fails, retry it according to the second delays in
// backoff.  Only errors of the type E are retried.  If too many failures
// occur, return the last error.
func Retry[E error](secondDelays []float64, fn func() error) error {
	var err error
	var attempt = 0
	for _, secondDelay := range secondDelays {
		attempt++

		err = fn()

		var retryError E
		if errors.As(err, &retryError) {
			delay := time.Duration(secondDelay * float64(time.Second))
			log.WithFields(log.Fields{
				"attempt": attempt,
				"delay":   delay.String(),
			}).WithError(retryError).Info("retryable error occurred")
			time.Sleep(delay)
			continue
		}
		return err
	}
	return err
}
