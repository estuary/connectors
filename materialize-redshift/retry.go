package connector

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

const maxAttempts = 5

var retryDelay = 5 * time.Second

type pgErrorMatch struct {
	code, message string
}

var retriableErrors = []pgErrorMatch{
	{code: "XX000", message: "1023"}, // serializable isolation violation
	{code: "40P01"},                  // deadlock detected
}

func isRetriable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	for _, m := range retriableErrors {
		if pgErr.Code == m.code && (m.message == "" || pgErr.Message == m.message) {
			return true
		}
	}
	return false
}

func retry(ctx context.Context, fn func() error) error {
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || !isRetriable(err) || attempt == maxAttempts {
			return err
		}
		var delay = time.Duration(attempt) * retryDelay
		log.WithFields(log.Fields{
			"attempt": attempt,
			"retryIn": delay.String(),
			"error":   err,
		}).Warn("transaction rolled back by a retriable error and will be retried")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}
