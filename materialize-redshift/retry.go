package connector

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

const maxTxnAttempts = 5

// serializationRetryDelay is the backoff unit between attempts: attempt n
// waits n times this.
var serializationRetryDelay = 5 * time.Second

// isSerializationFailure reports whether err is Redshift's serializable
// isolation violation (ERROR: 1023, raised as an internal error whose message
// is the code) or a deadlock. Both roll the transaction back, and Redshift's
// documented remedy is to run it again.
func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return (pgErr.Code == "XX000" && pgErr.Message == "1023") || pgErr.Code == "40P01"
}

// retryOnSerializationFailure runs fn, one Redshift transaction, again after
// a serialization failure, with backoff, up to maxTxnAttempts times.
func retryOnSerializationFailure(ctx context.Context, fn func() error) error {
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || !isSerializationFailure(err) || attempt == maxTxnAttempts {
			return err
		}
		var delay = time.Duration(attempt) * serializationRetryDelay
		log.WithFields(log.Fields{
			"attempt": attempt,
			"retryIn": delay.String(),
			"error":   err,
		}).Warn("transaction rolled back by a serialization failure and will be retried")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}
