package main

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"time"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	log "github.com/sirupsen/logrus"
)

// retryPolicy bounds retries of transient ClickHouse failures by both attempt
// count and total elapsed time. After the bounds are exhausted the last error
// is returned and the shard fails as it would without retries, so a
// persistently unhealthy cluster still surfaces promptly.
type retryPolicy struct {
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	maxElapsed     time.Duration
}

// transientRetryPolicy rides out a ClickHouse Cloud replica recycle, which
// typically resolves within seconds to a couple of minutes.
var transientRetryPolicy = retryPolicy{
	maxAttempts:    8,
	initialBackoff: time.Second,
	maxBackoff:     30 * time.Second,
	maxElapsed:     5 * time.Minute,
}

// retry invokes op, retrying with exponential backoff while retryable reports
// the error as worth retrying. Because the underlying clickhouse.Conn is a
// pool that discards broken connections, each retry naturally dials fresh --
// on a load-balanced endpoint this can land on a healthy replica.
func (p retryPolicy) retry(ctx context.Context, desc string, retryable func(error) bool, op func() error) error {
	var deadline = time.Now().Add(p.maxElapsed)
	var backoff = p.initialBackoff

	for attempt := 1; ; attempt++ {
		var err = op()
		if err == nil || !retryable(err) {
			return err
		}
		if attempt >= p.maxAttempts || !time.Now().Add(backoff).Before(deadline) {
			return err
		}

		log.WithFields(log.Fields{
			"op":      desc,
			"attempt": attempt,
			"backoff": backoff.String(),
			"error":   err.Error(),
		}).Warn("retrying transient ClickHouse error")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > p.maxBackoff {
			backoff = p.maxBackoff
		}
	}
}

// isTransientErr reports whether err is a transient connection-level failure
// of the kind seen when a ClickHouse Cloud replica is recycled mid-query:
// connection reset, EOF on the native protocol, or an i/o timeout. Server
// exceptions (e.g. code 241 OOM kills, DDL errors) are never transient --
// they need the query itself to change, not a retry -- and neither is
// cancellation of our own context.
func isTransientErr(err error) bool {
	var exc *clickhouseproto.Exception
	if errors.As(err, &exc) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	// Any other net.Error (dial failure, read/write timeout) also indicates a
	// connection-level problem rather than a server-side rejection.
	var netErr net.Error
	return errors.As(err, &netErr)
}
