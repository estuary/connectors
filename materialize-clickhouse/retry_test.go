package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"
)

func TestIsTransientErr(t *testing.T) {
	for _, tt := range []struct {
		name      string
		err       error
		transient bool
	}{
		{
			// The exact shape reported in the field: clickhouse-go wraps the
			// underlying read error with %w.
			name:      "EOF reading first block packet",
			err:       fmt.Errorf("query processing: failed to read first block packet from 1.2.3.4:9440 (conn_id=7): %w", io.EOF),
			transient: true,
		},
		{
			name:      "unexpected EOF",
			err:       fmt.Errorf("read: %w", io.ErrUnexpectedEOF),
			transient: true,
		},
		{
			name:      "connection reset",
			err:       &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
			transient: true,
		},
		{
			name:      "broken pipe",
			err:       &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE},
			transient: true,
		},
		{
			name:      "connection refused on dial",
			err:       &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
			transient: true,
		},
		{
			name:      "i/o timeout",
			err:       fmt.Errorf("read: %w", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("i/o timeout")}),
			transient: true,
		},
		{
			name:      "server OOM kill exception is not retryable",
			err:       fmt.Errorf("querying: %w", &clickhouseproto.Exception{Code: 241, Message: "Memory limit (total) exceeded"}),
			transient: false,
		},
		{
			name:      "server DDL exception is not retryable",
			err:       &clickhouseproto.Exception{Code: 60, Message: "Unknown table"},
			transient: false,
		},
		{
			name:      "context cancellation is not retryable",
			err:       context.Canceled,
			transient: false,
		},
		{
			name:      "context deadline is not retryable",
			err:       fmt.Errorf("querying: %w", context.DeadlineExceeded),
			transient: false,
		},
		{
			name:      "arbitrary error is not retryable",
			err:       errors.New("something else entirely"),
			transient: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.transient, isTransientErr(tt.err))
		})
	}
}

func TestRetryPolicy(t *testing.T) {
	var policy = retryPolicy{
		maxAttempts:    4,
		initialBackoff: time.Millisecond,
		maxBackoff:     4 * time.Millisecond,
		maxElapsed:     time.Minute,
	}
	var transientErr = fmt.Errorf("read: %w", io.EOF)
	var ctx = context.Background()

	t.Run("succeeds without retrying", func(t *testing.T) {
		var calls int
		require.NoError(t, policy.retry(ctx, "op", isTransientErr, func() error {
			calls++
			return nil
		}))
		require.Equal(t, 1, calls)
	})

	t.Run("retries transient errors until success", func(t *testing.T) {
		var calls int
		require.NoError(t, policy.retry(ctx, "op", isTransientErr, func() error {
			if calls++; calls < 3 {
				return transientErr
			}
			return nil
		}))
		require.Equal(t, 3, calls)
	})

	t.Run("does not retry non-transient errors", func(t *testing.T) {
		var calls int
		var permanentErr = errors.New("schema mismatch")
		require.ErrorIs(t, policy.retry(ctx, "op", isTransientErr, func() error {
			calls++
			return permanentErr
		}), permanentErr)
		require.Equal(t, 1, calls)
	})

	t.Run("gives up after maxAttempts", func(t *testing.T) {
		var calls int
		require.ErrorIs(t, policy.retry(ctx, "op", isTransientErr, func() error {
			calls++
			return transientErr
		}), io.EOF)
		require.Equal(t, policy.maxAttempts, calls)
	})

	t.Run("gives up when elapsed time is exhausted", func(t *testing.T) {
		var shortPolicy = retryPolicy{
			maxAttempts:    100,
			initialBackoff: 50 * time.Millisecond,
			maxBackoff:     50 * time.Millisecond,
			maxElapsed:     time.Millisecond,
		}
		var calls int
		require.ErrorIs(t, shortPolicy.retry(ctx, "op", isTransientErr, func() error {
			calls++
			return transientErr
		}), io.EOF)
		require.Equal(t, 1, calls)
	})

	t.Run("stops retrying when the predicate declines", func(t *testing.T) {
		var calls int
		require.ErrorIs(t, policy.retry(ctx, "op", func(error) bool { return calls < 2 }, func() error {
			calls++
			return transientErr
		}), io.EOF)
		require.Equal(t, 2, calls)
	})

	t.Run("returns promptly when the context is cancelled during backoff", func(t *testing.T) {
		var slowPolicy = retryPolicy{
			maxAttempts:    100,
			initialBackoff: time.Minute,
			maxBackoff:     time.Minute,
			maxElapsed:     time.Hour,
		}
		cancelCtx, cancel := context.WithCancel(ctx)
		var calls int
		var done = make(chan error, 1)
		go func() {
			done <- slowPolicy.retry(cancelCtx, "op", isTransientErr, func() error {
				calls++
				return transientErr
			})
		}()
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, calls)
		case <-time.After(5 * time.Second):
			t.Fatal("retry did not return after context cancellation")
		}
	})
}
