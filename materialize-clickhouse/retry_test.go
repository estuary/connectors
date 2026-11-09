package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{
			// The exact shape reported in the field: clickhouse-go wraps the
			// underlying read error with %w.
			name: "EOF reading first block packet",
			err:  fmt.Errorf("query processing: failed to read first block packet from 1.2.3.4:9440 (conn_id=7): %w", io.EOF),
			want: retry.Retryable,
		},
		{
			name: "unexpected EOF",
			err:  fmt.Errorf("read: %w", io.ErrUnexpectedEOF),
			want: retry.Retryable,
		},
		{
			name: "connection reset",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
			want: retry.Retryable,
		},
		{
			name: "broken pipe",
			err:  &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE},
			want: retry.Retryable,
		},
		{
			name: "connection refused on dial",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
			want: retry.Retryable,
		},
		{
			name: "i/o timeout",
			err:  fmt.Errorf("read: %w", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("i/o timeout")}),
			want: retry.Retryable,
		},
		{
			// Lost Keeper sessions resolve on their own once the replica
			// reconnects, but the statement may have been applied first.
			name: "Keeper connection loss",
			err:  fmt.Errorf("moving: %w", &clickhouseproto.Exception{Code: 999, Message: "Coordination::Exception: Connection loss"}),
			want: retry.Retryable,
		},
		{
			name: "Keeper session expired",
			err:  &clickhouseproto.Exception{Code: 999, Message: "Coordination::Exception: Session expired (ZSESSIONEXPIRED)"},
			want: retry.Retryable,
		},
		{
			name: "server OOM kill exception is not retryable",
			err:  fmt.Errorf("querying: %w", &clickhouseproto.Exception{Code: 241, Message: "Memory limit (total) exceeded"}),
			want: retry.Fatal,
		},
		{
			name: "server DDL exception is not retryable",
			err:  &clickhouseproto.Exception{Code: 60, Message: "Unknown table"},
			want: retry.Fatal,
		},
		{
			name: "context cancellation is not retryable",
			err:  context.Canceled,
			want: retry.Fatal,
		},
		{
			name: "operation deadline",
			err:  fmt.Errorf("querying: %w", context.DeadlineExceeded),
			want: retry.Retryable,
		},
		{
			name: "arbitrary error is not retryable",
			err:  errors.New("something else entirely"),
			want: retry.Fatal,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classify(tt.err))
		})
	}
}

// TestIdempotentRetry checks that every transient failure classify reports is
// retried under the policy the call sites use.
func TestIdempotentRetry(t *testing.T) {
	var policy = idempotentRetry
	policy.InitialBackoff = 0

	for _, transient := range []error{
		fmt.Errorf("read: %w", io.EOF),
		&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		&clickhouseproto.Exception{Code: 999, Message: "Coordination::Exception: Connection loss"},
	} {
		var calls int
		require.NoError(t, policy.Do(t.Context(), "op", func(context.Context) error {
			if calls++; calls < 3 {
				return transient
			}
			return nil
		}))
		require.Equal(t, 3, calls)
	}
}
