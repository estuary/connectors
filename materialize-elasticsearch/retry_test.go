package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"testing"

	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{"rate limited", &responseError{status: http.StatusTooManyRequests}, retry.Retryable},
		{"bad gateway", fmt.Errorf("bulk: %w", &responseError{status: http.StatusBadGateway}), retry.Retryable},
		{"service unavailable", &responseError{status: http.StatusServiceUnavailable}, retry.Retryable},
		{"gateway timeout", &responseError{status: http.StatusGatewayTimeout}, retry.Retryable},
		{"internal server error", &responseError{status: http.StatusInternalServerError}, retry.Fatal},
		{"bad request", &responseError{status: http.StatusBadRequest}, retry.Fatal},
		{"transient bulk items", &bulkItemsError{count: 1, err: errors.New("(index) retry_on_primary_exception: r")}, retry.Retryable},
		{"connection refused", &transportError{&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}, retry.Retryable},
		{"connection reset", &transportError{&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}}, retry.Retryable},
		{"EOF", fmt.Errorf("bulk: %w", &transportError{io.EOF}), retry.Retryable},
		{"HTTP/2 stream error", &transportError{errors.New("stream error: stream ID 3; INTERNAL_ERROR")}, retry.Retryable},
		{"request deadline", &transportError{context.DeadlineExceeded}, retry.Retryable},
		{"context cancelled", &transportError{context.Canceled}, retry.Fatal},
		{"undecodable response", errors.New("failed to decode bulk response: unexpected EOF"), retry.Fatal},
		{"terminal bulk item", errors.New("(index) mapper_parsing_exception: r"), retry.Fatal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classify(tt.err))
		})
	}
}
