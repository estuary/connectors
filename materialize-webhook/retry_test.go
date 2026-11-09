package connector

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
		{"rate limited", &statusError{code: http.StatusTooManyRequests}, retry.Retryable},
		{"server error", &statusError{code: http.StatusBadGateway}, retry.Retryable},
		{"client error", &statusError{code: http.StatusBadRequest}, retry.Retryable},
		{"connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, retry.Retryable},
		{"connection reset", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, retry.Retryable},
		{"context cancelled", context.Canceled, retry.Fatal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classify(tt.err))
		})
	}
}

func TestSendWebhookRetries(t *testing.T) {
	var saved = sendRetry
	sendRetry.InitialBackoff = 0
	t.Cleanup(func() { sendRetry = saved })

	var bodies []string
	var server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		bodies = append(bodies, string(body))
		if len(bodies) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()

	var d transactor
	require.NoError(t, d.sendWebhook(t.Context(), server.URL, *bytes.NewBufferString(`[{"id":1}]`)))
	require.Equal(t, []string{`[{"id":1}]`, `[{"id":1}]`, `[{"id":1}]`}, bodies)
}
