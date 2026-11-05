package retry

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNetwork(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want Outcome
	}{
		{
			name: "connection refused",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)},
			want: Retryable,
		},
		{
			name: "dial timeout",
			err:  fmt.Errorf("connecting: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("i/o timeout")}),
			want: Retryable,
		},
		{
			name: "DNS lookup failure",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "db.example.com"}},
			want: Retryable,
		},
		{
			name: "EOF reading a response",
			err:  fmt.Errorf("failed to read first block packet: %w", io.EOF),
			want: Ambiguous,
		},
		{
			name: "unexpected EOF",
			err:  fmt.Errorf("read: %w", io.ErrUnexpectedEOF),
			want: Ambiguous,
		},
		{
			name: "connection reset",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)},
			want: Ambiguous,
		},
		{
			name: "broken pipe",
			err:  &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE},
			want: Ambiguous,
		},
		{
			name: "read timeout",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: errors.New("i/o timeout")},
			want: Ambiguous,
		},
		{
			name: "HTTP request refused",
			err:  &url.Error{Op: "Post", URL: "http://x", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}},
			want: Retryable,
		},
		{
			name: "HTTP request reset",
			err:  &url.Error{Op: "Post", URL: "http://x", Err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}},
			want: Ambiguous,
		},
		{
			name: "HTTP request with an untrusted certificate",
			err:  &url.Error{Op: "Post", URL: "https://x", Err: x509.UnknownAuthorityError{}},
			want: Ambiguous,
		},
		{
			name: "HTTP request cancelled",
			err:  &url.Error{Op: "Post", URL: "http://x", Err: context.Canceled},
			want: Fatal,
		},
		{
			name: "context cancelled",
			err:  fmt.Errorf("querying: %w", context.Canceled),
			want: Fatal,
		},
		{
			name: "operation deadline",
			err:  fmt.Errorf("querying: %w", context.DeadlineExceeded),
			want: Ambiguous,
		},

		{
			name: "anything else",
			err:  errors.New("syntax error"),
			want: Fatal,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, Network(tt.err))
		})
	}
}

func TestNetworkHTTPClientTimeout(t *testing.T) {
	var server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	var _, err = (&http.Client{Timeout: 10 * time.Millisecond}).Get(server.URL)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, Ambiguous, Network(err))
}

func TestHTTPStatus(t *testing.T) {
	for code, want := range map[int]Outcome{
		http.StatusOK:                  Fatal,
		http.StatusBadRequest:          Fatal,
		http.StatusUnauthorized:        Fatal,
		http.StatusNotFound:            Fatal,
		http.StatusConflict:            Fatal,
		http.StatusTooManyRequests:     Retryable,
		http.StatusInternalServerError: Ambiguous,
		http.StatusBadGateway:          Ambiguous,
		http.StatusServiceUnavailable:  Ambiguous,
		http.StatusGatewayTimeout:      Ambiguous,
	} {
		require.Equal(t, want, HTTPStatus(code), "status %d", code)
	}
}

func TestParseRetryAfter(t *testing.T) {
	var now = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

	for _, tt := range []struct {
		value  string
		want   time.Duration
		wantOk bool
	}{
		{value: "", wantOk: false},
		{value: "0", want: 0, wantOk: true},
		{value: "120", want: 2 * time.Minute, wantOk: true},
		{value: "-1", wantOk: false},
		{value: "Wed, 04 Nov 2026 12:00:30 GMT", want: 30 * time.Second, wantOk: true},
		{value: "Wed, 04 Nov 2026 11:59:00 GMT", want: 0, wantOk: true},
		{value: "soon", wantOk: false},
	} {
		t.Run(tt.value, func(t *testing.T) {
			var got, ok = ParseRetryAfter(tt.value, now)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestOutcomeString(t *testing.T) {
	require.Equal(t, "fatal", Fatal.String())
	require.Equal(t, "retryable", Retryable.String())
	require.Equal(t, "ambiguous", Ambiguous.String())
}
