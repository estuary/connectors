package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"
)

// Network classifies connection-level failures. A failure to connect is
// Retryable, since nothing was sent. A connection that ends or times out
// once open is Ambiguous: the request may already have been received. Context
// cancellation and deadlines, and every other error, are Fatal.
//
// A failed net/http request is an *url.Error, which is itself a net.Error, so
// it is Ambiguous unless it failed to connect, even when its cause won't go
// away on a retry, such as an untrusted TLS certificate.
func Network(err error) Outcome {
	// context.DeadlineExceeded is itself a net.Error, so it is checked first.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Fatal
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return Retryable
	}
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return Retryable
	}
	if opErr, ok := errors.AsType[*net.OpError](err); ok && opErr.Op == "dial" {
		return Retryable
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return Ambiguous
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return Ambiguous
	}
	return Fatal
}

// HTTPStatus classifies an HTTP response status. 429 Too Many Requests is
// Retryable, since the server refused the request. A 5xx is Ambiguous, since
// the server may have acted on the request before failing. Anything else is
// Fatal.
func HTTPStatus(code int) Outcome {
	switch {
	case code == http.StatusTooManyRequests:
		return Retryable
	case code >= 500 && code <= 599:
		return Ambiguous
	default:
		return Fatal
	}
}

// ParseRetryAfter parses a Retry-After header value, given either as seconds
// or as an HTTP date, into the delay it asks for from now.
func ParseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(at.Sub(now), 0), true
	}
	return 0, false
}
