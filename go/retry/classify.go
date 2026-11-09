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

// FailedToConnect reports whether err is a failure to open a connection, which
// sent nothing.
func FailedToConnect(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	} else if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	} else if _, ok := errors.AsType[*net.DNSError](err); ok {
		return true
	}
	var opErr, ok = errors.AsType[*net.OpError](err)
	return ok && opErr.Op == "dial"
}

// FailedAfterConnect reports whether err ended or timed out a connection once
// it was open, so the request may already have been received.
//
// context.DeadlineExceeded is such a timeout. Do never retries once its own
// context is done, so a deadline reaching a classifier is the operation's own,
// such as an http.Client Timeout or the Policy's AttemptTimeout.
//
// A failed net/http request is an *url.Error, which is itself a net.Error, so
// it counts unless it failed to connect, even when its cause won't go away on
// a retry, such as an untrusted TLS certificate.
func FailedAfterConnect(err error) bool {
	if errors.Is(err, context.Canceled) || FailedToConnect(err) {
		return false
	} else if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var _, ok = errors.AsType[net.Error](err)
	return ok
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
