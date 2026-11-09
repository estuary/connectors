package hubspot

import (
	"errors"
	"net/http"
	"time"

	"github.com/estuary/connectors/go/retry"
)

// requestRetry repeats requests that HubSpot refused for exceeding its rate
// limit.
var requestRetry = retry.Policy{
	Classify:       classifyRequest,
	MaxAttempts:    7,
	InitialBackoff: 2 * time.Second,
	MaxBackoff:     time.Minute,
	Jitter:         0.5,
}

// idempotentRetry also repeats requests that failed with a server error,
// which HubSpot may have applied first. Only requests that leave the same
// result when repeated use it.
var idempotentRetry = func() retry.Policy {
	var p = requestRetry
	p.Classify = classifyIdempotent
	return p
}()

func classifyRequest(err error) retry.Outcome {
	if apiErr, ok := errors.AsType[*APIError](err); ok && apiErr.StatusCode == http.StatusTooManyRequests {
		return retry.Retryable
	}
	return retry.Fatal
}

func classifyIdempotent(err error) retry.Outcome {
	if apiErr, ok := errors.AsType[*APIError](err); ok && apiErr.StatusCode >= 500 {
		return retry.Retryable
	}
	return classifyRequest(err)
}
