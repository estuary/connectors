package main

import (
	"errors"
	"time"

	"github.com/estuary/connectors/go/retry"
	"google.golang.org/api/googleapi"
)

// requestRetry repeats requests that the Sheets API refused for exceeding its
// per-minute quota.
var requestRetry = retry.Policy{
	Classify:       classify,
	MaxElapsed:     10 * time.Minute,
	InitialBackoff: 2 * time.Second,
	MaxBackoff:     time.Minute,
	Jitter:         0.5,
}

// idempotentRetry also repeats requests that failed with a server error,
// which the API may have applied first. Only requests that leave the same
// result when repeated use it.
var idempotentRetry = func() retry.Policy {
	var p = requestRetry
	p.RetryAmbiguous = true
	return p
}()

func classify(err error) retry.Outcome {
	if e, ok := errors.AsType[*googleapi.Error](err); ok {
		return retry.HTTPStatus(e.Code)
	}
	return retry.Fatal
}
