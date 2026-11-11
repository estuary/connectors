package connector

import (
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"time"

	"github.com/estuary/connectors/go/retry"
)

// putRetry retries PUTs to the internal stage. A PUT run again writes the same
// file to the same name with OVERWRITE=TRUE, so ambiguous failures are retried
// too.
var putRetry = retry.Policy{
	Classify:       classifyPut,
	RetryAmbiguous: true,
	MaxAttempts:    4,
	InitialBackoff: time.Second,
	MaxBackoff:     4 * time.Second,
	Jitter:         0.5,
}

// classifyPut treats every failure as possibly applied, because the driver
// reports a failed upload only as a message.
func classifyPut(error) retry.Outcome {
	return retry.Ambiguous
}

// pipeVisibleRetry waits out the eventual consistency of CREATE PIPE.
var pipeVisibleRetry = retry.Policy{
	Classify:       classifyPipeVisible,
	MaxElapsed:     10 * time.Minute,
	InitialBackoff: 5 * time.Second,
	MaxBackoff:     5 * time.Second,
	Jitter:         0.5,
}

var errPipeNotVisible = errors.New("pipe does not exist yet")

func classifyPipeVisible(err error) retry.Outcome {
	if errors.Is(err, errPipeNotVisible) {
		return retry.Retryable
	}
	return retry.Fatal
}

// tokenPersistedRetry polls a Snowpipe Streaming channel until it has
// persisted the offset token of the last file registered to it.
var tokenPersistedRetry = retry.Policy{
	Classify:       classifyTokenPersisted,
	MaxElapsed:     5 * time.Minute,
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
	QuietRetries:   10,
}

var errTokenNotPersisted = errors.New("channel offset token not persisted yet")

func classifyTokenPersisted(err error) retry.Outcome {
	if errors.Is(err, errTokenNotPersisted) || errors.Is(err, ErrTemporary) {
		return retry.Retryable
	}
	return retry.Fatal
}

// streamV2Retry retries Snowpipe Streaming requests, ambiguous failures
// included. An append of rows or of a staged file presents the channel's
// continuation token, and Snowflake buffers a repeat of the same token and rows
// once. Every other request reads state, mints a credential, or opens or drops
// a channel, which a repeat does again to the same effect.
//
// Each attempt has its own deadline, because one append carries megabytes and
// may need all of it.
var streamV2Retry = retry.Policy{
	Classify:       classifyStreamV2,
	RetryAmbiguous: true,
	MaxAttempts:    6,
	InitialBackoff: time.Second,
	MaxBackoff:     30 * time.Second,
	Jitter:         0.5,
	AttemptTimeout: 2 * time.Minute,
}

// streamV2MaxRetryAfter caps the delay a Retry-After header can ask for.
const streamV2MaxRetryAfter = time.Minute

func classifyStreamV2(err error) retry.Outcome {
	if apiErr, ok := errors.AsType[*streamV2APIError](err); ok {
		return apiErr.outcome
	} else if _, ok := errors.AsType[*streamV2ReadError](err); ok {
		return retry.Ambiguous
	}
	return retry.Network(err)
}

// streamV2Outcome classifies a failed response under Snowflake's retry policy.
// A 408 refused the request. A 499 is a proxy's, which may have passed the
// request on. A 404 whose body is not one of the API's error responses came
// from a proxy that did not pass the request on.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-error-handling#retry-policy
func streamV2Outcome(res *http.Response, body []byte) retry.Outcome {
	switch res.StatusCode {
	case http.StatusRequestTimeout:
		return retry.Retryable
	case 499:
		return retry.Ambiguous
	case http.StatusNotFound:
		var mediaType, _, _ = mime.ParseMediaType(res.Header.Get("Content-Type"))
		var e struct{ Code, Message string }
		if mediaType == "text/html" ||
			mediaType == "application/json" && json.Unmarshal(body, &e) == nil && e.Code == "" && e.Message == "" {
			return retry.Retryable
		}
		return retry.Fatal
	}
	return retry.HTTPStatus(res.StatusCode)
}
