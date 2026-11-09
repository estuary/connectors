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
// file to the same name with OVERWRITE=TRUE, so every failure is retried.
var putRetry = retry.Policy{
	Classify:       classifyPut,
	MaxAttempts:    4,
	InitialBackoff: time.Second,
	MaxBackoff:     4 * time.Second,
	Jitter:         0.5,
}

// classifyPut retries every failure, because the driver reports a failed
// upload only as a message.
func classifyPut(error) retry.Outcome {
	return retry.Retryable
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

// streamV2Retry retries Snowpipe Streaming requests, including failures that
// may follow the request taking effect. An append of rows or of a staged file presents the channel's
// continuation token, and Snowflake buffers a repeat of the same token and rows
// once. Every other request reads state, mints a credential, or opens or drops
// a channel, which a repeat does again to the same effect.
//
// Each attempt has its own deadline, because one append carries megabytes and
// may need all of it.
var streamV2Retry = retry.Policy{
	Classify:       classifyStreamV2,
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
		return retry.Retryable
	} else if retry.FailedToConnect(err) || retry.FailedAfterConnect(err) {
		return retry.Retryable
	}
	return retry.Fatal
}

// streamV2Outcome classifies a failed response under Snowflake's retry policy.
// A 408 refused the request. A 499 is a proxy's, which may have passed the
// request on. A 404 whose body is not one of the API's error responses came
// from a proxy that did not pass the request on.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-error-handling#retry-policy
func streamV2Outcome(res *http.Response, body []byte) retry.Outcome {
	switch res.StatusCode {
	case http.StatusRequestTimeout, 499, http.StatusTooManyRequests:
		return retry.Retryable
	case http.StatusNotFound:
		var mediaType, _, _ = mime.ParseMediaType(res.Header.Get("Content-Type"))
		var e struct{ Code, Message string }
		if mediaType == "text/html" ||
			mediaType == "application/json" && json.Unmarshal(body, &e) == nil && e.Code == "" && e.Message == "" {
			return retry.Retryable
		}
		return retry.Fatal
	}
	if res.StatusCode >= 500 {
		return retry.Retryable
	}
	return retry.Fatal
}
