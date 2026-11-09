package client

import (
	"errors"
	"net/http"
	"time"

	"github.com/estuary/connectors/go/retry"
)

// embeddingsRetry retries embedding requests that OpenAI rate limited or
// failed with an internal error. Creating embeddings changes nothing, so a
// request that failed after OpenAI processed it is safe to send again.
var embeddingsRetry = retry.Policy{
	Classify:       classify,
	MaxAttempts:    11,
	InitialBackoff: time.Second,
	MaxBackoff:     time.Minute,
	Jitter:         0.5,
}

func classify(err error) retry.Outcome {
	if statusErr, ok := errors.AsType[*StatusError](err); ok {
		switch statusErr.StatusCode {
		case http.StatusTooManyRequests, http.StatusInternalServerError:
			return retry.Retryable
		}
	}
	return retry.Fatal
}
