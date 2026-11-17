package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"

	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestRetryPolicies(t *testing.T) {
	var apiErr = func(code int) error {
		return fmt.Errorf("updating: %w", &googleapi.Error{Code: code})
	}

	for _, tt := range []struct {
		name              string
		err               error
		requestRetries    bool
		idempotentRetries bool
	}{
		{name: "rate limited", err: apiErr(http.StatusTooManyRequests), requestRetries: true, idempotentRetries: true},
		{name: "server error", err: apiErr(http.StatusServiceUnavailable), requestRetries: false, idempotentRetries: true},
		{name: "bad request", err: apiErr(http.StatusBadRequest), requestRetries: false, idempotentRetries: false},
		{name: "not an API error", err: errors.New("connection reset"), requestRetries: false, idempotentRetries: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				for _, p := range []struct {
					name    string
					policy  retry.Policy
					retries bool
				}{
					{"requestRetry", requestRetry, tt.requestRetries},
					{"idempotentRetry", idempotentRetry, tt.idempotentRetries},
				} {
					var calls int
					var err = p.policy.Do(t.Context(), "op", func(context.Context) error {
						calls++
						return tt.err
					})
					require.ErrorIs(t, err, tt.err, p.name)
					require.Equal(t, p.retries, calls > 1, p.name)
				}
			})
		})
	}
}
