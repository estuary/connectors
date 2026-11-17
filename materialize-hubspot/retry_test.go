package hubspot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestRetryPolicies(t *testing.T) {
	var apiErr = func(code int) error {
		return fmt.Errorf("unexpected response: %w", &APIError{StatusCode: code})
	}

	for _, tt := range []struct {
		name            string
		err             error
		requestCalls    int
		idempotentCalls int
	}{
		{name: "rate limited", err: apiErr(http.StatusTooManyRequests), requestCalls: 7, idempotentCalls: 7},
		{name: "server error", err: apiErr(http.StatusBadGateway), requestCalls: 1, idempotentCalls: 7},
		{name: "bad request", err: apiErr(http.StatusBadRequest), requestCalls: 1, idempotentCalls: 1},
		{name: "not an API error", err: errors.New("connection reset"), requestCalls: 1, idempotentCalls: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				for _, p := range []struct {
					name   string
					policy retry.Policy
					want   int
				}{
					{"requestRetry", requestRetry, tt.requestCalls},
					{"idempotentRetry", idempotentRetry, tt.idempotentCalls},
				} {
					var calls int
					var err = p.policy.Do(t.Context(), "op", func(context.Context) error {
						calls++
						return tt.err
					})
					require.ErrorIs(t, err, tt.err, p.name)
					require.Equal(t, p.want, calls, p.name)
				}
			})
		})
	}
}

func TestRetryWaitsOutRateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var start = time.Now()
		var at []time.Duration
		require.NoError(t, requestRetry.Do(t.Context(), "op", func(context.Context) error {
			at = append(at, time.Since(start))
			if len(at) < 3 {
				return fmt.Errorf("unexpected response: %w", &APIError{StatusCode: http.StatusTooManyRequests})
			}
			return nil
		}))
		require.Len(t, at, 3)
		require.GreaterOrEqual(t, at[1]-at[0], 10*time.Second)
		require.GreaterOrEqual(t, at[2]-at[1], 10*time.Second)
	})
}
