package client

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetryableStatus(t *testing.T) {
	for _, tc := range []struct {
		status    int
		retryable bool
	}{
		{status: http.StatusOK},
		{status: http.StatusBadRequest},
		{status: http.StatusUnauthorized},
		{status: http.StatusTooManyRequests, retryable: true},
		{status: http.StatusInternalServerError, retryable: true},
		{status: http.StatusBadGateway, retryable: true},
		{status: http.StatusServiceUnavailable, retryable: true},
		{status: http.StatusGatewayTimeout, retryable: true},
	} {
		require.Equal(t, tc.retryable, retryableStatus(tc.status), "status %d", tc.status)
	}
}
