package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestWritePoints(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls int
			var bodies []*trackedBody
			c := testWriteClient(t, func(req *http.Request) (*http.Response, error) {
				calls++
				requireWriteRequest(t, req)
				body, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Equal(t, "readings,sensor=s1 temp=21 1767225600000000000\n", string(body))
				code := status
				if calls == 3 {
					code = http.StatusNoContent
				}
				responseBody := &trackedBody{Reader: strings.NewReader(`{"message":"try again"}`)}
				bodies = append(bodies, responseBody)
				return &http.Response{StatusCode: code, Header: http.Header{"Retry-After": []string{"0"}}, Body: responseBody}, nil
			})
			_, err := c.writePoints(context.Background(), "readings,sensor=s1 temp=21 1767225600000000000\n")
			require.NoError(t, err)
			require.Equal(t, 3, calls)
			for _, body := range bodies {
				require.True(t, body.closed)
			}
		})
	}
}

func TestWritePointsErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		calls  int
		err    string
	}{
		{"unauthorized", http.StatusUnauthorized, `{"message":"unauthorized access"}`, 1, "HTTP 401: unauthorized access"},
		{"type conflict", http.StatusUnprocessableEntity, `{"message":"field type conflict: temp is float"}`, 1, "HTTP 422: field type conflict: temp is float"},
		{"partial type conflict", http.StatusUnprocessableEntity, `{"message":"failure writing points to database: partial write: field type conflict: input field \"temp\" on measurement \"readings\" is type integer, already exists as type float dropped=1"}`, 1, "HTTP 422: failure writing points to database: partial write: field type conflict"},
		{"retention and conflict", http.StatusUnprocessableEntity, `{"message":"partial write: field type conflict dropped=1: partial write: points beyond retention policy dropped=1"}`, 1, "HTTP 422: partial write: field type conflict"},
		{"bad request", http.StatusBadRequest, "invalid line protocol", 1, "HTTP 400: invalid line protocol"},
		{"server error", http.StatusInternalServerError, "server error", maxWriteAttempts, "HTTP 500: server error"},
		{"unexpected success", http.StatusOK, "", 1, "HTTP 200"},
		{"retry limit", http.StatusTooManyRequests, `{"message":"quota exceeded"}`, maxWriteAttempts, "HTTP 429: quota exceeded"},
		{"unavailable limit", http.StatusServiceUnavailable, `{"message":"unavailable"}`, maxWriteAttempts, "HTTP 503: unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			c := testWriteClient(t, func(req *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Retry-After": []string{"0"}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			_, err := c.writePoints(context.Background(), "readings value=1i 0\n")
			require.ErrorContains(t, err, tc.err)
			require.Equal(t, tc.calls, calls)
		})
	}
	transportErr := fmt.Errorf("test transport failure")
	c := testWriteClient(t, func(req *http.Request) (*http.Response, error) { return nil, transportErr })
	_, err := c.writePoints(context.Background(), "readings value=1i 0\n")
	require.ErrorIs(t, err, transportErr)
}

func TestWritePointsBeyondRetention(t *testing.T) {
	var calls int
	c := testWriteClient(t, func(req *http.Request) (*http.Response, error) {
		calls++
		body := `{"code":"unprocessable entity","message":"failure writing points to database: partial write: points beyond retention policy dropped=2"}`
		return &http.Response{StatusCode: http.StatusUnprocessableEntity, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	// InfluxDB already wrote the points within the retention period.
	dropped, err := c.writePoints(context.Background(), "readings value=1i 0\n")
	require.NoError(t, err)
	require.Equal(t, 2, dropped)
	require.Equal(t, 1, calls)
}

func TestWritePointsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	c := testWriteClient(t, func(req *http.Request) (*http.Response, error) {
		calls++
		cancel()
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{"Retry-After": []string{"30"}}, Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
	})
	_, err := c.writePoints(ctx, "readings value=1i 0\n")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestDeletePoints(t *testing.T) {
	request := deleteRequest{
		Start: "2026-01-01T00:00:00Z", Stop: "2026-01-01T00:00:00Z",
		Predicate: `_measurement="readings" AND "sensor"="lab 1"`,
	}
	for _, tc := range []struct {
		name     string
		statuses []int
		err      string
	}{
		{"success", []int{http.StatusNoContent}, ""},
		{"rate limited", []int{http.StatusTooManyRequests, http.StatusNoContent}, ""},
		{"unavailable", []int{http.StatusServiceUnavailable, http.StatusNoContent}, ""},
		{"invalid predicate", []int{http.StatusBadRequest}, "deleting InfluxDB points: HTTP 400: test failure"},
		{"unauthorized", []int{http.StatusUnauthorized}, "HTTP 401: test failure"},
		{"unexpected success", []int{http.StatusOK}, "HTTP 200"},
		{"server error", []int{500, 500, 500, 500, 500}, "HTTP 500: test failure"},
		{"retry limit", []int{429, 429, 429, 429, 429}, "HTTP 429: test failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var bodies []*trackedBody
			c := testWriteClient(t, func(req *http.Request) (*http.Response, error) {
				requireDeleteRequest(t, req)
				var got deleteRequest
				require.NoError(t, json.NewDecoder(req.Body).Decode(&got))
				require.Equal(t, request, got)
				require.Less(t, len(bodies), len(tc.statuses))
				status := tc.statuses[len(bodies)]
				body := &trackedBody{Reader: strings.NewReader(`{"message":"test failure"}`)}
				bodies = append(bodies, body)
				return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"0"}}, Body: body}, nil
			})
			err := c.deletePoints(context.Background(), request)
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.err)
			}
			require.Len(t, bodies, len(tc.statuses))
			for _, body := range bodies {
				require.True(t, body.closed)
			}
		})
	}
	transportErr := fmt.Errorf("test delete transport failure")
	c := testWriteClient(t, func(*http.Request) (*http.Response, error) { return nil, transportErr })
	require.ErrorIs(t, c.deletePoints(context.Background(), request), transportErr)
}

func TestDeletePointsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	c := testWriteClient(t, func(req *http.Request) (*http.Response, error) {
		calls++
		cancel()
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{"Retry-After": []string{"30"}}, Body: http.NoBody}, nil
	})
	require.ErrorIs(t, c.deletePoints(ctx, deleteRequest{}), context.Canceled)
	require.Equal(t, 1, calls)
}

func TestPostNetworkRetries(t *testing.T) {
	for _, endpoint := range []string{"write", "delete"} {
		for _, failure := range []error{syscall.ECONNREFUSED, io.EOF, io.ErrUnexpectedEOF, context.DeadlineExceeded} {
			t.Run(endpoint+"/"+failure.Error(), func(t *testing.T) {
				var calls int
				c := testWriteClient(t, func(req *http.Request) (*http.Response, error) {
					body, err := io.ReadAll(req.Body)
					require.NoError(t, err)
					require.Equal(t, "replayed body", string(body))
					calls++
					if calls == 1 {
						return nil, failure
					}
					return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
				})
				require.NoError(t, c.post(t.Context(), endpoint, "text/plain", "replayed body", nil))
				require.Equal(t, 2, calls)
			})
		}
	}
}

func TestPostRetryAfter(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		header string
		want   time.Duration
		valid  bool
	}{
		{"0", 0, true},
		{"2", 2 * time.Second, true},
		{"120", maxRetryDelay, true},
		{"9223372036854775807", maxRetryDelay, true},
		{now.Add(5 * time.Second).Format(http.TimeFormat), 5 * time.Second, true},
		{now.Add(time.Hour).Format(http.TimeFormat), maxRetryDelay, true},
		{now.Add(-time.Hour).Format(http.TimeFormat), 0, true},
		{"", 0, false},
		{"invalid", 0, false},
		{"-1", 0, false},
	} {
		t.Run(tc.header, func(t *testing.T) {
			c := testWriteClient(t, func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": []string{tc.header}}, Body: http.NoBody}, nil
			})
			c.retry.MaxAttempts = 1
			_, err := c.writePoints(t.Context(), "readings value=1i 0\n")
			var delay retry.Delayer
			require.Equal(t, tc.valid, errors.As(err, &delay))
			if tc.valid {
				require.InDelta(t, tc.want, delay.RetryDelay(), float64(time.Second))
				require.LessOrEqual(t, delay.RetryDelay(), maxRetryDelay)
			}
		})
	}
}

func TestPostCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls int
	c := testWriteClient(t, func(req *http.Request) (*http.Response, error) {
		calls++
		time.AfterFunc(20*time.Millisecond, cancel)
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": []string{"30"}}, Body: http.NoBody}, nil
	})
	_, err := c.writePoints(ctx, "readings value=1i 0\n")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func testWriteClient(t *testing.T, roundTrip roundTripFunc) *client {
	t.Helper()
	cfg, err := resolveEndpointConfig(testEndpoint)
	require.NoError(t, err)
	c, err := cfg.client(&http.Client{Transport: roundTrip})
	require.NoError(t, err)
	t.Cleanup(c.httpClient.CloseIdleConnections)
	c.retry.InitialBackoff = 0
	c.retry.Jitter = 0
	return c
}

func requireWriteRequest(t *testing.T, req *http.Request) {
	t.Helper()
	require.Equal(t, http.MethodPost, req.Method)
	require.Equal(t, "/proxy/api/v2/write", req.URL.Path)
	require.Equal(t, "estuary + test", req.URL.Query().Get("org"))
	require.Equal(t, "sensors & lab", req.URL.Query().Get("bucket"))
	require.Equal(t, "ns", req.URL.Query().Get("precision"))
	require.Len(t, req.URL.Query(), 3)
	require.Equal(t, "Token made-up-token", req.Header.Get("Authorization"))
	require.Equal(t, "text/plain; charset=utf-8", req.Header.Get("Content-Type"))
}

func requireDeleteRequest(t *testing.T, req *http.Request) {
	t.Helper()
	require.Equal(t, http.MethodPost, req.Method)
	require.Equal(t, "/proxy/api/v2/delete", req.URL.Path)
	require.Equal(t, "estuary + test", req.URL.Query().Get("org"))
	require.Equal(t, "sensors & lab", req.URL.Query().Get("bucket"))
	require.Len(t, req.URL.Query(), 2)
	require.Equal(t, "Token made-up-token", req.Header.Get("Authorization"))
	require.Equal(t, "application/json", req.Header.Get("Content-Type"))
	require.Equal(t, "application/json", req.Header.Get("Accept"))
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}
