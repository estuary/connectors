package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestClassifyStreamV2(t *testing.T) {
	var apiErr = func(status int, contentType, body string) error {
		var res = &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {contentType}}}
		return &streamV2APIError{Status: status, outcome: streamV2Outcome(res, []byte(body))}
	}

	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{"408", apiErr(http.StatusRequestTimeout, "application/json", `{"code":"TIMEOUT","message":"m"}`), retry.Retryable},
		{"429", apiErr(http.StatusTooManyRequests, "application/json", `{"code":"THROTTLED","message":"m"}`), retry.Retryable},
		{"429 with Retry-After", fmt.Errorf("POST x: %w", retry.WithDelay(apiErr(http.StatusTooManyRequests, "", ""), time.Second)), retry.Retryable},
		{"499", apiErr(499, "application/json", `{"code":"CLOSED","message":"m"}`), retry.Retryable},
		{"503", apiErr(http.StatusServiceUnavailable, "text/html", `<html></html>`), retry.Retryable},
		{"404 from a proxy", apiErr(http.StatusNotFound, "text/html; charset=utf-8", `<html>not found</html>`), retry.Retryable},
		{"404 with an empty JSON error", apiErr(http.StatusNotFound, "application/json", `{"code":"","message":""}`), retry.Retryable},
		{"404 from the API", apiErr(http.StatusNotFound, "application/json", `{"code":"ERR_TABLE_DOES_NOT_EXIST","message":"m"}`), retry.Fatal},
		{"400", apiErr(http.StatusBadRequest, "application/json", `{"code":"BAD","message":"m"}`), retry.Fatal},
		{"401", apiErr(http.StatusUnauthorized, "application/json", `{"code":"390186","message":"m"}`), retry.Fatal},
		{"channel status error", &streamV2APIError{Code: "ERR_CHANNEL_DOES_NOT_EXIST"}, retry.Fatal},
		{"refused connection", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, retry.Retryable},
		{"reset connection", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, retry.Retryable},
		{"failed response read", &streamV2ReadError{errors.New("http2: stream closed")}, retry.Retryable},
		{"cancelled", context.Canceled, retry.Fatal},
	} {
		require.Equal(t, tt.want, classifyStreamV2(tt.err), tt.name)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
}

// TestStreamV2SendRepeatsTheSameRequest covers an append whose connection
// reset after it was sent: the retry presents the same continuation token,
// request ID and rows, which Snowflake de-duplicates.
func TestStreamV2SendRepeatsTheSameRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type sent struct{ url, body string }
		var requests []sent
		var c = &streamV2Client{http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			var body, err = io.ReadAll(r.Body)
			require.NoError(t, err)
			requests = append(requests, sent{r.URL.String(), string(body)})
			if len(requests) == 1 {
				return nil, &net.OpError{Op: "read", Err: syscall.ECONNRESET}
			}
			return testResponse(http.StatusOK, `{"next_continuation_token":"c2"}`), nil
		})}}

		var out, err = c.send(t.Context(), http.MethodPost, "https://ingest/v2/streaming/rows?continuationToken=c1&requestId=r1", bytes.NewReader([]byte("rows")), nil)
		require.NoError(t, err)
		require.JSONEq(t, `{"next_continuation_token":"c2"}`, string(out))
		require.Len(t, requests, 2)
		require.Equal(t, requests[0], requests[1])
		require.Equal(t, "rows", requests[1].body)
	})
}

// TestStreamV2SendBoundsEachAttempt covers a request that never answers: each
// attempt ends at its own timeout and is retried.
func TestStreamV2SendBoundsEachAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls int
		var c = &streamV2Client{http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			<-r.Context().Done()
			return nil, r.Context().Err()
		})}}

		var start = time.Now()
		var _, err = c.send(t.Context(), http.MethodGet, "https://ingest/v2/streaming/probe", nil, nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorContains(t, err, "after 6 attempts")
		require.Equal(t, 6, calls)
		require.GreaterOrEqual(t, time.Since(start), 6*streamV2Retry.AttemptTimeout)
	})
}

func TestStagePolicies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls int
		var do = func(p retry.Policy, err error) error {
			calls = 0
			return p.Do(t.Context(), "op", func(context.Context) error {
				calls++
				return err
			})
		}

		var putErr = errors.New("failed to upload")
		require.ErrorIs(t, do(putRetry, putErr), putErr)
		require.Equal(t, 4, calls)

		var lookupErr = errors.New("finding pipe")
		require.ErrorIs(t, do(pipeVisibleRetry, lookupErr), lookupErr)
		require.Equal(t, 1, calls)

		var start = time.Now()
		require.ErrorIs(t, do(pipeVisibleRetry, errPipeNotVisible), errPipeNotVisible)
		require.GreaterOrEqual(t, calls, int(pipeVisibleRetry.MaxElapsed/pipeVisibleRetry.MaxBackoff))
		require.Less(t, time.Since(start), pipeVisibleRetry.MaxElapsed)
	})
}
