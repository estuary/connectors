package client

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{"rate limited", &StatusError{StatusCode: http.StatusTooManyRequests}, retry.Retryable},
		{"internal error", fmt.Errorf("wrapped: %w", &StatusError{StatusCode: http.StatusInternalServerError}), retry.Retryable},
		{"bad gateway", &StatusError{StatusCode: http.StatusBadGateway}, retry.Fatal},
		{"bad request", &StatusError{StatusCode: http.StatusBadRequest}, retry.Fatal},
		{"transport error", errors.New("connection reset"), retry.Fatal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classify(tt.err))
		})
	}
}

// statusTransport answers each request with the next status in its list,
// then with 200 and an empty embeddings response.
type statusTransport struct {
	statuses []int
	calls    int
}

func (s *statusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var status, body = http.StatusOK, `{"data":[{"embedding":[1],"index":0}]}`
	if s.calls < len(s.statuses) {
		status, body = s.statuses[s.calls], `{"error":{"message":"nope"}}`
	}
	s.calls++
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

func TestCreateEmbeddingsRetries(t *testing.T) {
	for _, tt := range []struct {
		name      string
		statuses  []int
		wantCalls int
		wantErr   int
	}{
		{"recovers from rate limits and internal errors", []int{429, 500, 429}, 4, 0},
		{"gives up after eleven attempts", []int{429, 429, 429, 429, 429, 429, 429, 429, 429, 429, 429, 429}, 11, 429},
		{"bad request is not retried", []int{400}, 1, 400},
		{"bad gateway is not retried", []int{502}, 1, 502},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var transport = &statusTransport{statuses: tt.statuses}
				var c = NewOpenAiClient("model", "", "key")
				c.http = &http.Client{Transport: transport}

				embeddings, err := c.CreateEmbeddings(t.Context(), []string{"doc"})
				require.Equal(t, tt.wantCalls, transport.calls)
				if tt.wantErr == 0 {
					require.NoError(t, err)
					require.Len(t, embeddings, 1)
					return
				}
				statusErr, ok := errors.AsType[*StatusError](err)
				require.True(t, ok)
				require.Equal(t, tt.wantErr, statusErr.StatusCode)
			})
		})
	}
}
