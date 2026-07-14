package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"resty.dev/v3"
)

func newTestSdkClient(t *testing.T, mux *http.ServeMux) (*sdkStreamClient, *httptest.Server) {
	t.Helper()

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	pkey, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	u, err := url.Parse(ts.URL)
	require.NoError(t, err)

	return &sdkStreamClient{
		r:        resty.New().SetDisableWarn(true),
		scheme:   "http",
		host:     u.Host,
		key:      pkey,
		user:     "TEST_USER",
		account:  "TEST_ACCOUNT",
		database: "TEST_DB",
	}, ts
}

func TestSdkStreamClient(t *testing.T) {
	ctx := context.Background()

	var appendedBodies [][]byte
	var appendedTokens []string
	committed := new(string)

	mux := http.NewServeMux()
	var serverHost string
	mux.HandleFunc("GET /v2/streaming/hostname", func(w http.ResponseWriter, r *http.Request) {
		require.Contains(t, r.Header.Get("Authorization"), "Bearer ")
		require.Equal(t, "KEYPAIR_JWT", r.Header.Get("X-Snowflake-Authorization-Token-Type"))
		w.Write([]byte(serverHost))
	})
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		require.Equal(t, "urn:ietf:params:oauth:grant-type:jwt-bearer", r.Form.Get("grant_type"))
		require.Equal(t, serverHost, r.Form.Get("scope"))
		w.Write([]byte("scoped-token"))
	})
	mux.HandleFunc("PUT /v2/streaming/databases/TEST_DB/schemas/TEST_SCHEMA/pipes/TBL-STREAMING/channels/CH", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer scoped-token", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"next_continuation_token": "0_1",
			"channel_status": map[string]any{
				"channel_status_code":         "SUCCESS",
				"last_committed_offset_token": nil,
				"rows_error_count":            0,
			},
		})
	})
	mux.HandleFunc("POST /v2/streaming/data/databases/TEST_DB/schemas/TEST_SCHEMA/pipes/TBL-STREAMING/channels/CH/rows", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "application/x-ndjson", r.Header.Get("Content-Type"))
		body := make([]byte, r.ContentLength)
		r.Body.Read(body)
		appendedBodies = append(appendedBodies, body)
		appendedTokens = append(appendedTokens, r.URL.Query().Get("offsetToken"))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status_code":             0,
			"message":                 "Data buffered",
			"next_continuation_token": fmt.Sprintf("0_%d", len(appendedTokens)+1),
		})
	})
	mux.HandleFunc("POST /v2/streaming/databases/TEST_DB/schemas/TEST_SCHEMA/pipes/TBL-STREAMING:bulk-channel-status", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ChannelNames []string `json:"channel_names"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, []string{"CH"}, req.ChannelNames)
		// Report the most recently appended token as committed.
		if len(appendedTokens) > 0 {
			*committed = appendedTokens[len(appendedTokens)-1]
		}
		var tok *string
		if *committed != "" {
			tok = committed
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"channel_statuses": map[string]any{
				"CH": map[string]any{
					"channel_status_code":         "SUCCESS",
					"last_committed_offset_token": tok,
					"rows_error_count":            0,
				},
			},
		})
	})

	c, ts := newTestSdkClient(t, mux)
	u, err := url.Parse(ts.URL)
	require.NoError(t, err)
	serverHost = u.Host

	open, err := c.openChannel(ctx, "TEST_SCHEMA", "TBL-STREAMING", "CH")
	require.NoError(t, err)
	require.Equal(t, "0_1", open.NextContinuationToken)
	require.Nil(t, open.ChannelStatus.LastCommittedOffsetToken)

	next, err := c.appendRows(ctx, "TEST_SCHEMA", "TBL-STREAMING", "CH", open.NextContinuationToken, "base:0", []byte(`{"A": 1}`+"\n"))
	require.NoError(t, err)
	require.Equal(t, "0_2", next)

	next, err = c.appendRows(ctx, "TEST_SCHEMA", "TBL-STREAMING", "CH", next, "base:1", []byte(`{"A": 2}`+"\n"))
	require.NoError(t, err)
	require.Equal(t, "0_3", next)

	require.Equal(t, []string{"base:0", "base:1"}, appendedTokens)
	require.Equal(t, [][]byte{[]byte(`{"A": 1}` + "\n"), []byte(`{"A": 2}` + "\n")}, appendedBodies)

	statuses, err := c.channelStatus(ctx, "TEST_SCHEMA", "TBL-STREAMING", []string{"CH"})
	require.NoError(t, err)
	require.Equal(t, "base:1", *statuses["CH"].LastCommittedOffsetToken)
}

func TestSdkStreamClientErrors(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		status int
		check  func(t *testing.T, err error)
	}{
		{
			name:   "permanent",
			status: http.StatusBadRequest,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrPermanent)
				require.ErrorContains(t, err, "no such pipe")
			},
		},
		{
			name:   "temporary server error",
			status: http.StatusServiceUnavailable,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrTemporary)
			},
		},
		{
			name:   "temporary throttle",
			status: http.StatusTooManyRequests,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrTemporary)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			var serverHost string
			mux.HandleFunc("GET /v2/streaming/hostname", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(serverHost))
			})
			mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("scoped-token"))
			})
			mux.HandleFunc("PUT /v2/streaming/databases/TEST_DB/schemas/TEST_SCHEMA/pipes/TBL-STREAMING/channels/CH", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				json.NewEncoder(w).Encode(map[string]any{"code": "390404", "message": "no such pipe"})
			})

			c, ts := newTestSdkClient(t, mux)
			u, err := url.Parse(ts.URL)
			require.NoError(t, err)
			serverHost = u.Host

			_, err = c.openChannel(ctx, "TEST_SCHEMA", "TBL-STREAMING", "CH")
			tt.check(t, err)
		})
	}
}

func TestSdkWaitForTokenCommitted(t *testing.T) {
	ctx := context.Background()

	makeStatus := func(committed *string, errorRows int) map[string]any {
		return map[string]any{
			"channel_statuses": map[string]any{
				"CH": map[string]any{
					"channel_status_code":         "SUCCESS",
					"last_committed_offset_token": committed,
					"rows_error_count":            errorRows,
					"last_error_message":          "row was rejected",
				},
			},
		}
	}
	strptr := func(s string) *string { return &s }

	tests := []struct {
		name      string
		responses []map[string]any
		check     func(t *testing.T, err error)
	}{
		{
			name: "committed after polling",
			responses: []map[string]any{
				makeStatus(nil, 0),
				makeStatus(strptr("base:0"), 0),
				makeStatus(strptr("base:1"), 0),
			},
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "error rows fail loudly",
			responses: []map[string]any{
				makeStatus(nil, 2),
			},
			check: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "2 rows that could not be ingested")
				require.ErrorContains(t, err, "row was rejected")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var call int
			mux := http.NewServeMux()
			mux.HandleFunc("POST /v2/streaming/databases/TEST_DB/schemas/TEST_SCHEMA/pipes/TBL-STREAMING:bulk-channel-status", func(w http.ResponseWriter, r *http.Request) {
				res := tt.responses[min(call, len(tt.responses)-1)]
				call++
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(res)
			})

			c, _ := newTestSdkClient(t, mux)
			c.ingestHost = c.host
			c.scopedToken = "scoped-token"
			c.expiry = time.Now().Add(time.Hour)

			m := &sdkStreamManager{c: c, channelName: "CH"}
			stream := &sdkTableStream{schema: "TEST_SCHEMA", pipe: "TBL-STREAMING"}
			tt.check(t, m.waitForTokenCommitted(ctx, stream, "base:1"))
		})
	}
}
