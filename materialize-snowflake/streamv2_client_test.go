package connector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	"github.com/stretchr/testify/require"
)

// newTestStreamV2Client builds a client against handler, which serves both the
// account host and the ingest host.
func newTestStreamV2Client(t *testing.T, handler http.HandlerFunc) *streamV2Client {
	t.Helper()
	var mux = http.NewServeMux()
	var ts = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	mux.HandleFunc("GET /v2/streaming/hostname", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, ts.Listener.Addr().String())
	})
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "token-"+r.Header.Get("X-Test-Token"))
	})
	mux.HandleFunc("/", handler)

	var cfg = &config{Credentials: &snowflake_auth.CredentialConfig{
		AuthType: snowflake_auth.JWT, User: "U", PrivateKey: fakeTestKey(),
	}}
	c, err := newStreamV2Client(cfg, ts.URL, "ACCT")
	require.NoError(t, err)
	return c
}

// TestStreamV2ClientRefreshesARevokedToken covers a 401 before the token's
// expiry: the request is retried once with a freshly exchanged token.
func TestStreamV2ClientRefreshesARevokedToken(t *testing.T) {
	var tokens atomic.Int32
	var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {
		if tokens.Add(1) == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"code":"TOKEN_REVOKED","message":"revoked"}`)
			return
		}
		fmt.Fprint(w, `{"ok":true}`)
	})

	var res struct{ OK bool }
	require.NoError(t, c.call(context.Background(), "GET", "probe", nil, nil, nil, &res))
	require.True(t, res.OK)
	require.Equal(t, int32(2), tokens.Load())

	// A second 401 in a row is an error, not a loop.
	tokens.Store(0)
	var c2 = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"code":"TOKEN_REVOKED","message":"revoked"}`)
	})
	var err = c2.call(context.Background(), "GET", "probe", nil, nil, nil, nil)
	require.ErrorContains(t, err, "TOKEN_REVOKED")
}

// TestStreamV2ClientHonorsRetryAfter covers throttling: a 429 with a Retry-After
// header is retried no sooner than the header says.
func TestStreamV2ClientHonorsRetryAfter(t *testing.T) {
	var calls atomic.Int32
	var first time.Time
	var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			first = time.Now()
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"code":"THROTTLED","message":"slow down"}`)
			return
		}
		fmt.Fprint(w, `{}`)
	})

	require.NoError(t, c.call(context.Background(), "GET", "probe", nil, nil, nil, nil))
	require.Equal(t, int32(2), calls.Load())
	require.GreaterOrEqual(t, time.Since(first), 2*time.Second)
}

// TestStreamV2ClientRefreshDoesNotBlockValidCallers covers a refresh begun ahead
// of expiry: while one caller's exchange is on the wire, other callers keep using
// the current token instead of queueing behind the exchange.
func TestStreamV2ClientRefreshDoesNotBlockValidCallers(t *testing.T) {
	var exchanges atomic.Int32
	var release = make(chan struct{})
	var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) })
	// Replace the token endpoint with one that blocks until released.
	var mux = http.NewServeMux()
	var ts = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	mux.HandleFunc("GET /v2/streaming/hostname", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, ts.Listener.Addr().String()) })
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
		if exchanges.Add(1) > 1 {
			<-release
		}
		fmt.Fprintf(w, "token-%d", exchanges.Load())
	})
	c.accountURL = ts.URL

	var ctx = context.Background()
	first, _, err := c.bearer(ctx)
	require.NoError(t, err)
	require.Equal(t, "token-1", first)

	// Move the token inside the refresh margin but not past expiry.
	c.tokenMu.Lock()
	c.expiry = time.Now().Add(time.Minute)
	c.tokenMu.Unlock()

	var refreshed = make(chan string)
	go func() {
		tok, _, err := c.bearer(ctx)
		require.NoError(t, err)
		refreshed <- tok
	}()
	// Wait for the refresher to be on the wire, then check that another caller is
	// handed the current token at once.
	require.Eventually(t, func() bool { return exchanges.Load() == 2 }, 5*time.Second, 10*time.Millisecond)
	var done = make(chan string)
	go func() { tok, _, _ := c.bearer(ctx); done <- tok }()
	select {
	case tok := <-done:
		require.Equal(t, "token-1", tok)
	case <-time.After(2 * time.Second):
		t.Fatal("a caller with a valid token blocked behind the refresh")
	}
	close(release)
	require.Equal(t, "token-2", <-refreshed)
}

// TestStreamV2PipePath covers object names that Snowflake resolves only when
// quoted, because the REST API reads each path segment as a SQL identifier.
func TestStreamV2PipePath(t *testing.T) {
	for _, tc := range []struct {
		database, schema, table string
		want                    string
	}{
		{"DB", "SCH", "MY_TABLE", `databases/DB/schemas/SCH/pipes/%22MY_TABLE-STREAMING%22`},
		{"DB", "SCH", "my-table", `databases/DB/schemas/SCH/pipes/%22my-table-STREAMING%22`},
		{"DB", "SCH", "order", `databases/DB/schemas/SCH/pipes/%22order-STREAMING%22`},
		{"DB", "SCH", "Mixed-Case", `databases/DB/schemas/SCH/pipes/%22Mixed-Case-STREAMING%22`},
		{"DB", "SCH", `a"b`, `databases/DB/schemas/SCH/pipes/%22a%22%22b-STREAMING%22`},
		{"DB", "SCH", "with space", `databases/DB/schemas/SCH/pipes/%22with%20space-STREAMING%22`},
		{"DB", "zz-names", "my-table", `databases/DB/schemas/%22zz-names%22/pipes/%22my-table-STREAMING%22`},
		{"DB", "order", "T", `databases/DB/schemas/%22order%22/pipes/%22T-STREAMING%22`},
		// A simple database name stays unquoted, so Snowflake folds its case as
		// it does for the configured connection database.
		{"estuary_db", "SCH", "T", `databases/estuary_db/schemas/SCH/pipes/%22T-STREAMING%22`},
		{"my-db", "SCH", "T", `databases/%22my-db%22/schemas/SCH/pipes/%22T-STREAMING%22`},
	} {
		require.Equal(t, tc.want, streamV2PipePath(tc.database, tc.schema, tc.table),
			"%s.%s.%s", tc.database, tc.schema, tc.table)
	}
}

// TestStreamV2ClientChannelStatusCodes covers the channel_status_code that open
// and bulk status report. Only a code with the ERR_ prefix fails the call.
func TestStreamV2ClientChannelStatusCodes(t *testing.T) {
	for _, tc := range []struct {
		code    string
		wantErr bool
	}{
		{"SUCCESS", false},
		{"ACTIVE", false},
		{"ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER", true},
	} {
		t.Run(tc.code, func(t *testing.T) {
			var status = fmt.Sprintf(`{"channel_status_code":%q,"last_committed_offset_token":"1","last_error_message":"msg"}`, tc.code)
			var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PUT" {
					fmt.Fprintf(w, `{"next_continuation_token":"c1","channel_status":%s}`, status)
				} else {
					fmt.Fprintf(w, `{"channel_statuses":{"CH":%s}}`, status)
				}
			})
			var ctx = context.Background()

			opened, err := c.OpenChannel(ctx, "DB", "SCH", "T", "CH")
			if tc.wantErr {
				require.ErrorContains(t, err, tc.code)
				// Register the channel so bulk status can be exercised too.
				c.channels["CH"] = &streamV2OpenChannel{pipePath: streamV2PipePath("DB", "SCH", "T")}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.code, opened.StatusCode)
			}

			got, err := c.ChannelStatus(ctx, "CH")
			if tc.wantErr {
				require.ErrorContains(t, err, tc.code)
			} else {
				require.NoError(t, err)
				require.Equal(t, "1", *got.CommittedToken)
			}
		})
	}
}
