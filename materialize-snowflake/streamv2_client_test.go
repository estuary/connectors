package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	"github.com/golang-jwt/jwt/v5"
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
	var c, err = newStreamV2Client(cfg, ts.URL, "ACCT")
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
	require.NoError(t, c.call(context.Background(), http.MethodGet, "probe", nil, nil, nil, &res))
	require.True(t, res.OK)
	require.Equal(t, int32(2), tokens.Load())

	// A second 401 in a row is an error, not a loop.
	tokens.Store(0)
	var c2 = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"code":"TOKEN_REVOKED","message":"revoked"}`)
	})
	var err = c2.call(context.Background(), http.MethodGet, "probe", nil, nil, nil, nil)
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

	require.NoError(t, c.call(context.Background(), http.MethodGet, "probe", nil, nil, nil, nil))
	require.Equal(t, int32(2), calls.Load())
	require.GreaterOrEqual(t, time.Since(first), 2*time.Second)
}

// TestStreamV2ClientRefreshesAheadOfExpiry covers a scoped token within
// streamV2TokenRefreshMargin of its expiry: the next request exchanges a fresh one.
func TestStreamV2ClientRefreshesAheadOfExpiry(t *testing.T) {
	var exchanges atomic.Int32
	var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) })
	var mux = http.NewServeMux()
	var ts = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	mux.HandleFunc("GET /v2/streaming/hostname", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, ts.Listener.Addr().String()) })
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
		// Each token expires one minute out, inside the refresh margin.
		var claims = jwt.RegisteredClaims{
			ID:        fmt.Sprint(exchanges.Add(1)),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		}
		var token, err = jwt.NewWithClaims(jwt.SigningMethodNone, claims).SignedString(jwt.UnsafeAllowNoneSignatureType)
		require.NoError(t, err)
		fmt.Fprint(w, token)
	})
	c.accountURL = ts.URL

	var ctx = context.Background()
	var first, _, err = c.bearer(ctx)
	require.NoError(t, err)
	second, _, err := c.bearer(ctx)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	require.Equal(t, int32(2), exchanges.Load())
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
				if r.Method == http.MethodPut {
					fmt.Fprintf(w, `{"next_continuation_token":"c1","channel_status":%s}`, status)
				} else {
					fmt.Fprintf(w, `{"channel_statuses":{"CH":%s}}`, status)
				}
			})
			var ctx = context.Background()

			var opened, err = c.OpenChannel(ctx, "DB", "SCH", "T", "CH")
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

// TestStreamV2Scope covers the OAuth scope of the token exchange. Snowflake
// resolves the role in it with SQL identifier rules, so the role must reach the
// scope exactly as configured, neither case-folded nor unquoted.
func TestStreamV2Scope(t *testing.T) {
	for _, tc := range []struct {
		role, want string
	}{
		{"", "ingest.example.com"},
		{"estuary_role", "session:role:estuary_role ingest.example.com"},
		{"ESTUARY_ROLE", "session:role:ESTUARY_ROLE ingest.example.com"},
		{`"Quoted_Role"`, `session:role:"Quoted_Role" ingest.example.com`},
	} {
		require.Equal(t, tc.want, streamV2Scope(tc.role, "ingest.example.com"), "role %q", tc.role)
	}
}

// TestStreamV2ClientNamesARefusedRole covers a token exchange refused because
// the user does not hold the configured role. The response mirrors the shape
// Snowflake returns.
func TestStreamV2ClientNamesARefusedRole(t *testing.T) {
	for _, tc := range []struct {
		role    string
		wantErr string
	}{
		{"estuary_role", `role "estuary_role" is not granted to user "U", or does not exist (unquoted role names are case-insensitive; quoted names must match exactly)`},
		{"", "HTTP 401, 390186"},
	} {
		var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {})
		var mux = http.NewServeMux()
		var ts = httptest.NewServer(mux)
		t.Cleanup(ts.Close)
		mux.HandleFunc("GET /v2/streaming/hostname", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, ts.Listener.Addr().String()) })
		mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"data":null,"code":"390186","message":"Role 'X' specified in the connect string is not granted to this user, or is not permitted for the credentials being used.","success":false,"headers":null}`)
		})
		c.accountURL = ts.URL
		c.role = tc.role

		var _, _, err = c.bearer(context.Background())
		require.ErrorContains(t, err, tc.wantErr, "role %q", tc.role)
		var apiErr *streamV2APIError
		require.ErrorAs(t, err, &apiErr)
		if tc.role == "" {
			require.NotContains(t, err.Error(), "is not granted to user")
		}
	}
}

// TestStreamV2ClientBoundsAHungTokenExchange covers a token endpoint that never
// answers: the exchange fails at streamV2TokenExchangeTimeout instead of waiting
// out every retry of send.
func TestStreamV2ClientBoundsAHungTokenExchange(t *testing.T) {
	var prior = streamV2TokenExchangeTimeout
	streamV2TokenExchangeTimeout = 200 * time.Millisecond
	t.Cleanup(func() { streamV2TokenExchangeTimeout = prior })

	var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {})
	var mux = http.NewServeMux()
	var ts = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	var release = make(chan struct{})
	t.Cleanup(func() { close(release) })
	mux.HandleFunc("GET /v2/streaming/hostname", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, ts.Listener.Addr().String()) })
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	c.accountURL = ts.URL

	var done = make(chan error, 1)
	go func() {
		var _, _, err = c.bearer(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("the token exchange outlived its timeout")
	}
}

// TestStreamV2ClientRejectsWhitespaceInRole covers a role containing whitespace,
// which the space-separated OAuth scope cannot carry intact, so the token exchange
// fails.
func TestStreamV2ClientRejectsWhitespaceInRole(t *testing.T) {
	for _, role := range []string{"my role", `"my role"`, "my\trole", " ESTUARY_ROLE"} {
		var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {})
		c.role = role
		var _, _, err = c.bearer(context.Background())
		require.ErrorContains(t, err, fmt.Sprintf("role %q contains whitespace", role))
	}
}

// TestStreamV2ClientWaitCommitPollsOnePipeOnce covers a commit wait on several
// channels of one pipe: each poll is a single bulk status request naming every
// channel still awaited.
func TestStreamV2ClientWaitCommitPollsOnePipeOnce(t *testing.T) {
	var polls [][]string
	var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Names []string `json:"channel_names"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		polls = append(polls, req.Names)

		// CH0 commits on the first poll, CH1 on the second.
		var statuses = map[string]map[string]string{}
		for _, name := range req.Names {
			var token = streamV2FormatOffsetToken(1, fullKeyRange)
			if name == "CH0" || len(polls) > 1 {
				token = streamV2FormatOffsetToken(2, fullKeyRange)
			}
			statuses[name] = map[string]string{"channel_status_code": "SUCCESS", "last_committed_offset_token": token}
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"channel_statuses": statuses}))
	})
	for _, name := range []string{"CH0", "CH1"} {
		c.channels[name] = &streamV2OpenChannel{pipePath: streamV2PipePath("DB", "SCH", "T")}
	}

	var want = streamV2FormatOffsetToken(2, fullKeyRange)
	var statuses, err = c.WaitCommit(context.Background(), map[string]string{"CH0": want, "CH1": want})
	require.NoError(t, err)
	require.Len(t, statuses, 2)
	require.Len(t, polls, 2)
	require.ElementsMatch(t, []string{"CH0", "CH1"}, polls[0])
	require.Equal(t, []string{"CH1"}, polls[1])
}

// TestStreamV2ClientWaitCommitFailsOnAForeignKeyRange covers a channel whose
// committed offset token carries the awaited offset under another key range,
// which only another writer can have committed. The wait fails at once.
func TestStreamV2ClientWaitCommitFailsOnAForeignKeyRange(t *testing.T) {
	var half = streamV2Range{keyBegin: 0, keyEnd: math.MaxUint32 / 2}
	var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"channel_statuses":{"CH":{"channel_status_code":"SUCCESS","last_committed_offset_token":%q}}}`,
			streamV2FormatOffsetToken(2, half))
	})
	c.channels["CH"] = &streamV2OpenChannel{pipePath: streamV2PipePath("DB", "SCH", "T")}

	var ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var _, err = c.WaitCommit(ctx, map[string]string{"CH": streamV2FormatOffsetToken(2, fullKeyRange)})
	require.ErrorContains(t, err, "something other than this task is appending to the channel")
}
