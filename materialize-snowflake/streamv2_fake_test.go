package connector

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// fakeSnowpipe is an in-memory Snowflake behind the Snowpipe Streaming REST API,
// which commits appends instantly.
//
// FAKE_SNOWPIPE_STATE names a JSON file to keep channel state in, which is what
// lets this fake's "Snowflake" outlive a manager: a channel reopened by a later
// session reports the committed offset token an earlier one left it with, and a
// channel dropped by one session is gone for all of them. The file is read before
// and written after every request, so tests may edit it between sessions.
//
// Fault injection via FAKE_SNOWPIPE_MODE:
//
//	commit_never_lands  accepts appends but never advances the committed token
//
// A row carrying the REJECT column stands in for a row Snowflake's ingestion
// discards: the append and the commit both succeed and the committed token still
// advances over it, but the channel's row-error count moves. The count a channel
// starts out with is seeded by FAKE_SNOWPIPE_ROWS_ERROR_COUNT.
type fakeSnowpipe struct {
	t            *testing.T
	mu           sync.Mutex
	statePath    string
	mode         string
	seededErrors int64
	continuation map[string]int
	state        fakeSnowpipeState
}

type fakeSnowpipeState struct {
	Committed map[string]string `json:"committed"`
	Errors    map[string]int64  `json:"errors"`
}

var fakeSnowpipeDecoder, _ = zstd.NewReader(nil)

// fakeSnowpipeLargestAppend is the largest uncompressed append body any fake has
// received, so a test can check the batch cap.
var fakeSnowpipeLargestAppend atomic.Int64

// fakeTestKey is a private key for the fake, which accepts any token.
var fakeTestKey = sync.OnceValue(func() string {
	var key, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	var der, _ = x509.MarshalPKCS8PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
})

// fakeStreamV2Credentials gives cfg credentials that the fake Snowpipe Streaming
// API accepts.
func fakeStreamV2Credentials(cfg *config) {
	cfg.Credentials.AuthType = snowflake_auth.JWT
	cfg.Credentials.PrivateKey = fakeTestKey()
	cfg.Credentials.User = "TEST_USER"
}

// fakeStreamV2Backend points the client of the manager at a fake Snowpipe
// Streaming API.
func fakeStreamV2Backend(t *testing.T, m *streamV2Manager) {
	t.Helper()
	m.client.accountURL = newFakeSnowpipe(t).URL
}

func newFakeSnowpipe(t *testing.T) *httptest.Server {
	t.Helper()
	var f = &fakeSnowpipe{
		t:            t,
		statePath:    os.Getenv("FAKE_SNOWPIPE_STATE"),
		mode:         os.Getenv("FAKE_SNOWPIPE_MODE"),
		continuation: make(map[string]int),
		state:        fakeSnowpipeState{Committed: map[string]string{}, Errors: map[string]int64{}},
	}
	if seeded := os.Getenv("FAKE_SNOWPIPE_ROWS_ERROR_COUNT"); seeded != "" {
		var n, err = strconv.ParseInt(seeded, 10, 64)
		require.NoError(t, err)
		f.seededErrors = n
	}

	var mux = http.NewServeMux()
	var ts = httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	mux.HandleFunc("GET /v2/streaming/hostname", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, ts.Listener.Addr().String())
	})
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "fake-scoped-token")
	})
	mux.HandleFunc("PUT /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}", f.open)
	mux.HandleFunc("DELETE /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}", f.drop)
	mux.HandleFunc("POST /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}", f.status)
	mux.HandleFunc("POST /v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/rows", f.append)
	return ts
}

// locked runs fn with the state loaded from the state file, and writes the state
// back afterwards.
func (f *fakeSnowpipe) locked(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statePath != "" {
		if raw, err := os.ReadFile(f.statePath); err == nil {
			require.NoError(f.t, json.Unmarshal(raw, &f.state))
		}
	}
	fn()
	if f.statePath != "" {
		var raw, err = json.Marshal(f.state)
		require.NoError(f.t, err)
		require.NoError(f.t, os.WriteFile(f.statePath, raw, 0o644))
	}
}

func (f *fakeSnowpipe) channelStatus(channel string) map[string]any {
	var errs, seeded = f.state.Errors[channel]
	if !seeded {
		errs = f.seededErrors
	}
	var status = map[string]any{
		"channel_status_code":         "SUCCESS",
		"last_committed_offset_token": nil,
		"rows_error_count":            errs,
		"last_error_message":          nil,
	}
	if token, ok := f.state.Committed[channel]; ok {
		status["last_committed_offset_token"] = token
	}
	if errs > 0 {
		status["last_error_message"] = "fake rejection"
	}
	return status
}

func (f *fakeSnowpipe) reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func (f *fakeSnowpipe) fail(w http.ResponseWriter, status int, code, message string) {
	f.reply(w, status, map[string]string{"code": code, "message": message})
}

func (f *fakeSnowpipe) open(w http.ResponseWriter, r *http.Request) {
	var channel = r.PathValue("channel")
	if strings.HasPrefix(r.PathValue("pipe"), "UNSUPPORTED") {
		f.fail(w, 400, "unsupported_table", "table not supported")
		return
	}
	f.locked(func() {
		f.continuation[channel]++
		f.reply(w, 200, map[string]any{
			"next_continuation_token": fmt.Sprint(f.continuation[channel]),
			"channel_status":          f.channelStatus(channel),
		})
	})
}

func (f *fakeSnowpipe) drop(w http.ResponseWriter, r *http.Request) {
	var channel = r.PathValue("channel")
	f.locked(func() {
		delete(f.state.Committed, channel)
		f.state.Errors[channel] = 0
		f.reply(w, 200, map[string]any{})
	})
}

func (f *fakeSnowpipe) status(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.PathValue("pipe"), ":bulk-channel-status") {
		http.NotFound(w, r)
		return
	}
	var req struct {
		Names []string `json:"channel_names"`
	}
	require.NoError(f.t, json.NewDecoder(r.Body).Decode(&req))
	f.locked(func() {
		var statuses = make(map[string]any)
		for _, name := range req.Names {
			statuses[name] = f.channelStatus(name)
		}
		f.reply(w, 200, map[string]any{"channel_statuses": statuses})
	})
}

func (f *fakeSnowpipe) append(w http.ResponseWriter, r *http.Request) {
	var channel = r.PathValue("channel")
	var query = r.URL.Query()
	var body, err = io.ReadAll(r.Body)
	require.NoError(f.t, err)
	if r.Header.Get("Content-Encoding") == "zstd" {
		body, err = fakeSnowpipeDecoder.DecodeAll(body, nil)
		require.NoError(f.t, err)
	}
	fakeSnowpipeLargestAppend.Store(max(fakeSnowpipeLargestAppend.Load(), int64(len(body))))

	var rejected int64
	var scanner = bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(nil, 64*1024*1024)
	for scanner.Scan() {
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			f.fail(w, 400, "invalid_rows", "row is not a JSON object: "+err.Error())
			return
		}
		if _, ok := row["REJECT"]; ok {
			rejected++
		}
	}
	if len(body) > 0 && body[len(body)-1] != '\n' {
		f.fail(w, 400, "invalid_rows", "last row lacks a newline")
		return
	}

	f.locked(func() {
		if query.Get("continuationToken") != fmt.Sprint(f.continuation[channel]) {
			f.fail(w, 400, "STALE_CONTINUATION_TOKEN_SEQUENCER", "stale continuation token")
			return
		}
		f.continuation[channel]++
		if f.mode != "commit_never_lands" {
			f.state.Committed[channel] = query.Get("endOffsetToken")
		}
		var errs, seeded = f.state.Errors[channel]
		if !seeded {
			errs = f.seededErrors
		}
		f.state.Errors[channel] = errs + rejected
		f.reply(w, 200, map[string]any{"next_continuation_token": fmt.Sprint(f.continuation[channel])})
	})
}
