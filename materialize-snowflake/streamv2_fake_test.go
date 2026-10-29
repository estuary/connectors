package connector

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
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
	"time"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	"github.com/estuary/connectors/go/blob"
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
	stage        *fakeStage
}

// fakeStage is an in-memory stage bucket, keyed by object name.
type fakeStage struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (s *fakeStage) Upload(_ context.Context, key string, r io.Reader, _ ...blob.WriterOption) error {
	var data, err = io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = data
	return nil
}

func (s *fakeStage) get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var data, ok = s.objects[key]
	return data, ok
}

// fakeSnowpipeFileAppends counts the file appends every fake has received, so a
// test can check which path an append took.
var fakeSnowpipeFileAppends atomic.Int64

type fakeSnowpipeState struct {
	Committed map[string]string `json:"committed"`
	Errors    map[string]int64  `json:"errors"`
}

// fakeSnowpipeGunzip decompresses a gzip append body or staged file.
func fakeSnowpipeGunzip(compressed []byte) ([]byte, error) {
	var r, err = gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

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
	var ts, f = newFakeSnowpipe(t)
	m.client.accountURL = ts.URL
	m.client.newUploader = func(context.Context, streamV2StageLocation) (streamV2Uploader, error) {
		return f.stage, nil
	}
}

func newFakeSnowpipe(t *testing.T) (*httptest.Server, *fakeSnowpipe) {
	t.Helper()
	var f = &fakeSnowpipe{
		t:            t,
		statePath:    os.Getenv("FAKE_SNOWPIPE_STATE"),
		mode:         os.Getenv("FAKE_SNOWPIPE_MODE"),
		continuation: make(map[string]int),
		state:        fakeSnowpipeState{Committed: map[string]string{}, Errors: map[string]int64{}},
		stage:        &fakeStage{objects: make(map[string][]byte)},
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
	mux.HandleFunc("POST /v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/filebuffer", f.appendFile)
	return ts, f
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
	if strings.HasSuffix(r.PathValue("pipe"), ":pipe-info") {
		f.pipeInfo(w, r)
		return
	} else if !strings.HasSuffix(r.PathValue("pipe"), ":bulk-channel-status") {
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
	if r.Header.Get("Content-Encoding") == "gzip" {
		body, err = fakeSnowpipeGunzip(body)
		require.NoError(f.t, err)
	}
	f.ingest(w, channel, query.Get("continuationToken"), query.Get("startOffsetToken"), query.Get("endOffsetToken"), body)
}

// pipeInfo lends the caller the fake's stage. The location type is one no real
// uploader handles, so a client must have been given the fake's stage directly.
func (f *fakeSnowpipe) pipeInfo(w http.ResponseWriter, r *http.Request) {
	var req struct {
		EncryptionVersion string `json:"encryption_version"`
	}
	require.NoError(f.t, json.NewDecoder(r.Body).Decode(&req))
	if req.EncryptionVersion == "" {
		f.fail(w, 400, "ERR_UNSUPPORTED_ENCRYPTION_VERSION", "")
		return
	}
	f.reply(w, 200, map[string]any{
		"rowset_stage_location": map[string]any{
			"location_type":  "FAKE",
			"bucket_name":    "fake-stage",
			"path_prefix":    "streamingrowset/" + r.PathValue("pipe") + "/",
			"creds":          map[string]string{},
			"expiry_time_ms": time.Now().Add(time.Hour).UnixMilli(),
		},
		"pipe_encryption_info": map[string]any{
			"pipe_key":           fakePipeKey,
			"pipe_key_id":        "fake-key-id",
			"diversifier":        fakePipeDiversifier,
			"encryption_version": req.EncryptionVersion,
		},
	})
}

// The key material every fake pipe lends.
var (
	fakePipeKey         = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	fakePipeDiversifier = "fake-diversifier"
)

// appendFile ingests the one fragment of a file uploaded to the fake's stage, as
// append ingests a request body.
func (f *fakeSnowpipe) appendFile(w http.ResponseWriter, r *http.Request) {
	var channel = r.PathValue("channel")
	var fragments []streamV2FileFragment
	require.NoError(f.t, json.NewDecoder(r.Body).Decode(&fragments))
	require.Len(f.t, fragments, 1)
	var fragment = fragments[0]

	var name, found = strings.CutPrefix(fragment.FilePath, "fake-stage/")
	require.True(f.t, found, "file path %q is not in the fake stage", fragment.FilePath)
	var body, ok = f.stage.get(name)
	if !ok {
		f.fail(w, 400, "ERR_FILE_NOT_FOUND", "no such staged file: "+fragment.FilePath)
		return
	}
	require.Equal(f.t, fragment.FileLengthBytes, len(body))
	require.Equal(f.t, fmt.Sprintf("%x", md5.Sum(body)), fragment.FileEtag)

	require.JSONEq(f.t, `"fake-key-id"`, string(fragment.MasterKeyID))
	require.Equal(f.t, fakePipeDiversifier, fragment.Diversifier)
	require.Equal(f.t, streamV2EncryptionVersion, fragment.EncryptionVersion)
	var key, err = streamV2FileKey(fakePipeKey)
	require.NoError(f.t, err)
	iv, err := base64.StdEncoding.DecodeString(fragment.IVBase64)
	require.NoError(f.t, err)
	block, err := aes.NewCipher(key)
	require.NoError(f.t, err)
	var plain = make([]byte, len(body))
	cipher.NewCTR(block, iv).XORKeyStream(plain, body)
	body = plain

	// Snowflake detects the compression from the file's contents.
	if decoded, err := fakeSnowpipeGunzip(body); err == nil {
		body = decoded
	}
	require.Equal(f.t, fragment.FragmentRowCount, bytes.Count(body, []byte{'\n'}))
	fakeSnowpipeFileAppends.Add(1)
	f.ingest(w, channel, r.URL.Query().Get("continuationToken"), fragment.FragmentStartOffsetToken, fragment.FragmentEndOffsetToken, body)
}

// ingest validates the NDJSON rows of one append and that its offset tokens span
// exactly those rows, checks and advances the channel's continuation token, and
// commits the append's end offset token.
func (f *fakeSnowpipe) ingest(w http.ResponseWriter, channel, continuation, startToken, endToken string, body []byte) {
	var rejected, rows int64
	var scanner = bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(nil, 64*1024*1024)
	for scanner.Scan() {
		rows++
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
	var start, _, startOK = streamV2ParseOffsetToken(startToken)
	var end, _, endOK = streamV2ParseOffsetToken(endToken)
	require.True(f.t, startOK && endOK, "offset tokens %q and %q", startToken, endToken)
	require.Equal(f.t, rows, end-start+1, "offset tokens %q to %q span a different number of rows than the append carries", startToken, endToken)

	f.locked(func() {
		if continuation != fmt.Sprint(f.continuation[channel]) {
			f.fail(w, 400, "STALE_CONTINUATION_TOKEN_SEQUENCER", "stale continuation token")
			return
		}
		f.continuation[channel]++
		if f.mode != "commit_never_lands" {
			f.state.Committed[channel] = endToken
		}
		var errs, seeded = f.state.Errors[channel]
		if !seeded {
			errs = f.seededErrors
		}
		f.state.Errors[channel] = errs + rejected
		f.reply(w, 200, map[string]any{"next_continuation_token": fmt.Sprint(f.continuation[channel])})
	})
}
