package connector

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	log "github.com/sirupsen/logrus"
)

// A request is retried up to streamV2MaxAttempts times, each bounded by
// streamV2RequestTimeout. The request timeout is independent of the commit wait,
// because one append carries megabytes and may take the whole timeout, while a
// commit wait is a series of short status polls.
const (
	streamV2RequestTimeout     = 2 * time.Minute
	streamV2CommitPollInterval = 5 * time.Second
	streamV2MaxAttempts        = 6
)

// streamV2MaxAppendBytes is Snowflake's limit on the compressed body of one append
// request. Snowflake refuses a larger body outright, whatever its encoding.
const streamV2MaxAppendBytes = 4 * 1024 * 1024

// streamV2WaitCommitTimeout bounds one wait for a commit. It is a var so that
// tests can shorten it.
var streamV2WaitCommitTimeout = 10 * time.Minute

// streamV2TokenExchangeTimeout bounds one token exchange, retries included,
// because every request that needs a token waits on the exchange. It is a var so
// that tests can shorten it.
var streamV2TokenExchangeTimeout = 30 * time.Second

// streamV2MaxErrorMessageBytes bounds the message of an error response, whose
// body may be an intermediary's whole HTML page.
const streamV2MaxErrorMessageBytes = 1 << 10

// streamV2RoleNotGrantedCode is the error code of a token exchange refused
// because the user does not hold the requested role.
const streamV2RoleNotGrantedCode = "390186"

// streamV2TokenRefreshMargin is how long before the scoped token expires that a
// refresh starts.
const streamV2TokenRefreshMargin = 5 * time.Minute

// streamV2APIError is an error the Snowpipe Streaming REST API reported, either
// as a non-2xx response or as a channel status code with the ERR_ prefix, in which
// case Status is zero.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#error-response-structure
type streamV2APIError struct {
	Status  int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *streamV2APIError) Error() string {
	if e.Status == 0 {
		return fmt.Sprintf("snowpipe streaming API error (%s): %s", e.Code, e.Message)
	}
	return fmt.Sprintf("snowpipe streaming API error (HTTP %d, %s): %s", e.Status, e.Code, e.Message)
}

// streamV2ChannelStatusResult is Snowflake's status for a channel. RowsErrorCount
// counts the rows Snowflake's ingestion rejected over the whole life of the
// channel. It is cumulative, and survives both a channel reopen and a new client
// session.
type streamV2ChannelStatusResult struct {
	StatusCode       string  `json:"channel_status_code"`
	CommittedToken   *string `json:"last_committed_offset_token"`
	RowsErrorCount   int64   `json:"rows_error_count"`
	LastErrorMessage string  `json:"last_error_message"`
}

// checkCode fails a status whose code has the ERR_ prefix. Any other code but
// SUCCESS is logged and accepted, because Snowflake does not document the codes
// it reports.
func (s *streamV2ChannelStatusResult) checkCode(channelName string) error {
	if strings.HasPrefix(s.StatusCode, "ERR_") {
		return &streamV2APIError{Code: s.StatusCode, Message: s.LastErrorMessage}
	} else if s.StatusCode != "SUCCESS" {
		log.WithFields(log.Fields{"channel": channelName, "code": s.StatusCode, "message": s.LastErrorMessage}).Warn("snowpipe streaming channel reported an unexpected status code")
	}
	return nil
}

// validateRejectedRows fails a channel on which Snowflake has ever rejected a row,
// because those rows are gone and only a backfill resets the count.
func (s *streamV2ChannelStatusResult) validateRejectedRows(channelName, table string) error {
	if s.RowsErrorCount == 0 {
		return nil
	}
	return fmt.Errorf(
		"channel %q had %d row(s) rejected and discarded by Snowflake, which reported: %s. Those rows are not in %s, and Snowflake's committed offset token has advanced past them, so they cannot be identified and re-sent. Backfill this binding",
		channelName, s.RowsErrorCount, s.LastErrorMessage, table,
	)
}

// streamV2Client is a client of the Snowpipe Streaming REST API. Each open
// channel carries the continuation token its next append must present, so
// appends to one channel must not run concurrently.
//
// Specifically, this client interacts with Snowpipe Streaming Named Channels.
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#named-channel-endpoints
type streamV2Client struct {
	http       *http.Client
	accountURL string
	key        *rsa.PrivateKey
	user       string
	account    string
	role       string
	zstd       *zstd.Encoder

	// ingestMu guards ingestURL, the ingest host's URL, which is discovered on
	// first use.
	ingestMu  sync.Mutex
	ingestURL string

	// tokenMu guards token, the scoped token or nil if none is held, and
	// serializes its exchange.
	tokenMu sync.Mutex
	token   *streamV2Token

	// channelsMu guards channels.
	channelsMu sync.Mutex
	channels   map[string]*streamV2OpenChannel

	// pipesMu guards pipes, the stage and key material held per pipe path.
	pipesMu sync.Mutex
	pipes   map[string]*streamV2PipeInfo
	// newUploader opens a stage location for upload. It is a field so that tests
	// can substitute an in-memory stage.
	newUploader func(ctx context.Context, stage streamV2StageLocation) (streamV2Uploader, error)
}

type streamV2OpenChannel struct {
	pipePath string
	// mu serializes appends, because each one must present the continuation
	// token the one before it returned.
	mu           sync.Mutex
	continuation string
}

// streamV2PipePath is the path of a table's default pipe under /v2/streaming/.
// database, schema and table are unquoted names. Snowflake reads each path
// segment as a SQL identifier, so a segment is quoted under the same rule as SQL.
func streamV2PipePath(database, schema, table string) string {
	return fmt.Sprintf("databases/%s/schemas/%s/pipes/%s",
		streamV2PathIdentifier(database), streamV2PathIdentifier(schema), streamV2PathIdentifier(table+streamV2DefaultPipeSuffix))
}

// streamV2PathIdentifier is name as a path-escaped SQL identifier.
func streamV2PathIdentifier(name string) string {
	if !isSimpleIdentifier(name) {
		name = `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
	return url.PathEscape(name)
}

func newStreamV2Client(cfg *config, accountURL, account string) (*streamV2Client, error) {
	var key, err = cfg.Credentials.ParsePrivateKey()
	if err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(4<<20))
	if err != nil {
		return nil, err
	}
	// Every channel appends to the one ingest host, so the default of two idle
	// connections per host would reconnect for most appends.
	var transport = http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 64
	var c = &streamV2Client{
		http:        &http.Client{Timeout: streamV2RequestTimeout, Transport: transport},
		accountURL:  accountURL,
		key:         key,
		user:        cfg.Credentials.User,
		account:     account,
		role:        cfg.Role,
		zstd:        enc,
		channels:    make(map[string]*streamV2OpenChannel),
		pipes:       make(map[string]*streamV2PipeInfo),
		newUploader: newStreamV2Uploader,
	}
	return c, nil
}

// streamV2Token is a scoped token for the ingest host.
type streamV2Token struct {
	access string
	expiry time.Time
}

// bearer returns a scoped token and the ingest host's URL. It exchanges a fresh
// token only when it holds none outside streamV2TokenRefreshMargin of expiry.
func (c *streamV2Client) bearer(ctx context.Context) (string, string, error) {
	var ingestURL, err = c.ingest(ctx)
	if err != nil {
		return "", "", err
	}
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token == nil || time.Until(c.token.expiry) <= streamV2TokenRefreshMargin {
		if c.token, err = c.exchangeToken(ctx, ingestURL); err != nil {
			return "", "", err
		}
	}
	return c.token.access, ingestURL, nil
}

// discardToken drops token, so that the next bearer exchanges a fresh one. A
// token that was already replaced is left alone, so that callers holding the
// same revoked token cause only one exchange.
func (c *streamV2Client) discardToken(token string) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token != nil && c.token.access == token {
		c.token = nil
	}
}

// jwtHeaders authenticates a request to the account host with a freshly signed JWT.
func (c *streamV2Client) jwtHeaders() (http.Header, error) {
	var jwtToken, _, err = generateJWTToken(c.key, c.user, c.account)
	if err != nil {
		return nil, err
	}
	return http.Header{
		"Authorization":                        {"Bearer " + jwtToken},
		"X-Snowflake-Authorization-Token-Type": {"KEYPAIR_JWT"},
	}, nil
}

// ingest returns the ingest host's URL, discovering it on first use.
func (c *streamV2Client) ingest(ctx context.Context) (string, error) {
	c.ingestMu.Lock()
	defer c.ingestMu.Unlock()
	if c.ingestURL != "" {
		return c.ingestURL, nil
	}

	var headers, err = c.jwtHeaders()
	if err != nil {
		return "", err
	}
	host, err := c.send(ctx, http.MethodGet, c.accountURL+"/v2/streaming/hostname", nil, headers)
	if err != nil {
		return "", fmt.Errorf("discovering the ingest host: %w", err)
	}
	ingest, err := url.Parse(c.accountURL)
	if err != nil {
		return "", fmt.Errorf("parsing the account URL: %w", err)
	}
	ingest.Host = strings.TrimSpace(string(host))
	c.ingestURL = ingest.String()
	return c.ingestURL, nil
}

// exchangeToken exchanges a fresh JWT for a scoped token for ingestURL.
func (c *streamV2Client) exchangeToken(ctx context.Context, ingestURL string) (*streamV2Token, error) {
	// The OAuth scope is space-separated, so a role containing whitespace would
	// split into separate scope entries.
	if strings.ContainsFunc(c.role, unicode.IsSpace) {
		return nil, fmt.Errorf("role %q contains whitespace, which the space-separated Snowpipe Streaming token scope cannot carry", c.role)
	}
	var headers, err = c.jwtHeaders()
	if err != nil {
		return nil, err
	}
	ingest, err := url.Parse(ingestURL)
	if err != nil {
		return nil, fmt.Errorf("parsing the ingest host's URL: %w", err)
	}
	var form = url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"scope":      {streamV2Scope(c.role, ingest.Host)},
	}
	headers.Set("Content-Type", "application/x-www-form-urlencoded")
	ctx, cancel := context.WithTimeout(ctx, streamV2TokenExchangeTimeout)
	defer cancel()
	res, err := c.send(ctx, http.MethodPost, c.accountURL+"/oauth/token", []byte(form.Encode()), headers)
	if err != nil {
		err = fmt.Errorf("exchanging the JWT for a scoped token: %w", err)
		var apiErr *streamV2APIError
		if c.role != "" && errors.As(err, &apiErr) && apiErr.Status == http.StatusUnauthorized && apiErr.Code == streamV2RoleNotGrantedCode {
			err = fmt.Errorf("role %q is not granted to user %q, or does not exist (unquoted role names are case-insensitive; quoted names must match exactly): %w", c.role, c.user, err)
		}
		return nil, err
	}
	var token = strings.TrimSpace(string(res))

	// The scoped token is a JWT, so its own exp claim is the expiry. A token that
	// does not parse as one is given the lifetime Snowflake has been observed to
	// grant.
	var expiry = time.Now().Add(time.Hour)
	var claims jwt.RegisteredClaims
	if _, _, err := jwt.NewParser().ParseUnverified(token, &claims); err == nil && claims.ExpiresAt != nil {
		expiry = claims.ExpiresAt.Time
	}
	return &streamV2Token{access: token, expiry: expiry}, nil
}

// streamV2Scope is the OAuth scope of a token for ingestHost, assuming role
// when one is set. Snowflake resolves the role with SQL identifier rules, so it
// is passed exactly as configured.
func streamV2Scope(role, ingestHost string) string {
	var scope = ingestHost
	if role != "" {
		scope = "session:role:" + role + " " + scope
	}
	return scope
}

// call sends one authenticated request to the ingest host and decodes its JSON
// response into result, if result is not nil. A scoped token can be revoked
// before it expires, so a 401 is retried once with a fresh token.
func (c *streamV2Client) call(ctx context.Context, method, path string, query url.Values, body []byte, headers http.Header, result any) error {
	if headers == nil {
		headers = http.Header{}
	}
	if headers.Get("Content-Type") == "" && body != nil {
		headers.Set("Content-Type", "application/json")
	}

	var res []byte
	for attempt := 1; ; attempt++ {
		var token, ingestURL, err = c.bearer(ctx)
		if err != nil {
			return err
		}
		headers.Set("Authorization", "Bearer "+token)

		var u = ingestURL + "/v2/streaming/" + path
		if len(query) > 0 {
			u += "?" + query.Encode()
		}
		res, err = c.send(ctx, method, u, body, headers)
		var apiErr *streamV2APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusUnauthorized && attempt == 1 {
			c.discardToken(token)
			continue
		} else if err != nil {
			return err
		}
		break
	}
	if result != nil {
		if err := json.Unmarshal(res, result); err != nil {
			return fmt.Errorf("decoding %s %s response: %w", method, path, err)
		}
	}
	return nil
}

// send performs one HTTP request, retrying throttling, server errors and
// transport errors with exponential backoff, and returns the response body.
func (c *streamV2Client) send(ctx context.Context, method, u string, body []byte, headers http.Header) ([]byte, error) {
	var delay = time.Second
	for attempt := 1; ; attempt++ {
		var req, err = http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header[k] = v
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "EstuaryFlow/1.0")

		res, err := c.http.Do(req)
		var apiErr *streamV2APIError
		var retryAfter time.Duration
		if err == nil {
			var out []byte
			out, err = io.ReadAll(res.Body)
			res.Body.Close()
			if err == nil && res.StatusCode/100 == 2 {
				return out, nil
			} else if err == nil {
				apiErr = &streamV2APIError{Status: res.StatusCode, Message: strings.TrimSpace(string(out))}
				json.Unmarshal(out, apiErr)
				if len(apiErr.Message) > streamV2MaxErrorMessageBytes {
					apiErr.Message = strings.ToValidUTF8(apiErr.Message[:streamV2MaxErrorMessageBytes], "") + "…"
				}
				err = apiErr
				// Retry-After is honored in its delay-seconds form.
				if secs, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil {
					retryAfter = min(time.Duration(secs)*time.Second, time.Minute)
				}
			}
		}

		var retryable = apiErr == nil || apiErr.Status == 429 || apiErr.Status >= 500
		if !retryable || attempt == streamV2MaxAttempts || ctx.Err() != nil {
			return nil, fmt.Errorf("%s %s: %w", method, req.URL.Host+req.URL.Path, err)
		}
		log.WithFields(log.Fields{"method": method, "path": req.URL.Path, "attempt": attempt, "error": err.Error()}).Warn("snowpipe streaming request failed; retrying")
		select {
		case <-time.After(max(delay, retryAfter)):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		delay = min(2*delay, 30*time.Second)
	}
}

// OpenChannel opens a channel by name on the default pipe of the table, including
// one that an earlier session left in Snowflake, and reports its status.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#open-a-named-channel
func (c *streamV2Client) OpenChannel(ctx context.Context, database, schema, table, channelName string) (*streamV2ChannelStatusResult, error) {
	var pipePath = streamV2PipePath(database, schema, table)

	var res struct {
		Continuation string                      `json:"next_continuation_token"`
		Status       streamV2ChannelStatusResult `json:"channel_status"`
	}
	if err := c.call(ctx, http.MethodPut, pipePath+"/channels/"+url.PathEscape(channelName), nil, []byte("{}"), nil, &res); err != nil {
		return nil, err
	}
	if err := res.Status.checkCode(channelName); err != nil {
		return nil, err
	}

	c.channelsMu.Lock()
	c.channels[channelName] = &streamV2OpenChannel{pipePath: pipePath, continuation: res.Continuation}
	c.channelsMu.Unlock()
	return &res.Status, nil
}

func (c *streamV2Client) channel(channelName string) (*streamV2OpenChannel, error) {
	c.channelsMu.Lock()
	defer c.channelsMu.Unlock()
	if ch, ok := c.channels[channelName]; ok {
		return ch, nil
	}
	return nil, fmt.Errorf("channel %q is not open", channelName)
}

// AppendRows sends one batch of NDJSON rows to the channel. startToken and endToken
// are the offset tokens of the batch's first and last rows. A batch whose
// compressed body exceeds streamV2MaxAppendBytes is appended through the pipe's
// stage instead.
//
// A retry after an ambiguous failure resends the same continuation token and
// the same rows, which Snowflake accepts as a duplicate of the first delivery
// and buffers once. The same token with different rows is rejected.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#append-rows-to-a-named-channel
func (c *streamV2Client) AppendRows(ctx context.Context, channelName, startToken, endToken string, rows []byte) error {
	var ch, err = c.channel(channelName)
	if err != nil {
		return err
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()

	var query = url.Values{
		"continuationToken": {ch.continuation},
		"startOffsetToken":  {startToken},
		"endOffsetToken":    {endToken},
		"requestId":         {uuid.NewString()},
	}
	var headers = http.Header{
		"Content-Type":     {"application/x-ndjson"},
		"Content-Encoding": {"zstd"},
	}
	var res struct {
		Continuation string `json:"next_continuation_token"`
	}
	var compressed = c.zstd.EncodeAll(rows, make([]byte, 0, len(rows)/4))
	if len(compressed) > streamV2MaxAppendBytes {
		return c.appendFile(ctx, ch, channelName, startToken, endToken, rows, compressed)
	}
	if err := c.call(ctx, http.MethodPost, "data/"+ch.pipePath+"/channels/"+url.PathEscape(channelName)+"/rows", query, compressed, headers, &res); err != nil {
		return err
	}
	ch.continuation = res.Continuation
	return nil
}

// ChannelStatus reports the channel's status as Snowflake holds it.
func (c *streamV2Client) ChannelStatus(ctx context.Context, channelName string) (*streamV2ChannelStatusResult, error) {
	var statuses, err = c.channelStatuses(ctx, []string{channelName})
	if err != nil {
		return nil, err
	}
	return statuses[channelName], nil
}

// channelStatuses reports the status of each channel as Snowflake holds it, in one
// request. Every channel must be open on the same pipe.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#get-named-channel-status-in-bulk
func (c *streamV2Client) channelStatuses(ctx context.Context, channelNames []string) (map[string]*streamV2ChannelStatusResult, error) {
	var pipePath string
	for _, name := range channelNames {
		var ch, err = c.channel(name)
		if err != nil {
			return nil, err
		} else if pipePath != "" && ch.pipePath != pipePath {
			return nil, fmt.Errorf("channels %q are not all open on one pipe", channelNames)
		}
		pipePath = ch.pipePath
	}

	var body, _ = json.Marshal(map[string][]string{"channel_names": channelNames})
	var res struct {
		Statuses map[string]*streamV2ChannelStatusResult `json:"channel_statuses"`
	}
	if err := c.call(ctx, http.MethodPost, pipePath+":bulk-channel-status", nil, body, nil, &res); err != nil {
		return nil, err
	}
	for _, name := range channelNames {
		var status, ok = res.Statuses[name]
		if !ok || status == nil {
			return nil, fmt.Errorf("channel %q is unknown to Snowflake", name)
		} else if err := status.checkCode(name); err != nil {
			return nil, err
		}
	}
	return res.Statuses, nil
}

// WaitCommit blocks until the committed offset token of each channel in tokens
// equals the token it maps to, and reports each channel's status as of the moment
// it did. Every channel must be open on the same pipe, so that one request polls
// them all. Only this connector appends to the channels, so a committed token
// this write path did not format, or one other than the awaited token at or
// beyond its offset, means another writer has the channel, and the wait fails at
// once rather than at its timeout.
func (c *streamV2Client) WaitCommit(ctx context.Context, tokens map[string]string) (map[string]*streamV2ChannelStatusResult, error) {
	var offsets = make(map[string]int64, len(tokens))
	for channelName, token := range tokens {
		var offset, _, ok = streamV2ParseOffsetToken(token)
		if !ok {
			return nil, fmt.Errorf("offset token %q is not one this write path formats", token)
		}
		offsets[channelName] = offset
	}
	ctx, cancel := context.WithTimeout(ctx, streamV2WaitCommitTimeout)
	defer cancel()

	var committed = make(map[string]*streamV2ChannelStatusResult, len(tokens))
	for {
		var pending []string
		for channelName := range tokens {
			if _, ok := committed[channelName]; !ok {
				pending = append(pending, channelName)
			}
		}
		if len(pending) == 0 {
			return committed, nil
		}
		slices.Sort(pending)

		var statuses, err = c.channelStatuses(ctx, pending)
		if err != nil && ctx.Err() != nil {
			return nil, waitCommitError(ctx, pending)
		} else if err != nil {
			return nil, err
		}
		for _, channelName := range pending {
			var status, token = statuses[channelName], tokens[channelName]
			if status.CommittedToken == nil {
				continue
			} else if *status.CommittedToken == token {
				committed[channelName] = status
			} else if offset, _, ok := streamV2ParseOffsetToken(*status.CommittedToken); !ok {
				return nil, fmt.Errorf("channel %q reports committed offset token %q, which is not one this write path formats: something other than this task is appending to the channel", channelName, *status.CommittedToken)
			} else if offset >= offsets[channelName] {
				return nil, fmt.Errorf("channel %q reports committed offset token %s in place of the awaited %s: something other than this task is appending to the channel", channelName, *status.CommittedToken, token)
			}
		}
		if len(committed) == len(tokens) {
			return committed, nil
		}

		select {
		case <-time.After(streamV2CommitPollInterval):
		case <-ctx.Done():
			return nil, waitCommitError(ctx, pending)
		}
	}
}

// waitCommitError is the error of a commit wait on the pending channels that
// ended with ctx.
func waitCommitError(ctx context.Context, pending []string) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("offset tokens of channels %q were not committed within %s: %w", pending, streamV2WaitCommitTimeout, ctx.Err())
	}
	return fmt.Errorf("waiting for offset tokens of channels %q to commit: %w", pending, ctx.Err())
}

// DropChannel drops a channel in Snowflake, which discards its committed offset
// token instead of leaving it for a later open of the same name to inherit.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#drop-a-named-channel
func (c *streamV2Client) DropChannel(ctx context.Context, database, schema, table, channelName string) error {
	var err = c.call(ctx, http.MethodDelete, streamV2PipePath(database, schema, table)+"/channels/"+url.PathEscape(channelName), nil, nil, nil, nil)
	var apiErr *streamV2APIError
	if errors.As(err, &apiErr) && apiErr.Code == "ERR_CHANNEL_NO_LONGER_EXISTS" {
		err = nil // already gone, which is the outcome a drop wants
	}
	if err != nil {
		return err
	}
	c.channelsMu.Lock()
	delete(c.channels, channelName)
	c.channelsMu.Unlock()
	return nil
}
