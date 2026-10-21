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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	log "github.com/sirupsen/logrus"
)

// A request is retried up to streamV2MaxAttempts times, each bounded by
// streamV2RequestTimeout, so one call can take several times longer than a
// commit wait before it fails. The two bounds are independent on purpose: a
// commit wait is one short status poll after another, while a single append
// carries megabytes and may legitimately take the whole request timeout.
const (
	streamV2RequestTimeout     = 2 * time.Minute
	streamV2CommitPollInterval = time.Second
	streamV2MaxAttempts        = 6
)

// streamV2MaxAppendBytes is the API's limit on the compressed body of one append.
const streamV2MaxAppendBytes = 4 * 1024 * 1024

// streamV2WaitCommitTimeout bounds one wait for a commit. It is a var so that
// tests can shorten it.
var streamV2WaitCommitTimeout = 10 * time.Minute

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
	ingestURL  string
	key        *rsa.PrivateKey
	user       string
	account    string
	role       string
	zstd       *zstd.Encoder

	// tokenMu guards the scoped token, its expiry, the ingest host and the
	// refreshing flag. It is released while a refresh is on the wire, and the flag
	// is what keeps a second caller from starting a refresh of its own.
	tokenMu    sync.Mutex
	token      string
	expiry     time.Time
	refreshing bool

	mu       sync.Mutex // guards channels
	channels map[string]*streamV2OpenChannel
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
	key, err := cfg.Credentials.ParsePrivateKey()
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
	return &streamV2Client{
		http:       &http.Client{Timeout: streamV2RequestTimeout, Transport: transport},
		accountURL: accountURL,
		key:        key,
		user:       cfg.Credentials.User,
		account:    account,
		role:       cfg.Role,
		zstd:       enc,
		channels:   make(map[string]*streamV2OpenChannel),
	}, nil
}

// bearer returns a scoped token and the ingest host's URL. The first call
// discovers the ingest host and exchanges a JWT for a token. Later calls return
// the token they find, and the one that first finds it within
// streamV2TokenRefreshMargin of expiry refreshes it while the others keep using
// the current token. A caller blocks only when no token is valid.
func (c *streamV2Client) bearer(ctx context.Context) (token, ingestURL string, err error) {
	c.tokenMu.Lock()
	var remaining = time.Until(c.expiry)
	if remaining > streamV2TokenRefreshMargin || (c.refreshing && remaining > 0) {
		defer c.tokenMu.Unlock()
		return c.token, c.ingestURL, nil
	}
	for c.refreshing {
		// Another caller is refreshing and the current token is expired, so wait
		// for that refresh.
		c.tokenMu.Unlock()
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return "", "", ctx.Err()
		}
		c.tokenMu.Lock()
		if time.Until(c.expiry) > 0 {
			defer c.tokenMu.Unlock()
			return c.token, c.ingestURL, nil
		}
	}
	c.refreshing = true
	ingestURL = c.ingestURL
	c.tokenMu.Unlock()

	token, expiry, err := c.refreshToken(ctx, &ingestURL)

	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	c.refreshing = false
	if err != nil {
		return "", "", err
	}
	c.ingestURL, c.token, c.expiry = ingestURL, token, expiry
	return token, ingestURL, nil
}

// refreshToken exchanges a fresh JWT for a scoped token and reports the token
// with its expiry. It discovers the ingest host when *ingestURL is empty.
func (c *streamV2Client) refreshToken(ctx context.Context, ingestURL *string) (string, time.Time, error) {
	jwtToken, _, err := generateJWTToken(c.key, c.user, c.account)
	if err != nil {
		return "", time.Time{}, err
	}
	var jwtHeaders = http.Header{
		"Authorization":                        {"Bearer " + jwtToken},
		"X-Snowflake-Authorization-Token-Type": {"KEYPAIR_JWT"},
	}

	if *ingestURL == "" {
		host, err := c.send(ctx, "GET", c.accountURL+"/v2/streaming/hostname", nil, jwtHeaders)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("discovering the ingest host: %w", err)
		}
		var scheme, _, _ = strings.Cut(c.accountURL, "://")
		*ingestURL = scheme + "://" + strings.TrimSpace(string(host))
	}

	var scope = strings.TrimPrefix(strings.TrimPrefix(*ingestURL, "https://"), "http://")
	if c.role != "" {
		scope = "session:role:" + c.role + " " + scope
	}
	var form = url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"scope":      {scope},
	}
	jwtHeaders.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := c.send(ctx, "POST", c.accountURL+"/oauth/token", []byte(form.Encode()), jwtHeaders)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("exchanging the JWT for a scoped token: %w", err)
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
	return token, expiry, nil
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
		token, ingestURL, err := c.bearer(ctx)
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
			// Another caller may have refreshed the token already, in which case
			// expiring it again would force one exchange per 401 received.
			c.tokenMu.Lock()
			if c.token == token {
				c.expiry = time.Time{}
			}
			c.tokenMu.Unlock()
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
		req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
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
				err = apiErr
				// Only the delay-seconds form of Retry-After is read; the HTTP-date
				// form falls through to the backoff.
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
	if err := c.call(ctx, "PUT", pipePath+"/channels/"+url.PathEscape(channelName), nil, []byte("{}"), nil, &res); err != nil {
		return nil, err
	}
	if err := res.Status.checkCode(channelName); err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.channels[channelName] = &streamV2OpenChannel{pipePath: pipePath, continuation: res.Continuation}
	c.mu.Unlock()
	return &res.Status, nil
}

func (c *streamV2Client) channel(channelName string) (*streamV2OpenChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.channels[channelName]; ok {
		return ch, nil
	}
	return nil, fmt.Errorf("channel %q is not open", channelName)
}

// AppendRows sends one batch of NDJSON rows to the channel. startToken and endToken
// are the offset tokens of the batch's first and last rows.
//
// A retry after an ambiguous failure resends the same continuation token and
// the same rows, which Snowflake accepts as a duplicate of the first delivery
// and buffers once. The same token with different rows is rejected.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#append-rows-to-a-named-channel
func (c *streamV2Client) AppendRows(ctx context.Context, channelName, startToken, endToken string, rows []byte) error {
	ch, err := c.channel(channelName)
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
		return fmt.Errorf("an append of %d bytes compresses to %d, over the API's limit of %d bytes, so a document this large cannot be written by snowpipe_streaming_v2", len(rows), len(compressed), streamV2MaxAppendBytes)
	}
	if err := c.call(ctx, "POST", "data/"+ch.pipePath+"/channels/"+url.PathEscape(channelName)+"/rows", query, compressed, headers, &res); err != nil {
		return err
	}
	ch.continuation = res.Continuation
	return nil
}

// ChannelStatus reports the channel's status as Snowflake holds it.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#get-named-channel-status-in-bulk
func (c *streamV2Client) ChannelStatus(ctx context.Context, channelName string) (*streamV2ChannelStatusResult, error) {
	ch, err := c.channel(channelName)
	if err != nil {
		return nil, err
	}

	body, _ := json.Marshal(map[string][]string{"channel_names": {channelName}})
	var res struct {
		Statuses map[string]*streamV2ChannelStatusResult `json:"channel_statuses"`
	}
	if err := c.call(ctx, "POST", ch.pipePath+":bulk-channel-status", nil, body, nil, &res); err != nil {
		return nil, err
	}
	var status, ok = res.Statuses[channelName]
	if !ok || status == nil {
		return nil, fmt.Errorf("channel %q is unknown to Snowflake", channelName)
	} else if err := status.checkCode(channelName); err != nil {
		return nil, err
	}
	return status, nil
}

// WaitCommit blocks until the channel's committed offset token equals token, and
// reports the channel's status as of that moment. Only this connector appends to
// the channel, so a committed offset beyond the one awaited means another writer
// has the channel, and the wait fails at once rather than at its timeout.
func (c *streamV2Client) WaitCommit(ctx context.Context, channelName, token string) (*streamV2ChannelStatusResult, error) {
	offset, _, ok := streamV2ParseOffsetToken(token)
	if !ok {
		return nil, fmt.Errorf("offset token %q is not one this write path formats", token)
	}
	ctx, cancel := context.WithTimeout(ctx, streamV2WaitCommitTimeout)
	defer cancel()

	for {
		status, err := c.ChannelStatus(ctx, channelName)
		if err != nil && ctx.Err() != nil {
			return nil, fmt.Errorf("offset token %s of channel %q was not committed within %s: %w", token, channelName, streamV2WaitCommitTimeout, ctx.Err())
		} else if err != nil {
			return nil, err
		} else if status.CommittedToken != nil && *status.CommittedToken == token {
			return status, nil
		} else if status.CommittedToken != nil {
			if committed, _, ok := streamV2ParseOffsetToken(*status.CommittedToken); ok && committed > offset {
				return nil, fmt.Errorf("channel %q reports committed offset token %s, beyond the awaited %s: something other than this task is appending to the channel", channelName, *status.CommittedToken, token)
			}
		}
		select {
		case <-time.After(streamV2CommitPollInterval):
		case <-ctx.Done():
			return nil, fmt.Errorf("offset token %s of channel %q was not committed within %s: %w", token, channelName, streamV2WaitCommitTimeout, ctx.Err())
		}
	}
}

// DropChannel drops a channel in Snowflake, which discards its committed offset
// token instead of leaving it for a later open of the same name to inherit.
//
// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#drop-a-named-channel
func (c *streamV2Client) DropChannel(ctx context.Context, database, schema, table, channelName string) error {
	var err = c.call(ctx, "DELETE", streamV2PipePath(database, schema, table)+"/channels/"+url.PathEscape(channelName), nil, nil, nil, nil)
	var apiErr *streamV2APIError
	if errors.As(err, &apiErr) && apiErr.Code == "ERR_CHANNEL_NO_LONGER_EXISTS" {
		err = nil // already gone, which is the outcome a drop wants
	}
	if err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.channels, channelName)
	c.mu.Unlock()
	return nil
}
