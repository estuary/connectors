package main

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"resty.dev/v3"
)

// sdkChannelStatus is the per-channel status reported by the open-channel and
// bulk-channel-status endpoints of the high-performance streaming API.
type sdkChannelStatus struct {
	StatusCode                string  `json:"channel_status_code"`
	LastCommittedOffsetToken  *string `json:"last_committed_offset_token"`
	DatabaseName              string  `json:"database_name"`
	SchemaName                string  `json:"schema_name"`
	PipeName                  string  `json:"pipe_name"`
	ChannelName               string  `json:"channel_name"`
	RowsInserted              int     `json:"rows_inserted"`
	RowsParsed                int     `json:"rows_parsed"`
	RowsErrorCount            int     `json:"rows_error_count"`
	LastErrorOffsetUpperBound *string `json:"last_error_offset_upper_bound"`
	LastErrorMessage          *string `json:"last_error_message"`
	LastErrorTimestampMs      *int64  `json:"last_error_timestamp_ms"`
}

type sdkOpenChannelResponse struct {
	NextContinuationToken string           `json:"next_continuation_token"`
	ChannelStatus         sdkChannelStatus `json:"channel_status"`
}

type sdkAppendRowsResponse struct {
	StatusCode            int    `json:"status_code"`
	Message               string `json:"message"`
	NextContinuationToken string `json:"next_continuation_token"`
}

type sdkBulkChannelStatusResponse struct {
	ChannelStatuses map[string]sdkChannelStatus `json:"channel_statuses"`
}

// sdkApiError is the error document returned by the high-performance streaming
// API, e.g. {"code": "391902", "message": "..."}.
type sdkApiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`

	httpStatus int
}

func (e *sdkApiError) Error() string {
	return fmt.Sprintf("%s (code %s, http %d)", e.Message, e.Code, e.httpStatus)
}

func (e *sdkApiError) Unwrap() error {
	// Server-side and throttling errors are worth retrying; anything else is a
	// misconfiguration or a permanent refusal.
	if e.httpStatus >= 500 || e.httpStatus == http.StatusTooManyRequests {
		return ErrTemporary
	}
	return ErrPermanent
}

// sdkStreamClient talks to the Snowpipe Streaming high-performance REST API.
// Account-level endpoints (hostname discovery and scoped token exchange) go to
// the configured account host, while channel and ingestion endpoints go to the
// account's discovered ingest host.
type sdkStreamClient struct {
	r        *resty.Client
	host     string
	key      *rsa.PrivateKey
	user     string
	account  string
	database string

	mu          sync.Mutex
	ingestHost  string
	scopedToken string
	expiry      time.Time
}

func newSdkStreamClient(cfg *config, account string) (*sdkStreamClient, error) {
	key, err := cfg.Credentials.ParsePrivateKey()
	if err != nil {
		return nil, err
	}

	return &sdkStreamClient{
		r:        resty.New(),
		host:     cfg.Host,
		key:      key,
		user:     cfg.Credentials.User,
		account:  account,
		database: cfg.Database,
	}, nil
}

// authorize returns the ingest host and a scoped token for it, performing
// hostname discovery and token exchange as needed. Scoped tokens are derived
// from a key-pair JWT and share its lifetime, so both are refreshed together.
func (c *sdkStreamClient) authorize(ctx context.Context) (string, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Until(c.expiry).Minutes() >= 5 && c.ingestHost != "" {
		return c.ingestHost, c.scopedToken, nil
	}

	jwt, expiry, err := generateJWTToken(c.key, c.user, c.account)
	if err != nil {
		return "", "", err
	}

	if c.ingestHost == "" {
		res, err := c.r.NewRequest().
			WithContext(ctx).
			SetHeader("Authorization", "Bearer "+jwt).
			SetHeader("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT").
			SetHeader("Accept", "application/json").
			Get("https://" + c.host + "/v2/streaming/hostname")
		if err != nil {
			return "", "", fmt.Errorf("discovering ingest hostname: %w", err)
		} else if !res.IsSuccess() {
			return "", "", fmt.Errorf("discovering ingest hostname: %s: %s", res.Status(), res.String())
		}
		c.ingestHost = res.String()
	}

	res, err := c.r.NewRequest().
		WithContext(ctx).
		SetHeader("Authorization", "Bearer "+jwt).
		SetFormData(map[string]string{
			"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
			"scope":      c.ingestHost,
		}).
		Post("https://" + c.host + "/oauth/token")
	if err != nil {
		return "", "", fmt.Errorf("exchanging scoped token: %w", err)
	} else if !res.IsSuccess() {
		return "", "", fmt.Errorf("exchanging scoped token: %s: %s", res.Status(), res.String())
	}

	c.scopedToken = res.String()
	c.expiry = expiry

	return c.ingestHost, c.scopedToken, nil
}

func (c *sdkStreamClient) pipeURL(ingestHost, schema, pipe string) string {
	return fmt.Sprintf("https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s",
		ingestHost, url.PathEscape(c.database), url.PathEscape(schema), url.PathEscape(pipe))
}

// openChannel creates or re-opens a channel of the pipe, returning the
// continuation token for the next append and the channel's last committed
// offset token, which is nil for a brand-new channel.
func (c *sdkStreamClient) openChannel(ctx context.Context, schema, pipe, channel string) (*sdkOpenChannelResponse, error) {
	ingestHost, token, err := c.authorize(ctx)
	if err != nil {
		return nil, err
	}

	var out sdkOpenChannelResponse
	apiErr := &sdkApiError{}
	res, err := c.r.NewRequest().
		WithContext(ctx).
		SetHeader("Authorization", "Bearer "+token).
		SetHeader("Accept", "application/json").
		SetHeader("Content-Type", "application/json").
		SetBody(`{}`).
		SetResult(&out).
		SetError(apiErr).
		Put(c.pipeURL(ingestHost, schema, pipe) + "/channels/" + url.PathEscape(channel))
	if err != nil {
		return nil, fmt.Errorf("opening channel %q: %w", channel, err)
	} else if !res.IsSuccess() {
		apiErr.httpStatus = res.StatusCode()
		return nil, fmt.Errorf("opening channel %q: %w", channel, apiErr)
	}

	return &out, nil
}

// appendRows sends a batch of NDJSON-encoded rows. The batch is buffered
// durably only once a subsequent channel status reports offsetToken (or a
// later token) as committed.
func (c *sdkStreamClient) appendRows(ctx context.Context, schema, pipe, channel, continuationToken, offsetToken string, rows []byte) (string, error) {
	ingestHost, token, err := c.authorize(ctx)
	if err != nil {
		return "", err
	}

	var out sdkAppendRowsResponse
	apiErr := &sdkApiError{}
	res, err := c.r.NewRequest().
		WithContext(ctx).
		SetHeader("Authorization", "Bearer "+token).
		SetHeader("Accept", "application/json").
		SetHeader("Content-Type", "application/x-ndjson").
		SetQueryParams(map[string]string{
			"continuationToken": continuationToken,
			"offsetToken":       offsetToken,
		}).
		SetBody(rows).
		SetResult(&out).
		SetError(apiErr).
		Post(fmt.Sprintf("https://%s/v2/streaming/data/databases/%s/schemas/%s/pipes/%s/channels/%s/rows",
			ingestHost, url.PathEscape(c.database), url.PathEscape(schema), url.PathEscape(pipe), url.PathEscape(channel)))
	if err != nil {
		return "", fmt.Errorf("appending rows to channel %q: %w", channel, err)
	} else if !res.IsSuccess() {
		apiErr.httpStatus = res.StatusCode()
		return "", fmt.Errorf("appending rows to channel %q: %w", channel, apiErr)
	}

	return out.NextContinuationToken, nil
}

// channelStatus reports the current status of the named channels of the pipe.
func (c *sdkStreamClient) channelStatus(ctx context.Context, schema, pipe string, channels []string) (map[string]sdkChannelStatus, error) {
	ingestHost, token, err := c.authorize(ctx)
	if err != nil {
		return nil, err
	}

	var out sdkBulkChannelStatusResponse
	apiErr := &sdkApiError{}
	res, err := c.r.NewRequest().
		WithContext(ctx).
		SetHeader("Authorization", "Bearer "+token).
		SetHeader("Accept", "application/json").
		SetHeader("Content-Type", "application/json").
		SetBody(map[string]any{"channel_names": channels}).
		SetResult(&out).
		SetError(apiErr).
		Post(c.pipeURL(ingestHost, schema, pipe) + ":bulk-channel-status")
	if err != nil {
		return nil, fmt.Errorf("getting channel status for pipe %q: %w", pipe, err)
	} else if !res.IsSuccess() {
		apiErr.httpStatus = res.StatusCode()
		return nil, fmt.Errorf("getting channel status for pipe %q: %w", pipe, apiErr)
	}

	return out.ChannelStatuses, nil
}
