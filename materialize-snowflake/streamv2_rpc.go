package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// Default timeouts for sidecar RPCs. wait_commit is bounded by how long the
// high-performance streaming backend may take to durably commit a batch, so it
// is much longer than the others.
const (
	rpcTimeoutConfigure   = 60 * time.Second
	rpcTimeoutOpenChannel = 60 * time.Second
	rpcTimeoutAppend      = 120 * time.Second
	rpcTimeoutWaitCommit  = 10 * time.Minute
	rpcTimeoutShutdown    = 5 * time.Second
)

// sidecarError is a structured error returned by the sidecar. Code carries the
// sidecar's error classification (e.g. "auth") for logging and diagnostics; all
// sidecar errors are fatal to the streaming v2 write path.
type sidecarError struct {
	Code    string
	Message string
}

func (e *sidecarError) Error() string {
	return fmt.Sprintf("sidecar error (%s): %s", e.Code, e.Message)
}

type rpcRequest struct {
	ID     uint64 `json:"id"`
	Op     string `json:"op"`
	Params any    `json:"params,omitempty"`
}

type rpcResponse struct {
	ID     uint64          `json:"id"`
	OK     bool            `json:"ok"`
	Error  string          `json:"error,omitempty"`
	Code   string          `json:"code,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

// sidecarClient is an NDJSON request/response client for the Python sidecar.
// Requests may be issued concurrently from multiple goroutines; responses are
// correlated by id. Any transport-level failure poisons the client and fails
// all in-flight and future calls, consistent with the crash-only supervision
// policy.
type sidecarClient struct {
	conn net.Conn

	writeMu sync.Mutex
	enc     *json.Encoder

	mu      sync.Mutex
	nextID  uint64
	pending map[uint64]chan *rpcResponse
	broken  error // non-nil once the read loop has terminated

	// died is closed by the supervisor when the sidecar process exits, and
	// diedErr() reports the exit as an error including captured stderr.
	died    <-chan struct{}
	diedErr func() error
}

func newSidecarClient(conn net.Conn, died <-chan struct{}, diedErr func() error) *sidecarClient {
	var c = &sidecarClient{
		conn:    conn,
		enc:     json.NewEncoder(conn),
		pending: make(map[uint64]chan *rpcResponse),
		died:    died,
		diedErr: diedErr,
	}
	go c.readLoop()
	return c
}

func (c *sidecarClient) readLoop() {
	var scanner = bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)

	for scanner.Scan() {
		var res rpcResponse
		if err := json.Unmarshal(scanner.Bytes(), &res); err != nil {
			c.fail(fmt.Errorf("malformed sidecar response: %w", err))
			return
		}

		c.mu.Lock()
		ch, ok := c.pending[res.ID]
		delete(c.pending, res.ID)
		c.mu.Unlock()

		if ok {
			ch <- &res
		}
	}

	var err = scanner.Err()
	if err == nil {
		err = fmt.Errorf("sidecar connection closed")
	}
	c.fail(err)
}

// fail poisons the client: all in-flight calls fail immediately and future
// calls fail without touching the connection.
func (c *sidecarClient) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broken != nil {
		return
	}
	c.broken = err
	for id, ch := range c.pending {
		delete(c.pending, id)
		close(ch)
	}
}

func (c *sidecarClient) call(ctx context.Context, op string, params any, timeout time.Duration, result any) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.mu.Lock()
	if c.broken != nil {
		var err = c.broken
		c.mu.Unlock()
		return c.callError(op, err)
	}
	c.nextID++
	var id = c.nextID
	var ch = make(chan *rpcResponse, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	c.writeMu.Lock()
	var err = c.enc.Encode(rpcRequest{ID: id, Op: op, Params: params})
	c.writeMu.Unlock()
	if err != nil {
		c.fail(fmt.Errorf("writing sidecar request: %w", err))
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return c.callError(op, err)
	}

	select {
	case res, ok := <-ch:
		if !ok {
			c.mu.Lock()
			var err = c.broken
			c.mu.Unlock()
			return c.callError(op, err)
		} else if !res.OK {
			return &sidecarError{Code: res.Code, Message: res.Error}
		} else if result != nil && len(res.Result) > 0 {
			if err := json.Unmarshal(res.Result, result); err != nil {
				return fmt.Errorf("decoding sidecar %q result: %w", op, err)
			}
		}
		return nil
	case <-c.died:
		return c.callError(op, c.diedErr())
	case <-ctx.Done():
		// A timed-out RPC leaves the connection in an indeterminate state, so
		// poison the client rather than risk mismatched correlation later.
		var err = fmt.Errorf("timed out or cancelled: %w", ctx.Err())
		c.fail(err)
		return c.callError(op, err)
	}
}

func (c *sidecarClient) callError(op string, err error) error {
	// If the process has died, its exit status and stderr tail are more useful
	// than a bare I/O error.
	select {
	case <-c.died:
		if derr := c.diedErr(); derr != nil {
			return fmt.Errorf("sidecar %q failed: %w", op, derr)
		}
	default:
	}
	return fmt.Errorf("sidecar %q failed: %w", op, err)
}

type sidecarProfile struct {
	Account    string `json:"account"`
	User       string `json:"user"`
	URL        string `json:"url"`
	PrivateKey string `json:"private_key"`
	Role       string `json:"role,omitempty"`
}

func (c *sidecarClient) Configure(ctx context.Context, profile sidecarProfile, authToken string) error {
	return c.call(ctx, "configure", struct {
		Profile sidecarProfile `json:"profile"`
		Auth    string         `json:"auth"`
	}{profile, authToken}, rpcTimeoutConfigure, nil)
}

type openChannelResult struct {
	CommittedToken *string `json:"committed_token"`
}

// OpenChannel opens (or reopens) a channel on the auto-created default pipe of
// the given table, returning Snowflake's authoritative latest committed offset
// token, or nil if the channel has never committed.
func (c *sidecarClient) OpenChannel(ctx context.Context, database, schema, table, channel string) (*string, error) {
	var res openChannelResult
	if err := c.call(ctx, "open_channel", struct {
		Database string `json:"database"`
		Schema   string `json:"schema"`
		Table    string `json:"table"`
		Channel  string `json:"channel"`
	}{database, schema, table, channel}, rpcTimeoutOpenChannel, &res); err != nil {
		return nil, err
	}
	return res.CommittedToken, nil
}

// Append sends a batch of rows tagged with a single batch-level offset token.
// Rows are the JSON objects to ingest, one per row, keyed by column name.
func (c *sidecarClient) Append(ctx context.Context, channel, token string, rows []json.RawMessage) error {
	return c.call(ctx, "append", struct {
		Channel string            `json:"channel"`
		Token   string            `json:"token"`
		Rows    []json.RawMessage `json:"rows"`
	}{channel, token, rows}, rpcTimeoutAppend, nil)
}

type waitCommitResult struct {
	CommittedToken *string `json:"committed_token"`
}

// WaitCommit blocks until the channel's committed offset token equals token.
func (c *sidecarClient) WaitCommit(ctx context.Context, channel, token string) error {
	var res waitCommitResult
	return c.call(ctx, "wait_commit", struct {
		Channel  string  `json:"channel"`
		Token    string  `json:"token"`
		TimeoutS float64 `json:"timeout_s"`
	}{channel, token, (rpcTimeoutWaitCommit - 30*time.Second).Seconds()}, rpcTimeoutWaitCommit, &res)
}

type channelStatusResult struct {
	CommittedToken   *string `json:"committed_token"`
	RowsErrorCount   int64   `json:"rows_error_count"`
	LastErrorMessage string  `json:"last_error_message"`
}

func (c *sidecarClient) ChannelStatus(ctx context.Context, channel string) (*channelStatusResult, error) {
	var res channelStatusResult
	if err := c.call(ctx, "channel_status", struct {
		Channel string `json:"channel"`
	}{channel}, rpcTimeoutOpenChannel, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *sidecarClient) CloseChannel(ctx context.Context, channel string) error {
	return c.call(ctx, "close_channel", struct {
		Channel string `json:"channel"`
	}{channel}, rpcTimeoutOpenChannel, nil)
}

// Shutdown asks the sidecar to drain and exit cleanly. Errors are expected if
// the process is already gone.
func (c *sidecarClient) Shutdown(ctx context.Context) error {
	return c.call(ctx, "shutdown", nil, rpcTimeoutShutdown, nil)
}
