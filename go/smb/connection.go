// Package smb connects to authenticated SMB2 and SMB3 shares.
package smb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hirochachacha/go-smb2"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	Host     string
	Share    string
	Username string
	Password string
	Domain   string

	// DialContext optionally supplies a transport. The default is a TCP dialer.
	DialContext func(context.Context, string, string) (net.Conn, error)
}

// Connection owns a session and mounted share. A nil or zero Connection is closed.
type Connection struct {
	cfg     Config
	address string
	mu      sync.Mutex
	conn    *connection
}

// Dial connects to address and mounts cfg.Share using signed NTLM authentication.
// cfg.Host identifies the server even when address points to a tunnel listener.
func Dial(ctx context.Context, cfg Config, address string) (*Connection, error) {
	conn, err := newConnection(ctx, cfg, address)
	if err != nil {
		return nil, err
	}
	return &Connection{cfg: cfg, address: address, conn: conn}, nil
}

func (c *Connection) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// WithShare retries fn once on a new session after a transport error. Callers
// must only use it for operations safe to repeat, such as metadata requests and
// file opens. Errors during an active file read or write are left to the caller.
// Callbacks are serialized; returned files must be closed before Connection.Close.
func (c *Connection) WithShare(ctx context.Context, fn func(*smb2.Share) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil {
		return os.ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.conn == nil {
		return os.ErrClosed
	}
	err := fn(c.conn.share.WithContext(ctx))
	var transportErr *smb2.TransportError
	if !errors.As(err, &transportErr) || ctx.Err() != nil {
		return err
	}

	log.WithError(err).Info("reconnecting to SMB share")
	c.conn.Close()
	c.conn, err = newConnection(ctx, c.cfg, c.address)
	if err != nil {
		return err
	}
	return fn(c.conn.share.WithContext(ctx))
}

type connection struct {
	conn    net.Conn
	session *smb2.Session
	share   *smb2.Share
}

func newDialer(cfg Config) smb2.Dialer {
	return smb2.Dialer{
		Negotiator: smb2.Negotiator{RequireMessageSigning: true},
		Initiator: &smb2.NTLMInitiator{
			User:     cfg.Username,
			Password: cfg.Password,
			Domain:   cfg.Domain,
		},
	}
}

func newConnection(ctx context.Context, cfg Config, address string) (*connection, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var dial = cfg.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	conn, err := dial(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("tcp connection to '%s': %w", address, err)
	}
	var c = &connection{conn: conn}
	var d = newDialer(cfg)
	if c.session, err = d.DialContext(ctx, &signingConn{Conn: conn}); err != nil {
		c.Close()
		return nil, fmt.Errorf("creating SMB session: %w", err)
	}
	// Identify the SMB server by its configured name, rather than the dialed
	// address or the tunnel's loopback listener.
	var share = fmt.Sprintf(`\\%s\%s`, cfg.Host, cfg.Share)
	if c.share, err = c.session.WithContext(ctx).Mount(share); err != nil {
		c.Close()
		return nil, fmt.Errorf("mounting share %q: %w", cfg.Share, err)
	}
	return c, nil
}

func (c *connection) Close() error {
	// Cleanup also runs after cancellation, so use a separate, bounded context.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var err error
	if c.share != nil {
		err = errors.Join(err, c.share.WithContext(ctx).Umount())
	}
	if c.session != nil {
		err = errors.Join(err, c.session.WithContext(ctx).Logoff())
	}
	// Logoff closes the transport on success. Still close it after a failed logoff.
	if closeErr := c.conn.Close(); !errors.Is(closeErr, net.ErrClosed) {
		err = errors.Join(err, closeErr)
	}
	return err
}
