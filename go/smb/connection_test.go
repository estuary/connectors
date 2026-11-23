package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hirochachacha/go-smb2"
	"github.com/stretchr/testify/require"
)

func TestConnectionRequiresSigning(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	var deadline = time.Now().Add(5 * time.Second)
	require.NoError(t, server.SetDeadline(deadline))
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	var result = make(chan error, 1)
	go func() {
		var dialer = newDialer(Config{Username: "test", Password: "test"})
		_, err := dialer.DialContext(ctx, client)
		result <- err
	}()

	// Read the transport header, SMB2 header, and NEGOTIATE fields through SecurityMode.
	var request [4 + 64 + 6]byte
	_, err := io.ReadFull(server, request[:])
	require.NoError(t, err)
	require.NoError(t, server.Close())
	select {
	case err := <-result:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	require.Equal(t, "\xfeSMB", string(request[4:8]))
	require.Equal(t, uint16(0), binary.LittleEndian.Uint16(request[4+12:]), "expected SMB2 NEGOTIATE")
	var securityMode = binary.LittleEndian.Uint16(request[4+64+4:])
	require.NotZero(t, securityMode&0x2, "SMB2_NEGOTIATE_SIGNING_REQUIRED must be set")
}

func TestConnectionClose(t *testing.T) {
	for _, tt := range []struct {
		name    string
		err     error
		wantErr error
	}{
		{name: "open transport"},
		{name: "transport closed by logoff", err: &net.OpError{Op: "close", Net: "tcp", Err: net.ErrClosed}},
		{name: "other close error", err: io.ErrUnexpectedEOF, wantErr: io.ErrUnexpectedEOF},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var conn = &testConn{err: tt.err}
			var source = &Connection{conn: &connection{conn: conn}}
			if err := source.Close(); tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, conn.closed)
			require.NoError(t, source.Close())
			require.Equal(t, 1, conn.closed)
		})
	}
}

type testConn struct {
	net.Conn
	err    error
	closed int
}

func (c *testConn) Close() error {
	c.closed++
	return c.err
}

func TestReconnectThroughTunnel(t *testing.T) {
	var address = "127.0.0.1:9445"
	var transport = &testConn{}
	var source = &Connection{
		address: address,
		conn:    &connection{conn: transport, share: &smb2.Share{}},
	}
	var dialErr = errors.New("test SMB reconnect failed")
	var dials int
	source.cfg.DialContext = func(_ context.Context, _, target string) (net.Conn, error) {
		dials++
		require.Equal(t, address, target, "reconnect through the existing tunnel")
		require.NotEqual(t, "smb.example:445", target)
		return nil, dialErr
	}
	err := source.WithShare(context.Background(), func(*smb2.Share) error {
		// This fake share has no session to unmount during reconnect cleanup.
		source.conn.share = nil
		return &smb2.TransportError{Err: io.EOF}
	})
	require.ErrorIs(t, err, dialErr)
	require.Equal(t, 1, dials)
	require.Equal(t, 1, transport.closed)
	require.NoError(t, source.Close())
}

func TestWithShareDoesNotReconnect(t *testing.T) {
	for _, tt := range []struct {
		name   string
		err    error
		cancel bool
	}{
		{"success", nil, false},
		{"permission denied", os.ErrPermission, false},
		{"missing file", os.ErrNotExist, false},
		{"cancelled transport", &smb2.TransportError{Err: io.EOF}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var conn = &testConn{}
			var source = &Connection{conn: &connection{conn: conn, share: &smb2.Share{}}}
			source.cfg.DialContext = func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("unexpected reconnect")
				return nil, nil
			}
			var calls int
			err := source.WithShare(ctx, func(*smb2.Share) error {
				calls++
				if tt.cancel {
					cancel()
				}
				return tt.err
			})
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, 1, calls)
			require.Zero(t, conn.closed)
			// The fake share has no session to unmount.
			source.conn.share = nil
			require.NoError(t, source.Close())
			require.Equal(t, 1, conn.closed)
		})
	}
}

func TestClosedConnection(t *testing.T) {
	for _, conn := range []*Connection{nil, {}} {
		require.NoError(t, conn.Close())
		var calls int
		var use = func(*smb2.Share) error { calls++; return nil }
		require.ErrorIs(t, conn.WithShare(context.Background(), use), os.ErrClosed)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, conn.WithShare(ctx, use), context.Canceled)
		require.Zero(t, calls)
	}
}

func TestWithShareReconnect(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{"successful retry", nil},
		{"retry fails", &smb2.TransportError{Err: io.ErrUnexpectedEOF}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			var done = make(chan struct{})
			go func() {
				defer close(done)
				defer server.Close()
				var sign = acceptSmbSession(t, server)
				for _, command := range []uint16{4, 2} { // TREE_DISCONNECT, LOGOFF.
					request, err := readSmbPacket(server)
					require.NoError(t, err)
					require.Equal(t, command, binary.LittleEndian.Uint16(request[12:]))
					var body = make([]byte, 4)
					binary.LittleEndian.PutUint16(body, 4)
					writeSmbPacket(t, server, sign(smbResponse(request, 0, body)))
				}
			}()

			var transport = &testConn{}
			var original = &connection{conn: transport, share: &smb2.Share{}}
			var dials int
			var source = &Connection{
				conn: original, address: "127.0.0.1:9445",
				cfg: Config{
					Host: "smb.example", Share: "data", Username: "test", Password: "test",
					DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
						dials++
						require.Equal(t, "tcp", network)
						require.Equal(t, "127.0.0.1:9445", address)
						return client, nil
					},
				},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var calls int
			err := source.WithShare(ctx, func(*smb2.Share) error {
				calls++
				if calls == 1 {
					// The fake original share has no session to unmount.
					original.share = nil
					return fmt.Errorf("opening file: %w", &smb2.TransportError{Err: io.EOF})
				}
				return tt.err
			})
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, 2, calls)
			require.Equal(t, 1, dials)
			require.Equal(t, 1, transport.closed)
			require.NotSame(t, original, source.conn)
			require.NoError(t, source.Close())
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
