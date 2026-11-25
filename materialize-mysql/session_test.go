package connector

import (
	"context"
	stdsql "database/sql"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// resetProxy forwards TCP connections to a target and can reset every
// client-facing connection it holds.
type resetProxy struct {
	ln      net.Listener
	mu      sync.Mutex
	clients []*net.TCPConn
}

func newResetProxy(t *testing.T, target string) *resetProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var p = &resetProxy{ln: ln}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			p.mu.Lock()
			p.clients = append(p.clients, client.(*net.TCPConn))
			p.mu.Unlock()
			go func() { io.Copy(upstream, client); upstream.Close() }()
			go func() { io.Copy(client, upstream); client.Close() }()
		}
	}()
	return p
}

// reset closes every client-facing connection with a TCP RST.
func (p *resetProxy) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.clients {
		c.SetLinger(0)
		c.Close()
	}
	p.clients = nil
	// The RST must reach the client's socket before its next write.
	time.Sleep(500 * time.Millisecond)
}

// killConnection closes conn from the server side.
func killConnection(t *testing.T, ctx context.Context, db *stdsql.DB, conn *stdsql.Conn) {
	t.Helper()
	var id int64
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT CONNECTION_ID();").Scan(&id))
	_, err := db.ExecContext(ctx, "KILL CONNECTION "+strconv.FormatInt(id, 10)+";")
	require.NoError(t, err)
}

// TestSessionBeginAfterConnectionLoss verifies that a session whose
// connection was lost between transactions begins the next transaction with
// its temporary tables in place.
func TestSessionBeginAfterConnectionLoss(t *testing.T) {
	startDatabase(t)

	var ctx = context.Background()

	// transact inserts one row into the temporary table and reads it back, as
	// one transaction that leaves the table empty.
	var setup = []string{"CREATE TEMPORARY TABLE flow_temp_session_test (id BIGINT NOT NULL PRIMARY KEY);"}
	var transact = func(t *testing.T, s *session) {
		txn, err := s.begin(ctx, setup)
		require.NoError(t, err)
		defer txn.Rollback()

		_, err = txn.ExecContext(ctx, "INSERT INTO flow_temp_session_test VALUES (1);")
		require.NoError(t, err)
		var count int
		require.NoError(t, txn.QueryRowContext(ctx, "SELECT COUNT(*) FROM flow_temp_session_test;").Scan(&count))
		require.Equal(t, 1, count)
		_, err = txn.ExecContext(ctx, "DELETE FROM flow_temp_session_test;")
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	for _, variant := range variants {
		for name, lose := range map[string]func(*testing.T, *stdsql.DB, *session, *resetProxy){
			// The network path drops the connection.
			"reset": func(_ *testing.T, _ *stdsql.DB, _ *session, proxy *resetProxy) {
				proxy.reset()
			},
			// The server closes the connection.
			"kill": func(t *testing.T, db *stdsql.DB, s *session, _ *resetProxy) {
				killConnection(t, ctx, db, s.conn)
			},
		} {
			t.Run(variant+"/"+name, func(t *testing.T) {
				var cfg = variantConfig(t, variant)
				var proxy = newResetProxy(t, cfg.Address)
				cfg.Address = proxy.ln.Addr().String()
				db, err := stdsql.Open("mysql", cfg.ToURI())
				require.NoError(t, err)
				defer db.Close()

				var s = &session{db: db}
				require.NoError(t, s.connect(ctx, setup))
				defer s.close()

				transact(t, s)
				lose(t, db, s, proxy)
				transact(t, s)
				transact(t, s)
			})
		}
	}
}
