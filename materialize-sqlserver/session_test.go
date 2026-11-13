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

	mssqldb "github.com/microsoft/go-mssqldb"
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

// TestSessionBeginAfterConnectionLoss verifies that a session whose
// connection was lost between transactions begins the next transaction with
// its temporary tables in place.
func TestSessionBeginAfterConnectionLoss(t *testing.T) {
	startDatabase(t)

	var ctx = context.Background()

	// transact bulk-copies one row into the temporary table and reads it
	// back, as one transaction that leaves the table empty.
	var setup = []string{"CREATE TABLE #flow_temp_session_test (id BIGINT NOT NULL);"}
	var transact = func(t *testing.T, s *session) {
		txn, err := s.begin(ctx, setup)
		require.NoError(t, err)
		defer txn.Rollback()

		batch, err := txn.PrepareContext(ctx, mssqldb.CopyIn("#flow_temp_session_test", mssqldb.BulkOptions{}, "id"))
		require.NoError(t, err)
		_, err = batch.ExecContext(ctx, int64(1))
		require.NoError(t, err)
		_, err = batch.ExecContext(ctx)
		require.NoError(t, err)

		var count int
		require.NoError(t, txn.QueryRowContext(ctx, "SELECT COUNT(*) FROM #flow_temp_session_test;").Scan(&count))
		require.Equal(t, 1, count)

		_, err = txn.ExecContext(ctx, "TRUNCATE TABLE #flow_temp_session_test;")
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	for name, lose := range map[string]func(*testing.T, *stdsql.DB, *session, *resetProxy){
		// The network path drops the connection.
		"reset": func(_ *testing.T, _ *stdsql.DB, _ *session, proxy *resetProxy) {
			proxy.reset()
		},
		// The server closes the connection.
		"kill": func(t *testing.T, db *stdsql.DB, s *session, _ *resetProxy) {
			var spid int
			require.NoError(t, s.conn.QueryRowContext(ctx, "SELECT @@SPID;").Scan(&spid))
			_, err := db.ExecContext(ctx, "KILL "+strconv.Itoa(spid)+";")
			require.NoError(t, err)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var cfg = testConfig()
			var proxy = newResetProxy(t, cfg.Address)
			cfg.Address = proxy.ln.Addr().String()
			connector, err := cfg.ToSQLConnector(ctx)
			require.NoError(t, err)
			var db = stdsql.OpenDB(connector)
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
