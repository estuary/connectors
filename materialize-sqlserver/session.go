package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
)

// session is a pinned connection that can be replaced, because temporary
// tables exist only on the connection that created them.
type session struct {
	db   *stdsql.DB
	conn *stdsql.Conn
}

// connect pins a new connection in place of the current one and executes
// setup on it.
func (s *session) connect(ctx context.Context, setup []string) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	for _, stmt := range setup {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			conn.Close()
			return fmt.Errorf("Exec(%s): %w", stmt, err)
		}
	}
	if s.conn != nil {
		s.conn.Close()
	}
	s.conn = conn
	return nil
}

// begin starts a transaction on the pinned connection. Pass as setup the
// statements that create the session's temporary tables.
func (s *session) begin(ctx context.Context, setup []string) (*stdsql.Tx, error) {
	return s.conn.BeginTx(ctx, &stdsql.TxOptions{})
}

func (s *session) close() {
	s.conn.Close()
}
