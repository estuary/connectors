package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"

	log "github.com/sirupsen/logrus"
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

// begin starts a transaction, first replacing a connection that the server
// no longer answers on. Pass as setup the statements that create the
// session's temporary tables. The replacement loses nothing because the
// temporary tables are empty between transactions.
func (s *session) begin(ctx context.Context, setup []string) (*stdsql.Tx, error) {
	if pingErr := s.conn.PingContext(ctx); pingErr != nil {
		log.WithError(pingErr).Info("reconnecting because the connection to the database was lost")
		if err := s.connect(ctx, setup); err != nil {
			return nil, fmt.Errorf("reconnecting after %q: %w", pingErr, err)
		}
	}
	return s.conn.BeginTx(ctx, &stdsql.TxOptions{})
}

func (s *session) close() {
	s.conn.Close()
}
