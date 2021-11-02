package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

type postgresSink struct {
	conn       *pgx.Conn
	table      string
	columns    []string
	primaryKey string
}

func newPostgresSink(ctx context.Context, uri, tableName, primaryKey, tableDef string) (*postgresSink, error) {
	var conn, err = pgx.Connect(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to DB: %w", err)
	}

	if err := conn.QueryRow(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName)).Scan(); err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("error dropping table %q: %w", tableName, err)
	}

	var createQuery = fmt.Sprintf(`CREATE TABLE %s%s;`, tableName, tableDef)
	logrus.WithField("query", createQuery).Debug("creating table")
	if err := conn.QueryRow(ctx, createQuery).Scan(); err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("error creating table %q: %w", tableName, err)
	}

	return &postgresSink{
		conn:       conn,
		table:      tableName,
		primaryKey: primaryKey,
	}, nil
}

func (s *postgresSink) Write(ctx context.Context, evts []changeEvent) error {
	var txn, err = s.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("error opening transaction: %w", err)
	}

	for _, evt := range evts {
		var query string
		var args []interface{}
		switch evt.op {
		case "Insert":
			query = fmt.Sprintf(`INSERT INTO %s(id, seq) VALUES ($1,$2);`, s.table)
			args = append(args, evt.id, evt.seq)
		case "Update":
			query = fmt.Sprintf(`UPDATE %s SET seq = $2 where id = $1;`, s.table)
			args = append(args, evt.id, evt.seq)
		case "Delete":
			query = fmt.Sprintf(`DELETE FROM %s WHERE id = $1;`, s.table)
			args = append(args, evt.id)
		default:
			return fmt.Errorf("invalid event type %q", evt.op)
		}
		if err := txn.QueryRow(ctx, query, args...).Scan(); err != nil && err != pgx.ErrNoRows {
			return fmt.Errorf("error executing query %q: %w", query, err)
		}
	}

	if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}
	return nil
}

func (s *postgresSink) Close(ctx context.Context) error {
	if err := s.conn.QueryRow(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, s.table)).Scan(); err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("error dropping table %q: %w", s.table, err)
	}
	return nil
}
