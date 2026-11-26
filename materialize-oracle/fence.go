package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"fmt"

	sql "github.com/estuary/connectors/materialize-sql"
	go_ora "github.com/sijms/go-ora/v2"
)

func installFence(ctx context.Context, db *stdsql.DB, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return sql.Fence{}, err
	}
	defer txn.Rollback()

	// Serialize installations, including the first insert of a new key range.
	if _, err := txn.ExecContext(ctx, "LOCK TABLE "+checkpoints.Identifier+" IN EXCLUSIVE MODE"); err != nil {
		return sql.Fence{}, fmt.Errorf("locking checkpoints: %w", err)
	}
	if _, err := txn.ExecContext(ctx, `UPDATE `+checkpoints.Identifier+`
SET FENCE = FENCE + 1 WHERE MATERIALIZATION = :1 AND KEY_END >= :2 AND KEY_BEGIN <= :3`,
		string(fence.Materialization), fence.KeyBegin, fence.KeyEnd); err != nil {
		return sql.Fence{}, fmt.Errorf("incrementing fence: %w", err)
	}

	var readBegin, readEnd uint32
	// Oracle reads an empty checkpoint back as NULL.
	var checkpoint stdsql.NullString
	err = txn.QueryRowContext(ctx, `SELECT FENCE, KEY_BEGIN, KEY_END, CHECKPOINT FROM `+checkpoints.Identifier+`
WHERE MATERIALIZATION = :1 AND KEY_BEGIN <= :2 AND KEY_END >= :3
ORDER BY KEY_END - KEY_BEGIN ASC FETCH FIRST 1 ROW ONLY`,
		string(fence.Materialization), fence.KeyBegin, fence.KeyEnd,
	).Scan(&fence.Fence, &readBegin, &readEnd, &checkpoint)
	if err == stdsql.ErrNoRows {
		readBegin, readEnd = 1, 0
	} else if err != nil {
		return sql.Fence{}, fmt.Errorf("reading checkpoint: %w", err)
	} else if fence.Checkpoint, err = base64.StdEncoding.DecodeString(checkpoint.String); err != nil {
		return sql.Fence{}, fmt.Errorf("decoding checkpoint: %w", err)
	}

	if readBegin != fence.KeyBegin || readEnd != fence.KeyEnd {
		if _, err := txn.ExecContext(ctx, `INSERT INTO `+checkpoints.Identifier+`
(MATERIALIZATION, KEY_BEGIN, KEY_END, FENCE, CHECKPOINT) VALUES (:1, :2, :3, :4, :5)`,
			string(fence.Materialization), fence.KeyBegin, fence.KeyEnd, fence.Fence,
			go_ora.Clob{String: base64.StdEncoding.EncodeToString(fence.Checkpoint), Valid: true},
		); err != nil {
			return sql.Fence{}, fmt.Errorf("inserting fence: %w", err)
		}
	}
	if err := txn.Commit(); err != nil {
		return sql.Fence{}, fmt.Errorf("committing fence: %w", err)
	}
	return fence, nil
}
