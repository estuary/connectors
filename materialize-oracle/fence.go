package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"fmt"

	sql "github.com/estuary/connectors/materialize-sql"
	go_ora "github.com/sijms/go-ora/v2"
)

func encodeCheckpoint(cp []byte) string {
	return base64.StdEncoding.EncodeToString(cp)
}

// installFence increments the fence of every checkpoint which overlaps the
// fence's key range, and reads or inserts the checkpoint of exactly that range.
func installFence(ctx context.Context, dialect sql.Dialect, db *stdsql.DB, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	var materialization, keyBegin, keyEnd, fenceId, checkpointId = dialect.Identifier("materialization"), dialect.Identifier("key_begin"),
		dialect.Identifier("key_end"), dialect.Identifier("fence"), dialect.Identifier("checkpoint")

	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return sql.Fence{}, fmt.Errorf("db.BeginTx: %w", err)
	}
	defer txn.Rollback()

	if _, err = txn.ExecContext(ctx, fmt.Sprintf(
		"UPDATE %s SET %s = %s + 1 WHERE %s = :1 AND %s >= :2 AND %s <= :3",
		checkpoints.Identifier, fenceId, fenceId, materialization, keyEnd, keyBegin,
	), fence.Materialization.String(), int64(fence.KeyBegin), int64(fence.KeyEnd)); err != nil {
		return sql.Fence{}, fmt.Errorf("incrementing fence: %w", err)
	}

	// Read the checkpoint with the narrowest [key_begin, key_end] which fully
	// overlaps our range.
	var readBegin, readEnd int64
	var checkpoint string
	if err = txn.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT %s, %s, %s, %s FROM %s WHERE %s = :1 AND %s <= :2 AND %s >= :3 ORDER BY %s - %s ASC FETCH FIRST 1 ROWS ONLY",
		fenceId, keyBegin, keyEnd, checkpointId, checkpoints.Identifier, materialization, keyBegin, keyEnd, keyEnd, keyBegin,
	), fence.Materialization.String(), int64(fence.KeyBegin), int64(fence.KeyEnd)).Scan(&fence.Fence, &readBegin, &readEnd, &checkpoint); err == stdsql.ErrNoRows {
		// An invalid range, which compares as unequal to trigger an insertion
		// below.
		readBegin, readEnd = 1, 0
	} else if err != nil {
		return sql.Fence{}, fmt.Errorf("scanning fence and checkpoint: %w", err)
	} else if fence.Checkpoint, err = base64.StdEncoding.DecodeString(checkpoint); err != nil {
		return sql.Fence{}, fmt.Errorf("decoding checkpoint: %w", err)
	}

	if readBegin != int64(fence.KeyBegin) || readEnd != int64(fence.KeyEnd) {
		// The checkpoint is the last bind, since it may be a LONG bind.
		if _, err = txn.ExecContext(ctx, fmt.Sprintf(
			"INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (:1, :2, :3, :4, :5)",
			checkpoints.Identifier, materialization, keyBegin, keyEnd, fenceId, checkpointId,
		), fence.Materialization.String(), int64(fence.KeyBegin), int64(fence.KeyEnd), fence.Fence,
			go_ora.Clob{String: encodeCheckpoint(fence.Checkpoint), Valid: true}); err != nil {
			return sql.Fence{}, fmt.Errorf("inserting fence: %w", err)
		}
	}

	if err = txn.Commit(); err != nil {
		return sql.Fence{}, fmt.Errorf("txn.Commit: %w", err)
	}
	return fence, nil
}

// updateFenceSQL updates the fence's checkpoint, bound as :1, if the fence
// still holds.
func updateFenceSQL(dialect sql.Dialect, fence sql.Fence) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = :1 WHERE %s = %s AND %s = %d AND %s = %d AND %s = %d",
		dialect.Identifier(fence.TablePath...),
		dialect.Identifier("checkpoint"),
		dialect.Identifier("materialization"), dialect.Literal(fence.Materialization.String()),
		dialect.Identifier("key_begin"), fence.KeyBegin,
		dialect.Identifier("key_end"), fence.KeyEnd,
		dialect.Identifier("fence"), fence.Fence,
	)
}
