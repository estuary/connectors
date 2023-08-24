package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"fmt"

	sql "github.com/estuary/connectors/materialize-sql"
	log "github.com/sirupsen/logrus"
)

// installFence is a modified version of StdInstallFence, adopted to SQLServer's
// syntax
func installFence(ctx context.Context, db *stdsql.DB, checkpoints sql.Table, fence sql.Fence, decodeFence func(string) ([]byte, error)) (sql.Fence, error) {
	var fenceId = sqlServerDialect.Identifier("fence")
	var materializationId = sqlServerDialect.Identifier("materialization")
	var keyEndId = sqlServerDialect.Identifier("key_end")
	var keyBeginId = sqlServerDialect.Identifier("key_begin")
	var checkpointId = sqlServerDialect.Identifier("checkpoint")

	var txn, err = db.BeginTx(ctx, nil)
	if err != nil {
		return sql.Fence{}, fmt.Errorf("db.BeginTx: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// Increment the fence value of _any_ checkpoint which overlaps our key range.
	if _, err = txn.Exec(
		fmt.Sprintf(`
			UPDATE %s
				SET %s=%s+1
				WHERE %s=%s
				AND %s>=%s
				AND %s<=%s
			;
			`,
			checkpoints.Identifier,
			fenceId, fenceId,
			materializationId,
			checkpoints.Keys[0].Placeholder,
			keyEndId,
			checkpoints.Keys[1].Placeholder,
			keyBeginId,
			checkpoints.Keys[2].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
	); err != nil {
		return sql.Fence{}, fmt.Errorf("incrementing fence: %w", err)
	}

	// Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
	var readBegin, readEnd uint32
	var checkpoint string

	if err = txn.QueryRow(
		fmt.Sprintf(`
			SELECT TOP 1 %s, %s, %s, %s
				FROM %s
				WHERE %s=%s
				AND %s<=%s
				AND %s>=%s
				ORDER BY %s - %s ASC
			;
			`,
			fenceId,
			keyBeginId,
			keyEndId,
			checkpointId,
			checkpoints.Identifier,
			materializationId,
			checkpoints.Keys[0].Placeholder,
			keyBeginId,
			checkpoints.Keys[1].Placeholder,
			keyEndId,
			checkpoints.Keys[2].Placeholder,
			keyEndId,
			keyBeginId,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
	).Scan(&fence.Fence, &readBegin, &readEnd, &checkpoint); err == stdsql.ErrNoRows {
		// Set an invalid range, which compares as unequal to trigger an insertion below.
		readBegin, readEnd = 1, 0
	} else if err != nil {
		return sql.Fence{}, fmt.Errorf("scanning fence and checkpoint: %w", err)
	} else if fence.Checkpoint, err = decodeFence(checkpoint); err != nil {
		return sql.Fence{}, fmt.Errorf("decodeFence(checkpoint): %w", err)
	}

	// If a checkpoint for this exact range doesn't exist then insert it now.
	if readBegin == fence.KeyBegin && readEnd == fence.KeyEnd {
		// Exists; no-op.
	} else if _, err = txn.Exec(
		fmt.Sprintf(
			"INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (%s, %s, %s, %s, %s);",
			checkpoints.Identifier,
			materializationId,
			keyBeginId,
			keyEndId,
			fenceId,
			checkpointId,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
			checkpoints.Values[0].Placeholder,
			checkpoints.Values[1].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
		fence.Fence,
		base64.StdEncoding.EncodeToString(fence.Checkpoint),
	); err != nil {
		log.WithField("q", 
		fmt.Sprintf(
			"INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (%s, %s, %s, %s, %s);",
			checkpoints.Identifier,
			materializationId,
			keyBeginId,
			keyEndId,
			fenceId,
			checkpointId,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
			checkpoints.Values[0].Placeholder,
			checkpoints.Values[1].Placeholder,
		)).Error("insertin fence")
		return sql.Fence{}, fmt.Errorf("inserting fence: %w", err)
	}

	err = txn.Commit()
	txn = nil // Disable deferred rollback.

	if err != nil {
		return sql.Fence{}, fmt.Errorf("txn.Commit: %w", err)
	}
	return fence, nil
}
