package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"fmt"

	sql "github.com/estuary/connectors/materialize-sql"
)

// installFence is a modified version of StdInstallFence, adopted to SQLServer's
// syntax
func installFence(ctx context.Context, dialect sql.Dialect, db *stdsql.DB, checkpoints sql.Table, fence sql.Fence, decodeFence func(string) ([]byte, error)) (sql.Fence, error) {
	var fenceId = dialect.Identifier("fence")
	var materializationId = dialect.Identifier("materialization")
	var keyEndId = dialect.Identifier("key_end")
	var keyBeginId = dialect.Identifier("key_begin")
	var checkpointId = dialect.Identifier("checkpoint")

	// Increment the fence value of _any_ checkpoint which overlaps our key range.
  if _, err := db.ExecContext(ctx,
		fmt.Sprintf(`
			UPDATE %s
				SET %s=%s+1
				WHERE %s=%s
				AND %s>=%d
				AND %s<=%d
			;
			`,
			checkpoints.Identifier,
			fenceId, fenceId,
			materializationId,
			databricksDialect.Literal(string(fence.Materialization)),
			keyEndId,
			fence.KeyBegin,
			keyBeginId,
			fence.KeyEnd,
		),
	); err != nil {
		return sql.Fence{}, fmt.Errorf("incrementing fence: %w", err)
	}

	// Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
	var readBegin, readEnd uint32
	var checkpoint string

  if err := db.QueryRowContext(ctx,
		fmt.Sprintf(`
			SELECT %s, %s, %s, %s
				FROM %s
				WHERE %s=%s
				AND %s<=%d
				AND %s>=%d
				ORDER BY %s - %s ASC
        LIMIT 1
			;
			`,
			fenceId,
			keyBeginId,
			keyEndId,
			checkpointId,
			checkpoints.Identifier,
			materializationId,
			databricksDialect.Literal(string(fence.Materialization)),
			keyBeginId,
			fence.KeyBegin,
			keyEndId,
			fence.KeyEnd,
			keyEndId,
			keyBeginId,
		),
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
  } else if _, err := db.ExecContext(ctx,
		fmt.Sprintf(
			"INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (%s, %d, %d, %d, %s);",
			checkpoints.Identifier,
			materializationId,
			keyBeginId,
			keyEndId,
			fenceId,
			checkpointId,
      databricksDialect.Literal(string(fence.Materialization)),
      fence.KeyBegin,
      fence.KeyEnd,
      fence.Fence,
      databricksDialect.Literal(string(base64.StdEncoding.EncodeToString(fence.Checkpoint))),
		),
	); err != nil {
		return sql.Fence{}, fmt.Errorf("inserting fence: %w", err)
	}

	return fence, nil
}
