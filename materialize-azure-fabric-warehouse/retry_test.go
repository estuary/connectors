package connector

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/estuary/connectors/go/retry"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{"deadlock victim", mssql.Error{Number: sqlServerDeadlockVictim, Message: "deadlocked"}, retry.Retryable},
		{"fabric snapshot update conflict", mssql.Error{Number: fabricSnapshotUpdateConflict, Message: "snapshot conflict"}, retry.Retryable},
		{"fabric snapshot update conflict (alt code)", mssql.Error{Number: fabricSnapshotUpdateConflictAlt, Message: "snapshot conflict"}, retry.Retryable},
		{"wrapped deadlock victim", fmt.Errorf("executing query: %w", mssql.Error{Number: sqlServerDeadlockVictim}), retry.Retryable},
		{"unrelated mssql error", mssql.Error{Number: 2627, Message: "unique constraint violation"}, retry.Fatal},
		{"non-mssql error", errors.New("boom"), retry.Fatal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classify(tt.err))
		})
	}
}

// TestCommitRetry checks that every transient failure classify reports is
// retried under the policy the call sites use.
func TestCommitRetry(t *testing.T) {
	var policy = commitRetry
	policy.InitialBackoff = 0

	for _, transient := range []error{
		mssql.Error{Number: sqlServerDeadlockVictim},
		mssql.Error{Number: fabricSnapshotUpdateConflict},
		mssql.Error{Number: fabricSnapshotUpdateConflictAlt},
	} {
		var calls int
		require.NoError(t, policy.Do(context.Background(), "op", func(context.Context) error {
			if calls++; calls < 3 {
				return transient
			}
			return nil
		}))
		require.Equal(t, 3, calls)
	}
}
