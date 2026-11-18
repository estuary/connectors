package connector

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/estuary/connectors/go/retry"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

// dialTimeout is how go-mssqldb reports a connection it couldn't open.
var dialTimeout = fmt.Errorf("unable to open tcp connection with host 'x.datawarehouse.fabric.microsoft.com:1433': %w",
	&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("i/o timeout")})

// connectionReset is a connection dropped after it was open.
var connectionReset = &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}

var systemUpdate = mssql.Error{Message: "login error: Couldn't complete the operation due to a system update"}

func TestClassifyTxn(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{"deadlock victim", mssql.Error{Number: sqlServerDeadlockVictim, Message: "deadlocked"}, retry.Retryable},
		{"fabric snapshot update conflict", mssql.Error{Number: fabricSnapshotUpdateConflict, Message: "snapshot conflict"}, retry.Retryable},
		{"fabric snapshot update conflict (alt code)", mssql.Error{Number: fabricSnapshotUpdateConflictAlt, Message: "snapshot conflict"}, retry.Retryable},
		{"wrapped deadlock victim", fmt.Errorf("executing query: %w", mssql.Error{Number: sqlServerDeadlockVictim}), retry.Retryable},
		{"conflict at COMMIT", fmt.Errorf("committing: %w", &commitError{mssql.Error{Number: fabricSnapshotUpdateConflict}}), retry.Retryable},
		{"system update at login", fmt.Errorf("store BeginTx: %w", systemUpdate), retry.Retryable},
		{"dial timeout", fmt.Errorf("store BeginTx: %w", dialTimeout), retry.Retryable},
		{"connection reset before COMMIT", fmt.Errorf("executing store query: %w", connectionReset), retry.Retryable},
		{"bad connection before COMMIT", fmt.Errorf("executing store query: %w", driver.ErrBadConn), retry.Retryable},
		// The transaction may have committed.
		{"connection reset at COMMIT", fmt.Errorf("committing: %w", &commitError{connectionReset}), retry.Ambiguous},
		{"bad connection at COMMIT", fmt.Errorf("committing: %w", &commitError{driver.ErrBadConn}), retry.Ambiguous},
		{"unrelated mssql error", mssql.Error{Number: 2627, Message: "unique constraint violation"}, retry.Fatal},
		{"cancelled", fmt.Errorf("executing store query: %w", context.Canceled), retry.Fatal},
		{"anything else", errors.New("boom"), retry.Fatal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifyTxn(tt.err))
		})
	}
	require.False(t, commitRetry.RetryAmbiguous)
}

// TestCommitRetry checks that every transient failure classifyTxn reports is
// retried under the policy the call sites use.
func TestCommitRetry(t *testing.T) {
	var policy = commitRetry
	policy.InitialBackoff = 0

	for _, transient := range []error{
		mssql.Error{Number: sqlServerDeadlockVictim},
		mssql.Error{Number: fabricSnapshotUpdateConflict},
		mssql.Error{Number: fabricSnapshotUpdateConflictAlt},
		systemUpdate,
		dialTimeout,
		connectionReset,
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
