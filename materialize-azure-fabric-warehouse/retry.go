package connector

import (
	"database/sql/driver"
	"errors"
	"strings"
	"time"

	"github.com/estuary/connectors/go/retry"
	mssql "github.com/microsoft/go-mssqldb"
)

const (
	// sqlServerDeadlockVictim is raised when the deadlock monitor kills a
	// transaction to resolve a deadlock; the caller is expected to retry it.
	// Fabric Warehouse uses the same number and message as on-prem SQL
	// Server for this one.
	sqlServerDeadlockVictim = 1205

	// fabricSnapshotUpdateConflict and fabricSnapshotUpdateConflictAlt are
	// raised under Fabric Warehouse's snapshot isolation when two
	// transactions write-conflict - which, because Fabric Warehouse locks
	// at table granularity, can happen even when they touch different rows
	// of the same table. These are Fabric-Warehouse-specific numbers: Fabric
	// renumbers this error away from on-prem SQL Server's 3960/3961, and
	// also rewords the message text, so don't assume classic SQL Server
	// error numbers apply here. See
	// https://learn.microsoft.com/en-us/fabric/data-warehouse/transactions.
	fabricSnapshotUpdateConflict    = 24556
	fabricSnapshotUpdateConflictAlt = 24706

	// fabricSystemUpdate is in the message of the login error Fabric returns
	// while the warehouse is being updated. Its error number isn't documented.
	fabricSystemUpdate = "due to a system update"
)

// commitRetry retries a fence-install or store-commit transaction that
// failed before it could commit: rolled back on a conflict with a concurrent
// writer (another materialization, or this connector's own commit racing a
// schema change), or cut off by a connection failure. Jitter keeps
// conflicting writers from retrying in lockstep.
var commitRetry = retry.Policy{
	Classify:       classifyTxn,
	MaxAttempts:    5,
	InitialBackoff: 5 * time.Second,
	MaxBackoff:     20 * time.Second,
	Jitter:         0.5,
}

// classify sorts the errors that resolve on a retry. A deadlock victim or a
// snapshot-isolation update conflict rolled its transaction back, and a login
// refused during a system update ran nothing, so both are Retryable. A
// connection failure is classified by retry.Network.
func classify(err error) retry.Outcome {
	if mssqlErr, ok := errors.AsType[mssql.Error](err); ok {
		switch {
		case mssqlErr.Number == sqlServerDeadlockVictim,
			mssqlErr.Number == fabricSnapshotUpdateConflict,
			mssqlErr.Number == fabricSnapshotUpdateConflictAlt,
			strings.Contains(mssqlErr.Message, fabricSystemUpdate):
			return retry.Retryable
		}
		return retry.Fatal
	}
	if errors.Is(err, driver.ErrBadConn) {
		return retry.Ambiguous
	}
	return retry.Network(err)
}

// classifyTxn classifies errors from a transaction. Fabric rolls back a
// transaction whose connection drops, so only an error raised by COMMIT can
// leave it committed.
func classifyTxn(err error) retry.Outcome {
	var outcome = classify(err)
	if _, atCommit := errors.AsType[*commitError](err); outcome == retry.Ambiguous && !atCommit {
		return retry.Retryable
	}
	return outcome
}

// commitError is an error returned by COMMIT.
type commitError struct{ err error }

func (e *commitError) Error() string { return e.err.Error() }
func (e *commitError) Unwrap() error { return e.err }
