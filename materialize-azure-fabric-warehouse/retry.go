package connector

import (
	"errors"
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
)

// commitRetry retries a fence-install or store-commit transaction that
// Fabric Warehouse rolled back on a conflict with a concurrent writer -
// another materialization, or this connector's own commit racing a schema
// change. Jitter keeps conflicting writers from retrying in lockstep.
var commitRetry = retry.Policy{
	Classify:       classify,
	MaxAttempts:    5,
	InitialBackoff: 5 * time.Second,
	MaxBackoff:     20 * time.Second,
	Jitter:         0.5,
}

// classify reports the Outcome of a transaction Fabric Warehouse rolled
// back: a deadlock victim or a snapshot-isolation update conflict are
// Retryable, since rolling back a transaction undoes everything it did.
func classify(err error) retry.Outcome {
	mssqlErr, ok := errors.AsType[mssql.Error](err)
	if !ok {
		return retry.Fatal
	}
	switch mssqlErr.Number {
	case sqlServerDeadlockVictim, fabricSnapshotUpdateConflict, fabricSnapshotUpdateConflictAlt:
		return retry.Retryable
	}
	return retry.Fatal
}
