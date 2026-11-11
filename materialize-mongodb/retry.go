package connector

import (
	"errors"
	"fmt"
	"time"

	"github.com/estuary/connectors/go/retry"
	"go.mongodb.org/mongo-driver/mongo"
)

// storeRetry re-sends the models of a bulk write that failed with transient
// per-item write errors, which were not written. An error from the command
// itself is not retried here: the driver has already retried it as a
// retryable write, which the server de-duplicates by transaction number, and a
// re-issued command would lose that.
var storeRetry = retry.Policy{
	Classify:       classify,
	MaxAttempts:    5,
	InitialBackoff: time.Second,
	MaxBackoff:     5 * time.Second,
	Jitter:         0.5,
}

// retryableWriteErrorCodes are the codes of transient per-item write errors,
// taken from the mongo-driver's own retryableCodes set
// (x/mongo/driver/errors.go). 11000 (E11000 duplicate key) is deliberately
// absent: a strict InsertOneModel on a !Exists store is a tripwire, and a
// duplicate key must fail fast.
var retryableWriteErrorCodes = map[int]bool{
	11600: true, // InterruptedAtShutdown
	11602: true, // InterruptedDueToReplStateChange
	10107: true, // NotWritablePrimary
	13435: true, // NotPrimaryNoSecondaryOk
	13436: true, // NotPrimaryOrSecondary
	189:   true, // PrimarySteppedDown
	91:    true, // ShutdownInProgress
	7:     true, // HostNotFound
	6:     true, // HostUnreachable
	89:    true, // NetworkTimeout
	9001:  true, // SocketException
	262:   true, // ExceededTimeLimit
}

// unwrittenModelsError reports the models of a bulk write that failed with
// transient write errors.
type unwrittenModelsError struct {
	count int
	err   error
}

func (e *unwrittenModelsError) Error() string {
	return fmt.Sprintf("%d write models failed with transient errors: %s", e.count, e.err)
}

func (e *unwrittenModelsError) Unwrap() error { return e.err }

func classify(err error) retry.Outcome {
	if _, ok := errors.AsType[*unwrittenModelsError](err); ok {
		return retry.Retryable
	}
	return retry.Fatal
}

// transientFailures returns the indices of the models that a bulk write failed
// with transient write errors, or false if it failed in any other way.
func transientFailures(err error) ([]int, bool) {
	var bwe, ok = errors.AsType[mongo.BulkWriteException](err)
	if !ok || bwe.WriteConcernError != nil || len(bwe.WriteErrors) == 0 {
		return nil, false
	}

	var indices = make([]int, 0, len(bwe.WriteErrors))
	for _, we := range bwe.WriteErrors {
		if !retryableWriteErrorCodes[we.Code] {
			return nil, false
		}
		indices = append(indices, we.Index)
	}
	return indices, true
}
