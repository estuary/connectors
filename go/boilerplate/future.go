package boilerplate

import (
	"fmt"

	"go.gazette.dev/core/broker/client"
)

// OpFuture represents an operation which is executing in the background. The operation has
// completed when Done selects. Err may be invoked to determine whether the operation succeeded or
// failed. This is copied from gazette's `client` package.
type OpFuture interface {
	// Done selects when operation background execution has finished.
	Done() <-chan struct{}
	// Err blocks until Done() and returns the final error of the OpFuture.
	Err() error
}

// AsyncOperation is a simple, minimal implementation of the OpFuture interface.
type AsyncOperation = client.AsyncOperation

// NewAsyncOperation returns a new AsyncOperation.
var NewAsyncOperation = client.NewAsyncOperation

// FinishedOperation is a convenience that returns an already-resolved AsyncOperation.
var FinishedOperation = client.FinishedOperation

// RunAsyncOperation invokes the given function asynchronously and returns an OpFuture which will
// resolve with its completion or panic.
func RunAsyncOperation(fn func() error) OpFuture {
	var op = NewAsyncOperation()

	go func(op *AsyncOperation) {
		defer func() {
			if op != nil {
				op.Resolve(fmt.Errorf("operation had an internal panic"))
			}
		}()

		op.Resolve(fn())
		op = nil
	}(op)

	return op
}
