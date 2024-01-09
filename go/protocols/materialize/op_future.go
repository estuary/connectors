package materialize

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/broker/client"
)

// OpFuture represents an operation which is executing in the background. The
// operation has completed when Done selects. Err may be invoked to determine
// whether the operation succeeded or failed.
// This is inspired by gazette's `client` package.
type OpFuture interface {
	// Done selects when the operation's background execution has finished. The channel
	// also includes an optional ConnectorState to be emitted as part of the
	// Acknowledge response
	Done() <-chan *pf.ConnectorState
	// Err blocks until Done() and returns the final error of the OpFuture.
	Err() error
}

// A gazette `client.Operation` with the addition of supporting emitting a
// connector state
type AsyncOperation struct {
	inner   *client.AsyncOperation
	stateCh <-chan *pf.ConnectorState
}

func (o AsyncOperation) Done() <-chan *pf.ConnectorState {
	var out = make(chan *pf.ConnectorState)
	go func() {
		<-o.inner.Done()
		out <- <-o.stateCh
	}()

	return out
}

func (o AsyncOperation) Err() error {
	return o.inner.Err()
}

func RunAsyncOperation(fn func() (*pf.ConnectorState, error)) OpFuture {
	var innerOp = client.NewAsyncOperation()
	var stateCh = make(chan *pf.ConnectorState)
	var op = AsyncOperation{inner: innerOp, stateCh: stateCh}

	go func(inner *client.AsyncOperation) {
		defer func() {
			if inner != nil {
				inner.Resolve(fmt.Errorf("operation had an internal panic"))
			}
		}()

		state, err := fn()
		inner.Resolve(err)
		inner = nil

		go func() {
			stateCh <- state
			close(stateCh)
		}()
	}(innerOp)

	return op
}

func FinishedOperation(err error) OpFuture {
	var innerOp = client.NewAsyncOperation()
	innerOp.Resolve(err)

	var stateCh = make(chan *pf.ConnectorState)
	close(stateCh)

	var op = AsyncOperation{inner: innerOp, stateCh: stateCh}

	return op
}
