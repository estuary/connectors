package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/sirupsen/logrus"
	pc "go.gazette.dev/core/consumer/protocol"
)

// MaterializeStream is the basic interface used for sending and receiving
// protocol messages.
type MaterializeStream interface {
	Send(*pm.Response) error
	RecvMsg(*pm.Request) error
}

// Transactor is a store-agnostic interface for a materialization connector
// that implements Flow materialization protocol transactions.
type Transactor interface {
	// UnmarshalState is called only on transactor startup if there is a persisted state
	// for this task
	UnmarshalState(json.RawMessage) error

	// Load implements the transaction load phase by consuming Load requests
	// from the LoadIterator and calling the provided `loaded` callback.
	// Load can ignore keys which are not found in the store, and it may
	// defer calls to `loaded` for as long as it wishes, so long as `loaded`
	// is called for every found document prior to returning.
	//
	// If this Transactor chooses to uses concurrency in StartCommit, note
	// that Load may be called while the OpFuture returned by StartCommit
	// is still running. However, absent an error, LoadIterator.Next() will
	// not return false until that OpFuture has resolved.
	//
	// Typically a Transactor that chooses to use concurrency should "stage"
	// loads for later evaluation, and then evaluate all loads upon that
	// commit resolving, or even wait until Next() returns false.
	//
	// Waiting for the prior commit ensures that evaluated loads reflect the
	// updates of that prior transaction, and thus meet the formal "read-committed"
	// guarantee required by the runtime.
	Load(_ *LoadIterator, loaded func(binding int, doc json.RawMessage) error) error
	// Store consumes Store requests from the StoreIterator and returns
	// a StartCommitFunc which is used to commit the stored transaction.
	// StartCommitFunc may be nil, which indicate that commits are a
	// no-op -- for example, as in an at-least-once materialization that
	// doesn't use a ConnectorState checkpoint.
	Store(*StoreIterator) (StartCommitFunc, error)

	// Acknowledge the commit of a completed transaction.
	// Acknowledge is run after both a) request.Acknowledge has been received from the runtime,
	// and also b) after an OpFuture returned by StartCommit has resolved.
	// It may use the state populated by UnmarshalState, or the state updated as part of StartCommitFunc
	//
	// It returns an optional ConnectorState update which will be applied in a best-effort fashion
	// upon its successful completion.
	//
	// Acknowledge may perform long-running, idempotent operations such as merging staged
	// updates into a base table. Upon its successful return, response.Acknowledged is sent to
	// the runtime, allowing the next pipelined transaction to begin to close.
	Acknowledge(context.Context) (*pf.ConnectorState, error)

	// Destroy the Transactor, releasing any held resources.
	Destroy()
}

// StartCommitFunc begins to commit a stored transaction.
// Upon its return a commit operation may still be running in the background,
// and the returned OpFuture must resolve with its completion.
// (Upon its resolution, Acknowledged will be sent to the Runtime).
//
// # When using the "Remote Store is Authoritative" pattern:
//
// StartCommitFunc must include `runtimeCheckpoint` within its endpoint
// transaction and either immediately or asynchronously commit.
// If the Transactor commits synchronously, it may return a nil OpFuture.
//
// # When using the "Recovery Log is Authoritative with Idempotent Apply" pattern:
//
// StartCommitFunc must return a ConnectorState checkpoint which encodes the staged
// application. It must begin an asynchronous application of this staged
// update, immediately returning its OpFuture.
//
// In the case of idempotent apply pattern, async operations that actually commit
// the data in the destination store must do so in Acknowledge() to ensure
// that the ConnectorState returned by StartCommit has been durably committed to the runtime
// recovery log.
//
// Note it's possible that the ConnectorState may commit to the log,
// but then the runtime or this Transactor may crash before the application
// is able to complete. For this reason, on initialization a Transactor must
// take care to (re-)apply a staged update in the opened ConnectorState as part of
// Acknowledge().
//
// If StartCommitFunc fails, it should return a pre-resolved OpFuture
// which carries its error (for example, via FinishedOperation()).
type StartCommitFunc = func(
	_ context.Context,
	runtimeCheckpoint *pc.Checkpoint,
) (*pf.ConnectorState, OpFuture)

// RunTransactions processes materialization protocol transactions
// over the established stream against a Connector.
func RunTransactions(
	ctx context.Context,
	stream MaterializeStream,
	open pm.Request_Open,
	opened pm.Response_Opened,
	transactor Transactor,
) (_err error) {
	defer transactor.Destroy()

	if err := open.Validate(); err != nil {
		return fmt.Errorf("open is invalid: %w", err)
	} else if err := opened.Validate(); err != nil {
		return fmt.Errorf("opened is invalid: %w", err)
	}

	if open.StateJson != nil {
		if err := transactor.UnmarshalState(open.StateJson); err != nil {
			return fmt.Errorf("transactor.UnmarshalState: %w", err)
		}
	}

	var rxRequest = pm.Request{Open: &open}
	var txResponse, err = writeOpened(stream, &opened)
	if err != nil {
		return err
	}
	logrus.WithField("eventType", "connectorStatus").Info("Running")

	var (
		// awaitErr is the last await() result,
		// and is readable upon its close of its parameter `awaitDoneCh`.
		awaitErr error
		// loadErr is the last loadAll() result,
		// and is readable upon its close of its parameter `loadDoneCh`.
		loadErr error
	)

	// await is a closure which awaits the completion of a previously
	// started commit, and then writes Acknowledged to the runtime.
	// It has an exclusive ability to write to `stream` until it returns.
	var await = func(
		ourCommitOp OpFuture, // Resolves when the prior commit completes.
		awaitDoneCh chan<- struct{}, // To be closed upon return.
		loadDoneCh <-chan struct{}, // Signaled when load() has completed.
	) (__out error) {

		defer func() {
			awaitErr = __out
			close(awaitDoneCh)
		}()

		// Wait for commit to complete, with cancellation checks.
		select {
		case <-ourCommitOp.Done():
			if err := ourCommitOp.Err(); err != nil {
				return err
			}
		case <-loadDoneCh:
			// load() must have error'd, as it otherwise cannot
			// complete until we send Acknowledged.
			return nil
		}

		if ackState, err := transactor.Acknowledge(ctx); err != nil {
			return err
		} else if err := writeAcknowledged(stream, ackState, &txResponse); err != nil {
			return err
		}

		return nil
	}

	// load is a closure for async execution of Transactor.Load.
	var load = func(
		it *LoadIterator,
		loadDoneCh chan<- struct{}, // To be closed upon return.
	) (__out error) {

		var loaded int
		defer func() {
			loadErr = __out
			close(loadDoneCh)
		}()

		var err = transactor.Load(it, func(binding int, doc json.RawMessage) error {
			if it.err != nil {
				panic(fmt.Sprintf("loaded called without first checking LoadIterator.Err(): %v", it.err))
			} else if it.awaitDoneCh != nil {
				panic("loaded called without first calling LoadIterator.WaitForAcknowledged()")
			} else if awaitErr != nil {
				// We cannot write a Loaded response if await() failed, as it would
				// be an out-of-order response (a protocol violation). Bail out.
				return context.Canceled
			}

			loaded++
			return writeLoaded(stream, &txResponse, binding, doc)
		})

		if it.awaitDoneCh == nil && awaitErr != nil {
			return nil // Cancelled by await() error.
		} else if it.err != nil {
			// Prefer the iterator's error over `err` as it's earlier in the chain
			// of dependency and is likely causal of (or equal to) `err`.
			return it.err
		}
		return err
	}

	// ourCommitOp is a future for the last async startCommit().
	var ourCommitOp OpFuture = FinishedOperation(nil)
	var loadCtx, loadCancel = context.WithCancel(ctx)
	defer loadCancel()

	for {
		var (
			awaitDoneCh = make(chan struct{}) // Signals await() is done.
			loadDoneCh  = make(chan struct{}) // Signals load() is done.
			loadIt      = LoadIterator{stream: stream, request: &rxRequest, awaitDoneCh: awaitDoneCh, ctx: loadCtx}
		)

		if err = readAcknowledge(stream, &rxRequest); err != nil {
			return err
		}

		// Await the commit of the prior transaction, then notify the runtime.
		// On completion, Acknowledged has been written to the stream,
		// and a concurrent load() phase may now begin to close.
		// At exit, `awaitDoneCh` is closed and `awaitErr` is its status.
		go await(ourCommitOp, awaitDoneCh, loadDoneCh)

		// Begin an async load of the current transaction.
		// At exit, `loadDoneCh` is closed and `loadErr` is its status.
		go load(&loadIt, loadDoneCh)

		// Join over await() and load().
		for awaitDoneCh != nil || loadDoneCh != nil {
			select {
			case <-awaitDoneCh:
				if awaitErr != nil {
					// Before calling transactor.Destroy, we need to make sure that the load phase
					// is gracefully cancelled to allow for graceful shutdown of the underlying
					// connector and to avoid resource leaks from the load phase (e.g. connections to database)
					loadCancel()
					return fmt.Errorf("commit failed: %w", awaitErr)
				}
				awaitDoneCh = nil
			case <-loadDoneCh:
				if loadErr != nil && loadErr != io.EOF {
					return fmt.Errorf("transactor.Load: %w", loadErr)
				}
				loadDoneCh = nil
			}
		}

		if loadErr == io.EOF {
			return nil // Graceful shutdown.
		}

		if err = validateIsFlush(&rxRequest); err != nil {
			return err
		} else if err = writeFlushed(stream, &txResponse); err != nil {
			return err
		}

		// Process all Store requests until StartCommit is read.
		var storeIt = StoreIterator{stream: stream, request: &rxRequest, ctx: ctx}
		var startCommit, err = transactor.Store(&storeIt)
		if storeIt.err != nil {
			err = storeIt.err // Prefer an iterator error as it's more directly causal.
		}
		if err != nil {
			return fmt.Errorf("transactor.Store: %w", err)
		}
		var runtimeCheckpoint *pc.Checkpoint
		if runtimeCheckpoint, err = checkpointFromStartCommit(&rxRequest); err != nil {
			return err
		}

		// `startCommit` may be nil to indicate a no-op commit.
		var stateUpdate *pf.ConnectorState = nil
		if startCommit != nil {
			stateUpdate, ourCommitOp = startCommit(ctx, runtimeCheckpoint)
		}
		// As a convenience, map a nil OpFuture to a pre-resolved one so the
		// rest of our handling can ignore the nil case.
		if ourCommitOp == nil {
			ourCommitOp = FinishedOperation(nil)
		}

		// If startCommit returned a pre-resolved error, fail-fast and don't
		// send StartedCommit to the runtime, as `stateUpdate` may be invalid.
		select {
		case <-ourCommitOp.Done():
			if err = ourCommitOp.Err(); err != nil {
				return fmt.Errorf("transactor.StartCommit: %w", err)
			}
		default:
		}

		if err = writeStartedCommit(stream, &txResponse, stateUpdate); err != nil {
			return err
		}
	}
}
