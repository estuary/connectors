package boilerplate

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/estuary/connectors/go/protocol"
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
)

var (
	inputReader  io.Reader = os.Stdin
	outputWriter io.Writer = os.Stdout
	logWriter    io.Writer = os.Stderr
)

type MaterializeDriver interface {
	// Spec should produce a synchronous response with a SpecResponse of the driver's specification.
	Spec(context.Context, protocol.SpecRequest) (*protocol.SpecResponse, error)

	// Validate should produce a synchronous response with a ValidateResponse of the validated
	// bindings.
	Validate(context.Context, protocol.ValidateRequest) (*protocol.ValidateResponse, error)

	// Apply should produce a synchronous response with an ApplyResponse of the actions taken by the
	// driver.
	Apply(context.Context, protocol.ApplyRequest) (*protocol.ApplyResponse, error)

	// Open should produce a synchronous response with an OpenResponse optionally containing the
	// Flow runtime checkpoint that processing should resume from. If empty, the most recent
	// checkpoint of the Flow recovery log is used.
	//
	// Or, a driver may send value []byte{0xf8, 0xff, 0xff, 0xff, 0xf, 0x1} as a base64-encoded
	// string to explicitly begin processing from a zero-valued checkpoint, effectively rebuilding
	// the materialization from scratch.
	Open(context.Context, protocol.OpenRequest) (*protocol.OpenResponse, error)

	// Load implements the transaction load phase by consuming LoadRequests from the LoadRequest
	// channel and calling the provided `loaded` callback. Load can ignore keys which are not found
	// in the store, and it may defer calls to `loaded` for as long as it wishes, so long as
	// `loaded` is called for every found document prior to returning.
	//
	// Loads should not be evaluated until the prior transaction has finished committing. Waiting
	// for the prior commit ensures that evaluated loads reflect the updates of that prior
	// transaction, and thus meet the formal "read-committed" guarantee required by the runtime.
	//
	// The LoadRequest channel will be closed only when a "flush" request has been received from the
	// runtime. This signals that the runtime has received acknowledgement that the driver's prior
	// commit has completed successfully and that no more loads will be sent. The driver should
	// usually read from this channel until it is closed prior to evaluating its loads, typically by
	// staging the received load requests for execution after the channel is closed.
	Load(context.Context, <-chan protocol.LoadRequest, func(protocol.LoadResponse)) error

	// Store consumes StoreRequests from the StoreRequest channel and returns a StartCommitFn which
	// is used to commit the stored transaction. StartCommitFn may be nil, which indicates that
	// commits are a no-op -- as in an at-least-once materialization that doesn't use a
	// DriverCheckpoint.
	Store(context.Context, <-chan protocol.StoreRequest) (StartCommitFn, error)
}

// StartCommitFn begins to commit a stored transaction. Upon its return a commit operation may still
// be running in the background, and the returned OpFuture must resolve with its completion. (Upon
// its resolution, Acknowledged will be sent to the Runtime).
//
// # When using the "Remote Store is Authoritative" pattern:
//
// StartCommitFn must include `runtimeCheckpoint` within its endpoint transaction and either
// immediately or asynchronously commit. If the Transactor commits synchronously, it may return a
// nil OpFuture.
//
// # When using the "Recovery Log is Authoritative with Idempotent Apply" pattern:
//
// StartCommitFn must return a DriverCheckpoint which encodes the staged application. It must begin
// an asynchronous application of this staged update, immediately returning its OpFuture.
//
// That async application MUST await a future signal of `runtimeAckCh` before taking action,
// however, to ensure that the DriverCheckpoint returned by StartCommit has been durably committed
// to the runtime recovery log. `runtimeAckCh` is closed when an Acknowledge request is received
// from the runtime, indicating that the transaction and its DriverCheckpoint have been committed to
// the runtime recovery log.
//
// Note that it's possible that the DriverCheckpoint may commit to the log, but then the runtime or
// this Transactor may crash before the application is able to complete. For this reason, on
// initialization a Transactor must take care to (re-)apply a staged update in the opened
// DriverCheckpoint.
//
// If StartCommitFn fails, it should return a pre-resolved OpFuture which carries its error (for
// example, via FinishedOperation()).
type StartCommitFn = func(ctx context.Context, runtimeCheckpoint string, runtimeAckCh <-chan struct{}) (*protocol.StartCommitResponse, OpFuture)

type logConfig struct {
	Level  string `long:"log.level" env:"LOG_LEVEL" default:"info" choice:"info" choice:"INFO" choice:"debug" choice:"DEBUG" choice:"warn" choice:"WARN" description:"Logging level"`
	Format string `long:"log.format" env:"LOG_FORMAT" default:"text" choice:"json" choice:"text" choice:"color" description:"Logging output format"`
}

func (c *logConfig) Configure() {
	if c.Format == "json" {
		log.SetFormatter(&log.JSONFormatter{})
	} else if c.Format == "text" {
		log.SetFormatter(&log.TextFormatter{})
	} else if c.Format == "color" {
		log.SetFormatter(&log.TextFormatter{ForceColors: true})
	}

	if lvl, err := log.ParseLevel(c.Level); err != nil {
		log.WithField("err", err).Fatal("unrecognized log level")
	} else {
		log.SetLevel(lvl)
	}

	log.SetOutput(logWriter)
}

func RunMaterialization(driver MaterializeDriver) {
	var logConfig = &logConfig{}
	var parser = flags.NewParser(logConfig, flags.Default)
	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}
	logConfig.Configure()

	ctx := context.Background()

	if err := handleCmds(ctx, driver); err != nil {
		log.Error(err)
		os.Exit(1)
	}

	os.Exit(0)
}

// runState encapsulates the state of the driver throughout the transactions protocol messages. This
// state allows for processing loads and commits asynchronously.
type runState struct {
	loadChan            chan protocol.LoadRequest
	loadComplete        chan struct{}
	storeChan           chan protocol.StoreRequest
	storeComplete       chan struct{}
	driverStartCommitFn StartCommitFn
	runtimeAck          chan struct{}
	driverCommitFuture  OpFuture
}

func newRunState() *runState {
	return &runState{
		loadChan:            nil,
		loadComplete:        make(chan struct{}),
		storeChan:           nil,
		storeComplete:       make(chan struct{}),
		driverStartCommitFn: nil,
		runtimeAck:          make(chan struct{}),
		driverCommitFuture:  client.FinishedOperation(nil),
	}
}

func handleCmds(ctx context.Context, driver MaterializeDriver) error {
	state := newRunState()

	lines := bufio.NewScanner(inputReader)
	buf := make([]byte, 0, 4096)
	// The default maximum buffer size for bufio.Scanner is 64 kilobytes, which is too small for
	// messages commonly handled by connectors. There is no real reason to limit the maximum size of
	// messages, although handling extraordinarily large messages will not be practical due to
	// container memory limits.
	lines.Buffer(buf, math.MaxInt)

	for lines.Scan() {
		switch cmd := mustParseCommand(lines.Bytes()).(type) {
		case protocol.SpecRequest:
			log.Debug("received spec request")

			res, err := driver.Spec(ctx, cmd)
			if err != nil {
				return err
			}
			mustEmit(protocol.Speced, res)
		case protocol.ValidateRequest:
			log.Debug("received validate request")

			res, err := driver.Validate(ctx, cmd)
			if err != nil {
				return err
			}
			mustEmit(protocol.Validated, res)
		case protocol.ApplyRequest:
			log.Debug("received apply request")

			res, err := driver.Apply(ctx, cmd)
			if err != nil {
				return err
			}
			mustEmit(protocol.Applied, res)
		case protocol.OpenRequest:
			log.Debug("received open request")

			res, err := driver.Open(ctx, cmd)
			if err != nil {
				return err
			}
			mustEmit(protocol.Opened, res)
		case protocol.AcknowledgeRequest:
			// Receiving "acknowledge" from the runtime indicates that the runtime has successfully
			// committed the prior transaction to the recovery log. If the driver provided a
			// driverCommitFuture it will now be asynchronously awaited prior to sending
			// "acknowledged" to the runtime.
			log.Debug("received acknowledge request")

			go func() {
				// Signal the driverCommitFuture that the runtimeAck has been received.
				close(state.runtimeAck)

				// Wait for the driverCommitFuture to complete.
				<-state.driverCommitFuture.Done()

				// Prepare for the next round.
				state.runtimeAck = make(chan struct{})

				if err := state.driverCommitFuture.Err(); err != nil {
					exitWithError(err)
				}
				mustEmit(protocol.Acknowledged, protocol.AcknowledgeResponse{})
			}()
		case protocol.LoadRequest:
			log.Debug("received load request")

			handleLoad := func(loadResponse protocol.LoadResponse) {
				mustEmit(protocol.Loaded, loadResponse)
			}

			// Start the driver Load function if it hasn't already been started this round.
			if state.loadChan == nil {
				state.loadChan = make(chan protocol.LoadRequest)
				go func() {
					if err := driver.Load(ctx, state.loadChan, handleLoad); err != nil {
						exitWithError(err)
					}

					state.loadComplete <- struct{}{}
				}()
			}

			state.loadChan <- cmd
		case protocol.FlushRequest:
			log.Debug("received flush request")

			// This channel may be nil if no load requests were received, as would the be the case
			// when running delta updates.
			if state.loadChan == nil {
				mustEmit(protocol.Flushed, protocol.FlushResponse{})
				continue
			}

			// Close the loadChan of the in-progress driver Load function so that it can complete.
			close(state.loadChan)

			<-state.loadComplete
			state.loadChan = nil // Prepare for the next round

			mustEmit(protocol.Flushed, protocol.FlushResponse{})
		case protocol.StoreRequest:
			log.Debug("received store request")

			// Start the driver Store function if it hasn't already been started this round.
			if state.storeChan == nil {
				state.storeChan = make(chan protocol.StoreRequest)
				go func() {
					var err error
					if state.driverStartCommitFn, err = driver.Store(ctx, state.storeChan); err != nil {
						exitWithError(err)
					}
					state.storeComplete <- struct{}{}
				}()
			}

			state.storeChan <- cmd
		case protocol.StartCommitRequest:
			log.Debug("received startCommit request")

			// Close the storeChan of the in-progress driver Store function so that it can complete.
			close(state.storeChan)

			<-state.storeComplete
			state.storeChan = nil // Prepare for the next round

			var driverCheckpoint *protocol.StartCommitResponse
			if state.driverStartCommitFn != nil {
				driverCheckpoint, state.driverCommitFuture = state.driverStartCommitFn(ctx, cmd.RuntimeCheckpoint, state.runtimeAck)

				state.driverStartCommitFn = nil // Prepare for the next round
			}

			// As a convenience, map a nil OpFuture to a pre-resolved one so the rest of our
			// handling can ignore the nil case.
			if state.driverCommitFuture == nil {
				state.driverCommitFuture = FinishedOperation(nil)
			}

			// If startCommit returned a pre-resolved error, fail-fast and don't send
			// StartedCommit to the runtime, as `driverCheckpoint` may be invalid.
			select {
			case <-state.driverCommitFuture.Done():
				if err := state.driverCommitFuture.Err(); err != nil {
					return fmt.Errorf("driver.StartCommit: %w", err)
				}
			default:
			}

			if driverCheckpoint == nil {
				driverCheckpoint = &protocol.StartCommitResponse{
					DriverCheckpoint: json.RawMessage("{}"),
					MergePatch:       true,
				}
			}

			mustEmit(protocol.StartedCommit, driverCheckpoint)
		}
	}

	return lines.Err()
}
