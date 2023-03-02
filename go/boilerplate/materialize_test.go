package boilerplate

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/estuary/connectors/go/protocol"
	"github.com/stretchr/testify/require"
)

type mockDriver struct {
	doSpec     func(context.Context, protocol.SpecRequest) (*protocol.SpecResponse, error)
	doValidate func(context.Context, protocol.ValidateRequest) (*protocol.ValidateResponse, error)
	doApply    func(context.Context, protocol.ApplyRequest) (*protocol.ApplyResponse, error)
	doOpen     func(context.Context, protocol.OpenRequest) (*protocol.OpenResponse, error)
	doLoad     func(context.Context, <-chan protocol.LoadRequest, func(protocol.LoadResponse)) error
	doStore    func(context.Context, <-chan protocol.StoreRequest) (StartCommitFn, error)
}

func (d *mockDriver) Spec(ctx context.Context, req protocol.SpecRequest) (*protocol.SpecResponse, error) {
	return d.doSpec(ctx, req)
}

func (d *mockDriver) Validate(ctx context.Context, req protocol.ValidateRequest) (*protocol.ValidateResponse, error) {
	return d.doValidate(ctx, req)
}

func (d *mockDriver) Apply(ctx context.Context, req protocol.ApplyRequest) (*protocol.ApplyResponse, error) {
	return d.doApply(ctx, req)
}

func (d *mockDriver) Open(ctx context.Context, req protocol.OpenRequest) (*protocol.OpenResponse, error) {
	return d.doOpen(ctx, req)
}

func (d *mockDriver) Load(ctx context.Context, reqs <-chan protocol.LoadRequest, loadedFn func(protocol.LoadResponse)) error {
	return d.doLoad(ctx, reqs, loadedFn)
}

func (d *mockDriver) Store(ctx context.Context, reqs <-chan protocol.StoreRequest) (StartCommitFn, error) {
	return d.doStore(ctx, reqs)
}

func TestBoilerplate_Setup(t *testing.T) {
	var inputWriter io.Writer
	var outputReader io.Reader
	var err error

	inputReader, inputWriter, err = os.Pipe()
	require.NoError(t, err)
	outputReader, outputWriter, err = os.Pipe()
	require.NoError(t, err)

	out := bufio.NewScanner(outputReader)

	driver := &mockDriver{
		doSpec: func(ctx context.Context, req protocol.SpecRequest) (*protocol.SpecResponse, error) {
			return &protocol.SpecResponse{}, nil
		},
		doValidate: func(ctx context.Context, req protocol.ValidateRequest) (*protocol.ValidateResponse, error) {
			return &protocol.ValidateResponse{}, nil
		},
		doApply: func(ctx context.Context, req protocol.ApplyRequest) (*protocol.ApplyResponse, error) {
			return &protocol.ApplyResponse{}, nil
		},
		doOpen: func(ctx context.Context, req protocol.OpenRequest) (*protocol.OpenResponse, error) {
			return &protocol.OpenResponse{}, nil
		},
	}

	go func() {
		handleCmds(context.Background(), driver)
	}()

	tests := []struct {
		reqCmd  string
		reqMsg  interface{}
		wantCmd string
		wantMsg interface{}
	}{
		{
			reqCmd:  "spec",
			reqMsg:  protocol.SpecRequest{},
			wantCmd: "spec",
			wantMsg: protocol.SpecResponse{},
		},
		{
			reqCmd:  "validate",
			reqMsg:  protocol.ValidateRequest{},
			wantCmd: "validated",
			wantMsg: protocol.ValidateResponse{},
		},
		{
			reqCmd:  "apply",
			reqMsg:  protocol.ApplyRequest{},
			wantCmd: "applied",
			wantMsg: protocol.ApplyResponse{},
		},
		{
			reqCmd:  "open",
			reqMsg:  protocol.OpenRequest{},
			wantCmd: "opened",
			wantMsg: protocol.OpenResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.reqCmd, func(t *testing.T) {
			_, err = inputWriter.Write(marshalTestCmd(t, tt.reqCmd, tt.reqMsg))
			require.NoError(t, err)
			require.True(t, out.Scan())
			require.Equal(t, marshalTestResponse(t, tt.wantCmd, tt.wantMsg), out.Bytes())
		})
	}
}

func TestBoilerplate_TxnLifecycle_Sync(t *testing.T) {
	var inputWriter io.Writer
	var outputReader io.Reader
	var err error

	inputReader, inputWriter, err = os.Pipe()
	require.NoError(t, err)
	outputReader, outputWriter, err = os.Pipe()
	require.NoError(t, err)

	out := bufio.NewScanner(outputReader)

	startCommitCalls := &testCounter{}
	executeCommitCalls := &testCounter{}

	driver := &mockDriver{
		doLoad: func(ctx context.Context, reqs <-chan protocol.LoadRequest, loaded func(protocol.LoadResponse)) error {
			<-reqs
			loaded(protocol.LoadResponse{})
			return nil
		},
		doStore: func(ctx context.Context, reqs <-chan protocol.StoreRequest) (StartCommitFn, error) {
			<-reqs
			startCommitCalls.inc()

			return func(ctx context.Context, runtimeCheckpoint string, runtimeAckCh <-chan struct{}) (*protocol.StartCommitResponse, OpFuture) {
				commitOp := RunAsyncOperation(func() error {
					<-runtimeAckCh
					executeCommitCalls.inc()
					return nil
				})

				return nil, commitOp
			}, nil
		},
	}

	go func() {
		handleCmds(context.Background(), driver)
	}()

	// Run through several rounds of a simulated runtime sending transaction requests to the driver,
	// explicitly waiting for the expected response from the driver.
	for round := 0; round < 5; round++ {
		_, err = inputWriter.Write(marshalTestCmd(t, "acknowledge", protocol.AcknowledgeRequest{}))
		require.NoError(t, err)
		require.True(t, out.Scan())
		require.Equal(t, marshalTestResponse(t, "acknowledged", protocol.AcknowledgeResponse{}), out.Bytes())

		_, err = inputWriter.Write(marshalTestCmd(t, "load", protocol.LoadRequest{}))
		require.NoError(t, err)
		require.True(t, out.Scan())
		require.Equal(t, marshalTestResponse(t, "loaded", protocol.LoadResponse{}), out.Bytes())

		_, err = inputWriter.Write(marshalTestCmd(t, "flush", protocol.FlushRequest{}))
		require.NoError(t, err)
		require.True(t, out.Scan())
		require.Equal(t, marshalTestResponse(t, "flushed", protocol.FlushResponse{}), out.Bytes())

		_, err = inputWriter.Write(marshalTestCmd(t, "store", protocol.StoreRequest{}))
		require.NoError(t, err)

		require.Equal(t, round, startCommitCalls.get())
		require.Equal(t, round, executeCommitCalls.get())

		_, err = inputWriter.Write(marshalTestCmd(t, "startCommit", protocol.StartCommitRequest{}))
		require.NoError(t, err)
		require.True(t, out.Scan())
		require.Equal(t, marshalTestResponse(t, "startedCommit", protocol.StartCommitResponse{
			DriverCheckpoint: json.RawMessage("{}"),
			MergePatch:       true,
		}), out.Bytes())

		require.Equal(t, round+1, startCommitCalls.get())
		require.Equal(t, round, executeCommitCalls.get())

		_, err = inputWriter.Write(marshalTestCmd(t, "acknowledge", protocol.AcknowledgeRequest{}))
		require.NoError(t, err)
		require.True(t, out.Scan())
		require.Equal(t, marshalTestResponse(t, "acknowledged", protocol.AcknowledgeResponse{}), out.Bytes())

		require.Equal(t, round+1, startCommitCalls.get())
		require.Equal(t, round+1, executeCommitCalls.get())

		// Loop for the next transaction round.
	}
}

func TestBoilerplate_TxnLifecycle_Async(t *testing.T) {
	var inputWriter io.Writer
	var outputReader io.Reader
	var err error

	inputReader, inputWriter, err = os.Pipe()
	require.NoError(t, err)
	outputReader, outputWriter, err = os.Pipe()
	require.NoError(t, err)

	out := bufio.NewScanner(outputReader)

	startCommitCalls := &testCounter{}
	executeCommitCalls := &testCounter{}

	driver := &mockDriver{
		doLoad: func(ctx context.Context, reqs <-chan protocol.LoadRequest, loaded func(protocol.LoadResponse)) error {
			for range reqs {
				loaded(protocol.LoadResponse{})
			}
			return nil
		},
		doStore: func(ctx context.Context, reqs <-chan protocol.StoreRequest) (StartCommitFn, error) {
			for range reqs {
				<-reqs
			}
			startCommitCalls.inc()

			return func(ctx context.Context, runtimeCheckpoint string, runtimeAckCh <-chan struct{}) (*protocol.StartCommitResponse, OpFuture) {
				commitOp := RunAsyncOperation(func() error {
					<-runtimeAckCh
					executeCommitCalls.inc()
					return nil
				})

				return nil, commitOp
			}, nil
		},
	}

	go func() {
		handleCmds(context.Background(), driver)
	}()

	for round := 0; round < 5; round++ {
		// Send "acknowledge" followed immediately by multiple load requests.
		_, err = inputWriter.Write(marshalTestCmd(t, "acknowledge", protocol.AcknowledgeRequest{}))
		require.NoError(t, err)
		for load := 0; load < 10; load++ {
			_, err = inputWriter.Write(marshalTestCmd(t, "load", protocol.LoadRequest{}))
			require.NoError(t, err)
		}
		// Read the "acknowledged" and "loaded" responses, which may have been written in any order.
		for idx := 0; idx < 11; idx++ {
			require.True(t, out.Scan())
		}

		// On round 0, these won't have been called at all yet. On round >= 1, their values will
		// reflect the increment from the end of the last round.
		require.Equal(t, round, startCommitCalls.get())
		require.Equal(t, round, executeCommitCalls.get())

		// "flush" gets an immediate "flushed" response.
		_, err = inputWriter.Write(marshalTestCmd(t, "flush", protocol.FlushRequest{}))
		require.NoError(t, err)
		require.True(t, out.Scan())
		require.Equal(t, marshalTestResponse(t, "flushed", protocol.FlushResponse{}), out.Bytes())

		// Send multiple "store" requests, which do not produce a response.
		for store := 0; store < 10; store++ {
			_, err = inputWriter.Write(marshalTestCmd(t, "store", protocol.StoreRequest{}))
			require.NoError(t, err)
		}

		// "startCommit" gets an immediate "startedCommit" response.
		_, err = inputWriter.Write(marshalTestCmd(t, "startCommit", protocol.StartCommitRequest{}))
		require.NoError(t, err)
		require.True(t, out.Scan())
		require.Equal(t, marshalTestResponse(t, "startedCommit", protocol.StartCommitResponse{
			DriverCheckpoint: json.RawMessage("{}"),
			MergePatch:       true,
		}), out.Bytes())

		require.Equal(t, round+1, startCommitCalls.get())
		require.Equal(t, round, executeCommitCalls.get())

		// Loop for the next transaction round.
	}
}

func marshalTestCmd(t *testing.T, cmd string, msg interface{}) []byte {
	t.Helper()

	envelope := map[string]interface{}{
		cmd: msg,
	}

	out, err := json.Marshal(envelope)
	require.NoError(t, err)
	out = append(out, '\n')
	return out
}

func marshalTestResponse(t *testing.T, cmd string, msg interface{}) []byte {
	t.Helper()

	envelope := map[string]interface{}{
		cmd: msg,
	}

	out, err := json.Marshal(envelope)
	require.NoError(t, err)
	return out
}

// testCounter can be thought of as the driver endpoint, which will be accessed concurrently by the
// driver async operations and the testing code assertions. The lock is required to prevent a
// datarace between the driver async operations and the testing assertions.
type testCounter struct {
	sync.Mutex
	count int
}

func (t *testCounter) inc() {
	t.Lock()
	defer t.Unlock()

	t.count++
}

func (t *testCounter) get() int {
	t.Lock()
	defer t.Unlock()

	return t.count
}
