package boilerplate

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	pm "github.com/estuary/flow/go/protocols/materialize"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

// RunMain is the boilerplate main function of a materialization connector.
func RunMain(srv pm.DriverServer) {
	var parser = flags.NewParser(nil, flags.Default)
	var ctx, _ = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)

	var cmd = cmdCommon{
		ctx: ctx,
		srv: srv,
		r:   bufio.NewReaderSize(os.Stdin, 1<<21), // 2MB buffer.
		w:   protoio.NewUint32DelimitedWriter(os.Stdout, binary.LittleEndian),
	}

	parser.AddCommand("spec", "Write the connector specification",
		"Write the connector specification to stdout, then exit", &specCmd{cmd})
	parser.AddCommand("validate", "Validate a materialization",
		"Validate a proposed ValidateRequest read from stdin", &validateCmd{cmd})
	parser.AddCommand("apply-upsert", "Apply an insert or update of a materialization",
		"Apply a proposed insert or update ApplyRequest read from stdin", &applyUpsertCmd{cmd})
	parser.AddCommand("apply-delete", "Apply a deletion of a materialization",
		"Apply a proposed delete ApplyRequest read from stdin", &applyDeleteCmd{cmd})
	parser.AddCommand("transactions", "Run materialization transactions",
		"Run a stream of transactions read from stdin", &transactionsCmd{cmd})

	var _, err = parser.Parse()
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

type cmdCommon struct {
	ctx context.Context
	srv pm.DriverServer
	r   *bufio.Reader
	w   protoio.Writer
}

type protoValidator interface {
	proto.Message
	Validate() error
}

func (c *cmdCommon) readMsg(m protoValidator) error {
	var lengthBytes [4]byte

	if _, err := io.ReadFull(c.r, lengthBytes[:]); err == io.EOF {
		return err
	} else if err != nil {
		return fmt.Errorf("reading message length: %w", err)
	}

	var len = int(binary.LittleEndian.Uint32(lengthBytes[:]))

	var b, err = peekMessage(c.r, len)
	if err != nil {
		return err
	}

	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("decoding message: %w", err)
	} else if err = m.Validate(); err != nil {
		return fmt.Errorf("validating message: %w", err)
	}

	return nil
}

func peekMessage(br *bufio.Reader, size int) ([]byte, error) {
	// Fetch next length-delimited message into a buffer.
	// In the garden-path case, we decode directly from the
	// bufio.Reader internal buffer without copying
	// (having just read the length, the message itself
	// is probably _already_ in the bufio.Reader buffer).
	//
	// If the message is larger than the internal buffer,
	// we allocate and read it directly.
	var bs, err = br.Peek(size)
	if err == nil {
		// The Discard contract guarantees we won't error.
		// It's safe to reference |bs| until the next Peek.
		if _, err = br.Discard(size); err != nil {
			panic(err)
		}
		return bs, nil
	}

	// Non-garden path: we must allocate and read a larger buffer.
	if errors.Is(err, bufio.ErrBufferFull) {
		bs = make([]byte, size)
		if _, err = io.ReadFull(br, bs); err != nil {
			return nil, fmt.Errorf("reading message (directly): %w", err)
		}
		return bs, nil
	}

	return nil, fmt.Errorf("reading message (into buffer): %w", err)
}

type specCmd struct{ cmdCommon }
type validateCmd struct{ cmdCommon }
type applyUpsertCmd struct{ cmdCommon }
type applyDeleteCmd struct{ cmdCommon }
type transactionsCmd struct{ cmdCommon }

func (c specCmd) Execute(args []string) error {
	var req pm.SpecRequest

	if err := c.readMsg(&req); err != nil {
		return fmt.Errorf("reading request: %w", err)
	} else if resp, err := c.srv.Spec(c.ctx, &req); err != nil {
		return err
	} else if err = c.w.WriteMsg(resp); err != nil {
		return fmt.Errorf("writing response: %w", err)
	}
	return nil
}

func (c validateCmd) Execute(args []string) error {
	var req pm.ValidateRequest

	if err := c.readMsg(&req); err != nil {
		return fmt.Errorf("reading request: %w", err)
	} else if resp, err := c.srv.Validate(c.ctx, &req); err != nil {
		return err
	} else if err = c.w.WriteMsg(resp); err != nil {
		return fmt.Errorf("writing response: %w", err)
	}
	return nil
}

func (c applyUpsertCmd) Execute(args []string) error {
	var req pm.ApplyRequest

	if err := c.readMsg(&req); err != nil {
		return fmt.Errorf("reading request: %w", err)
	} else if resp, err := c.srv.ApplyUpsert(c.ctx, &req); err != nil {
		return err
	} else if err = c.w.WriteMsg(resp); err != nil {
		return fmt.Errorf("writing response: %w", err)
	}

	return nil
}

func (c applyDeleteCmd) Execute(args []string) error {
	var req pm.ApplyRequest

	if err := c.readMsg(&req); err != nil {
		return fmt.Errorf("reading request: %w", err)
	}

	// TODO(johnny): Individual connectors need to be better behaved around deletion.
	// This is a stop-gap measure to get task deletion unblocked in the control plane.
	var resp, err = c.srv.ApplyDelete(c.ctx, &req)
	if err != nil {
		resp = &pm.ApplyResponse{ActionDescription: err.Error()}

		logrus.WithFields(logrus.Fields{"error": err}).
			Error("failed to delete the materialization from the endpoint")
	}

	if err = c.w.WriteMsg(resp); err != nil {
		return fmt.Errorf("writing response: %w", err)
	}

	return nil
}

func (c transactionsCmd) Execute(args []string) error {
	return c.srv.Transactions(&txnAdapter{cmdCommon: c.cmdCommon})
}

// txnAdapter is a pm.Driver_TransactionsServer built from
// os.Stdin and os.Stdout.
type txnAdapter struct{ cmdCommon }

func (a *txnAdapter) Send(m *pm.TransactionResponse) error {
	return a.SendMsg(m)
}
func (a *txnAdapter) Recv() (*pm.TransactionRequest, error) {
	m := new(pm.TransactionRequest)
	if err := a.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
func (a *txnAdapter) Context() context.Context {
	return a.ctx
}
func (a *txnAdapter) SetHeader(metadata.MD) error {
	panic("SetHeader is not supported")
}
func (a *txnAdapter) SendHeader(metadata.MD) error {
	panic("SendHeader is not supported")
}
func (a *txnAdapter) SetTrailer(metadata.MD) {
	panic("SetTrailer is not supported")
}
func (a *txnAdapter) SendMsg(m interface{}) error {
	return a.w.WriteMsg(m.(proto.Message))
}
func (a *txnAdapter) RecvMsg(m interface{}) error {
	return a.readMsg(m.(protoValidator))
}
