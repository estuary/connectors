package boilerplate

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	pc "github.com/estuary/flow/go/protocols/capture"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

// RunMain is the boilerplate main function of a capture connector.
func RunMain(srv pc.DriverServer) {
	var parser = flags.NewParser(nil, flags.Default)
	var ctx, _ = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)

	var conn net.Conn
	if server, err := net.Listen("tcp", ":2222"); err != nil {
		logrus.WithFields(logrus.Fields{"error": err}).
			Fatal("failed to start tcp server")
	} else if conn, err = server.Accept(); err != nil {
		logrus.WithFields(logrus.Fields{"error": err}).
			Fatal("failed to accept connection on tcp server")
	}

	var cmd = cmdCommon{
		ctx: ctx,
		srv: srv,
		r:   bufio.NewReaderSize(conn, 1<<21), // 2MB buffer.
		w:   protoio.NewUint32DelimitedWriter(conn, binary.LittleEndian),
	}

	parser.AddCommand("spec", "Write the connector specification",
		"Write the connector specification to stdout, then exit", &specCmd{cmd})
	parser.AddCommand("validate", "Validate a materialization",
		"Validate a proposed ValidateRequest read from stdin", &validateCmd{cmd})
	parser.AddCommand("discover", "Enumerate available streams",
		"Connect to the target system and enumerate streams available to be captured", &discoverCmd{cmd})
	parser.AddCommand("apply-upsert", "Apply an insert or update of a materialization",
		"Apply a proposed insert or update ApplyRequest read from stdin", &applyUpsertCmd{cmd})
	parser.AddCommand("apply-delete", "Apply a deletion of a materialization",
		"Apply a proposed delete ApplyRequest read from stdin", &applyDeleteCmd{cmd})
	parser.AddCommand("pull", "Pull data from the target system",
		"Connect to the target system and begin pulling data from the selected streams", &pullCmd{cmd})

	var _, err = parser.Parse()
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

type cmdCommon struct {
	ctx context.Context
	srv pc.DriverServer
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
type discoverCmd struct{ cmdCommon }
type applyUpsertCmd struct{ cmdCommon }
type applyDeleteCmd struct{ cmdCommon }
type pullCmd struct{ cmdCommon }

func (c specCmd) Execute(args []string) error {
	var req pc.SpecRequest
	log.Debug("executing Spec subcommand")

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
	var req pc.ValidateRequest
	log.Debug("executing Validate subcommand")

	if err := c.readMsg(&req); err != nil {
		return fmt.Errorf("reading request: %w", err)
	} else if resp, err := c.srv.Validate(c.ctx, &req); err != nil {
		return err
	} else if err = c.w.WriteMsg(resp); err != nil {
		return fmt.Errorf("writing response: %w", err)
	}
	return nil
}

func (c discoverCmd) Execute(args []string) error {
	var req pc.DiscoverRequest
	log.Debug("executing Discover subcommand")

	if err := c.readMsg(&req); err != nil {
		return fmt.Errorf("reading request: %w", err)
	} else if resp, err := c.srv.Discover(c.ctx, &req); err != nil {
		return err
	} else if err = c.w.WriteMsg(resp); err != nil {
		return fmt.Errorf("writing response: %w", err)
	}
	return nil
}

func (c applyUpsertCmd) Execute(args []string) error {
	var req pc.ApplyRequest

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
	var req pc.ApplyRequest

	if err := c.readMsg(&req); err != nil {
		return fmt.Errorf("reading request: %w", err)
	} else if resp, err := c.srv.ApplyDelete(c.ctx, &req); err != nil {
		// Translate delete errors into a successful response so that task
		// deletion cannot fail.
		resp = &pc.ApplyResponse{ActionDescription: err.Error()}
		logrus.WithFields(logrus.Fields{"error": err}).
			Error("failed to delete the capture cleanly")
	} else if err = c.w.WriteMsg(resp); err != nil {
		return fmt.Errorf("writing response: %w", err)
	}

	return nil
}

func (c pullCmd) Execute(args []string) error {
	log.Debug("executing Pull subcommand")
	return c.srv.Pull(&pullAdapter{cmdCommon: c.cmdCommon})
}

// pullAdapter is a pc.Driver_PullServer built from
// os.Stdin and os.Stdout.
type pullAdapter struct{ cmdCommon }

func (a *pullAdapter) Context() context.Context {
	return a.ctx
}

func (a *pullAdapter) Send(m *pc.PullResponse) error {
	return a.SendMsg(m)
}
func (a *pullAdapter) Recv() (*pc.PullRequest, error) {
	m := new(pc.PullRequest)
	if err := a.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
func (a *pullAdapter) SendMsg(m interface{}) error {
	return a.w.WriteMsg(m.(proto.Message))
}
func (a *pullAdapter) RecvMsg(m interface{}) error {
	return a.readMsg(m.(protoValidator))
}
func (a *pullAdapter) SendHeader(metadata.MD) error {
	panic("SendHeader is not supported")
}
func (a *pullAdapter) SetHeader(metadata.MD) error {
	panic("SetHeader is not supported")
}
func (a *pullAdapter) SetTrailer(metadata.MD) {
	panic("SetTrailer is not supported")
}
