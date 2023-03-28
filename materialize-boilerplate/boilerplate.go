package boilerplate

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	pm "github.com/estuary/flow/go/protocols/materialize"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

type Connector interface {
	Spec(context.Context, *pm.Request_Spec) (*pm.Response_Spec, error)
	Validate(context.Context, *pm.Request_Validate) (*pm.Response_Validated, error)
	Apply(context.Context, *pm.Request_Apply) (*pm.Response_Applied, error)
	NewTransactor(context.Context, pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error)
}

// ConnectorServer wraps a Connector to implement the pc.ConnectorServer gRPC service interface.
type ConnectorServer struct {
	Connector
}

// RunMain is the boilerplate main function of a materialization connector.
func RunMain(connector Connector) {
	switch format := getEnvDefault("LOG_FORMAT", "color"); format {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
	case "text":
		log.SetFormatter(&log.TextFormatter{})
	case "color":
		log.SetFormatter(&log.TextFormatter{ForceColors: true})
	default:
		log.WithField("format", format).Fatal("invalid LOG_FORMAT (expected 'json', 'text', or 'color')")
	}

	if lvl, err := log.ParseLevel(getEnvDefault("LOG_LEVEL", "info")); err != nil {
		log.WithFields(log.Fields{"level": lvl, "error": err}).Fatal("unrecognized log level")
	} else {
		log.SetLevel(lvl)
	}

	var ctx, _ = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	var stream pm.Connector_MaterializeServer

	switch codec := getEnvDefault("FLOW_RUNTIME_CODEC", "proto"); codec {
	case "proto":
		log.Debug("using protobuf codec")
		stream = newProtoCodec(ctx)
	case "json":
		log.Debug("using json codec")
		stream = newJsonCodec(ctx)
	default:
		log.WithField("codec", codec).Fatal("invalid FLOW_RUNTIME_CODEC (expected 'json', or 'proto')")
	}

	var server = ConnectorServer{connector}

	if err := server.Materialize(stream); err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		_, _ = os.Stderr.Write([]byte("\n"))
		os.Exit(1)
	}
	os.Exit(0)
}

func getEnvDefault(name, def string) string {
	var s = os.Getenv(name)
	if s == "" {
		return def
	}
	return s
}

var _ pm.ConnectorServer = &ConnectorServer{}

func (s *ConnectorServer) Materialize(stream pm.Connector_MaterializeServer) error {
	var ctx = stream.Context()

	for {
		var request, err = stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if err = request.Validate_(); err != nil {
			return fmt.Errorf("validating request: %w", err)
		}

		switch {
		case request.Spec != nil:
			var response, err = s.Connector.Spec(ctx, request.Spec)
			if err != nil {
				return err
			}
			response.Protocol = 3032023

			if err := stream.Send(&pm.Response{Spec: response}); err != nil {
				return err
			}
		case request.Validate != nil:
			if response, err := s.Connector.Validate(ctx, request.Validate); err != nil {
				return err
			} else if err := stream.Send(&pm.Response{Validated: response}); err != nil {
				return err
			}
		case request.Apply != nil:
			if response, err := s.Connector.Apply(ctx, request.Apply); err != nil {
				return err
			} else if err := stream.Send(&pm.Response{Applied: response}); err != nil {
				return err
			}
		case request.Open != nil:
			transactor, opened, err := s.Connector.NewTransactor(ctx, *request.Open)
			if err != nil {
				return err
			}
			return pm.RunTransactions(stream, *request.Open, *opened, transactor)
		default:
			return fmt.Errorf("unexpected request %#v", request)
		}
	}
}

func newProtoCodec(ctx context.Context) pm.Connector_MaterializeServer {
	return &protoCodec{
		ctx: ctx,
		r:   bufio.NewReaderSize(os.Stdin, 1<<21),
		w:   protoio.NewUint32DelimitedWriter(os.Stdout, binary.LittleEndian),
	}
}

type protoCodec struct {
	ctx context.Context
	r   *bufio.Reader
	w   protoio.Writer
}

func (c *protoCodec) Context() context.Context {
	return c.ctx
}

func (c *protoCodec) Send(m *pm.Response) error {
	return c.SendMsg(m)
}
func (c *protoCodec) Recv() (*pm.Request, error) {
	var m = new(pm.Request)
	if err := c.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
func (c *protoCodec) SendMsg(m interface{}) error {
	return c.w.WriteMsg(m.(proto.Message))
}
func (c *protoCodec) RecvMsg(m interface{}) error {
	var lengthBytes [4]byte

	if _, err := io.ReadFull(c.r, lengthBytes[:]); err == io.EOF {
		return err
	} else if err != nil {
		return fmt.Errorf("reading message length: %w", err)
	}

	var len = int(binary.LittleEndian.Uint32(lengthBytes[:]))

	var b, err = c.peekMessage(len)
	if err != nil {
		return err
	}

	if err := proto.Unmarshal(b, m.(proto.Message)); err != nil {
		return fmt.Errorf("decoding message: %w", err)
	}
	return nil
}
func (c *protoCodec) SendHeader(metadata.MD) error {
	panic("SendHeader is not supported")
}
func (c *protoCodec) SetHeader(metadata.MD) error {
	panic("SetHeader is not supported")
}
func (c *protoCodec) SetTrailer(metadata.MD) {
	panic("SetTrailer is not supported")
}

func (c *protoCodec) peekMessage(size int) ([]byte, error) {
	// Fetch next length-delimited message into a buffer.
	// In the garden-path case, we decode directly from the
	// bufio.Reader internal buffer without copying
	// (having just read the length, the message itself
	// is probably _already_ in the bufio.Reader buffer).
	//
	// If the message is larger than the internal buffer,
	// we allocate and read it directly.
	var bs, err = c.r.Peek(size)
	if err == nil {
		// The Discard contract guarantees we won't error.
		// It's safe to reference |bs| until the next Peek.
		if _, err = c.r.Discard(size); err != nil {
			panic(err)
		}
		return bs, nil
	}

	// Non-garden path: we must allocate and read a larger buffer.
	if errors.Is(err, bufio.ErrBufferFull) {
		bs = make([]byte, size)
		if _, err = io.ReadFull(c.r, bs); err != nil {
			return nil, fmt.Errorf("reading message (directly): %w", err)
		}
		return bs, nil
	}

	return nil, fmt.Errorf("reading message (into buffer): %w", err)
}

func newJsonCodec(ctx context.Context) pm.Connector_MaterializeServer {
	return &jsonCodec{
		ctx: ctx,
		marshaler: jsonpb.Marshaler{
			EnumsAsInts:  false,
			EmitDefaults: false,
			Indent:       "", // Compact.
			OrigName:     false,
			AnyResolver:  nil,
		},
		unmarshaler: jsonpb.Unmarshaler{
			AllowUnknownFields: true,
			AnyResolver:        nil,
		},
		decoder: json.NewDecoder(bufio.NewReaderSize(os.Stdin, 1<<21)),
	}
}

type jsonCodec struct {
	ctx         context.Context
	marshaler   jsonpb.Marshaler
	unmarshaler jsonpb.Unmarshaler
	decoder     *json.Decoder
}

func (c *jsonCodec) Context() context.Context {
	return c.ctx
}
func (c *jsonCodec) Send(m *pm.Response) error {
	return c.SendMsg(m)
}
func (c *jsonCodec) Recv() (*pm.Request, error) {
	var m = new(pm.Request)
	if err := c.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
func (c *jsonCodec) SendMsg(m interface{}) error {
	var w bytes.Buffer

	if err := c.marshaler.Marshal(&w, m.(proto.Message)); err != nil {
		return fmt.Errorf("marshal response to json: %w", err)
	}
	_ = w.WriteByte('\n')

	var _, err = os.Stdout.Write(w.Bytes())
	return err
}
func (c *jsonCodec) RecvMsg(m interface{}) error {
	m.(proto.Message).Reset()
	return c.unmarshaler.UnmarshalNext(c.decoder, m.(proto.Message))
}
func (c *jsonCodec) SendHeader(metadata.MD) error {
	panic("SendHeader is not supported")
}
func (c *jsonCodec) SetHeader(metadata.MD) error {
	panic("SetHeader is not supported")
}
func (c *jsonCodec) SetTrailer(metadata.MD) {
	panic("SetTrailer is not supported")
}
