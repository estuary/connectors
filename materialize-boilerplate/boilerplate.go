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
	"time"

	"net/http"
	_ "net/http/pprof"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	pm "github.com/estuary/flow/go/protocols/materialize"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type Connector interface {
	Spec(context.Context, *pm.Request_Spec) (*pm.Response_Spec, error)
	Validate(context.Context, *pm.Request_Validate) (*pm.Response_Validated, error)
	Apply(context.Context, *pm.Request_Apply) (*pm.Response_Applied, error)
	NewTransactor(context.Context, pm.Request_Open, *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error)
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

	lvl, err := log.ParseLevel(getEnvDefault("LOG_LEVEL", "info"))
	if err != nil {
		log.WithFields(log.Fields{"level": lvl, "error": err}).Fatal("unrecognized log level")
	} else {
		log.SetLevel(lvl)
	}

	var ctx, _ = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	var stream m.MaterializeStream

	switch codec := getEnvDefault("FLOW_RUNTIME_CODEC", "proto"); codec {
	case "proto":
		log.Debug("using protobuf codec")
		stream = newProtoCodec()
	case "json":
		log.Debug("using json codec")
		stream = newJsonCodec()
	default:
		log.WithField("codec", codec).Fatal("invalid FLOW_RUNTIME_CODEC (expected 'json', or 'proto')")
	}

	go func() {
		log.WithField("port", 6060).Debug("starting pprof server")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.WithField("err", err).Info("pprof server shut down unexpectedly")
		}
	}()

	if err := materialize(ctx, stream, connector, lvl); err != nil {
		cerrors.HandleFinalError(err)
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

func materialize(ctx context.Context, stream m.MaterializeStream, connector Connector, lvl log.Level) error {
	for {
		var request pm.Request
		if err := stream.RecvMsg(&request); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if err = request.Validate_(); err != nil {
			return fmt.Errorf("validating request: %w", err)
		}

		switch {
		case request.Spec != nil:
			var response, err = connector.Spec(ctx, request.Spec)
			if err != nil {
				return err
			}
			response.Protocol = 3032023

			if err := stream.Send(&pm.Response{Spec: response}); err != nil {
				return err
			}
		case request.Validate != nil:
			if response, err := connector.Validate(ctx, request.Validate); err != nil {
				return err
			} else if err := stream.Send(&pm.Response{Validated: response}); err != nil {
				return err
			}
		case request.Apply != nil:
			if response, err := connector.Apply(ctx, request.Apply); err != nil {
				return err
			} else if err := stream.Send(&pm.Response{Applied: response}); err != nil {
				return err
			}
		case request.Open != nil:
			be := newBindingEvents()

			openStart := time.Now()
			log.Info("requesting materialization Open")
			stop := repeatAsync(func() { log.Info("materialization Open in progress") }, loggingFrequency)
			transactor, opened, options, err := connector.NewTransactor(ctx, *request.Open, be)
			if err != nil {
				return err
			}
			stop(func() {
				log.WithFields(log.Fields{"took": time.Since(openStart).String()}).Info("finished waiting for materialization Open")
			})

			if options == nil {
				options = &MaterializeOptions{}
			}

			ts, err := newTransactionsStream(ctx, stream, lvl, *options, be)
			if err != nil {
				return fmt.Errorf("creating transactions stream: %w", err)
			}

			return m.RunTransactions(ctx, ts, *request.Open, *opened, transactor)
		default:
			return fmt.Errorf("unexpected request %#v", request)
		}
	}
}

func newProtoCodec() m.MaterializeStream {
	return &protoCodec{
		r: bufio.NewReaderSize(os.Stdin, 1<<21),
		w: protoio.NewUint32DelimitedWriter(os.Stdout, binary.LittleEndian),
	}
}

type protoCodec struct {
	r *bufio.Reader
	w protoio.Writer
}

func (c *protoCodec) Send(m *pm.Response) error {
	return c.w.WriteMsg(m)
}

func (c *protoCodec) RecvMsg(m *pm.Request) error {
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

	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("decoding message: %w", err)
	}
	return nil
}

func (c *protoCodec) peekMessage(size int) ([]byte, error) {
	// TODO(johnny): Remove me when we resolve the json.RawMessage casting issue.
	var buf = make([]byte, size)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return nil, fmt.Errorf("reading message (directly): %w", err)
	}
	return buf, nil

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

func newJsonCodec() m.MaterializeStream {
	return &jsonCodec{
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
	marshaler   jsonpb.Marshaler
	unmarshaler jsonpb.Unmarshaler
	decoder     *json.Decoder
}

func (c *jsonCodec) Send(m *pm.Response) error {
	var w bytes.Buffer

	if err := c.marshaler.Marshal(&w, m); err != nil {
		return fmt.Errorf("marshal response to json: %w", err)
	}
	_ = w.WriteByte('\n')

	var _, err = os.Stdout.Write(w.Bytes())
	return err
}

func (c *jsonCodec) RecvMsg(m *pm.Request) error {
	m.Reset()
	return c.unmarshaler.UnmarshalNext(c.decoder, m)
}
