package boilerplate

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"net/http"
	_ "net/http/pprof"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/minio/highwayhash"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

const outputWriteBuffer = 1 * 1024 * 1024 // 1MiB output buffer to amortize write syscall overhead

// StateKey is used to key binding-specific state a connector's driver checkpoint. It's just a type
// alias for a string to help differentiate any old string from where a specific state key from the
// runtime should be used.
type StateKey string

type Connector interface {
	Spec(context.Context, *pc.Request_Spec) (*pc.Response_Spec, error)
	Discover(context.Context, *pc.Request_Discover) (*pc.Response_Discovered, error)
	Validate(context.Context, *pc.Request_Validate) (*pc.Response_Validated, error)
	Apply(context.Context, *pc.Request_Apply) (*pc.Response_Applied, error)
	Pull(open *pc.Request_Open, stream *PullOutput) error
}

// ConnectorServer wraps a Connector to implement the pc.ConnectorServer gRPC service interface.
type ConnectorServer struct {
	Connector
}

// RunMain is the boilerplate main function of a capture connector.
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
	var stream pc.Connector_CaptureServer

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

	go func() {
		log.WithField("port", 6060).Debug("starting pprof server")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.WithField("err", err).Info("pprof server shut down unexpectedly")
		}
	}()

	var server = ConnectorServer{connector}

	if err := server.Capture(stream); err != nil {
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

var _ pc.ConnectorServer = &ConnectorServer{}

func (s *ConnectorServer) Capture(stream pc.Connector_CaptureServer) error {
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

			if err := stream.Send(&pc.Response{Spec: response}); err != nil {
				return err
			}
		case request.Discover != nil:
			if response, err := s.Connector.Discover(ctx, request.Discover); err != nil {
				return err
			} else if err := stream.Send(&pc.Response{Discovered: response}); err != nil {
				return err
			}
		case request.Validate != nil:
			if response, err := s.Connector.Validate(ctx, request.Validate); err != nil {
				return err
			} else if err := stream.Send(&pc.Response{Validated: response}); err != nil {
				return err
			}
		case request.Apply != nil:
			if response, err := s.Connector.Apply(ctx, request.Apply); err != nil {
				return err
			} else if err := stream.Send(&pc.Response{Applied: response}); err != nil {
				return err
			}
		case request.Open != nil:
			return s.Connector.Pull(request.Open, &PullOutput{Connector_CaptureServer: stream})
		default:
			return fmt.Errorf("unexpected request %#v", request)
		}
	}
}

// PullOutput is a convenience wrapper around a pc.Connector_CaptureServer
// which makes it simpler to emit documents and state checkpoints.
// TODO(johnny): A better name would be PullStream, but I'm refraining right now to avoid churn.
type PullOutput struct {
	sync.Mutex
	pc.Connector_CaptureServer
}

// Ready sends a PullResponse_Opened message to indicate that the capture has started.
func (out *PullOutput) Ready(explicitAcknowledgements bool) error {
	log.Debug("sending PullResponse.Opened")
	out.Lock()
	defer out.Unlock()
	if err := out.Send(&pc.Response{
		Opened: &pc.Response_Opened{
			ExplicitAcknowledgements: explicitAcknowledgements,
		},
	}); err != nil {
		return fmt.Errorf("error sending PullResponse.Opened: %w", err)
	}
	return nil
}

// Documents emits one or more documents to the specified binding index.
func (out *PullOutput) Documents(binding int, docs ...json.RawMessage) error {
	log.WithField("count", len(docs)).Trace("emitting documents")

	var messages []*pc.Response
	for _, doc := range docs {
		messages = append(messages, &pc.Response{
			Captured: &pc.Response_Captured{
				Binding: uint32(binding),
				DocJson: doc,
			},
		})
	}

	// Emit all the messages with a single mutex acquisition so that a single Documents()
	// call is atomic (no Checkpoint outputs can be interleaved with it) even when handling
	// multiple documents at once.
	out.Lock()
	defer out.Unlock()
	for _, msg := range messages {
		if err := out.Send(msg); err != nil {
			return fmt.Errorf("writing captured documents: %w", err)
		}
	}
	return nil
}

// Checkpoint emits a state checkpoint, with or without RFC 7396 patch-merge semantics.
func (out *PullOutput) Checkpoint(checkpoint json.RawMessage, merge bool) error {
	log.WithFields(log.Fields{
		"checkpoint": checkpoint,
		"merge":      merge,
	}).Trace("emitting checkpoint")

	var msg = &pc.Response{
		Checkpoint: &pc.Response_Checkpoint{
			State: &pf.ConnectorState{
				UpdatedJson: checkpoint,
				MergePatch:  merge,
			},
		},
	}

	out.Lock()
	defer out.Unlock()
	if err := out.Send(msg); err != nil {
		return fmt.Errorf("writing checkpoint: %w", err)
	}
	return nil
}

// Emit documents and checkpoints with a common lock, this is useful if there
// are multiple threads (goroutines) processing documents and emitting
// checkpoints. In those instances using this method can allow each thread to
// ensure that they only checkpoint their own documents
func (out *PullOutput) DocumentsAndCheckpoint(checkpoint json.RawMessage, merge bool, binding int, docs ...json.RawMessage) error {
	log.WithField("count", len(docs)).Trace("emitting documents")

	var messages []*pc.Response
	for _, doc := range docs {
		messages = append(messages, &pc.Response{
			Captured: &pc.Response_Captured{
				Binding: uint32(binding),
				DocJson: doc,
			},
		})
	}

	log.WithFields(log.Fields{
		"checkpoint": checkpoint,
		"merge":      merge,
	}).Trace("emitting checkpoint")

	var cp = &pc.Response{
		Checkpoint: &pc.Response_Checkpoint{
			State: &pf.ConnectorState{
				UpdatedJson: checkpoint,
				MergePatch:  merge,
			},
		},
	}

	out.Lock()
	defer out.Unlock()
	for _, msg := range messages {
		if err := out.Send(msg); err != nil {
			return fmt.Errorf("writing captured documents: %w", err)
		}
	}
	if err := out.Send(cp); err != nil {
		return fmt.Errorf("writing checkpoint: %w", err)
	}
	return nil
}

func newProtoCodec(ctx context.Context) pc.Connector_CaptureServer {
	var bw = bufio.NewWriterSize(os.Stdout, outputWriteBuffer)
	return &protoCodec{
		ctx: ctx,
		r:   bufio.NewReaderSize(os.Stdin, 1<<21),
		bw:  bw,
		pw:  protoio.NewUint32DelimitedWriter(bw, binary.LittleEndian),
	}
}

type protoCodec struct {
	ctx context.Context
	r   *bufio.Reader
	bw  *bufio.Writer
	pw  protoio.Writer
}

func (c *protoCodec) Context() context.Context {
	return c.ctx
}

func (c *protoCodec) Send(m *pc.Response) error {
	if err := c.SendMsg(m); err != nil {
		return err
	}

	// Flush the buffered output writer after anything *other* than a captured document.
	// This logic assumes that either a captured document is present or this is some
	// other sort of message, which is always true in practice.
	if m.Captured == nil {
		return c.bw.Flush()
	}
	return nil
}
func (c *protoCodec) Recv() (*pc.Request, error) {
	var m = new(pc.Request)
	if err := c.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
func (c *protoCodec) SendMsg(m interface{}) error {
	return c.pw.WriteMsg(m.(proto.Message))
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

func newJsonCodec(ctx context.Context) pc.Connector_CaptureServer {
	var bw = bufio.NewWriterSize(os.Stdout, outputWriteBuffer)
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
		bw:      bw,
	}
}

type jsonCodec struct {
	ctx         context.Context
	marshaler   jsonpb.Marshaler
	unmarshaler jsonpb.Unmarshaler
	decoder     *json.Decoder
	bw          *bufio.Writer
}

func (c *jsonCodec) Context() context.Context {
	return c.ctx
}
func (c *jsonCodec) Send(m *pc.Response) error {
	if err := c.SendMsg(m); err != nil {
		return err
	}

	// Flush the buffered output writer after anything *other* than a captured document.
	// This logic assumes that either a captured document is present or this is some
	// other sort of message, which is always true in practice.
	if m.Captured == nil {
		return c.bw.Flush()
	}
	return nil
}
func (c *jsonCodec) Recv() (*pc.Request, error) {
	var m = new(pc.Request)
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

	var _, err = c.bw.Write(w.Bytes())
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

// highwayHashKey is a fixed 32 bytes (as required by HighwayHash) read from /dev/random.
// DO NOT MODIFY this value, as it is required to have consistent hash results.
var highwayHashKey, _ = hex.DecodeString("332757d16f0fb1cf2d4f676f85e34c6a8b85aa58f42bb081449d8eb2e4ed529f")

func hwHashPartition(partitionId []byte) uint32 {
	return uint32(highwayhash.Sum64(partitionId, highwayHashKey) >> 32)
}

func RangeIncludesHwHash(r *pf.RangeSpec, partitionID []byte) bool {
	var hashed = hwHashPartition(partitionID)
	return RangeIncludes(r, hashed)
}

func RangeIncludes(r *pf.RangeSpec, hash uint32) bool {
	return hash >= r.KeyBegin && hash <= r.KeyEnd
}

// RangeContain is the result of checking whether one Range contains another.
type RangeContain int

const (
	NoRangeContain      RangeContain = 0
	PartialRangeContain RangeContain = 1
	FullRangeContain    RangeContain = 2
)

func RangeContained(rangeOne *pf.RangeSpec, rangeTwo *pf.RangeSpec) RangeContain {
	var includesBegin = RangeIncludes(rangeOne, rangeTwo.KeyBegin)
	var includesEnd = RangeIncludes(rangeOne, rangeTwo.KeyEnd)

	if includesBegin && includesEnd {
		return FullRangeContain
	} else if includesBegin != includesEnd {
		return PartialRangeContain
	} else {
		return NoRangeContain
	}
}

// Intersection returns the intersection of two overlapping Ranges. If the ranges do not
// overlap, this function will panic.
func RangeIntersection(rangeOne *pf.RangeSpec, rangeTwo *pf.RangeSpec) pf.RangeSpec {
	var result = pf.RangeSpec{}
	result.KeyBegin = rangeOne.KeyBegin
	result.KeyEnd = rangeOne.KeyEnd

	if rangeTwo.KeyBegin > rangeOne.KeyBegin {
		result.KeyBegin = rangeTwo.KeyBegin
	}
	if rangeTwo.KeyEnd < rangeOne.KeyEnd {
		result.KeyEnd = rangeTwo.KeyEnd
	}
	if result.KeyBegin > result.KeyEnd {
		panic("intersected partition ranges that do not overlap")
	}
	return result
}
