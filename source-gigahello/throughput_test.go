package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/require"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	protoio "github.com/gogo/protobuf/io"
	broker_protocol "go.gazette.dev/core/broker/protocol"
	consumer_protocol "go.gazette.dev/core/consumer/protocol"
)

func Benchmark_1MBps(b *testing.B)     { bench(b, "Hello, world!", 300000, 1.0) }
func Benchmark_10MBps(b *testing.B)    { bench(b, "Hello, world!", 3000000, 10.0) }
func Benchmark_100MBps(b *testing.B)   { bench(b, "Hello, world!", 30000000, 100.0) }
func Benchmark_Max(b *testing.B)       { bench(b, "Hello, world!", 100000000, -1) }
func Benchmark_Large_Max(b *testing.B) { bench(b, "Lorem ipsum dolor sit amet", 100000000, -1) }

func bench(b *testing.B, message string, numMessages int, rate float64) {
	var ctx, cs = context.Background(), testCaptureSpec(b)
	cs.EndpointSpec.(*config).Rate = float32(rate)
	cs.Bindings = discoverBindings(ctx, b, cs, regexp.MustCompile(`.*`))
	setBindingMessage(cs.Bindings[0], message)
	setShutdownAfter(numMessages)

	var totalOutputBytes int64
	for b.Loop() {
		totalOutputBytes += runCapture(context.Background(), b, cs)
	}

	b.ReportMetric(0, "ns/op")
	b.ReportMetric(float64(totalOutputBytes)/float64(b.N*numMessages), "bytes/doc")
	b.ReportMetric(b.Elapsed().Seconds(), "seconds")
	b.ReportMetric(float64(b.N*numMessages)/float64(b.Elapsed().Seconds()), "rows/sec")
	b.ReportMetric(float64(totalOutputBytes)/1000000, "MB")
	b.ReportMetric(float64(totalOutputBytes)/float64(1000000*b.Elapsed().Seconds()), "MBps")
}

func runCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) int64 {
	t.Helper()

	endpointSpecJSON, err := json.Marshal(cs.EndpointSpec)
	require.NoError(t, err)

	var open proto.Message = &pc.Request{
		Open: &pc.Request_Open{
			StateJson: cs.Checkpoint,
			Capture: &pf.CaptureSpec{
				Name:       "acmeCo/test-capture/source-something",
				ConfigJson: endpointSpecJSON,
				Bindings:   cs.Bindings,
				// Dummies to satisfy protocol validation, specific values shouldn't be important.
				ShardTemplate: &consumer_protocol.ShardSpec{
					Id: "capture/acmeCo/test-capture/source-something/69f23210cf8ccfcd/00000000-00000000",

					MaxTxnDuration: 1 * time.Second,
				},
				RecoveryLogTemplate: &broker_protocol.JournalSpec{
					Name:        broker_protocol.Journal("recovery/capture/acmeCo/test-capture/source-something/69f23210cf8ccfcd/00000000-00000000"),
					Replication: 3,
					Fragment: broker_protocol.JournalSpec_Fragment{
						Length:           512 * 1024 * 1024, // 512 MiB
						CompressionCodec: broker_protocol.CompressionCodec_GZIP,
						RefreshInterval:  30 * time.Second,
					},
				},
			},
			Range: &pf.RangeSpec{
				KeyBegin:    0,
				KeyEnd:      math.MaxUint32,
				RClockBegin: 0,
				RClockEnd:   math.MaxUint32,
			},
		},
	}

	// Serialize the Open request into a byte buffer which will act as the input reader for the benchmark run.
	var input bytes.Buffer
	err = protoio.NewUint32DelimitedWriter(&input, binary.LittleEndian).WriteMsg(open)
	require.NoError(t, err)

	// Run the capture, with an output io.Writer that merely tallies up the number of bytes written.
	var outputBytes int64
	var output = WriterFunc(func(bs []byte) (n int, err error) {
		outputBytes += int64(len(bs))
		return len(bs), nil
	})
	require.NoError(t, boilerplate.InnerMain(ctx, driver{}, &input, output))
	cs.Checkpoint = FinalStateCheckpoint
	return outputBytes

}

type WriterFunc func(p []byte) (n int, err error)

func (w WriterFunc) Write(p []byte) (n int, err error) { return w(p) }
