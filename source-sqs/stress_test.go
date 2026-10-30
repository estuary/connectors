package main

// Stress tests and benchmarks for performance work and profiling. None of
// these run in CI. Go benchmarks only run under an explicit -bench flag, and
// the TestStress* macro tests additionally skip unless STRESS_TEST=yes.
//
// Transform microbenchmark (no infrastructure needed):
//
//	go test ./source-sqs/ -run '^$' -bench BenchmarkMakeDocument -benchmem \
//	  -cpuprofile cpu.out
//
// End-to-end pipeline stress against LocalStack (or real AWS via
// AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY):
//
//	docker compose -f source-sqs/docker-compose.yaml up --wait
//	STRESS_TEST=yes TEST_DATABASE=yes go test ./source-sqs/ -v \
//	  -run TestStressCapture -cpuprofile cpu.out
//
// Knobs (env): STRESS_MESSAGES (default 20000), STRESS_BODY_BYTES (default
// 1024), STRESS_GROUPS (FIFO test, default 32), STRESS_ACK_DELAY (a Go
// duration simulating runtime commit latency, default 0).
//
// Against LocalStack, absolute numbers measure the emulator as much as the
// connector, so treat them as relative (regression-spotting and profiling)
// rather than AWS throughput. Point at real AWS for absolute numbers.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
)

func BenchmarkMakeDocument(b *testing.B) {
	var attributes = map[string]string{
		"SentTimestamp":           "1784055166151",
		"ApproximateReceiveCount": "1",
	}

	for _, tc := range []struct {
		name string
		body string
	}{
		{"json-256B", `{"id": 12345, "kind": "small", "payload": "` + strings.Repeat("x", 180) + `"}`},
		{"json-2KiB", `{"id": 12345, "kind": "medium", "payload": "` + strings.Repeat("x", 2000) + `"}`},
		{"json-64KiB", `{"id": 12345, "kind": "large", "payload": "` + strings.Repeat("x", 64*1024) + `"}`},
		{"json-1MiB-max", `{"id": 12345, "kind": "max", "payload": "` + strings.Repeat("x", 1024*1024-64) + `"}`},
		{"text-2KiB", strings.Repeat("plain text ", 190)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var msg = types.Message{
				MessageId:  aws.String("d6bf4b71-0a55-4c3e-9c6e-000000000000"),
				Body:       aws.String(tc.body),
				Attributes: attributes,
			}
			b.SetBytes(int64(len(tc.body)))
			for b.Loop() {
				if _, err := makeDocument("https://sqs.us-east-1.amazonaws.com/123/bench-queue", msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func stressEnvInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func stressConfig(t *testing.T) config {
	t.Helper()
	if os.Getenv("STRESS_TEST") != "yes" {
		t.Skipf("skipping %q: ${STRESS_TEST} != \"yes\" (stress tests never run in CI)", t.Name())
	}
	return testConfig(t)
}

// benchStream is a minimal Connector_CaptureServer that counts captured
// documents and acknowledges checkpoints like the runtime would, with an
// optional simulated commit latency. Unlike the snapshot-test harness it
// applies no sanitizers or validators, so CPU profiles of a stress run show
// connector code rather than test scaffolding.
type benchStream struct {
	ctx      context.Context
	ackDelay time.Duration

	mu          sync.Mutex
	ackReady    chan struct{} // closed and replaced to signal pending checkpoints
	pendingAcks uint32
	docs        int64
	docBytes    int64
	onDocument  func(docs int64)
}

func newBenchStream(ctx context.Context, ackDelay time.Duration, onDocument func(docs int64)) *benchStream {
	return &benchStream{
		ctx:        ctx,
		ackDelay:   ackDelay,
		ackReady:   make(chan struct{}),
		onDocument: onDocument,
	}
}

func (s *benchStream) Send(m *pc.Response) error {
	if m.Captured != nil {
		s.mu.Lock()
		s.docs++
		s.docBytes += int64(len(m.Captured.DocJson))
		var docs = s.docs
		s.mu.Unlock()
		s.onDocument(docs)
	} else if m.Checkpoint != nil {
		s.mu.Lock()
		s.pendingAcks++
		close(s.ackReady)
		s.ackReady = make(chan struct{})
		s.mu.Unlock()
	}
	return nil
}

func (s *benchStream) Recv() (*pc.Request, error) {
	for {
		s.mu.Lock()
		var pending = s.pendingAcks
		var ready = s.ackReady
		s.mu.Unlock()

		if pending > 0 {
			if s.ackDelay > 0 {
				select {
				case <-time.After(s.ackDelay):
				case <-s.ctx.Done():
					return nil, io.EOF
				}
			}
			s.mu.Lock()
			var n = s.pendingAcks
			s.pendingAcks = 0
			s.mu.Unlock()
			return &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: n}}, nil
		}

		select {
		case <-ready:
		case <-s.ctx.Done():
			return nil, io.EOF
		}
	}
}

func (s *benchStream) totals() (docs int64, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.docs, s.docBytes
}

func (s *benchStream) Context() context.Context     { return s.ctx }
func (s *benchStream) SendMsg(m any) error          { panic("SendMsg is not supported") }
func (s *benchStream) RecvMsg(m any) error          { panic("RecvMsg is not supported") }
func (s *benchStream) SendHeader(metadata.MD) error { panic("SendHeader is not supported") }
func (s *benchStream) SetHeader(metadata.MD) error  { panic("SetHeader is not supported") }
func (s *benchStream) SetTrailer(metadata.MD)       {}

// seedQueue loads the queue with n messages of roughly bodyBytes each,
// in parallel, optionally spread round-robin across FIFO groups.
func seedQueue(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string, n, bodyBytes, groups int) {
	t.Helper()

	var payload = strings.Repeat("x", max(bodyBytes-48, 16))
	// STRESS_PARTS > 0 adds a low-cardinality "part" field (i % parts) to each
	// body, for capturing into logically-partitioned collections when
	// measuring journal-count scaling.
	var docParts = stressEnvInt("STRESS_PARTS", 0)
	// Deduplication IDs incorporate a per-run nonce so that repeated seedings
	// of the same FIFO queue aren't silently collapsed by SQS's 5-minute
	// producer dedup window.
	var runID = uuid.NewString()[:8]
	const seeders = 16
	var group, groupCtx = errgroup.WithContext(ctx)
	var perSeeder = (n + seeders - 1) / seeders

	for s := range seeders {
		var start = s * perSeeder
		var end = min(start+perSeeder, n)
		group.Go(func() error {
			for batchStart := start; batchStart < end; batchStart += 10 {
				var batchEnd = min(batchStart+10, end)
				var entries = make([]types.SendMessageBatchRequestEntry, 0, batchEnd-batchStart)
				for i := batchStart; i < batchEnd; i++ {
					var body = fmt.Sprintf(`{"idx": %d, "payload": %q}`, i, payload)
					if docParts > 0 {
						body = fmt.Sprintf(`{"idx": %d, "part": %d, "payload": %q}`, i, i%docParts, payload)
					}
					var entry = types.SendMessageBatchRequestEntry{
						Id:          aws.String(strconv.Itoa(i - batchStart)),
						MessageBody: aws.String(body),
					}
					if groups > 0 {
						entry.MessageGroupId = aws.String(fmt.Sprintf("group-%03d", i%groups))
						entry.MessageDeduplicationId = aws.String(fmt.Sprintf("%s-%d", runID, i))
					}
					entries = append(entries, entry)
				}
				var resp, err = client.SendMessageBatch(groupCtx, &sqs.SendMessageBatchInput{
					QueueUrl: aws.String(queueURL),
					Entries:  entries,
				})
				if err != nil {
					return err
				} else if len(resp.Failed) > 0 {
					return fmt.Errorf("seeding: %d entries failed (first: %s)", len(resp.Failed), aws.ToString(resp.Failed[0].Message))
				}
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
}

// runStressCapture seeds a queue, runs the real Pull pipeline until every
// message has been captured, and reports capture throughput plus how long
// the post-capture delete drain took.
func runStressCapture(t *testing.T, queueName string, groups int) {
	var ctx = context.Background()

	var conf = stressConfig(t)
	var client = testClient(t, ctx, &conf)
	var queueURL = createQueue(t, ctx, client, queueName)

	var messages = stressEnvInt("STRESS_MESSAGES", 20_000)
	var bodyBytes = stressEnvInt("STRESS_BODY_BYTES", 1024)
	var ackDelay, _ = time.ParseDuration(os.Getenv("STRESS_ACK_DELAY"))

	var seedStart = time.Now()
	seedQueue(t, ctx, client, queueURL, messages, bodyBytes, groups)
	t.Logf("seeded %d messages of ~%dB in %s", messages, bodyBytes, time.Since(seedStart).Round(time.Millisecond))

	var resourceJSON, err = json.Marshal(resource{QueueURL: queueURL})
	require.NoError(t, err)
	configJSON, err := json.Marshal(&conf)
	require.NoError(t, err)

	var open = &pc.Request_Open{
		Capture: &pf.CaptureSpec{
			Name:       "acmeCo/stress/source-sqs",
			ConfigJson: configJSON,
			Bindings: []*pf.CaptureSpec_Binding{{
				ResourceConfigJson: resourceJSON,
				ResourcePath:       []string{queueURL},
			}},
		},
	}

	var captureCtx, cancel = context.WithCancel(ctx)
	defer cancel()

	// Cancelling the moment the last document arrives would strand the tail.
	// Its checkpoints wouldn't be acknowledged and its deletes never
	// dispatched, so those messages would redeliver after the visibility
	// timeout. Instead, note the capture-complete time, keep the stream (and
	// its acknowledgement flow) alive until the queue fully drains, and only
	// then shut down.
	var once sync.Once
	var captureElapsed time.Duration
	var capturedAll = make(chan struct{})
	var captureStart = time.Now()
	var stream = newBenchStream(captureCtx, ackDelay, func(docs int64) {
		if docs >= int64(messages) {
			once.Do(func() {
				captureElapsed = time.Since(captureStart)
				close(capturedAll)
			})
		}
	})

	var pullDone = make(chan error, 1)
	go func() {
		pullDone <- driver{}.Pull(open, &boilerplate.PullOutput{Connector_CaptureServer: stream})
	}()

	select {
	case <-capturedAll:
	case err := <-pullDone:
		t.Fatalf("capture ended before all messages were received: %v", err)
	case <-time.After(10 * time.Minute):
		t.Fatal("timed out waiting for capture to receive all messages")
	}

	var drainStart = time.Now()
	requireQueueDrained(t, ctx, client, queueURL)
	var drainElapsed = time.Since(drainStart)

	cancel()
	if err := <-pullDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("capture failed: %v", err)
	}

	var docs, capturedBytes = stream.totals()
	require.GreaterOrEqual(t, docs, int64(messages))

	t.Logf("captured %d docs (%.1f MB) in %s: %.0f msgs/sec, %.1f MB/sec (ackDelay=%s, groups=%d)",
		docs, float64(capturedBytes)/1e6, captureElapsed.Round(time.Millisecond),
		float64(docs)/captureElapsed.Seconds(), float64(capturedBytes)/1e6/captureElapsed.Seconds(),
		ackDelay, groups)
	t.Logf("delete drain completed %s after last document", drainElapsed.Round(time.Millisecond))
}

// TestStressSeed is a load generator, not a test. It seeds an existing queue
// (STRESS_QUEUE_URL) and exits, for driving a capture that's running under a
// real Flow stack. FIFO queues get STRESS_GROUPS message groups. Re-run it to
// keep a sustained backlog while measuring Flow-side commit+ack latency.
func TestStressSeed(t *testing.T) {
	var conf = stressConfig(t)
	var queueURL = os.Getenv("STRESS_QUEUE_URL")
	if queueURL == "" {
		t.Skip("set STRESS_QUEUE_URL to the queue to seed")
	}
	var name, _, err = parseQueueURL(queueURL)
	require.NoError(t, err)

	var groups = 0
	if isFifoQueueName(name) {
		groups = stressEnvInt("STRESS_GROUPS", 32)
	}
	var messages = stressEnvInt("STRESS_MESSAGES", 20_000)
	var bodyBytes = stressEnvInt("STRESS_BODY_BYTES", 1024)

	var ctx = context.Background()
	var client = testClient(t, ctx, &conf)
	var start = time.Now()
	seedQueue(t, ctx, client, queueURL, messages, bodyBytes, groups)
	var elapsed = time.Since(start)
	t.Logf("seeded %d messages of ~%dB (groups=%d) in %s: %.0f msgs/sec",
		messages, bodyBytes, groups, elapsed.Round(time.Millisecond), float64(messages)/elapsed.Seconds())
}

func TestStressCapture(t *testing.T) {
	runStressCapture(t, "test-stress-standard", 0)
}

func TestStressCaptureFifo(t *testing.T) {
	runStressCapture(t, "test-stress-ordered.fifo", stressEnvInt("STRESS_GROUPS", 32))
}
