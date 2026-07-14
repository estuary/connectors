package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var captureSanitizers = map[string]*regexp.Regexp{
	"<MESSAGE_ID>":                           regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`),
	"<TIMESTAMP>":                            regexp.MustCompile(`[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z`),
	`"sequenceNumber":"<SEQUENCE_NUMBER>"`:   regexp.MustCompile(`"sequenceNumber":"[0-9]+"`),
	"https://sqs.test.example/123456789012/": queueURLPrefixRe,
}

func testCaptureSpec(t *testing.T, conf *config, queueURLs ...string) *st.CaptureSpec {
	t.Helper()

	// Modest concurrency keeps the LocalStack emulator responsive. Defaults
	// are sized for real AWS at high throughput.
	prevReceivers, prevDeleteWorkers := receiversPerBinding, deleteWorkers
	receiversPerBinding, deleteWorkers = 4, 4
	t.Cleanup(func() { receiversPerBinding, deleteWorkers = prevReceivers, prevDeleteWorkers })

	var bindings []*pf.CaptureSpec_Binding
	for _, queueURL := range queueURLs {
		resourceJSON, err := json.Marshal(resource{QueueURL: queueURL})
		require.NoError(t, err)
		name, _, err := parseQueueURL(queueURL)
		require.NoError(t, err)
		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: resourceJSON,
			ResourcePath:       []string{queueURL},
			StateKey:           name,
		})
	}

	return &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: conf,
		Bindings:     bindings,
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   captureSanitizers,
	}
}

// advanceCapture runs the capture until no documents have arrived for a
// shutdown delay, then cancels it. The delay must cover a long-poll gap plus
// the ack/delete round trip for the final checkpoint.
func advanceCapture(t testing.TB, cs *st.CaptureSpec) {
	t.Helper()

	captureCtx, cancelCapture := context.WithCancel(context.Background())
	shutdownWatchdog := time.AfterFunc(60*time.Second, func() {
		log.Debug("capture initial timeout expired with no data")
		cancelCapture()
	})

	cs.Capture(captureCtx, t, func(data json.RawMessage) {
		shutdownWatchdog.Reset(2 * time.Second)
	})

	for _, err := range cs.Errors {
		require.NoError(t, err)
	}
}

func sendTestMessages(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string, bodies []string) {
	t.Helper()
	for start := 0; start < len(bodies); start += 10 {
		batch := bodies[start:min(start+10, len(bodies))]
		entries := make([]types.SendMessageBatchRequestEntry, len(batch))
		for i, body := range batch {
			entries[i] = types.SendMessageBatchRequestEntry{
				Id:          aws.String(strconv.Itoa(i)),
				MessageBody: aws.String(body),
			}
		}
		resp, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  entries,
		})
		require.NoError(t, err)
		require.Empty(t, resp.Failed)
	}
}

// requireQueueDrained asserts that every message was deleted from the queue.
// Captured documents only ever have their receipts deleted after the runtime
// acknowledged the enclosing checkpoint, so a drained queue proves the full
// emit -> acknowledge -> delete path ran for every message.
func requireQueueDrained(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(queueURL),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		require.NoError(t, err)

		visible := attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
		inflight := attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]
		if visible == "0" && inflight == "0" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue %q not drained: %s visible, %s in flight", queueURL, visible, inflight)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func sendFifoMessages(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string, group string, bodies []string) {
	t.Helper()
	for start := 0; start < len(bodies); start += 10 {
		batch := bodies[start:min(start+10, len(bodies))]
		entries := make([]types.SendMessageBatchRequestEntry, len(batch))
		for i, body := range batch {
			entries[i] = types.SendMessageBatchRequestEntry{
				Id:                     aws.String(strconv.Itoa(i)),
				MessageBody:            aws.String(body),
				MessageGroupId:         aws.String(group),
				MessageDeduplicationId: aws.String(fmt.Sprintf("%s-%02d", group, start+i)),
			}
		}
		resp, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  entries,
		})
		require.NoError(t, err)
		require.Empty(t, resp.Failed)
	}
}

// TestCaptureFifoOrdering exercises the ordered-capture property: SQS locks
// a message group while its messages are in flight, and the connector
// deletes only after the runtime acknowledges, so per-group emission order
// matches queue order. It doubles as a check that the emulator implements
// FIFO group locking; if it doesn't, the per-group order assertion fails.
func TestCaptureFifoOrdering(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client := testClient(t, ctx, &conf)
	queueURL := createQueue(t, ctx, client, "test-capture-ordered.fifo")

	groups := []string{"group-a", "group-b", "group-c"}
	const perGroup = 60
	for _, group := range groups {
		var bodies []string
		for i := range perGroup {
			bodies = append(bodies, fmt.Sprintf(`{"group": %q, "seq": %d}`, group, i))
		}
		sendFifoMessages(t, ctx, client, queueURL, group, bodies)
	}

	cs := testCaptureSpec(t, &conf, queueURL)

	// Collect documents in emission order and assert that within each group,
	// sequence positions are strictly increasing. Cross-group interleaving is
	// arbitrary, but per-group order is the guarantee.
	captureCtx, cancelCapture := context.WithCancel(context.Background())
	shutdownWatchdog := time.AfterFunc(60*time.Second, func() { cancelCapture() })

	lastSeq := make(map[string]int)
	var captured int
	cs.Capture(captureCtx, t, func(data json.RawMessage) {
		shutdownWatchdog.Reset(2 * time.Second)

		var doc struct {
			Group string `json:"group"`
			Seq   int    `json:"seq"`
			Meta  struct {
				MessageGroupID string `json:"messageGroupId"`
				SequenceNumber string `json:"sequenceNumber"`
			} `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(data, &doc))
		if doc.Group == "" && doc.Meta.MessageGroupID == "" {
			return // The callback also fires for state checkpoints ({}).
		}
		require.Equal(t, doc.Group, doc.Meta.MessageGroupID, "_meta.messageGroupId must match the group the message was sent to")
		require.NotEmpty(t, doc.Meta.SequenceNumber)

		last, seen := lastSeq[doc.Group]
		if seen {
			require.Greater(t, doc.Seq, last, "group %q emitted out of order", doc.Group)
		}
		lastSeq[doc.Group] = doc.Seq
		captured++
	})
	for _, err := range cs.Errors {
		require.NoError(t, err)
	}
	require.Equal(t, len(groups)*perGroup, captured)

	cupaloy.SnapshotT(t, cs.Summary())
	requireQueueDrained(t, ctx, client, queueURL)
}

// TestCaptureRedelivery exercises the at-least-once path. Messages received
// by a consumer that never deletes them (simulating a crash between receive
// and Flow commit) become visible again after the visibility timeout and are
// re-captured, with approximateReceiveCount reflecting the redelivery.
func TestCaptureRedelivery(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client := testClient(t, ctx, &conf)
	queueURL := createQueue(t, ctx, client, "test-capture-redelivery")

	sendTestMessages(t, ctx, client, queueURL, []string{
		`{"idx": 0, "kind": "redelivered"}`,
		`{"idx": 1, "kind": "redelivered"}`,
		`{"idx": 2, "kind": "redelivered"}`,
	})

	// A raw receive with a short per-receive visibility override locks the
	// messages and then abandons them, as a crashed consumer would.
	received := 0
	for received < 3 {
		resp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     5,
			VisibilityTimeout:   1,
		})
		require.NoError(t, err)
		received += len(resp.Messages)
	}
	time.Sleep(1500 * time.Millisecond) // Let the visibility timeout expire.

	cs := testCaptureSpec(t, &conf, queueURL)
	advanceCapture(t, cs)
	cupaloy.SnapshotT(t, cs.Summary())

	requireQueueDrained(t, ctx, client, queueURL)
}

func TestCaptureStandard(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client := testClient(t, ctx, &conf)
	queueURL := createQueue(t, ctx, client, "test-capture-standard")

	var bodies []string
	for i := range 20 {
		bodies = append(bodies, fmt.Sprintf(`{"idx": %d, "kind": "json-object"}`, i))
	}
	bodies = append(bodies,
		`plain text message`,
		`[1, 2, 3]`,
		`42`,
		`{"idx": 99, "_meta": {"spoofed": "the connector's _meta must win"}}`,
	)
	sendTestMessages(t, ctx, client, queueURL, bodies)

	cs := testCaptureSpec(t, &conf, queueURL)
	advanceCapture(t, cs)
	cupaloy.SnapshotT(t, cs.Summary())

	requireQueueDrained(t, ctx, client, queueURL)
}

// TestCaptureMultiQueue exercises a capture with more than one binding.
// One queue receives traffic while the other stays empty, with idleAfter
// and the long-poll window tightened so the empty binding's receive fleet
// shrinks to scouts during the run. The snapshot pins that documents route
// to the right binding via _meta.queueUrl, and the drain assertion proves
// the busy binding is unimpeded by the idle one.
func TestCaptureMultiQueue(t *testing.T) {
	ctx := context.Background()

	prevIdle, prevPoll := idleAfter, longPollWaitSeconds
	idleAfter, longPollWaitSeconds = 100*time.Millisecond, 1
	t.Cleanup(func() { idleAfter, longPollWaitSeconds = prevIdle, prevPoll })

	conf := testConfig(t)
	client := testClient(t, ctx, &conf)
	busyURL := createQueue(t, ctx, client, "test-capture-multi-busy")
	idleURL := createQueue(t, ctx, client, "test-capture-multi-idle")

	var bodies []string
	for i := range 12 {
		bodies = append(bodies, fmt.Sprintf(`{"idx": %d, "queue": "busy"}`, i))
	}
	sendTestMessages(t, ctx, client, busyURL, bodies)

	cs := testCaptureSpec(t, &conf, busyURL, idleURL)
	advanceCapture(t, cs)
	cupaloy.SnapshotT(t, cs.Summary())

	requireQueueDrained(t, ctx, client, busyURL)
}
