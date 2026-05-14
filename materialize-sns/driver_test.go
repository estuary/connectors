package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
)

func TestSpec(t *testing.T) {
	t.Parallel()

	driver := driver{}
	response, err := driver.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// LocalStack defaults.
const (
	localstackEndpoint = "http://localhost:4566"
	localstackRegion   = "us-east-1"
	localstackAccount  = "000000000000"
)

// dummyCollectionSpec returns a minimal CollectionSpec that satisfies the Flow proto-level
// Validate() checks invoked by driver.Apply / driver.Validate. The PartitionTemplate fields are
// the smallest set that JournalSpec.Validate accepts; the runtime would normally populate these.
func dummyCollectionSpec(name pf.Collection) pf.CollectionSpec {
	return pf.CollectionSpec{
		Name:    name,
		Key:     []string{"/id"},
		UuidPtr: "/_meta/uuid",
		Projections: []pf.Projection{
			{Field: "flow_document", Ptr: ""},
			{Field: "id", Ptr: "/id", IsPrimaryKey: true},
		},
		PartitionTemplate: dummyJournalSpec(string(name) + "/pivot=00"),
	}
}

// dummyJournalSpec returns the smallest JournalSpec that satisfies broker JournalSpec.Validate.
func dummyJournalSpec(name string) *pb.JournalSpec {
	return &pb.JournalSpec{
		Name:        pb.Journal(name),
		Replication: 3,
		Fragment: pb.JournalSpec_Fragment{
			Length:           512 * 1024 * 1024,
			CompressionCodec: pb.CompressionCodec_GZIP,
			RefreshInterval:  30 * time.Second,
		},
	}
}

// dummyShardSpec returns the smallest ShardSpec that satisfies consumer ShardSpec.Validate.
func dummyShardSpec(id string) *pc.ShardSpec {
	return &pc.ShardSpec{
		Id:             pc.ShardID(id),
		MaxTxnDuration: time.Second,
	}
}

func testConfig() *config {
	return &config{
		Region: localstackRegion,
		Credentials: &CredentialsConfig{
			AuthType: AWSAccessKey,
			AccessKeyCredentials: AccessKeyCredentials{
				AWSAccessKeyID:     "test",
				AWSSecretAccessKey: "test",
			},
		},
		Advanced: advancedConfig{
			Endpoint: localstackEndpoint,
		},
	}
}

// buildAWSClients constructs raw SNS and SQS clients pointed at LocalStack. We need an SQS client
// in the test (the connector itself only uses SNS) to subscribe queues to topics and read back
// published messages for assertions.
func buildAWSClients(t *testing.T, ctx context.Context) (*sns.Client, *sqs.Client) {
	t.Helper()
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion(localstackRegion),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	ep := localstackEndpoint
	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) { o.BaseEndpoint = &ep })
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) { o.BaseEndpoint = &ep })
	return snsClient, sqsClient
}

// arnFor returns an SNS ARN in the LocalStack account.
func arnFor(name string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s", localstackRegion, localstackAccount, name)
}

// queueARNFor returns an SQS ARN in the LocalStack account.
func queueARNFor(name string) string {
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", localstackRegion, localstackAccount, name)
}

// createTopicAndSubscribedQueue creates a topic + queue pair and subscribes the queue to the topic
// with RawMessageDelivery enabled so the SQS body is the unwrapped SNS Message. Returns the topic
// name (the new resource-config field) and the queue URL.
func createTopicAndSubscribedQueue(t *testing.T, ctx context.Context, snsClient *sns.Client, sqsClient *sqs.Client, baseName string, fifo bool) (topicName string, queueURL string) {
	t.Helper()

	topicName = baseName
	queueName := baseName + "-q"
	topicAttrs := map[string]string{}
	queueAttrs := map[string]string{}
	if fifo {
		topicName += ".fifo"
		queueName += ".fifo"
		topicAttrs["FifoTopic"] = "true"
		topicAttrs["ContentBasedDeduplication"] = "true"
		queueAttrs["FifoQueue"] = "true"
		queueAttrs["ContentBasedDeduplication"] = "true"
	}

	topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name:       aws.String(topicName),
		Attributes: topicAttrs,
	})
	require.NoError(t, err)

	queue, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(queueName),
		Attributes: queueAttrs,
	})
	require.NoError(t, err)

	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARNFor(queueName)),
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
		ReturnSubscriptionArn: true,
	})
	require.NoError(t, err)

	return topicName, *queue.QueueUrl
}

// drainQueue long-polls the queue until `expected` messages have been received or the deadline is
// hit. Messages are deleted as they are read.
func drainQueue(t *testing.T, ctx context.Context, sqsClient *sqs.Client, queueURL string, expected int) []sqstypes.Message {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var out []sqstypes.Message
	for len(out) < expected && time.Now().Before(deadline) {
		recv, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(queueURL),
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       2,
			MessageAttributeNames: []string{"All"},
			AttributeNames:        []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
		})
		require.NoError(t, err)
		for _, msg := range recv.Messages {
			out = append(out, msg)
			_, err := sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
			require.NoError(t, err)
		}
	}
	require.Lenf(t, out, expected, "expected %d messages from %s, got %d", expected, queueURL, len(out))
	return out
}

// applyOnBindings invokes driver.Apply for a synthetic materialization with the given resources.
func applyOnBindings(t *testing.T, ctx context.Context, d driver, cfg *config, resources []resource) (*pm.Response_Applied, error) {
	t.Helper()
	cfgJSON, err := json.Marshal(cfg)
	require.NoError(t, err)

	var bindings []*pf.MaterializationSpec_Binding
	for _, r := range resources {
		rJSON, err := json.Marshal(r)
		require.NoError(t, err)
		bindings = append(bindings, &pf.MaterializationSpec_Binding{
			ResourceConfigJson: rJSON,
			ResourcePath:       []string{r.TopicName},
			Collection: dummyCollectionSpec("tests/dummy"),
		})
	}

	return d.Apply(ctx, &pm.Request_Apply{
		Materialization: &pf.MaterializationSpec{
			Name:                "tests/sns",
			ConfigJson:          cfgJSON,
			ConnectorType:       pf.MaterializationSpec_IMAGE,
			Bindings:            bindings,
			ShardTemplate:       dummyShardSpec("tests/sns/shard"),
			RecoveryLogTemplate: dummyJournalSpec("recovery/tests/sns/log"),
		},
		Version: "v1",
	})
}

// newTransactorFor builds a transactor for the given resources, mirroring the protocol path.
func newTransactorFor(t *testing.T, ctx context.Context, d driver, cfg *config, resources []resource) *transactor {
	t.Helper()
	cfgJSON, err := json.Marshal(cfg)
	require.NoError(t, err)

	var bindings []*pf.MaterializationSpec_Binding
	for _, r := range resources {
		rJSON, err := json.Marshal(r)
		require.NoError(t, err)
		bindings = append(bindings, &pf.MaterializationSpec_Binding{
			ResourceConfigJson: rJSON,
			ResourcePath:       []string{r.TopicName},
		})
	}

	tx, _, _, err := d.NewTransactor(ctx, pm.Request_Open{
		Materialization: &pf.MaterializationSpec{
			Name:          "tests/sns",
			ConfigJson:    cfgJSON,
			ConnectorType: pf.MaterializationSpec_IMAGE,
			Bindings:      bindings,
		},
	}, nil)
	require.NoError(t, err)
	return tx.(*transactor)
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	// Ensure shared docker network exists; ignore failure if it already does.
	_ = exec.Command("docker", "network", "create", "flow-test").Run()

	cmd := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait")
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "docker compose up failed: %s", string(out))
	t.Cleanup(func() {
		_ = exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	snsClient, sqsClient := buildAWSClients(t, ctx)
	d := driver{}
	cfg := testConfig()

	t.Run("apply_creates_missing_topics", func(t *testing.T) {
		stdName := "apply-std"
		fifoName := "apply-fifo.fifo"

		resp, err := applyOnBindings(t, ctx, d, cfg, []resource{
			{TopicName: stdName}, {TopicName: fifoName},
		})
		require.NoError(t, err)

		// Re-applying is idempotent: topics already exist, no new actions reported.
		resp2, err := applyOnBindings(t, ctx, d, cfg, []resource{
			{TopicName: stdName}, {TopicName: fifoName},
		})
		require.NoError(t, err)

		// Confirm FIFO attributes were set on creation; standard topics should not have them.
		fifoAttrs, err := snsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
			TopicArn: aws.String(arnFor(fifoName)),
		})
		require.NoError(t, err)
		stdAttrs, err := snsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
			TopicArn: aws.String(arnFor(stdName)),
		})
		require.NoError(t, err)

		snap := map[string]any{
			"first_apply":  applySnapshot(resp),
			"second_apply": applySnapshot(resp2),
			"fifo_topic_attributes": map[string]string{
				"FifoTopic":                 fifoAttrs.Attributes["FifoTopic"],
				"ContentBasedDeduplication": fifoAttrs.Attributes["ContentBasedDeduplication"],
			},
			"standard_topic_attributes": map[string]string{
				"FifoTopic":                 stdAttrs.Attributes["FifoTopic"],
				"ContentBasedDeduplication": stdAttrs.Attributes["ContentBasedDeduplication"],
			},
		}
		snapshotJSON(t, snap)
	})

	t.Run("apply_errors_on_invalid_topic_name", func(t *testing.T) {
		// Collection names like "acmeCo/orders" must be normalized by Flow's UI into a valid SNS
		// topic name (no slashes). If something slips through, the resource validator rejects it.
		_, err := applyOnBindings(t, ctx, d, cfg, []resource{{TopicName: "acmeCo/orders"}})
		require.Error(t, err)
		snapshotJSON(t, map[string]string{"error": err.Error()})
	})

	t.Run("publish_standard", func(t *testing.T) {
		stdName, queueURL := createTopicAndSubscribedQueue(t, ctx, snsClient, sqsClient, "std-publish", false)
		tx := newTransactorFor(t, ctx, d, cfg, []resource{{TopicName: stdName}})

		docs := []json.RawMessage{
			json.RawMessage(`{"id":1,"v":"a"}`),
			json.RawMessage(`{"id":2,"v":"b"}`),
			json.RawMessage(`{"id":3,"v":"c"}`),
		}

		eg, egCtx := errgroup.WithContext(ctx)
		eg.SetLimit(publishConcurrency)
		for i, doc := range docs {
			require.NoError(t, tx.publishOne(egCtx, eg, 0, []byte(fmt.Sprintf("k%d", i)), doc))
		}
		require.NoError(t, eg.Wait())

		msgs := drainQueue(t, ctx, sqsClient, queueURL, len(docs))
		snapshotJSON(t, normalizeStandardMessages(msgs))
	})

	t.Run("publish_fifo_attributes_and_dedup", func(t *testing.T) {
		fifoName, queueURL := createTopicAndSubscribedQueue(t, ctx, snsClient, sqsClient, "fifo-publish", true)
		tx := newTransactorFor(t, ctx, d, cfg, []resource{{TopicName: fifoName}})

		keyA := []byte("group-a")
		keyB := []byte("group-b")
		docs := []json.RawMessage{
			json.RawMessage(`{"i":0,"g":"a"}`),
			json.RawMessage(`{"i":1,"g":"a"}`),
			json.RawMessage(`{"i":2,"g":"b"}`),
			json.RawMessage(`{"i":3,"g":"b"}`),
		}
		keys := [][]byte{keyA, keyA, keyB, keyB}

		eg, egCtx := errgroup.WithContext(ctx)
		eg.SetLimit(publishConcurrency)
		for i, doc := range docs {
			require.NoError(t, tx.publishOne(egCtx, eg, 0, keys[i], doc))
		}
		require.NoError(t, eg.Wait())

		initial := drainQueue(t, ctx, sqsClient, queueURL, len(docs))

		// Re-publishing identical (key, body) pairs within the dedup window should be suppressed
		// by the connector's explicit MessageDeduplicationId.
		eg2, egCtx2 := errgroup.WithContext(ctx)
		eg2.SetLimit(publishConcurrency)
		dup := json.RawMessage(`{"i":"dup"}`)
		require.NoError(t, tx.publishOne(egCtx2, eg2, 0, keyA, dup))
		require.NoError(t, tx.publishOne(egCtx2, eg2, 0, keyA, dup))
		require.NoError(t, tx.publishOne(egCtx2, eg2, 0, keyA, dup))
		require.NoError(t, eg2.Wait())

		afterDedup := drainQueue(t, ctx, sqsClient, queueURL, 1)

		snapshotJSON(t, map[string]any{
			"initial_publish": normalizeFifoMessages(initial),
			"after_3_dup_publishes": map[string]any{
				"received_count": len(afterDedup),
				"messages":       normalizeFifoMessages(afterDedup),
			},
		})
	})

	t.Run("publish_rejects_oversize_document", func(t *testing.T) {
		stdName, _ := createTopicAndSubscribedQueue(t, ctx, snsClient, sqsClient, "oversize", false)
		tx := newTransactorFor(t, ctx, d, cfg, []resource{{TopicName: stdName}})

		// One byte past the SNS limit triggers the pre-check before we ever call Publish.
		oversize := make([]byte, maxMessageBytes+1)
		for i := range oversize {
			oversize[i] = 'a'
		}

		eg, egCtx := errgroup.WithContext(ctx)
		eg.SetLimit(publishConcurrency)
		err := tx.publishOne(egCtx, eg, 0, []byte("k"), json.RawMessage(oversize))
		require.NoError(t, eg.Wait())
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds the SNS")
	})
}

// snapshotJSON marshals v to indented JSON and compares it against the named cupaloy snapshot for
// the current (sub)test. Re-run with UPDATE_SNAPSHOTS=true to refresh.
func snapshotJSON(t *testing.T, v any) {
	t.Helper()
	formatted, err := json.MarshalIndent(v, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

type stdMessageView struct {
	Body string `json:"body"`
}

type fifoMessageView struct {
	Body                   string `json:"body"`
	MessageGroupID         string `json:"message_group_id"`
	MessageDeduplicationID string `json:"message_deduplication_id"`
}

// normalizeStandardMessages strips volatile SQS metadata and sorts by body for snapshot stability.
func normalizeStandardMessages(msgs []sqstypes.Message) []stdMessageView {
	out := make([]stdMessageView, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, stdMessageView{Body: *m.Body})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Body < out[j].Body })
	return out
}

// normalizeFifoMessages preserves the connector-controlled fields (body, group ID, dedup ID) and
// sorts by body. Group ID and dedup ID are deterministic because they're derived from
// PackedKeyHash_HH64 and sha256 respectively, so they're stable across runs.
func normalizeFifoMessages(msgs []sqstypes.Message) []fifoMessageView {
	out := make([]fifoMessageView, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, fifoMessageView{
			Body:                   *m.Body,
			MessageGroupID:         m.Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)],
			MessageDeduplicationID: m.Attributes[string(sqstypes.MessageSystemAttributeNameMessageDeduplicationId)],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Body < out[j].Body })
	return out
}

// applySnapshot reduces an Apply response to its stable, snapshot-friendly fields.
func applySnapshot(resp *pm.Response_Applied) map[string]any {
	return map[string]any{
		"action_description": resp.ActionDescription,
	}
}
