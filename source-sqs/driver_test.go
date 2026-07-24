package main

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

// queueURLPrefixRe matches the endpoint-and-account prefix of queue URLs, which
// varies between LocalStack and real AWS (host form and account ID). Sanitizing
// it keeps snapshots deterministic across both backends.
var queueURLPrefixRe = regexp.MustCompile(`https?://[^"/\\]+/[0-9]+/`)

func sanitizeQueueURLs(s string) string {
	return queueURLPrefixRe.ReplaceAllString(s, "https://sqs.test.example/123456789012/")
}

// useRealAWS returns true if AWS credentials are provided via environment variables.
func useRealAWS() bool {
	return os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != ""
}

func testConfig(t *testing.T) config {
	t.Helper()
	if testing.Short() {
		t.Skipf("skipping %q in short mode: integration tests require LocalStack or real AWS", t.Name())
	}
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	if useRealAWS() {
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
		return config{
			Region: region,
			Credentials: &CredentialsConfig{
				AuthType: AWSAccessKey,
				AccessKeyCredentials: AccessKeyCredentials{
					AWSAccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
					AWSSecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
				},
			},
		}
	}

	return config{
		Region: "us-east-1",
		Credentials: &CredentialsConfig{
			AuthType: AWSAccessKey,
			AccessKeyCredentials: AccessKeyCredentials{
				AWSAccessKeyID:     "x",
				AWSSecretAccessKey: "x",
			},
		},
		Advanced: advancedConfig{
			Endpoint: "http://localhost:4566",
		},
	}
}

func testClient(t *testing.T, ctx context.Context, cfg *config) *sqs.Client {
	t.Helper()
	client, err := buildClient(ctx, cfg, 0)
	require.NoError(t, err)
	return client
}

// createQueue creates a queue and registers cleanup, returning its URL. Note
// that against real AWS, a deleted queue's name is unusable for 60 seconds, so
// back-to-back test runs may need a pause. LocalStack has no such restriction.
func createQueue(t *testing.T, ctx context.Context, client *sqs.Client, name string) string {
	t.Helper()

	input := &sqs.CreateQueueInput{QueueName: aws.String(name)}
	if isFifoQueueName(name) {
		input.Attributes = map[string]string{
			string(types.QueueAttributeNameFifoQueue): "true",
		}
	}

	created, err := client.CreateQueue(ctx, input)
	require.NoError(t, err, "failed to create queue %q", name)
	t.Cleanup(func() {
		_, _ = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: created.QueueUrl})
	})
	return *created.QueueUrl
}

func TestDiscover(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client := testClient(t, ctx, &conf)

	createQueue(t, ctx, client, "test-discover-standard")
	createQueue(t, ctx, client, "test-discover-ordered.fifo")

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &conf,
	}

	bindings := cs.Discover(ctx, t, regexp.MustCompile(`/test-discover-`))
	cupaloy.SnapshotT(t, sanitizeQueueURLs(st.SummarizeBindings(t, bindings)))
}

func TestValidate(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client := testClient(t, ctx, &conf)

	standardURL := createQueue(t, ctx, client, "test-validate-standard")
	fifoURL := createQueue(t, ctx, client, "test-validate-ordered.fifo")

	configJSON, err := json.Marshal(conf)
	require.NoError(t, err)

	mkBinding := func(queueURL string) *pc.Request_Validate_Binding {
		resourceJSON, err := json.Marshal(resource{QueueURL: queueURL})
		require.NoError(t, err)
		return &pc.Request_Validate_Binding{ResourceConfigJson: resourceJSON}
	}

	validated, err := driver{}.Validate(ctx, &pc.Request_Validate{
		ConfigJson: configJSON,
		Bindings:   []*pc.Request_Validate_Binding{mkBinding(standardURL), mkBinding(fifoURL)},
	})
	require.NoError(t, err)
	require.Len(t, validated.Bindings, 2)
	require.Equal(t, []string{standardURL}, validated.Bindings[0].ResourcePath)
	require.Equal(t, []string{fifoURL}, validated.Bindings[1].ResourcePath)
}

func TestValidateNonexistentQueue(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client := testClient(t, ctx, &conf)

	// Derive a plausible URL for a queue that doesn't exist from one that does.
	existingURL := createQueue(t, ctx, client, "test-validate-exists")
	missingURL := regexp.MustCompile(`/[^/]+$`).ReplaceAllString(existingURL, "/test-validate-does-not-exist")

	configJSON, err := json.Marshal(conf)
	require.NoError(t, err)
	resourceJSON, err := json.Marshal(resource{QueueURL: missingURL})
	require.NoError(t, err)

	_, err = driver{}.Validate(ctx, &pc.Request_Validate{
		ConfigJson: configJSON,
		Bindings:   []*pc.Request_Validate_Binding{{ResourceConfigJson: resourceJSON}},
	})
	require.ErrorContains(t, err, "not accessible")
}
