package main

// Integration tests run against LocalStack EventBridge in docker compose.
//
// EventBridge has no read API. To verify what was published, this test rig
// stands up an SQS queue and an EventBridge rule that forwards every event
// on the test bus to that queue. Per-resource snapshots are then produced
// by draining the queue and filtering by the binding's source/detail_type.
//
// The SQS infrastructure lives entirely in this file (the production
// connector has no SQS dependency). The wrapper type
// `materializationUnderTest` embeds the production `*materialization` and
// overrides `SnapshotTestResource` to do the SQS drain. The bus + queue +
// rule + target are all provisioned up front by `provisionLocalStackInfra` since
// `Setup()` is never invoked on the test-process materializer instance —
// the boilerplate test rig drives the actual transactions through a
// `flowctl preview` subprocess.
//
// Per project convention this file starts and stops docker compose itself;
// callers should NOT run docker compose externally before invoking
// `go test`.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/bradleyjkemp/cupaloy"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	bptest "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

// testAll controls whether integration tests run against real AWS in
// addition to the LocalStack docker-compose stack. Running the full
// matrix requires a pre-provisioned EventBridge bus on real AWS plus
// credentials in `testdata/.local/config.aws.yaml` (gitignored), so by
// default we only exercise the local stack. Opt in to the full matrix
// by setting the `EVENTBRIDGE_TEST_ALL` environment variable or passing
// `-eventbridge.test-all` to `go test`.
var testAll = flag.Bool("eventbridge.test-all", false, "run integration tests against real AWS in addition to LocalStack")

const (
	// LocalStack-side resource names — provisioned by provisionLocalStackInfra.
	testBusName   = "flow-test-bus"
	testQueueName = "flow-test-queue"
	testRuleName  = "flow-test-rule"
	testTargetID  = "flow-test-target"

	// Real-AWS-side queue name — provisioned out-of-band on the AWS account
	// hosting the integration-tests bus. The SnapshotTestResource path
	// drains this queue when the materialization runs against real AWS
	// (i.e. when cfg.Advanced.Endpoint is empty).
	awsTestQueueName = "integration-tests"
)

func TestSpec(t *testing.T) {
	t.Parallel()

	resp, err := driver{}.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires docker")
	}

	require.NoError(t,
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run(),
		"docker compose up failed",
	)
	t.Cleanup(func() {
		_ = exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	provisionLocalStackInfra(t)

	makeResourceFn := func(s string, delta bool) resource {
		return resource{Source: "flow.test", DetailType: s}
	}

	all := *testAll || os.Getenv("EVENTBRIDGE_TEST_ALL") != ""

	materializeSpec := "testdata/materialize-local.flow.yaml"
	applySpec := "testdata/apply-local.flow.yaml"
	if all {
		materializeSpec = "testdata/materialize.flow.yaml"
		applySpec = "testdata/apply.flow.yaml"
	}

	t.Run("materialize", func(t *testing.T) {
		bptest.RunMaterializationTestParallel(t, newMaterializationUnderTest, materializeSpec, makeResourceFn, nil)
	})

	t.Run("apply", func(t *testing.T) {
		bptest.RunApplyTestParallel(t, &driver{}, newMaterializationUnderTest, applySpec, makeResourceFn)
	})
}

// materializationUnderTest wraps the production materialization with an SQS
// sidecar so that SnapshotTestResource can return what was published. The
// production type has no awareness of SQS.
//
// The wrapper drains the LocalStack queue when the materialization is
// configured against an endpoint override, and the real-AWS
// `integration-tests` queue otherwise. Both queues are pre-wired (LocalStack
// via provisionLocalStackInfra, AWS via out-of-band setup) to receive every event
// published to their respective bus.
type materializationUnderTest struct {
	*materialization
	sqsClient *sqs.Client
	queueName string

	// Populated lazily on first SnapshotTestResource call. The drain is done
	// once and cached because each binding shares the same queue and we
	// filter in-memory by source/detail-type.
	queueURL    string
	drainOnce   sync.Once
	drainErr    error
	drainedMsgs []receivedEvent
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &materializationUnderTest{}

func newMaterializationUnderTest(ctx context.Context, name string, cfg config, flags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	base, err := newMaterialization(ctx, name, cfg, flags)
	if err != nil {
		return nil, err
	}

	awsCfg, err := awsConfigFor(ctx, cfg)
	if err != nil {
		return nil, err
	}

	queueName := awsTestQueueName
	if cfg.Advanced.Endpoint != "" {
		queueName = testQueueName
	}

	return &materializationUnderTest{
		materialization: base.(*materialization),
		sqsClient:       sqs.NewFromConfig(awsCfg),
		queueName:       queueName,
	}, nil
}

// SnapshotTestResource drains the SQS sidecar queue (LocalStack or real AWS,
// chosen by which queue the wrapper was constructed against) and returns
// events whose source + detail-type match the requested resource path.
func (m *materializationUnderTest) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	if m.queueURL == "" {
		out, err := m.sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName: aws.String(m.queueName),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("locating test SQS queue %q: %w", m.queueName, err)
		}
		m.queueURL = aws.ToString(out.QueueUrl)
	}

	m.drainOnce.Do(func() {
		m.drainedMsgs, m.drainErr = drainAllMessages(ctx, m.sqsClient, m.queueURL)
	})
	if m.drainErr != nil {
		return nil, nil, m.drainErr
	}

	if len(path) != 3 {
		return nil, nil, fmt.Errorf("unexpected resource path length %d (want 3)", len(path))
	}
	wantSource, wantDetailType := path[1], path[2]

	var rows [][]any
	for _, ev := range m.drainedMsgs {
		if ev.Source != wantSource || ev.DetailType != wantDetailType {
			continue
		}
		rows = append(rows, []any{string(ev.Detail)})
	}

	if len(rows) == 0 {
		return nil, nil, nil
	}

	sort.Slice(rows, func(i, j int) bool {
		return fmt.Sprint(rows[i][0]) < fmt.Sprint(rows[j][0])
	})

	return []string{"detail"}, rows, nil
}

// provisionLocalStackInfra brings up the bus + SQS queue + EventBridge rule +
// target so the connector subprocess has somewhere to publish and the test
// process has a queue to drain. Idempotent — re-runs are safe.
func provisionLocalStackInfra(t *testing.T) {
	t.Helper()

	cfg := config{
		Region:       "us-east-1",
		EventBusName: testBusName,
		Credentials: &CredentialsConfig{
			AuthType: AWSAccessKey,
			AccessKeyCredentials: AccessKeyCredentials{
				AWSAccessKeyID:     "test",
				AWSSecretAccessKey: "test",
			},
		},
		Advanced: advancedConfig{Endpoint: "http://localhost:4566"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	awsCfg, err := awsConfigFor(ctx, cfg)
	require.NoError(t, err)

	eb := eventbridge.NewFromConfig(awsCfg)
	sq := sqs.NewFromConfig(awsCfg)

	if _, err := eb.CreateEventBus(ctx, &eventbridge.CreateEventBusInput{
		Name: aws.String(testBusName),
	}); err != nil && !alreadyExists(err) {
		t.Fatalf("CreateEventBus: %v", err)
	}

	qOut, err := sq.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(testQueueName),
	})
	require.NoError(t, err, "CreateQueue")
	queueURL := aws.ToString(qOut.QueueUrl)

	attrOut, err := sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err, "GetQueueAttributes")
	queueArn := attrOut.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	if _, err := eb.PutRule(ctx, &eventbridge.PutRuleInput{
		Name:         aws.String(testRuleName),
		EventBusName: aws.String(testBusName),
		EventPattern: aws.String(`{"source":[{"prefix":""}]}`),
		State:        ebtypes.RuleStateEnabled,
	}); err != nil {
		t.Fatalf("PutRule: %v", err)
	}

	if _, err := eb.PutTargets(ctx, &eventbridge.PutTargetsInput{
		Rule:         aws.String(testRuleName),
		EventBusName: aws.String(testBusName),
		Targets: []ebtypes.Target{{
			Id:  aws.String(testTargetID),
			Arn: aws.String(queueArn),
		}},
	}); err != nil {
		t.Fatalf("PutTargets: %v", err)
	}
}

func alreadyExists(err error) bool {
	if err == nil {
		return false
	}
	var rae *ebtypes.ResourceAlreadyExistsException
	return errors.As(err, &rae)
}

// receivedEvent matches the JSON shape EventBridge writes to SQS via a rule
// target.
type receivedEvent struct {
	Source     string          `json:"source"`
	DetailType string          `json:"detail-type"`
	Detail     json.RawMessage `json:"detail"`
}

// drainAllMessages polls the queue until two consecutive empty receives,
// deleting each message it observes so a re-run starts clean.
func drainAllMessages(ctx context.Context, sq *sqs.Client, queueURL string) ([]receivedEvent, error) {
	var out []receivedEvent
	emptyPolls := 0
	for emptyPolls < 2 {
		resp, err := sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			return nil, fmt.Errorf("ReceiveMessage: %w", err)
		}
		if len(resp.Messages) == 0 {
			emptyPolls++
			continue
		}
		emptyPolls = 0
		for _, msg := range resp.Messages {
			var ev receivedEvent
			if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &ev); err != nil {
				return nil, fmt.Errorf("decoding SQS body: %w", err)
			}
			out = append(out, ev)
			if _, err := sq.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				return nil, fmt.Errorf("DeleteMessage: %w", err)
			}
		}
	}
	return out, nil
}

func TestCheckPutEventsPermission(t *testing.T) {
	t.Parallel()

	const (
		callerIdentityXML = `<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
    <Arn>arn:aws:iam::123456789012:user/test-user</Arn>
    <UserId>AIDATEST</UserId>
    <Account>123456789012</Account>
  </GetCallerIdentityResult>
  <ResponseMetadata><RequestId>test-req-id</RequestId></ResponseMetadata>
</GetCallerIdentityResponse>`

		simulateAllowedXML = `<SimulatePrincipalPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <SimulatePrincipalPolicyResult>
    <IsTruncated>false</IsTruncated>
    <EvaluationResults>
      <member>
        <EvalActionName>events:PutEvents</EvalActionName>
        <EvalDecision>allowed</EvalDecision>
        <EvalResourceName>arn:aws:events:us-east-1:123456789012:event-bus/test-bus</EvalResourceName>
        <MatchedStatements/><MissingContextValues/>
      </member>
    </EvaluationResults>
  </SimulatePrincipalPolicyResult>
  <ResponseMetadata><RequestId>test-req-id</RequestId></ResponseMetadata>
</SimulatePrincipalPolicyResponse>`

		simulateDeniedXML = `<SimulatePrincipalPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <SimulatePrincipalPolicyResult>
    <IsTruncated>false</IsTruncated>
    <EvaluationResults>
      <member>
        <EvalActionName>events:PutEvents</EvalActionName>
        <EvalDecision>implicitDeny</EvalDecision>
        <EvalResourceName>arn:aws:events:us-east-1:123456789012:event-bus/test-bus</EvalResourceName>
        <MatchedStatements/><MissingContextValues/>
      </member>
    </EvaluationResults>
  </SimulatePrincipalPolicyResult>
  <ResponseMetadata><RequestId>test-req-id</RequestId></ResponseMetadata>
</SimulatePrincipalPolicyResponse>`

		accessDeniedXML = `<ErrorResponse>
  <Error>
    <Type>Sender</Type>
    <Code>AccessDenied</Code>
    <Message>User is not authorized</Message>
  </Error>
  <RequestId>test-req-id</RequestId>
</ErrorResponse>`

		testBusARN = "arn:aws:events:us-east-1:123456789012:event-bus/test-bus"
	)

	makeServer := func(failGetCallerIdentity, failSimulate bool, decision string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := r.ParseForm(); err != nil {
				http.Error(w, "bad form", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "text/xml")
			switch r.FormValue("Action") {
			case "GetCallerIdentity":
				if failGetCallerIdentity {
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, accessDeniedXML)
					return
				}
				fmt.Fprint(w, callerIdentityXML)
			case "SimulatePrincipalPolicy":
				if failSimulate {
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, accessDeniedXML)
					return
				}
				if decision == "allowed" {
					fmt.Fprint(w, simulateAllowedXML)
				} else {
					fmt.Fprint(w, simulateDeniedXML)
				}
			}
		}))
	}

	makeCfg := func(endpoint string) config {
		return config{
			Region:       "us-east-1",
			EventBusName: "test-bus",
			Credentials: &CredentialsConfig{
				AuthType: AWSAccessKey,
				AccessKeyCredentials: AccessKeyCredentials{
					AWSAccessKeyID:     "test",
					AWSSecretAccessKey: "test",
				},
			},
			Advanced: advancedConfig{Endpoint: endpoint},
		}
	}

	t.Run("allowed", func(t *testing.T) {
		t.Parallel()
		srv := makeServer(false, false, "allowed")
		defer srv.Close()
		prereqs := &cerrors.PrereqErr{}
		checkPutEventsPermission(context.Background(), makeCfg(srv.URL), testBusARN, prereqs)
		require.Equal(t, 0, prereqs.Len())
	})

	t.Run("denied", func(t *testing.T) {
		t.Parallel()
		srv := makeServer(false, false, "implicitDeny")
		defer srv.Close()
		prereqs := &cerrors.PrereqErr{}
		checkPutEventsPermission(context.Background(), makeCfg(srv.URL), testBusARN, prereqs)
		require.Equal(t, 1, prereqs.Len())
	})

	t.Run("GetCallerIdentity_forbidden", func(t *testing.T) {
		t.Parallel()
		srv := makeServer(true, false, "")
		defer srv.Close()
		prereqs := &cerrors.PrereqErr{}
		checkPutEventsPermission(context.Background(), makeCfg(srv.URL), testBusARN, prereqs)
		require.Equal(t, 0, prereqs.Len())
	})

	t.Run("SimulatePrincipalPolicy_forbidden", func(t *testing.T) {
		t.Parallel()
		srv := makeServer(false, true, "")
		defer srv.Close()
		prereqs := &cerrors.PrereqErr{}
		checkPutEventsPermission(context.Background(), makeCfg(srv.URL), testBusARN, prereqs)
		require.Equal(t, 0, prereqs.Len())
	})
}
