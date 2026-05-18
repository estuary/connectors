# materialize-eventbridge integration test resources

The cloud-backed integration tests in `driver_test.go` use AWS resources
provisioned out-of-band. This file documents what exists and how to
recreate it on a fresh account.

## Current resources (Estuary-managed)

- **Account:** `076183946664`
- **Region:** `us-east-2`
- **EventBridge bus:** `integration-tests`
- **EventBridge rule:** `integration-tests-rule` on the bus above,
  pattern matching every event, target = the SQS queue below
- **SQS queue:** `integration-tests`
  - Resource policy allows `events.amazonaws.com` to call
    `sqs:SendMessage`, scoped via `aws:SourceArn` to the rule ARN
- **IAM user:** `materialize-eventbridge-tests`
  - Inline policy grants:
    - `events:PutEvents`, `events:DescribeEventBus` on the bus ARN
    - `sqs:GetQueueAttributes`, `sqs:GetQueueUrl`,
      `sqs:ReceiveMessage`, `sqs:DeleteMessage` on the queue ARN
  - Static access key for this user is committed (SOPS-encrypted)
    in `testdata/config.aws.yaml` under `credentials.auth_type:
    AWSAccessKey` alongside `region` and `event_bus_name`. The
    connector also supports `credentials.auth_type: AWSIAM` (assume
    role + STS) for production use; integration tests use static keys
    because the test rig has no role-trust setup.

The rule + SQS queue are *test plumbing only* — production users do
not need them. The connector itself only writes to the bus; the queue
exists so `driver_test.go` can observe what was published.

## Recreate on a fresh account

The commands below assume `AWS_REGION` is set and the caller has admin
in the target account. Names match the originals; substitute as
desired.

```bash
# 1. Bus
aws events create-event-bus --name integration-tests

# 2. Queue
QURL=$(aws sqs create-queue --queue-name integration-tests \
  --query QueueUrl --output text)
QARN=$(aws sqs get-queue-attributes --queue-url "$QURL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)
BUS_ARN=$(aws events describe-event-bus --name integration-tests \
  --query Arn --output text)

# 3. Rule on the bus, forward everything to the queue
aws events put-rule --name integration-tests-rule \
  --event-bus-name integration-tests \
  --event-pattern '{"account":["'"$(aws sts get-caller-identity --query Account --output text)"'"]}'
RULE_ARN=$(aws events describe-rule --name integration-tests-rule \
  --event-bus-name integration-tests --query Arn --output text)
aws events put-targets --rule integration-tests-rule \
  --event-bus-name integration-tests \
  --targets "Id=1,Arn=$QARN"

# 4. Allow EventBridge to deliver into the queue, scoped to the rule
aws sqs set-queue-attributes --queue-url "$QURL" --attributes '{
  "Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"events.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"'"$QARN"'\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"'"$RULE_ARN"'\"}}}]}"
}'

# 5. IAM user + inline policy + access key
aws iam create-user --user-name materialize-eventbridge-tests
aws iam put-user-policy --user-name materialize-eventbridge-tests \
  --policy-name materialize-eventbridge-tests \
  --policy-document '{"Version":"2012-10-17","Statement":[
    {"Effect":"Allow","Action":["events:PutEvents","events:DescribeEventBus"],"Resource":"'"$BUS_ARN"'"},
    {"Effect":"Allow","Action":["sqs:GetQueueAttributes","sqs:GetQueueUrl","sqs:ReceiveMessage","sqs:DeleteMessage"],"Resource":"'"$QARN"'"}
  ]}'
aws iam create-access-key --user-name materialize-eventbridge-tests
```

Take the access-key id and secret from step 5 and re-encrypt
`testdata/config.aws.yaml` with SOPS (the GCP KMS key is referenced
inside that file's `sops` block).

## Drift warning

Nothing in CI verifies these commands stay in sync with the live
account. If a test fails with permission errors after an AWS-side
change, treat the live policy as ground truth and update this file.
