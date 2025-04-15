package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sts"

	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type driver struct{}

type config struct {
	SNSTopicARN string `json:"snsTopicArn" jsonschema:"title=SNS Topic ARN,description=AWS SNS Topic ARN to publish messages to."`
	Region      string `json:"region" jsonschema:"title=AWS Region,description=AWS Region where SNS is configured."`
	AccessKey   string `json:"accessKey,omitempty" jsonschema:"title=AWS Access Key,description=AWS IAM Access Key (leave empty if using Role ARN)."`
	SecretKey   string `json:"secretKey,omitempty" jsonschema:"title=AWS Secret Key,description=AWS IAM Secret Key (leave empty if using Role ARN)."`
	RoleARN     string `json:"roleArn,omitempty" jsonschema:"title=AWS Role ARN,description=IAM Role ARN to assume (leave empty if using Access Key & Secret Key)."`
}

func (c config) Validate() error {
	if c.SNSTopicARN == "" {
		return fmt.Errorf("snsTopicArn is required")
	}
	if c.Region == "" {
		return fmt.Errorf("region is required")
	}
	// Either RoleARN OR (AccessKey + SecretKey) must be provided
	if c.RoleARN == "" && (c.AccessKey == "" || c.SecretKey == "") {
		return fmt.Errorf("either roleArn or both accessKey & secretKey must be provided")
	}
	return nil
}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := json.Marshal(config{})
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	return &pm.Response_Spec{
		ConfigSchemaJson: json.RawMessage(endpointSchema),
	}, nil
}

type transactor struct {
	snsClient *sns.SNS
	topicARN  string
}

func (d *driver) NewTransactor(ctx context.Context, open pm.Request_Open, _ *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	var cfg config
	if err := pf.UnmarshalStrict(open.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	awsConfig := &aws.Config{
		Region: aws.String(cfg.Region),
	}

	// If Access Key and Secret Key are provided, use them
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, "")
	}

	// If Role ARN is provided, assume the IAM role
	if cfg.RoleARN != "" {
		sess := session.Must(session.NewSession(awsConfig))
		stsClient := sts.New(sess)

		assumeRoleInput := &sts.AssumeRoleInput{
			RoleArn:         aws.String(cfg.RoleARN),
			RoleSessionName: aws.String("MaterializerSession"),
		}

		roleOutput, err := stsClient.AssumeRole(assumeRoleInput)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to assume role: %w", err)
		}

		awsConfig.Credentials = credentials.NewStaticCredentials(
			*roleOutput.Credentials.AccessKeyId,
			*roleOutput.Credentials.SecretAccessKey,
			*roleOutput.Credentials.SessionToken,
		)
	}

	// Create session with updated credentials
	sess := session.Must(session.NewSession(awsConfig))
	snsClient := sns.New(sess)

	return &transactor{
		snsClient: snsClient,
		topicARN:  cfg.SNSTopicARN,
	}, &pm.Response_Opened{}, nil, nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	for it.Next() {
		msg, err := json.Marshal(it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("json.Marshal: %w", err)
		}
		_, err = t.snsClient.Publish(&sns.PublishInput{
			TopicArn: aws.String(t.topicARN),
			Message:  aws.String(string(msg)),
		})
		if err != nil {
			return nil, fmt.Errorf("sns.Publish: %w", err)
		}
	}
	return nil, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }
func (t *transactor) Destroy()                                                    {}

func main() { boilerplate.RunMain(new(driver)) }
