package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/estuary/connectors/go/auth/iam"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/invopop/jsonschema"
)

type AuthType string

const (
	AWSAccessKey AuthType = "AWSAccessKey"
	AWSIAM       AuthType = "AWSIAM"
)

type AccessKeyCredentials struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=AWS Access Key ID for reading from SQS." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access Key,description=AWS Secret Access Key for reading from SQS." jsonschema_extras:"secret=true,order=2"`
}

type CredentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	AccessKeyCredentials
	iam.IAMConfig
}

func (CredentialsConfig) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(AWSAccessKey),
		schemagen.OneOfSubSchema("Access Key", AccessKeyCredentials{}, string(AWSAccessKey)),
		schemagen.OneOfSubSchema("AWS IAM", iam.AWSConfig{}, string(AWSIAM)),
	)
}

func (c *CredentialsConfig) Validate() error {
	switch c.AuthType {
	case AWSAccessKey:
		if c.AWSAccessKeyID == "" {
			return errors.New("missing 'aws_access_key_id'")
		}
		if c.AWSSecretAccessKey == "" {
			return errors.New("missing 'aws_secret_access_key'")
		}
		return nil
	case AWSIAM:
		return c.ValidateIAM()
	}
	return fmt.Errorf("unknown 'auth_type': %q", c.AuthType)
}

// Since AccessKeyCredentials and IAMConfig have conflicting JSON field names,
// only parse the embedded struct selected by the discriminator.
func (c *CredentialsConfig) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		AuthType AuthType `json:"auth_type"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	c.AuthType = discriminator.AuthType

	switch c.AuthType {
	case AWSAccessKey:
		return json.Unmarshal(data, &c.AccessKeyCredentials)
	case AWSIAM:
		return json.Unmarshal(data, &c.IAMConfig)
	}
	return fmt.Errorf("unexpected auth_type: %q", c.AuthType)
}

type config struct {
	Region      string             `json:"region" jsonschema:"title=AWS Region,description=AWS region of the SQS queues (e.g. us-east-1)." jsonschema_extras:"order=1"`
	Credentials *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=2"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	Endpoint string `json:"endpoint,omitempty" jsonschema:"title=AWS Endpoint,description=The AWS endpoint URI to connect to. Use for VPC interface endpoints or SQS-compatible APIs not provided by AWS."`
}

func (c *config) Validate() error {
	if c.Region == "" {
		return errors.New("missing 'region'")
	}
	if c.Credentials == nil {
		return errors.New("missing 'credentials'")
	}
	return c.Credentials.Validate()
}

func (c *config) CredentialsProvider() (aws.CredentialsProvider, error) {
	switch c.Credentials.AuthType {
	case AWSAccessKey:
		return credentials.NewStaticCredentialsProvider(
			c.Credentials.AWSAccessKeyID, c.Credentials.AWSSecretAccessKey, ""), nil
	case AWSIAM:
		return c.Credentials.IAMTokens.AWSCredentialsProvider()
	}
	return nil, fmt.Errorf("unknown 'auth_type': %q", c.Credentials.AuthType)
}

type resource struct {
	QueueURL string `json:"queueUrl" jsonschema:"title=Queue URL,description=Full URL of the SQS queue (e.g. https://sqs.us-east-1.amazonaws.com/123456789012/my-queue)."`
}

func (r *resource) Validate() error {
	if r.QueueURL == "" {
		return errors.New("missing 'queueUrl'")
	}
	if _, _, err := parseQueueURL(r.QueueURL); err != nil {
		return err
	}
	return nil
}

// awsQueueHostRe matches the standard AWS SQS endpoint host form, capturing
// the region. Custom endpoints (LocalStack, VPC endpoints) won't match, which
// yields an empty region from parseQueueURL.
var awsQueueHostRe = regexp.MustCompile(`^sqs\.([a-z0-9-]+)\.amazonaws\.com(\.cn)?$`)

// parseQueueURL extracts the queue name from a queue URL of the form
// <endpoint>/<account-id>/<queue-name>, and the AWS region when the host
// follows the standard AWS form.
func parseQueueURL(queueURL string) (name string, region string, err error) {
	u, err := url.Parse(queueURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", "", fmt.Errorf("queue URL %q is not a valid URL", queueURL)
	}

	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(segments) != 2 || segments[0] == "" || segments[1] == "" {
		return "", "", fmt.Errorf("queue URL %q does not have the expected <endpoint>/<account-id>/<queue-name> form", queueURL)
	}

	if m := awsQueueHostRe.FindStringSubmatch(u.Hostname()); m != nil {
		region = m[1]
	}
	return segments[1], region, nil
}

// isFifoQueueName reports whether a queue is FIFO. AWS enforces the `.fifo`
// suffix at queue creation for FIFO queues and forbids it on standard queues.
func isFifoQueueName(name string) bool {
	return strings.HasSuffix(name, ".fifo")
}
