package iam

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/invopop/jsonschema"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

const (
	AWSIAM = "AWSIAM"
	GCPIAM = "GCPIAM"
)

type IAMConfig struct {
	AuthType string `json:"auth_type"`

	User string `json:"user,omitempty"`

	AWSRegion     string `json:"aws_region,omitempty"`
	AWSRole       string `json:"aws_role_arn,omitempty"`
	AWSExternalId string `json:"aws_external_id,omitempty"`

	AWSAccessKeyID     string `json:"aws_access_key_id,omitempty"`
	AWSSecretAccessKey string `json:"aws_secret_access_key,omitempty"`
	AWSSessionToken    string `json:"aws_session_token,omitempty"`

	GCPServiceAccount string `json:"gcp_service_account_to_impersonate,omitempty"`
	GCPToken          string `json:"gcp_access_token,omitempty"`
}

func (c *IAMConfig) ValidateIAM() error {
	switch c.AuthType {
	case AWSIAM:
		if c.User == "" {
			return errors.New("missing 'user'")
		}
		if c.AWSRegion == "" {
			return errors.New("missing 'aws_region'")
		}
		if c.AWSRole == "" {
			return errors.New("missing 'aws_role'")
		}
		if c.AWSExternalId == "" {
			return errors.New("missing 'aws_external_id'")
		}
	case GCPIAM:
		if c.User == "" {
			return errors.New("missing 'user'")
		}
		if c.GCPServiceAccount == "" {
			return errors.New("missing 'gcp_service_account_to_impersonate'")
		}
	}

	return nil
}

func (c IAMConfig) Provider() string {
	if c.GCPToken != "" {
		return "google"
	} else if c.AWSAccessKeyID != "" {
		return "aws"
	} else {
		return ""
	}
}

func (c IAMConfig) AWSCredentialsProvider() aws.CredentialsProvider {
	return credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID:     c.AWSAccessKeyID,
			SecretAccessKey: c.AWSSecretAccessKey,
			SessionToken:    c.AWSSessionToken,
			Source:          "flow-iam-generated",
		},
	}
}

func (c IAMConfig) GoogleToken() string {
	return c.GCPToken
}

// JSONSchema allows for the schema to be (semi-)manually specified when used with the
// github.com/invopop/jsonschema package in go-schema-gen, to fullfill the required schema shape for
// our oauth
func (IAMConfig) JSONSchema() *jsonschema.Schema {
	aws := orderedmap.New[string, *jsonschema.Schema]()
	aws.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: AWSIAM,
		Const:   AWSIAM,
		Extras: map[string]interface{}{
			"x-iam-auth-provider": "aws",
		},
	})
	aws.Set("user", &jsonschema.Schema{
		Description: "The database user to authenticate as.",
		Type:        "string",
		Default:     "flow_capture",
	})
	aws.Set("aws_region", &jsonschema.Schema{
		Type:        "string",
		Description: "AWS Region of your database",
		Enum:        []any{"us-east-2", "us-east-1", "us-west-1", "us-west-2", "af-south-1", "ap-east-1", "ap-south-2", "ap-southeast-3", "ap-southeast-5", "ap-southeast-4", "ap-south-1", "ap-northeast-3", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2", "ap-east-2", "ap-southeast-7", "ap-northeast-1", "ca-central-1", "ca-west-1", "eu-central-1", "eu-west-1", "eu-west-2", "eu-south-1", "eu-west-3", "eu-south-2", "eu-north-1", "eu-central-2", "il-central-1", "mx-central-1", "me-south-1", "me-central-1", "sa-east-1", "us-gov-east-1", "us-gov-west-1"},
	})
	aws.Set("aws_role_arn", &jsonschema.Schema{
		Type:        "string",
		Description: "AWS Role which has rds-db:connect access to be assumed by Estuary",
	})
	aws.Set("aws_external_id", &jsonschema.Schema{
		Type:        "string",
		Description: "External ID to use when assuming the AWS Role",
	})

	gcp := orderedmap.New[string, *jsonschema.Schema]()
	gcp.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: GCPIAM,
		Const:   GCPIAM,
	})
	gcp.Set("user", &jsonschema.Schema{
		Description: "The database user to authenticate as.",
		Type:        "string",
		Default:     "flow_capture",
	})
	gcp.Set("gcp_service_account_to_impersonate", &jsonschema.Schema{
		Type:        "string",
		Description: "GCP Service Account email for Cloud SQL IAM authentication, use a randomly-generated service account name for security.",
		Extras: map[string]interface{}{
			"secret": true,
		},
	})
	return &jsonschema.Schema{
		Title: "Authentication",
		OneOf: []*jsonschema.Schema{
			{
				Title:      "AWS IAM",
				Required:   []string{"auth_type", AWSIAM},
				Properties: aws,
			},
			{
				Title:      "Google Cloud IAM",
				Required:   []string{"auth_type", GCPIAM},
				Properties: gcp,
			},
		},
		Extras: map[string]interface{}{
			"discriminator": map[string]string{"propertyName": "auth_type"},
			"x-iam-auth":    true,
		},
		Type: "object",
	}
}
