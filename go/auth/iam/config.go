package iam

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/invopop/jsonschema"
	schemagen "github.com/estuary/connectors/go/schema-gen"
)


type AuthType string

const (
	AWSIAM   AuthType = "AWSIAM"
	GCPIAM   AuthType = "GCPIAM"
	AzureIAM AuthType = "AzureIAM"
)

type AWSConfig struct {
	AWSRegion string `json:"aws_region" jsonschema:"title=AWS Region,description=AWS Region of your database"`
	AWSRole   string `json:"aws_role_arn" jsonschema:"title=AWS Role ARN,description=AWS Role which has access to the resource which will be assumed by Flow"`
}

type GCPConfig struct {
	GCPServiceAccount   string `json:"gcp_service_account_to_impersonate" jsonschema:"title=Service Account,description=GCP Service Account email for Cloud SQL IAM authentication"`
	GCPWorkloadAudience string `json:"gcp_workload_identity_pool_audience" jsonschema:"title=Workload Identity Pool Audience,description=GCP Workload Identity Pool Audience in the format //iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider"`
}

type AzureConfig struct {
	AzureClientID string `json:"azure_client_id" jsonschema:"title=Azure Client ID,description=Azure App Registration Client ID for Azure Active Directory authentication"`
	AzureTenantID string `json:"azure_tenant_id" jsonschema:"title=Azure Tenant ID,description=Azure Tenant ID for Azure Active Directory authentication"`
}

type AWSTokens struct {
	AWSAccessKeyID     string `json:"aws_access_key_id,omitempty"`
	AWSSecretAccessKey string `json:"aws_secret_access_key,omitempty"`
	AWSSessionToken    string `json:"aws_session_token,omitempty"`
}

type GCPTokens struct {
	GCPAccessToken  string `json:"gcp_access_token,omitempty"`
}

type AzureTokens struct {
	AzureAccessToken string `json:"azure_access_token,omitempty"`
}

type IAMTokens struct {
	AWSTokens
	GCPTokens
	AzureTokens
}

type IAMConfig struct {
	AuthType AuthType `json:"auth_type"`

	AWSConfig
	GCPConfig
	AzureConfig

	IAMTokens
}

func (c *IAMConfig) ValidateIAM() error {
	switch c.AuthType {
	case AWSIAM:
		if c.AWSRegion == "" {
			return errors.New("missing 'aws_region'")
		}
		if c.AWSRole == "" {
			return errors.New("missing 'aws_role'")
		}
	case GCPIAM:
		if c.GCPServiceAccount == "" {
			return errors.New("missing 'gcp_service_account_to_impersonate'")
		}
		if c.GCPWorkloadAudience == "" {
			return errors.New("missing 'gcp_workload_identity_pool_audience'")
		}
	case AzureIAM:
		if c.AzureClientID == "" {
			return errors.New("missing 'azure_client_id'")
		}
		if c.AzureTenantID == "" {
			return errors.New("missing 'azure_tenant_id'")
		}
	}

	return nil
}

func (c IAMTokens) Provider() string {
	if c.GCPAccessToken != "" {
		return "google"
	} else if c.AWSAccessKeyID != "" {
		return "aws"
	} else if c.AzureAccessToken != "" {
		return "azure"
	} else {
		return ""
	}
}

func (c IAMTokens) AWSCredentialsProvider() aws.CredentialsProvider {
	return credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID:     c.AWSAccessKeyID,
			SecretAccessKey: c.AWSSecretAccessKey,
			SessionToken:    c.AWSSessionToken,
			Source:          "flow-iam-generated",
		},
	}
}

func (c IAMTokens) GoogleToken() string {
	return c.GCPAccessToken
}

func (c IAMTokens) AzureToken() string {
	return c.AzureAccessToken
}

func (IAMConfig) OneOfSubSchemas() []schemagen.OneOfSubSchemaT {
	return []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("AWS IAM", AWSConfig{}, string(AWSIAM)),
		schemagen.OneOfSubSchema("Google Cloud IAM", GCPConfig{}, string(GCPIAM)),
		schemagen.OneOfSubSchema("Azure IAM", AzureConfig{}, string(AzureIAM)),
	}
}

func (c IAMConfig) JSONSchema() *jsonschema.Schema {
	schema := schemagen.OneOfSchema("Authentication", "", "auth_type", string(AWSIAM),
		c.OneOfSubSchemas()...
	)

	return schema
}
