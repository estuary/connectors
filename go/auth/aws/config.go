package aws

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/iancoleman/orderedmap"
	"github.com/invopop/jsonschema"
)

const (
	ACCESS_KEY_AUTH_TYPE         = "AccessKey"
	IAM_ROLES_ANYWHERE_AUTH_TYPE = "IamRolesAnywhere"
)

type CredentialConfig struct {
	AuthType string `json:"authType"`

	AWSAccessKeyID     string `json:"awsAccessKeyId"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey"`

	ClientPrivateKey  string `json:"clientPrivateKey"`
	ClientCertificate string `json:"clientCertificate"`
	TrustAnchorArn    string `json:"trustAnchorArn"`
	ProfileArn        string `json:"profileArn"`
	RoleArn           string `json:"roleArn"`
}

func (c *CredentialConfig) Validate() error {
	switch c.AuthType {
	case ACCESS_KEY_AUTH_TYPE:
		return c.validateAccessKeyCreds()
	case IAM_ROLES_ANYWHERE_AUTH_TYPE:
		return c.validateIamRolesAnywhereCreds()
	default:
		return fmt.Errorf("invalid credentials auth type %q", c.AuthType)
	}
}

func (c *CredentialConfig) validateAccessKeyCreds() error {
	if c.AWSAccessKeyID == "" {
		return fmt.Errorf("missing aws access key id")
	} else if c.AWSSecretAccessKey == "" {
		return fmt.Errorf("missing aws secret access key")
	}

	return nil
}

func (c *CredentialConfig) validateIamRolesAnywhereCreds() error {
	if c.ClientPrivateKey == "" {
		return fmt.Errorf("missing client private key")
	} else if c.ClientCertificate == "" {
		return fmt.Errorf("missing client certificate")
	} else if c.TrustAnchorArn == "" {
		return fmt.Errorf("missing trust anchor arn")
	} else if c.ProfileArn == "" {
		return fmt.Errorf("missing profile arn")
	} else if c.RoleArn == "" {
		return fmt.Errorf("missing role arn")
	}

	return nil
}

func (c *CredentialConfig) CredentialsProvider() (aws.CredentialsProvider, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	if c.AuthType == ACCESS_KEY_AUTH_TYPE {
		return credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""), nil
	}

	expiryOpt := func(options *aws.CredentialsCacheOptions) {
		options.ExpiryWindow = 10 * time.Second
	}

	return aws.NewCredentialsCache(&rolesAnywhereProvider{
		ClientPrivateKey:  c.ClientPrivateKey,
		ClientCertificate: c.ClientCertificate,
		TrustAnchorArnStr: c.TrustAnchorArn,
		ProfileArnStr:     c.ProfileArn,
		RoleArn:           c.RoleArn,
	}, expiryOpt), nil
}

func (CredentialConfig) JSONSchema() *jsonschema.Schema {
	accessKeyProps := orderedmap.New()
	accessKeyProps.Set("authType", &jsonschema.Schema{
		Type:    "string",
		Default: ACCESS_KEY_AUTH_TYPE,
		Const:   ACCESS_KEY_AUTH_TYPE,
	})
	accessKeyProps.Set("awsAccessKeyId", &jsonschema.Schema{
		Title:       "AWS Access Key ID",
		Description: "Access Key ID to use for authorization.",
		Type:        "string",
		Extras: map[string]interface{}{
			"order": 0,
		},
	})
	accessKeyProps.Set("awsSecretAccessKey", &jsonschema.Schema{
		Title:       "AWS Secret Access Key",
		Description: "Access secret access key to use for authorization.",
		Type:        "string",
		Extras: map[string]interface{}{
			"order":  1,
			"secret": true,
		},
	})

	iamRolesAnywhereProps := orderedmap.New()
	iamRolesAnywhereProps.Set("authType", &jsonschema.Schema{
		Type:    "string",
		Default: IAM_ROLES_ANYWHERE_AUTH_TYPE,
		Const:   IAM_ROLES_ANYWHERE_AUTH_TYPE,
	})
	iamRolesAnywhereProps.Set("clientPrivateKey", &jsonschema.Schema{
		Title:       "Client Private Key",
		Description: "Client private key.",
		Type:        "string",
		Extras: map[string]interface{}{
			"order":     0,
			"secret":    true,
			"multiline": true,
		},
	})
	iamRolesAnywhereProps.Set("clientCertificate", &jsonschema.Schema{
		Title:       "Client certificate",
		Description: "Client certificate.",
		Type:        "string",
		Extras: map[string]interface{}{
			"order":     1,
			"secret":    true,
			"multiline": true,
		},
	})
	iamRolesAnywhereProps.Set("trustAnchorArn", &jsonschema.Schema{
		Title:       "Trust Anchor ARN",
		Description: "Trust anchor to use for authentication.",
		Type:        "string",
		Examples:    []interface{}{"arn:aws:rolesanywhere:region:account:trust-anchor/TA_ID"},
		Extras: map[string]interface{}{
			"order": 2,
		},
	})
	iamRolesAnywhereProps.Set("profileArn", &jsonschema.Schema{
		Title:       "Profile ARN",
		Description: "Profile to pull policies from.",
		Type:        "string",
		Examples:    []interface{}{"arn:aws:rolesanywhere:region:account:profile/PROFILE_ID"},
		Extras: map[string]interface{}{
			"order": 3,
		},
	})
	iamRolesAnywhereProps.Set("roleArn", &jsonschema.Schema{
		Title:       "Role ARN",
		Description: "Target role to assume.",
		Type:        "string",
		Examples:    []interface{}{"arn:aws:iam::account:role/role-name-with-path"},
		Extras: map[string]interface{}{
			"order": 4,
		},
	})

	return &jsonschema.Schema{
		Title:       "Authentication",
		Description: "AWS authentication credentials.",
		Default:     map[string]string{"authType": ACCESS_KEY_AUTH_TYPE},
		OneOf: []*jsonschema.Schema{
			{
				Title:      "Access Key",
				Required:   []string{"authType", "awsAccessKeyId", "awsSecretAccessKey"},
				Properties: accessKeyProps,
			},
			{
				Title:      "IAM Roles Anywhere",
				Required:   []string{"authType", "clientPrivateKey", "clientCertificate", "trustAnchorArn", "profileArn", "roleArn"},
				Properties: iamRolesAnywhereProps,
			},
		},
		Extras: map[string]interface{}{
			"discriminator": map[string]string{"propertyName": "authType"},
		},
		Type: "object",
	}
}
