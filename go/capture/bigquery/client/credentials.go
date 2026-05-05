package client

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/estuary/connectors/go/auth/iam"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/invopop/jsonschema"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

type AuthType string

const (
	CredentialsJSON AuthType = "CredentialsJSON"
	GCPIAM          AuthType = "GCPIAM"
)

type CredentialsJSONConfig struct {
	CredentialsJSON string `json:"credentials_json" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization." jsonschema_extras:"secret=true,multiline=true,order=0"`
}

type Credentials struct {
	AuthType AuthType `json:"auth_type"`

	CredentialsJSONConfig
	iam.IAMConfig
}

func (Credentials) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("Credentials JSON", CredentialsJSONConfig{}, string(CredentialsJSON)),
	}
	subSchemas = append(subSchemas,
		schemagen.OneOfSubSchema("GCP IAM", iam.GCPConfig{}, string(GCPIAM)))

	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(CredentialsJSON), subSchemas...)
}

func (c *Credentials) Validate() error {
	switch c.AuthType {
	case CredentialsJSON:
		if c.CredentialsJSON == "" {
			return errors.New("missing 'credentials_json'")
		}

		// Sanity check: Are the provided credentials valid JSON? A common error is to upload
		// credentials that are not valid JSON, and the resulting error is fairly cryptic if fed
		// directly to bigquery.NewClient.
		if !json.Valid([]byte(c.CredentialsJSON)) {
			return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
		}
		return nil
	case GCPIAM:
		if err := c.ValidateIAM(); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("unknown 'auth_type'")
}

func (c *Credentials) ClientOption() (option.ClientOption, error) {
	switch c.AuthType {
	case CredentialsJSON:
		return option.WithCredentialsJSON([]byte(c.CredentialsJSON)), nil
	case GCPIAM:
		return option.WithTokenSource(oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: c.GoogleToken()},
		)), nil
	}
	return nil, fmt.Errorf("unknown 'auth_type'")
}
