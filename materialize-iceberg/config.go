package main

import (
	"context"
	"fmt"
	"strings"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/estuary/connectors/go/dbt"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-iceberg/catalog"
	"github.com/invopop/jsonschema"
)

type config struct {
	URL                   string                     `json:"url" jsonschema:"title=URL,description=Base URL for the catalog. If you are using AWS Glue as a catalog this should look like 'https://glue.<region>.amazonaws.com/iceberg'. Otherwise it may look like 'https://yourserver.com/api/catalog'." jsonschema_extras:"order=0"`
	Warehouse             string                     `json:"warehouse" jsonschema:"title=Warehouse,description=Warehouse to connect to. For AWS Glue this is the account ID." jsonschema_extras:"order=1"`
	Namespace             string                     `json:"namespace" jsonschema:"title=Namespace,description=Namespace for bound collection tables (unless overridden within the binding resource configuration).," jsonschema_extras:"order=2,pattern=^[^.]*$"`
	Location              string                     `json:"location,omitempty" jsonschema:"title=Location,description=Base location for the catalog tables. Required if using AWS Glue as a catalog. Example: 's3://your_bucket/your_prefix/'" jsonschema_extras:"order=3"`
	HardDelete            bool                       `json:"hard_delete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. It is disabled by default and _meta/op in the destination will signify whether rows have been deleted (soft-delete)." jsonschema_extras:"order=4"`
	CatalogAuthentication catalogAuthConfig          `json:"catalog_authentication"`
	Compute               computeConfig              `json:"compute"`
	Schedule              boilerplate.ScheduleConfig `json:"sync_schedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger         dbt.JobConfig              `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt Job when new data is available"`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"url", c.URL},
		{"warehouse", c.Warehouse},
		{"namespace", c.Namespace},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if strings.Contains(c.Namespace, ".") {
		return fmt.Errorf("namespace %q must not contain dots", c.Namespace)
	}

	if ca := c.CatalogAuthentication.CatalogAuthType; ca == "" {
		return fmt.Errorf("catalog authentication type is required")
	} else if ca != catalogAuthTypeClientCredential && ca != catalogAuthTypeSigV4 {
		return fmt.Errorf("invalid catalog authentication type %q", c.CatalogAuthentication.CatalogAuthType)
	} else if ca == catalogAuthTypeClientCredential {
		if err := c.CatalogAuthentication.catalogAuthClientCredentialConfig.Validate(); err != nil {
			return err
		}
	} else if ca == catalogAuthTypeSigV4 {
		if err := c.CatalogAuthentication.catalogAuthSigV4Config.Validate(); err != nil {
			return err
		}
	}

	if ct := c.Compute.ComputeType; ct == "" {
		return fmt.Errorf("compute type is required")
	} else if ct != computeTypeEmrServerless {
		return fmt.Errorf("invalid compute type %q", c.Compute.ComputeType)
	} else if ct == computeTypeEmrServerless {
		if err := c.Compute.emrConfig.Validate(); err != nil {
			return err
		}
	}

	if err := c.Schedule.Validate(); err != nil {
		return err
	} else if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	return nil
}

type catalogAuthType string

const (
	catalogAuthTypeClientCredential catalogAuthType = "OAuth 2.0 Client Credentials"
	catalogAuthTypeSigV4            catalogAuthType = "AWS SigV4"
)

type catalogAuthConfig struct {
	CatalogAuthType catalogAuthType `json:"catalog_auth_type"`

	catalogAuthClientCredentialConfig
	catalogAuthSigV4Config
}

func (catalogAuthConfig) JSONSchema() *jsonschema.Schema {
	return oneOfSchema("Catalog Authentication", "Iceberg Catalog Authentication Configuration", "catalog_auth_type", string(catalogAuthTypeClientCredential),
		oneOfInput{"OAuth 2.0 Client Credentials", catalogAuthClientCredentialConfig{}, string(catalogAuthTypeClientCredential)},
		oneOfInput{"AWS SigV4 Authentication", catalogAuthSigV4Config{}, string(catalogAuthTypeSigV4)},
	)
}

type catalogAuthClientCredentialConfig struct {
	Credential string `json:"credential" jsonschema:"title=Catalog Credential,description=Credential for connecting to the REST catalog. Must be in the format of '<client_id>:<client_secret>' for OAuth client credentials. For Bearer authentication use '<token>'." jsonschema_extras:"order=1,secret=true"`
	Scope      string `json:"scope,omitempty" jsonschema:"title=Scope,description=Authorization scope for connecting to the catalog when using OAuth client credentials. Example: 'PRINCIPAL_ROLE:your_principal'" jsonschema_extras:"order=2"`
}

func (c catalogAuthClientCredentialConfig) Validate() error {
	var requiredProperties = [][]string{
		{"credential", c.Credential},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

type catalogAuthSigV4Config struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=Access Key ID for authentication." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=Secret Access Key for authentication." jsonschema_extras:"secret=true,order=2"`
	Region             string `json:"region" jsonschema:"title=Region,description=AWS region for authentication." jsonschema_extras:"order=3"`
}

func (c catalogAuthSigV4Config) Validate() error {
	var requiredProperties = [][]string{
		{"aws_access_key_id", c.AWSAccessKeyID},
		{"aws_secret_access_key", c.AWSSecretAccessKey},
		{"region", c.Region},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

type computeType string

const (
	computeTypeEmrServerless computeType = "AWS EMR Serverless"
)

type computeConfig struct {
	ComputeType computeType `json:"compute_type"`

	// There is only one option for now, but more may be added in the future.
	emrConfig
}

func (computeConfig) JSONSchema() *jsonschema.Schema {
	return oneOfSchema("Compute", "Compute Configuration", "compute_type", string(computeTypeEmrServerless),
		oneOfInput{"AWS EMR Serverless", emrConfig{}, string(computeTypeEmrServerless)},
	)
}

type emrConfig struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=Access Key ID for authenticating with EMR and writing data to the staging bucket." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=Secret Access Key for authenticating with EMR and writing data to the staging bucket." jsonschema_extras:"secret=true,order=2"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region of the EMR application and staging bucket." jsonschema_extras:"order=3"`
	ApplicationId      string `json:"application_id" jsonschema:"title=Application ID,description=ID of the EMR serverless application." jsonschema_extras:"order=4"`
	ExecutionRoleArn   string `json:"execution_role_arn" jsonschema:"title=Execution Role ARN,description=ARN of the EMR serverless execution role used to run jobs." jsonschema_extras:"order=5"`
	// TODO: Make these just 'bucket' and 'bucket_path' in their json key
	Bucket                   string        `json:"aws_bucket" jsonschema:"title=Bucket,description=Bucket to store staged data files." jsonschema_extras:"order=6"`
	BucketPath               string        `json:"aws_bucket_path,omitempty" jsonschema:"title=Bucket Path,description=Optional prefix that will be used to store staged data files." jsonschema_extras:"order=7"`
	EmrCatalogAuthentication emrAuthConfig `json:"emr_catalog_authentication"`
}

func (c emrConfig) Validate() error {
	var requiredProperties = [][]string{
		{"aws_access_key_id", c.AWSAccessKeyID},
		{"aws_secret_access_key", c.AWSSecretAccessKey},
		{"region", c.Region},
		{"application_id", c.ApplicationId},
		{"execution_role_arn", c.ExecutionRoleArn},
		{"bucket", c.Bucket},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.BucketPath != "" {
		if strings.HasPrefix(c.BucketPath, "/") {
			return fmt.Errorf("bucket path %q cannot start with /", c.BucketPath)
		}
	}

	if c.EmrCatalogAuthentication.EmrAuthType == "" {
		return fmt.Errorf("emr catalog authentication type is required")
	} else if c.EmrCatalogAuthentication.EmrAuthType != emrAuthTypeClientCredential && c.EmrCatalogAuthentication.EmrAuthType != emrAuthTypeSigV4 {
		return fmt.Errorf("invalid emr catalog authentication type %q", c.EmrCatalogAuthentication.EmrAuthType)
	} else if c.EmrCatalogAuthentication.EmrAuthType == emrAuthTypeClientCredential {
		if err := c.EmrCatalogAuthentication.emrAuthClientCredentialConfig.Validate(); err != nil {
			return err
		}
	} else if c.EmrCatalogAuthentication.EmrAuthType == emrAuthTypeSigV4 {
		if err := c.EmrCatalogAuthentication.emtAuthSigV4Config.Validate(); err != nil {
			return err
		}
	}

	return nil
}

type emrAuthType string

const (
	emrAuthTypeClientCredential emrAuthType = "OAuth 2.0 Client Credentials"
	emrAuthTypeSigV4            emrAuthType = "AWS SigV4"
)

type emrAuthConfig struct {
	EmrAuthType emrAuthType `json:"emr_auth_type"`

	emrAuthClientCredentialConfig
	emtAuthSigV4Config
}

func (emrAuthConfig) JSONSchema() *jsonschema.Schema {
	return oneOfSchema("EMR Authentication", "EMR Catalog Authentication Configuration", "emr_auth_type", string(emrAuthTypeClientCredential),
		oneOfInput{"OAuth 2.0 Client Credentials in AWS Secrets Manager", emrAuthClientCredentialConfig{}, string(emrAuthTypeClientCredential)},
		oneOfInput{"AWS SigV4", emtAuthSigV4Config{}, string(emrAuthTypeSigV4)},
	)
}

type emrAuthClientCredentialConfig struct {
	CredentialSecretName string `json:"credential_secret_name" jsonschema:"title=AWS Secrets Manager Secret Name,description=The secret name in Secrets Manager containing the catalog credential value. The secret must be a single key/value pair with the value containing the credential. The credential format is the same as for the catalog authentication configuration." jsonschema_extras:"order=1,secret=true"`
	Scope                string `json:"scope,omitempty" jsonschema:"title=Scope,description=Authorization scope for connecting to the catalog when using OAuth client credentials. Example: 'PRINCIPAL_ROLE:your_principal'" jsonschema_extras:"order=2"`
}

func (c emrAuthClientCredentialConfig) Validate() error {
	var requiredProperties = [][]string{
		{"credential_secret_name", c.CredentialSecretName},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

type emtAuthSigV4Config struct{}

func (c emtAuthSigV4Config) Validate() error { return nil }

func (c config) toCatalog(ctx context.Context) (*catalog.Catalog, error) {
	var opts []catalog.CatalogOption
	if c.CatalogAuthentication.CatalogAuthType == catalogAuthTypeClientCredential {
		cfg := c.CatalogAuthentication.catalogAuthClientCredentialConfig

		var scope *string
		if cfg.Scope != "" {
			scope = &cfg.Scope
		}
		opts = append(opts, catalog.WithClientCredential(cfg.Credential, scope))
	} else if c.CatalogAuthentication.CatalogAuthType == catalogAuthTypeSigV4 {
		cfg := c.CatalogAuthentication.catalogAuthSigV4Config

		opts = append(opts, catalog.WithSigV4(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.Region))
	}

	return catalog.NewCatalog(ctx, c.URL, c.Warehouse, opts...)
}

func (c config) toS3Client(ctx context.Context) (*s3.Client, error) {
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.Compute.AWSAccessKeyID, c.Compute.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(c.Compute.Region),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg), nil
}

func (c config) toEmrClient(ctx context.Context) (*emr.Client, error) {
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.Compute.AWSAccessKeyID, c.Compute.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(c.Compute.Region),
	)
	if err != nil {
		return nil, err
	}

	return emr.NewFromConfig(awsCfg), nil

}

type resource struct {
	Table     string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Namespace string `json:"schema,omitempty" jsonschema:"title=Alternative Namespace,description=Alternative Namespace for this table (optional)." jsonschema_extras:"x-schema-name=true"`
}

func (r resource) Validate() error {
	var requiredProperties = [][]string{
		{"table", r.Table},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

func (r resource) Parameters() (path []string, deltaUpdates bool, err error) {
	return sanitizePath(r.Namespace, r.Table), false, nil
}

// Iceberg catalogs are generally case-insensitive and do not allow dots in
// namespace or table names.
func sanitizePath(path ...string) []string {
	out := []string{}
	for _, p := range path {
		out = append(out, strings.ToLower(strings.ReplaceAll(p, ".", "_")))
	}

	return out
}

type oneOfInput struct {
	title    string
	instance any
	default_ string
}

func oneOfSchema(title, description, discriminator, default_ string, inputs ...oneOfInput) *jsonschema.Schema {
	var oneOfs []*jsonschema.Schema

	for _, input := range inputs {
		config := schemagen.GenerateSchema(input.title, input.instance)
		config.Properties.Set(discriminator, &jsonschema.Schema{
			Type:    "string",
			Default: input.default_,
			Const:   input.default_,
			Extras:  map[string]any{"order": 0},
		})
		config.Properties.MoveToFront(discriminator)
		oneOfs = append(oneOfs, config)
	}

	return &jsonschema.Schema{
		Title:       title,
		Description: description,
		Default:     map[string]string{discriminator: default_},
		OneOf:       oneOfs,
		Extras: map[string]any{
			"discriminator": map[string]string{"propertyName": discriminator},
		},
		Type: "object",
	}
}
