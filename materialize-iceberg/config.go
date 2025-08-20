package connector

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/cespare/xxhash/v2"
	"github.com/estuary/connectors/go/blob"
	"github.com/estuary/connectors/go/dbt"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/estuary/connectors/materialize-iceberg/catalog"
	"github.com/invopop/jsonschema"
)

// TODO(whb): It would be nice to have a configuration for making the table use
// "Merge on Read" when performing DML, but that is broken in the latest version
// of Spark. The default is "Copy on Write" which may be suboptimal for some
// cases. See https://github.com/apache/iceberg/issues/11341 &
// https://github.com/apache/iceberg/issues/11709
type config struct {
	URL                   string            `json:"url" jsonschema:"title=URL,description=Base URL for the catalog. If you are using AWS Glue as a catalog this should look like 'https://glue.<region>.amazonaws.com/iceberg'. Otherwise it may look like 'https://yourserver.com/api/catalog'." jsonschema_extras:"order=0"`
	Warehouse             string            `json:"warehouse" jsonschema:"title=Warehouse,description=Warehouse to connect to. For AWS Glue this is the account ID." jsonschema_extras:"order=1"`
	Namespace             string            `json:"namespace" jsonschema:"title=Namespace,description=Namespace for bound collection tables (unless overridden within the binding resource configuration).," jsonschema_extras:"order=2,pattern=^[^.]*$"`
	BaseLocation          string            `json:"base_location,omitempty" jsonschema:"title=Base Location,description=Base location for the catalog tables. Required if using AWS Glue as a catalog. Example: 's3://your_bucket/your_prefix/'" jsonschema_extras:"order=3"`
	HardDelete            bool              `json:"hard_delete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. It is disabled by default and _meta/op in the destination will signify whether rows have been deleted (soft-delete)." jsonschema_extras:"order=4"`
	CatalogAuthentication catalogAuthConfig `json:"catalog_authentication"`
	Compute               computeConfig     `json:"compute"`
	Schedule              m.ScheduleConfig  `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger         dbt.JobConfig     `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt Job when new data is available"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	LowercaseColumnNames bool   `json:"lowercase_column_names,omitempty" jsonschema:"title=Lowercase Column Names,description=Create all columns with lowercase names. This is necessary for compatibility with some systems such as querying S3 Table Buckets with Athena."`
	FeatureFlags         string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
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

	ca := c.CatalogAuthentication.CatalogAuthType
	ct := c.Compute.ComputeType

	if ca == "" {
		return fmt.Errorf("catalog authentication type is required")
	} else if ca != catalogAuthTypeClientCredential && ca != catalogAuthTypeSigV4 {
		return fmt.Errorf("invalid catalog authentication type %q", ca)
	} else if ca == catalogAuthTypeClientCredential {
		if err := c.CatalogAuthentication.catalogAuthClientCredentialConfig.Validate(); err != nil {
			return err
		}
	} else if ca == catalogAuthTypeSigV4 {
		if err := c.CatalogAuthentication.catalogAuthSigV4Config.Validate(); err != nil {
			return err
		}
	}

	if ct == "" {
		return fmt.Errorf("compute type is required")
	} else if ct != computeTypeEmrServerless {
		return fmt.Errorf("invalid compute type %q", ct)
	} else if ct == computeTypeEmrServerless {
		if err := c.Compute.emrConfig.Validate(); err != nil {
			return err
		}
	}

	if ct == computeTypeEmrServerless && ca == catalogAuthTypeClientCredential {
		if c.Compute.emrConfig.SystemsManagerPrefix == "" {
			return fmt.Errorf("must specify Systems Manager Prefix for AWS EMR Serverless compute with Client Credentials catalog authentication")
		}
	}

	if err := c.Schedule.Validate(); err != nil {
		return err
	} else if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	return nil
}

func (c config) DefaultNamespace() string {
	return c.Namespace
}

func (c config) FeatureFlags() (raw string, defaults map[string]bool) {
	return c.Advanced.FeatureFlags, make(map[string]bool)
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
	return schemagen.OneOfSchema("Catalog Authentication", "Iceberg Catalog Authentication Configuration", "catalog_auth_type", string(catalogAuthTypeClientCredential),
		schemagen.OneOfSubSchema("OAuth 2.0 Client Credentials", catalogAuthClientCredentialConfig{}, string(catalogAuthTypeClientCredential)),
		schemagen.OneOfSubSchema("AWS SigV4 Authentication", catalogAuthSigV4Config{}, string(catalogAuthTypeSigV4)),
	)
}

type catalogAuthClientCredentialConfig struct {
	Oauth2ServerURI string `json:"oauth2_server_uri" jsonschema:"title=OAuth 2.0 Server URI,default=v1/oauth/tokens,description=OAuth 2.0 server URI for requesting access tokens when using OAuth client credentials. Usually this should be 'v1/oauth/tokens'." jsonschema_extras:"order=0"`
	Credential      string `json:"credential" jsonschema:"title=Catalog Credential,description=Credential for connecting to the REST catalog. Must be in the format of '<client_id>:<client_secret>' for OAuth client credentials. For Bearer authentication use '<token>'." jsonschema_extras:"order=1,secret=true"`
	Scope           string `json:"scope,omitempty" jsonschema:"title=Scope,description=Authorization scope for connecting to the catalog when using OAuth client credentials. Example: 'PRINCIPAL_ROLE:your_principal'" jsonschema_extras:"order=2"`
}

func (c catalogAuthClientCredentialConfig) Validate() error {
	var requiredProperties = [][]string{
		{"oauth2_server_uri", c.Oauth2ServerURI},
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
	SigningName        string `json:"signing_name" jsonschema:"title=Signing Name,description=Signing Name for SigV4 authentication.,enum=glue,enum=s3tables,default=glue" jsonschema_extras:"order=4"`
}

func (c catalogAuthSigV4Config) Validate() error {
	var requiredProperties = [][]string{
		{"aws_access_key_id", c.AWSAccessKeyID},
		{"aws_secret_access_key", c.AWSSecretAccessKey},
		{"region", c.Region},
		{"signing_name", c.SigningName},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.SigningName != "glue" && c.SigningName != "s3tables" {
		return fmt.Errorf("signing_name must be 'glue' or 's3tables'")
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
	return schemagen.OneOfSchema("Compute", "Compute Configuration", "compute_type", string(computeTypeEmrServerless),
		schemagen.OneOfSubSchema("AWS EMR Serverless", emrConfig{}, string(computeTypeEmrServerless)),
	)
}

type emrConfig struct {
	AWSAccessKeyID       string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=Access Key ID for authenticating with EMR and writing data to the staging bucket." jsonschema_extras:"order=1"`
	AWSSecretAccessKey   string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=Secret Access Key for authenticating with EMR and writing data to the staging bucket." jsonschema_extras:"secret=true,order=2"`
	Region               string `json:"region" jsonschema:"title=Region,description=Region of the EMR application and staging bucket." jsonschema_extras:"order=3"`
	ApplicationId        string `json:"application_id" jsonschema:"title=Application ID,description=ID of the EMR serverless application." jsonschema_extras:"order=4"`
	ExecutionRoleArn     string `json:"execution_role_arn" jsonschema:"title=Execution Role ARN,description=ARN of the EMR serverless execution role used to run jobs." jsonschema_extras:"order=5"`
	Bucket               string `json:"bucket" jsonschema:"title=Bucket,description=Bucket to store staged data files." jsonschema_extras:"order=6"`
	BucketPath           string `json:"bucket_path,omitempty" jsonschema:"title=Bucket Path,description=Optional prefix that will be used to store staged data files." jsonschema_extras:"order=7"`
	SystemsManagerPrefix string `json:"systems_manager_prefix,omitempty" jsonschema:"title=System Manager Prefix,description=Prefix for parameters in Systems Manager as an absolute directory path (must start and end with /). This is required when using Client Credentials for catalog authentication." jsonschema_extras:"pattern=^/.+/$,order=8"`
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

	if c.SystemsManagerPrefix != "" {
		if !strings.HasPrefix(c.SystemsManagerPrefix, "/") || !strings.HasSuffix(c.SystemsManagerPrefix, "/") {
			return fmt.Errorf("systems manager prefix %q must start and end with /", c.SystemsManagerPrefix)
		}
	}

	return nil
}

func (c config) toCatalog(ctx context.Context) (*catalog.Catalog, error) {
	var opts []catalog.CatalogOption
	if c.CatalogAuthentication.CatalogAuthType == catalogAuthTypeClientCredential {
		cfg := c.CatalogAuthentication.catalogAuthClientCredentialConfig

		var scope *string
		if cfg.Scope != "" {
			scope = &cfg.Scope
		}
		opts = append(opts, catalog.WithClientCredential(cfg.Credential, cfg.Oauth2ServerURI, scope))
	} else if c.CatalogAuthentication.CatalogAuthType == catalogAuthTypeSigV4 {
		cfg := c.CatalogAuthentication.catalogAuthSigV4Config
		opts = append(opts, catalog.WithSigV4(cfg.SigningName, cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.Region))
	}

	return catalog.New(ctx, c.URL, c.Warehouse, opts...)
}

func (c config) toAwsConfig(ctx context.Context) (*aws.Config, error) {
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.Compute.AWSAccessKeyID, c.Compute.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(c.Compute.Region),
	)
	if err != nil {
		return nil, err
	}

	return &awsCfg, nil
}

func (c config) toBucket(ctx context.Context) (blob.Bucket, error) {
	awsCfg, err := c.toAwsConfig(ctx)
	if err != nil {
		return nil, err
	}

	return blob.NewS3Bucket(ctx, c.Compute.Bucket, awsCfg.Credentials)
}

func (c config) toEmrClient(ctx context.Context) (*emr.Client, error) {
	awsCfg, err := c.toAwsConfig(ctx)
	if err != nil {
		return nil, err
	}

	return emr.NewFromConfig(*awsCfg), nil
}

func (c config) toSsmClient(ctx context.Context) (*ssm.Client, error) {
	awsCfg, err := c.toAwsConfig(ctx)
	if err != nil {
		return nil, err
	}

	return ssm.NewFromConfig(*awsCfg), nil
}

type resource struct {
	Table     string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Namespace string `json:"namespace,omitempty" jsonschema:"title=Alternative Namespace,description=Alternative Namespace for this table (optional)." jsonschema_extras:"x-schema-name=true"`
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

func (r resource) WithDefaults(cfg config) resource {
	if r.Namespace == "" {
		r.Namespace = cfg.Namespace
	}

	return r
}

func (r resource) Parameters() (path []string, deltaUpdates bool, err error) {
	return sanitizePath(r.Namespace, r.Table), false, nil
}

// Iceberg catalogs are generally case-insensitive and do not allow dots in
// namespace or table names. AWS Glue is also very picky about anything other
// than alphanumerics or underscores in namespace or table names.
var pathSanitizeRegex = regexp.MustCompile("(?i)[^a-z0-9_]")

func sanitizePath(path ...string) []string {
	out := []string{}
	for _, p := range path {
		out = append(out, strings.ToLower(pathSanitizeRegex.ReplaceAllString(p, "_")))
	}

	return out
}

// sanitizeAndAppendHash adapts an input into a reasonably human-readable
// representation, sanitizing problematic characters and including a hash of the
// "original" value to guarantee a unique (with respect to the input) and
// deterministic output.
func sanitizeAndAppendHash(in ...any) string {
	strs := make([]string, 0, len(in))
	for _, i := range in {
		strs = append(strs, fmt.Sprintf("%v", i))
	}

	joined := strings.Join(strs, "_")
	sanitized := pathSanitizeRegex.ReplaceAllString(joined, "_")
	if len(sanitized) > 64 {
		// Limit the length of the "human readable" part of the table name to
		// something reasonable.
		sanitized = sanitized[:64]
	}

	return fmt.Sprintf("%s_%016X", sanitized, xxhash.Sum64String(joined))
}
