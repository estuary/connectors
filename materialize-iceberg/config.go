package connector

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/cespare/xxhash/v2"
	"github.com/estuary/connectors/go/auth/iam"
	"github.com/estuary/connectors/go/blob"
	"github.com/estuary/connectors/go/dbt"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/estuary/connectors/materialize-iceberg/catalog"
	"github.com/invopop/jsonschema"
	"github.com/segmentio/encoding/json"
)

// TODO(whb): It would be nice to have a configuration for making the table use
// "Merge on Read" when performing DML, but that is broken in the latest version
// of Spark. The default is "Copy on Write" which may be suboptimal for some
// cases. See https://github.com/apache/iceberg/issues/11341 &
// https://github.com/apache/iceberg/issues/11709
type config struct {
	URL                   string                `json:"url" jsonschema:"title=URL,description=Base URL for the catalog. If you are using AWS Glue as a catalog this should look like 'https://glue.<region>.amazonaws.com/iceberg'. Otherwise it may look like 'https://yourserver.com/api/catalog'." jsonschema_extras:"order=0"`
	Warehouse             string                `json:"warehouse" jsonschema:"title=Warehouse,description=Warehouse to connect to. For AWS Glue this is the account ID." jsonschema_extras:"order=1"`
	Namespace             string                `json:"namespace" jsonschema:"title=Namespace,description=Namespace for bound collection tables (unless overridden within the binding resource configuration).," jsonschema_extras:"order=2,pattern=^[^.]*$"`
	BaseLocation          string                `json:"base_location,omitempty" jsonschema:"title=Base Location,description=Base location for the catalog tables. Required if using AWS Glue as a catalog. Example: 's3://your_bucket/your_prefix/'" jsonschema_extras:"order=3"`
	HardDelete            bool                  `json:"hard_delete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. It is disabled by default and _meta/op in the destination will signify whether rows have been deleted (soft-delete)." jsonschema_extras:"order=4"`
	Credentials           *catalogAuthConfig    `json:"credentials" jsonschema_extras:"x-iam-auth=true,order=5"`
	CatalogAuthentication *oldCatalogAuthConfig `json:"catalog_authentication,omitempty" jsonschema:"-"`
	Compute               computeConfig         `json:"compute"`
	Schedule              m.ScheduleConfig      `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger         dbt.JobConfig         `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt Job when new data is available"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
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

	catalogAuth, err := c.CatalogAuthConfig()
	if err != nil {
		return err
	}

	if err := catalogAuth.Validate(); err != nil {
		return err
	}
	if err := c.Compute.Validate(); err != nil {
		return err
	}

	if c.Compute.Credentials != nil && c.Compute.Credentials.AuthType == emrAuthTypeUseCatalogAuth {
		if c.Credentials.AuthType != catalogAuthTypeSigV4 && c.Credentials.AuthType != catalogAuthTypeAWSIAM {
			return fmt.Errorf("must use AWS EMR Serverless with SigV4 or IAM catalog credentials")
		}
	}

	if c.Compute.ComputeType == computeTypeEmrServerless && catalogAuth.AuthType == catalogAuthTypeClientCredential {
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

// Returns the signing name for a catalog URL.  Only AWS glue and s3tables URLs
// have a signing name.
func SigningName(catalogURL string) (string, error) {
	u, err := url.Parse(catalogURL)
	if err != nil {
		return "", err
	}

	hostname := u.Hostname()
	if strings.HasPrefix(hostname, "glue.") {
		return "glue", nil
	}
	if strings.HasPrefix(hostname, "s3tables.") {
		return "s3tables", nil
	}
	return "", errors.New("could not determine signing name")
}

// CatalogAuthConfig returns the configured catalogAuthConfig, this can be in
// different locations within the config in order to provide backwards
// compatability.
func (c config) CatalogAuthConfig() (*catalogAuthConfig, error) {
	if c.Credentials != nil {
		return c.Credentials, nil
	}
	if c.CatalogAuthentication != nil {
		return c.CatalogAuthentication.ToCatalogAuthConfig()
	}
	return nil, errors.New("missing 'credentials'")
}

type catalogAuthType string

const (
	oldCatalogAuthTypeClientCredential catalogAuthType = "OAuth 2.0 Client Credentials"
	oldCatalogAuthTypeSigV4            catalogAuthType = "AWS SigV4"
)

type oldCatalogAuthConfig struct {
	CatalogAuthType catalogAuthType `json:"catalog_auth_type"`

	catalogAuthClientCredentialConfig
	oldCatalogAuthSigV4Config
}

func (c *oldCatalogAuthConfig) ToCatalogAuthConfig() (*catalogAuthConfig, error) {
	switch c.CatalogAuthType {
	case oldCatalogAuthTypeClientCredential:
		return &catalogAuthConfig{
			AuthType:                          catalogAuthTypeClientCredential,
			catalogAuthClientCredentialConfig: c.catalogAuthClientCredentialConfig,
		}, nil
	case oldCatalogAuthTypeSigV4:
		return &catalogAuthConfig{
			AuthType: catalogAuthTypeSigV4,
			catalogAuthSigV4Config: catalogAuthSigV4Config{
				AWSAccessKeyID:     c.oldCatalogAuthSigV4Config.AWSAccessKeyID,
				AWSSecretAccessKey: c.oldCatalogAuthSigV4Config.AWSSecretAccessKey,
				AWSRegion:          c.oldCatalogAuthSigV4Config.Region,
			},
		}, nil
	}
	return nil, errors.New("invalid 'catalog_auth_type'")
}

type oldCatalogAuthSigV4Config struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=Access Key ID for authentication." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=Secret Access Key for authentication." jsonschema_extras:"secret=true,order=2"`
	Region             string `json:"region" jsonschema:"title=Region,description=AWS region for authentication." jsonschema_extras:"order=3"`
	SigningName        string `json:"signing_name" jsonschema:"-"`
}

type AuthType string

const (
	catalogAuthTypeClientCredential AuthType = "OAuth 2.0 Client Credentials"
	catalogAuthTypeSigV4            AuthType = "AWS SigV4"
	catalogAuthTypeAWSIAM           AuthType = "AWS IAM"
)

type catalogAuthConfig struct {
	AuthType AuthType `json:"auth_type"`

	catalogAuthClientCredentialConfig
	catalogAuthSigV4Config
	iam.IAMConfig
}

// Since the SigV4 and IAMConfig have conflicting JSON field names, only parse
// the embedded struct of interest.
func (c *catalogAuthConfig) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		AuthType AuthType `json:"auth_type"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	c.AuthType = discriminator.AuthType

	switch c.AuthType {
	case catalogAuthTypeClientCredential:
		return json.Unmarshal(data, &c.catalogAuthClientCredentialConfig)
	case catalogAuthTypeSigV4:
		return json.Unmarshal(data, &c.catalogAuthSigV4Config)
	case catalogAuthTypeAWSIAM:
		return json.Unmarshal(data, &c.IAMConfig)
	}
	return fmt.Errorf("unexpected auth_type: %s", c.AuthType)
}

func (c *catalogAuthConfig) Validate() error {
	switch c.AuthType {
	case catalogAuthTypeClientCredential:
		return c.catalogAuthClientCredentialConfig.Validate()
	case catalogAuthTypeSigV4:
		return c.catalogAuthSigV4Config.Validate()
	case catalogAuthTypeAWSIAM:
		return c.ValidateIAM()
	}
	return fmt.Errorf("unexpected catalog auth type %q", c.AuthType)
}

func (catalogAuthConfig) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("Catalog Authentication", "Iceberg Catalog Authentication Configuration", "auth_type", string(catalogAuthTypeClientCredential),
		schemagen.OneOfSubSchema("OAuth 2.0 Client Credentials", catalogAuthClientCredentialConfig{}, string(catalogAuthTypeClientCredential)),
		schemagen.OneOfSubSchema("AWS SigV4 Authentication", catalogAuthSigV4Config{}, string(catalogAuthTypeSigV4)),
		schemagen.OneOfSubSchema("AWS IAM Authentication", iam.AWSConfig{}, string(catalogAuthTypeAWSIAM)),
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
	AWSRegion          string `json:"aws_region" jsonschema:"title=Region,description=AWS region for authentication." jsonschema_extras:"order=3"`
}

func (c catalogAuthSigV4Config) Validate() error {
	var requiredProperties = [][]string{
		{"aws_access_key_id", c.AWSAccessKeyID},
		{"aws_secret_access_key", c.AWSSecretAccessKey},
		{"aws_region", c.AWSRegion},
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

func (c computeConfig) Validate() error {
	switch c.ComputeType {
	case computeTypeEmrServerless:
		return c.emrConfig.Validate()
	}
	return fmt.Errorf("invalid compute type %q", c.ComputeType)
}

func (computeConfig) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("Compute", "Compute Configuration", "compute_type", string(computeTypeEmrServerless),
		schemagen.OneOfSubSchema("AWS EMR Serverless", emrConfig{}, string(computeTypeEmrServerless)),
	)
}

type emrAuthType string

const (
	emrAuthTypeAWSAccessKey   emrAuthType = "AWSAccessKey"
	emrAuthTypeUseCatalogAuth emrAuthType = "UseCatalogAuth"
)

type AccessKeyCredentials struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=Access Key ID for writing data to the bucket." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=Secret Access Key for writing data to the bucket." jsonschema_extras:"secret=true,order=2"`
}

func (c AccessKeyCredentials) Validate() error {
	var requiredProperties = [][]string{
		{"aws_access_key_id", c.AWSAccessKeyID},
		{"aws_secret_access_key", c.AWSSecretAccessKey},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

type emrIAMConfig struct{}

type emrCredentials struct {
	AuthType emrAuthType `json:"auth_type"`

	AccessKeyCredentials
	emrIAMConfig
}

func (c *emrCredentials) Validate() error {
	switch c.AuthType {
	case emrAuthTypeAWSAccessKey:
		return c.AccessKeyCredentials.Validate()
	case emrAuthTypeUseCatalogAuth:
		return nil
	}
	return fmt.Errorf("unexpected EMR auth type %q", c.AuthType)
}

func (emrCredentials) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("AWS EMR Authentication", "", "auth_type", string(emrAuthTypeAWSAccessKey),
		schemagen.OneOfSubSchema("AWS AccessKey", AccessKeyCredentials{}, string(emrAuthTypeAWSAccessKey)),
		schemagen.OneOfSubSchema("Use Catalog Authentication", emrIAMConfig{}, string(emrAuthTypeUseCatalogAuth)),
	)
}

type emrConfig struct {
	AWSAccessKeyID       string          `json:"aws_access_key_id" jsonschema:"-"`
	AWSSecretAccessKey   string          `json:"aws_secret_access_key" jsonschema:"-" jsonschema_extras:"secret=true"`
	Region               string          `json:"region" jsonschema:"title=Region,description=Region of the EMR application and staging bucket." jsonschema_extras:"order=3"`
	ApplicationId        string          `json:"application_id" jsonschema:"title=Application ID,description=ID of the EMR serverless application." jsonschema_extras:"order=4"`
	ExecutionRoleArn     string          `json:"execution_role_arn" jsonschema:"title=Execution Role ARN,description=ARN of the EMR serverless execution role used to run jobs." jsonschema_extras:"order=5"`
	Bucket               string          `json:"bucket" jsonschema:"title=Bucket,description=Bucket to store staged data files." jsonschema_extras:"order=6"`
	BucketPath           string          `json:"bucket_path,omitempty" jsonschema:"title=Bucket Path,description=Optional prefix that will be used to store staged data files." jsonschema_extras:"order=7"`
	SystemsManagerPrefix string          `json:"systems_manager_prefix,omitempty" jsonschema:"title=System Manager Prefix,description=Prefix for parameters in Systems Manager as an absolute directory path (must start and end with /). This is required when using Client Credentials for catalog authentication." jsonschema_extras:"pattern=^/.+/$,order=8"`
	Credentials          *emrCredentials `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=9"`
}

func (c emrConfig) Validate() error {
	var requiredProperties = [][]string{
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

	if c.Credentials == nil {
		if c.AWSAccessKeyID == "" {
			return errors.New("missing 'aws_access_key_id'")
		}
		if c.AWSSecretAccessKey == "" {
			return errors.New("missing 'aws_secret_access_key'")
		}
	} else {
		if err := c.Credentials.Validate(); err != nil {
			return err
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
	catalogAuth, err := c.CatalogAuthConfig()
	if err != nil {
		return nil, err
	}

	var opts []catalog.CatalogOption
	if catalogAuth.AuthType == catalogAuthTypeClientCredential {
		cfg := catalogAuth.catalogAuthClientCredentialConfig

		var scope *string
		if cfg.Scope != "" {
			scope = &cfg.Scope
		}
		opts = append(opts, catalog.WithClientCredential(cfg.Credential, cfg.Oauth2ServerURI, scope))
	} else if catalogAuth.AuthType == catalogAuthTypeSigV4 {
		cfg := catalogAuth.catalogAuthSigV4Config
		signingName, err := SigningName(c.URL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, catalog.WithSigV4(
			signingName,
			cfg.AWSAccessKeyID,
			cfg.AWSSecretAccessKey,
			cfg.AWSRegion,
			"",
		))
	} else if catalogAuth.AuthType == catalogAuthTypeAWSIAM {
		cfg := catalogAuth.IAMConfig
		signingName, err := SigningName(c.URL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, catalog.WithSigV4(
			signingName,
			cfg.AWSAccessKeyID,
			cfg.AWSSecretAccessKey,
			cfg.AWSRegion,
			cfg.AWSSessionToken,
		))
	}

	return catalog.New(ctx, c.URL, c.Warehouse, opts...)
}

func (c config) toAwsConfig(ctx context.Context) (*aws.Config, error) {
	var credProvider aws.CredentialsProvider
	if c.Compute.Credentials == nil {
		credProvider = credentials.NewStaticCredentialsProvider(
			c.Compute.AWSAccessKeyID,
			c.Compute.AWSSecretAccessKey,
			"",
		)
	} else if c.Compute.Credentials.AuthType == emrAuthTypeAWSAccessKey {
		credProvider = credentials.NewStaticCredentialsProvider(
			c.Compute.Credentials.AWSAccessKeyID,
			c.Compute.Credentials.AWSSecretAccessKey,
			"",
		)
	} else if c.Compute.Credentials.AuthType == emrAuthTypeUseCatalogAuth && c.Credentials.AuthType == catalogAuthTypeSigV4 {
		credProvider = credentials.NewStaticCredentialsProvider(
			c.Credentials.AWSAccessKeyID,
			c.Credentials.AWSSecretAccessKey,
			"",
		)
	} else if c.Compute.Credentials.AuthType == emrAuthTypeUseCatalogAuth && c.Credentials.AuthType == catalogAuthTypeAWSIAM {
		var err error
		credProvider, err = c.Credentials.IAMTokens.AWSCredentialsProvider()
		if err != nil {
			return nil, err
		}
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(credProvider),
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
