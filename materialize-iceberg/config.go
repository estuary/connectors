package main

import (
	"context"
	"fmt"
	"strings"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/estuary/connectors/go/dbt"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/invopop/jsonschema"
)

type config struct {
	Namespace     string                     `json:"namespace" jsonschema:"title=Namespace,description=Namespace for bound collection tables (unless overridden within the binding resource configuration).," jsonschema_extras:"order=0,pattern=^[^.]*$"`
	HardDelete    bool                       `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. It is disabled by default and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=1"`
	Catalog       catalogConfig              `json:"catalog"`
	Compute       computeConfig              `json:"compute"`
	Schedule      boilerplate.ScheduleConfig `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger dbt.JobConfig              `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt Job when new data is available"`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
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

	if ct := c.Catalog.CatalogType; ct == "" {
		return fmt.Errorf("catalog type is required")
	} else if ct != catalogTypeRest && ct != catalogTypeGlue {
		return fmt.Errorf("invalid catalog type %q", c.Catalog.CatalogType)
	} else if ct == catalogTypeRest {
		if err := c.Catalog.restCatalogConfig.Validate(); err != nil {
			return err
		}
	} else if ct == catalogTypeGlue {
		// TODO
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

func (c config) toCatalog(ctx context.Context) (*catalog, error) {
	return newCatalog(ctx, c.Catalog)
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

type catalogType string

const (
	catalogTypeRest catalogType = "Iceberg REST Server"
	catalogTypeGlue catalogType = "AWS Glue"
)

type catalogConfig struct {
	CatalogType catalogType `json:"catalog_type"`

	restCatalogConfig
	glueCatalogConfig
}

type restCatalogConfig struct {
	URL        string `json:"url" jsonschema:"title=URL,description=Base URL for the REST catalog. Example: 'https://yourserver.com/api/catalog'" jsonschema_extras:"order=1"`
	Credential string `json:"credential" jsonschema:"title=Credential,description=Credential for connecting to the catalog usually in the format of '<client_id>:<client_secret>'" jsonschema_extras:"order=2"`
	Warehouse  string `json:"warehouse" jsonschema:"title=Warehouse,description=Warehouse to connect to." jsonschema_extras:"order=3"`
	Scope      string `json:"scope,omitempty" jsonschema:"title=Scope,description=Authorization scope for connecting to the catalog. Example: 'PRINCIPAL_ROLE:your_principal'" jsonschema_extras:"order=4"`
}

func (c restCatalogConfig) Validate() error {
	var requiredProperties = [][]string{
		{"url", c.URL},
		{"credential", c.Credential},
		{"warehouse", c.Warehouse},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

type glueCatalogConfig struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=Access Key ID for authenticating with Glue." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=Secret Access Key for authenticating with Glue." jsonschema_extras:"secret=true,order=2"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region of the Glue catalog." jsonschema_extras:"order=3"`
	GlueWarehouse      string `json:"glue_warehouse" jsonschema:"title=Glue Warehouse (Account ID),description=Glue warehouse to connect to. This is usually your AWS account ID." jsonschema_extras:"order=4"`
	Location           string `json:"location" jsonschema:"title=Location,description=Base location for the catalog tables. Example: 's3://your_bucket/your_prefix/'" jsonschema_extras:"order=5"`
}

func (c glueCatalogConfig) Validate() error {
	var requiredProperties = [][]string{
		{"aws_access_key_id", c.AWSAccessKeyID},
		{"aws_secret_access_key", c.AWSSecretAccessKey},
		{"region", c.Region},
		{"glue_warehouse", c.GlueWarehouse},
		{"location", c.Location},
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

	emrConfig
}

type emrConfig struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=Access Key ID for authenticating with EMR and writing data to the staging bucket." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=Secret Access Key for authenticating with EMR and writing data to the staging bucket." jsonschema_extras:"secret=true,order=2"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region of the EMR application and staging bucket." jsonschema_extras:"order=3"`
	ApplicationId      string `json:"application_id" jsonschema:"title=Application ID,description=ID of the EMR serverless application." jsonschema_extras:"order=4"`
	ExecutionRoleArn   string `json:"execution_role_arn" jsonschema:"title=Execution Role ARN,description=ARN of the EMR serverless execution role used to run jobs." jsonschema_extras:"order=5"`
	Bucket             string `json:"aws_bucket" jsonschema:"title=Bucket,description=Bucket to store staged data files." jsonschema_extras:"order=6"`
	BucketPath         string `json:"aws_bucket_path,omitempty" jsonschema:"title=Bucket Path,description=Optional prefix that will be used to store staged data files." jsonschema_extras:"order=7"`
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

	return nil
}

func (catalogConfig) JSONSchema() *jsonschema.Schema {
	return oneOfSchema("Catalog", "Catalog Configuration", "catalog_type", string(catalogTypeRest),
		oneOfInput{"REST Catalog", restCatalogConfig{}, string(catalogTypeRest)},
		oneOfInput{"AWS Glue Catalog", glueCatalogConfig{}, string(catalogTypeGlue)},
	)
}

func (computeConfig) JSONSchema() *jsonschema.Schema {
	return oneOfSchema("Compute", "Compute Configuration", "compute_type", string(computeTypeEmrServerless),
		oneOfInput{"AWS EMR Serverless", emrConfig{}, string(computeTypeEmrServerless)},
	)
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
