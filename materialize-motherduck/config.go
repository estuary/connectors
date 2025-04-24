package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"fmt"
	"strings"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	sql "github.com/estuary/connectors/materialize-sql"
)

type config struct {
	Token              string `json:"token" jsonschema:"title=Motherduck Service Token,description=Service token for authenticating with MotherDuck." jsonschema_extras:"secret=true,order=0"`
	Database           string `json:"database" jsonschema:"title=Database,description=The database to materialize to." jsonschema_extras:"order=1"`
	Schema             string `json:"schema" jsonschema:"title=Database Schema,default=main,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=2"`
	Bucket             string `json:"bucket" jsonschema:"title=S3 Staging Bucket,description=Name of the S3 bucket to use for staging data loads. Must not contain dots (.)" jsonschema_extras:"order=3,pattern=^[^.]*$"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for reading and writing data to the S3 staging bucket." jsonschema_extras:"order=4"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for reading and writing data to the S3 staging bucket." jsonschema_extras:"secret=true,order=5"`
	Region             string `json:"region" jsonschema:"title=S3 Bucket Region,description=Region of the S3 staging bucket." jsonschema_extras:"order=6"`
	BucketPath         string `json:"bucketPath,omitempty" jsonschema:"title=Bucket Path,description=An optional prefix that will be used to store objects in S3." jsonschema_extras:"order=7"`
	HardDelete         bool   `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=8"`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"token", c.Token},
		{"database", c.Database},
		{"schema", c.Schema},
		{"bucket", c.Bucket},
		{"awsAccessKeyId", c.AWSAccessKeyID},
		{"awsSecretAccessKey", c.AWSSecretAccessKey},
		{"region", c.Region},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	// Sanity check that the provided authentication token is a well-formed JWT. It it's not, the
	// sql.Open function used elsewhere will return an error string that is difficult to comprehend.
	// This check isn't perfect but it should catch most of the blatantly obvious error cases.
	for _, part := range strings.Split(c.Token, ".") {
		if _, err := base64.RawURLEncoding.DecodeString(part); err != nil {
			return fmt.Errorf("invalid token: must be a base64 encoded JWT")
		}
	}

	if c.BucketPath != "" {
		// If BucketPath starts with a / trim the leading / so that we don't end up with repeated /
		// chars in the URI and so that the object key does not start with a /.
		c.BucketPath = strings.TrimPrefix(c.BucketPath, "/")
	}

	// The bucket name must not contain any dots, since this breaks server
	// certificate validation on MotherDuck's side.
	if strings.Contains(c.Bucket, ".") {
		return fmt.Errorf("bucket name must not contain '.'")
	}

	return nil
}

func (c *config) db(ctx context.Context) (*stdsql.DB, error) {
	var userAgent = "Estuary"

	db, err := stdsql.Open("duckdb", fmt.Sprintf("md:%s?motherduck_token=%s&custom_user_agent=%s", c.Database, c.Token, userAgent))
	if err != nil {
		if strings.Contains(err.Error(), "Jwt header is an invalid JSON") {
			return nil, fmt.Errorf("invalid token: unauthenticated")
		}
		return nil, err
	}

	for idx, c := range []string{
		"SET autoinstall_known_extensions=1;",
		"SET autoload_known_extensions=1;",
		fmt.Sprintf("SET s3_access_key_id='%s'", c.AWSAccessKeyID),
		fmt.Sprintf("SET s3_secret_access_key='%s';", c.AWSSecretAccessKey),
		fmt.Sprintf("SET s3_region='%s';", c.Region),
	} {
		if _, err := db.ExecContext(ctx, c); err != nil {
			return nil, fmt.Errorf("executing setup command %d: %w", idx, err)
		}
	}

	return db, err
}

func (c *config) toS3Client(ctx context.Context) (*s3.Client, error) {
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(c.Region),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg), nil
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)." jsonschema_extras:"x-schema-name=true"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"title=Delta Update,description=Should updates to this table be done via delta updates." jsonschema_extras:"x-delta-updates=true"`

	database string
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{
		// Default to the endpoint schema. This will be over-written by a present `schema` property
		// within `raw`.
		Schema:   ep.Config.(*config).Schema,
		database: ep.Config.(*config).Database,
	}
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}

	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.database, c.Schema, c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}
