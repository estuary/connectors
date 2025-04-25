package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/estuary/connectors/go/blob"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/invopop/jsonschema"
	"google.golang.org/api/option"
)

type config struct {
	Token         string              `json:"token" jsonschema:"title=Motherduck Service Token,description=Service token for authenticating with MotherDuck." jsonschema_extras:"secret=true,order=0"`
	Database      string              `json:"database" jsonschema:"title=Database,description=The database to materialize to." jsonschema_extras:"order=1"`
	Schema        string              `json:"schema" jsonschema:"title=Database Schema,default=main,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=2"`
	HardDelete    bool                `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=3"`
	StagingBucket stagingBucketConfig `json:"stagingBucket" jsonschema_extras:"order=4"`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"token", c.Token},
		{"database", c.Database},
		{"schema", c.Schema},
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

	switch c.StagingBucket.StagingBucketType {
	case stagingBucketTypeS3:
		for _, req := range [][]string{
			{"bucketS3", c.StagingBucket.BucketS3},
			{"awsAccessKeyId", c.StagingBucket.AWSAccessKeyID},
			{"awsSecretAccessKey", c.StagingBucket.AWSSecretAccessKey},
			{"region", c.StagingBucket.Region},
		} {
			if req[1] == "" {
				return fmt.Errorf("missing '%s'", req[0])
			}
		}

		// The bucket name must not contain any dots, since this breaks server
		// certificate validation on MotherDuck's side.
		if strings.Contains(c.StagingBucket.BucketS3, ".") {
			return fmt.Errorf("bucket name must not contain '.'")
		}
	case stagingBucketTypeGCS:
		for _, req := range [][]string{
			{"bucketGCS", c.StagingBucket.BucketGCS},
			{"credentialsJSON", c.StagingBucket.CredentialsJSON},
			{"gcsHMACAccessID", c.StagingBucket.GCSHMACAccessID},
			{"gcsHMACSecret", c.StagingBucket.GCSHMACSecret},
		} {
			if req[1] == "" {
				return fmt.Errorf("missing '%s'", req[0])
			}
		}

		if !json.Valid([]byte(c.StagingBucket.CredentialsJSON)) {
			return fmt.Errorf("invalid credentialsJSON: must be valid JSON")
		} else if len(c.StagingBucket.GCSHMACAccessID) != 61 {
			return fmt.Errorf("invalid gcsHMACAccessID: must be 61 characters for service accounts")
		} else if l := len(c.StagingBucket.GCSHMACSecret); l != 40 {
			return fmt.Errorf("invalid gcsHMACSecret: must be 40 characters")
		} else if _, err := base64.StdEncoding.DecodeString(c.StagingBucket.GCSHMACSecret); err != nil {
			return fmt.Errorf("invalid gcsHMACSecret: must be base64 encoded")
		}
	case "":
		return fmt.Errorf("missing 'stagingBucketType'")
	default:
		return fmt.Errorf("unknown staging bucket type %q", c.StagingBucket.StagingBucketType)
	}

	return nil
}

type stagingBucketType string

const (
	stagingBucketTypeS3  stagingBucketType = "S3"
	stagingBucketTypeGCS stagingBucketType = "GCS"
)

type stagingBucketConfig struct {
	StagingBucketType stagingBucketType `json:"stagingBucketType"`

	stagingBucketS3Config
	stagingBucketGCSConfig
}

func (stagingBucketConfig) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("Staging Bucket Configuration", "Staging Bucket Configuration", "stagingBucketType", string(stagingBucketTypeS3),
		schemagen.OneOfSubSchema("S3", stagingBucketS3Config{}, string(stagingBucketTypeS3)),
		schemagen.OneOfSubSchema("GCS", stagingBucketGCSConfig{}, string(stagingBucketTypeGCS)),
	)
}

type stagingBucketS3Config struct {
	BucketS3           string `json:"bucketS3" jsonschema:"title=S3 Staging Bucket,description=Name of the S3 bucket to use for staging data loads. Must not contain dots (.)" jsonschema_extras:"order=0,pattern=^[^.]*$"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for the S3 staging bucket." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for the S3 staging bucket." jsonschema_extras:"secret=true,order=2"`
	Region             string `json:"region" jsonschema:"title=S3 Bucket Region,description=Region of the S3 staging bucket." jsonschema_extras:"order=3"`
	BucketPathS3       string `json:"bucketPathS3,omitempty" jsonschema:"title=Bucket Path,description=An optional prefix that will be used to store objects in the staging bucket." jsonschema_extras:"order=4"`
}

type stagingBucketGCSConfig struct {
	BucketGCS       string `json:"bucketGCS" jsonschema:"title=GCS Staging Bucket,description=Name of the GCS bucket to use for staging data loads." jsonschema_extras:"order=0"`
	CredentialsJSON string `json:"credentialsJSON" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization to the staging bucket." jsonschema_extras:"secret=true,multiline=true,order=1"`
	GCSHMACAccessID string `json:"gcsHMACAccessID" jsonschema:"title=HMAC Access ID,description=HMAC access ID for the service account." jsonschema_extras:"order=2"`
	GCSHMACSecret   string `json:"gcsHMACSecret" jsonschema:"title=HMAC Secret,description=HMAC secret for the service account." jsonschema_extras:"secret=true,order=3"`
	BucketPathGCS   string `json:"bucketPathGCS,omitempty" jsonschema:"title=Bucket Path,description=An optional prefix that will be used to store objects in the staging bucket." jsonschema_extras:"order=4"`
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

	// This temporary secret in MotherDuck is session-specific.
	var createTempSecret string
	switch c.StagingBucket.StagingBucketType {
	case stagingBucketTypeS3:
		createTempSecret = fmt.Sprintf(`CREATE SECRET (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s'
		);`, c.StagingBucket.AWSAccessKeyID, c.StagingBucket.AWSSecretAccessKey, c.StagingBucket.Region)
	case stagingBucketTypeGCS:
		createTempSecret = fmt.Sprintf(`CREATE SECRET (
			TYPE GCS,
			KEY_ID '%s',
			SECRET '%s'
		);`, c.StagingBucket.GCSHMACAccessID, c.StagingBucket.GCSHMACSecret)
	}

	for idx, c := range []string{
		"SET autoinstall_known_extensions=1;",
		"SET autoload_known_extensions=1;",
		createTempSecret,
	} {
		if _, err := db.ExecContext(ctx, c); err != nil {
			return nil, fmt.Errorf("executing setup command %d: %w", idx, err)
		}
	}

	return db, err
}

func (c *config) toBucketAndPath(ctx context.Context) (blob.Bucket, string, error) {
	var bucket blob.Bucket
	var path string
	var err error

	switch c.StagingBucket.StagingBucketType {
	case stagingBucketTypeS3:
		creds := credentials.NewStaticCredentialsProvider(
			c.StagingBucket.AWSAccessKeyID,
			c.StagingBucket.AWSSecretAccessKey,
			"",
		)
		if bucket, err = blob.NewS3Bucket(ctx, c.StagingBucket.BucketS3, creds); err != nil {
			return nil, "", fmt.Errorf("creating S3 bucket: %w", err)
		}
		path = c.StagingBucket.BucketPathS3
	case stagingBucketTypeGCS:
		auth := option.WithCredentialsJSON([]byte(c.StagingBucket.CredentialsJSON))
		if bucket, err = blob.NewGCSBucket(ctx, c.StagingBucket.BucketGCS, auth); err != nil {
			return nil, "", fmt.Errorf("creating GCS bucket: %w", err)
		}
		path = c.StagingBucket.BucketPathGCS
	}

	return bucket, path, nil
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
