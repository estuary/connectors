package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/estuary/connectors/go/blob"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

var featureFlagDefaults = map[string]bool{
	"datetime_keys_as_string":          true,
	"retain_existing_data_on_backfill": false,
}

type config struct {
	Token         string              `json:"token" jsonschema:"title=Motherduck Service Token,description=Service token for authenticating with MotherDuck." jsonschema_extras:"secret=true,order=0"`
	Database      string              `json:"database" jsonschema:"title=Database,description=The database to materialize to." jsonschema_extras:"order=1"`
	Schema        string              `json:"schema" jsonschema:"title=Database Schema,default=main,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=2"`
	HardDelete    bool                `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=3"`
	StagingBucket stagingBucketConfig `json:"stagingBucket" jsonschema_extras:"order=4"`
	Schedule      m.ScheduleConfig    `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	NoFlowDocument bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false"`
	FeatureFlags   string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

func (c config) Validate() error {
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

	if err := c.Schedule.Validate(); err != nil {
		return err
	}

	// Sanity check that the provided authentication token is a well-formed JWT. It it's not, the
	// sql.Open function used elsewhere will return an error string that is difficult to comprehend.
	// This check isn't perfect but it should catch most of the blatantly obvious error cases.
	for part := range strings.SplitSeq(c.Token, ".") {
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

		if _, err := url.Parse(c.StagingBucket.Endpoint); err != nil {
			return fmt.Errorf("unable to parse endpoint: %w", err)
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
	case stagingBucketTypeAzure:
		for _, req := range [][]string{
			{"storageAccountName", c.StagingBucket.StorageAccountName},
			{"storageAccountKey", c.StagingBucket.StorageAccountKey},
			{"containerName", c.StagingBucket.ContainerName},
		} {
			if req[1] == "" {
				return fmt.Errorf("missing '%s'", req[0])
			}
		}

		// Azure storage account key is base64 encoded
		if _, err := base64.StdEncoding.DecodeString(c.StagingBucket.StorageAccountKey); err != nil {
			return fmt.Errorf("invalid storageAccountKey: must be base64 encoded")
		}

		// Azure storage account name: 3-24 characters, lowercase letters and numbers only
		storageAccountName := c.StagingBucket.StorageAccountName
		if len(storageAccountName) < 3 || len(storageAccountName) > 24 {
			return fmt.Errorf("invalid storageAccountName: must be between 3 and 24 characters")
		}
		if !regexp.MustCompile(`^[a-z0-9]+$`).MatchString(storageAccountName) {
			return fmt.Errorf("invalid storageAccountName: must contain only lowercase letters and numbers")
		}

		// Azure container name: 3-63 characters, lowercase letters/numbers/hyphens,
		// must start and end with letter or number, no consecutive hyphens
		containerName := c.StagingBucket.ContainerName
		if len(containerName) < 3 || len(containerName) > 63 {
			return fmt.Errorf("invalid containerName: must be between 3 and 63 characters")
		}
		if !regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`).MatchString(containerName) {
			return fmt.Errorf("invalid containerName: must start and end with a lowercase letter or number, and contain only lowercase letters, numbers, and hyphens")
		}
		if strings.Contains(containerName, "--") {
			return fmt.Errorf("invalid containerName: must not contain consecutive hyphens")
		}
	case "":
		return fmt.Errorf("missing 'stagingBucketType'")
	default:
		return fmt.Errorf("unknown staging bucket type %q", c.StagingBucket.StagingBucketType)
	}

	return nil
}

func (c config) DefaultNamespace() string {
	return c.Schema
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

type stagingBucketType string

const (
	stagingBucketTypeS3    stagingBucketType = "S3"
	stagingBucketTypeGCS   stagingBucketType = "GCS"
	stagingBucketTypeAzure stagingBucketType = "Azure"
)

type stagingBucketConfig struct {
	StagingBucketType stagingBucketType `json:"stagingBucketType"`

	stagingBucketS3Config
	stagingBucketGCSConfig
	stagingBucketAzureConfig
}

func (stagingBucketConfig) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("Staging Bucket Configuration", "Staging Bucket Configuration", "stagingBucketType", string(stagingBucketTypeS3),
		schemagen.OneOfSubSchema("S3", stagingBucketS3Config{}, string(stagingBucketTypeS3)),
		schemagen.OneOfSubSchema("GCS", stagingBucketGCSConfig{}, string(stagingBucketTypeGCS)),
		schemagen.OneOfSubSchema("Azure", stagingBucketAzureConfig{}, string(stagingBucketTypeAzure)),
	)
}

type stagingBucketS3Config struct {
	BucketS3           string `json:"bucketS3" jsonschema:"title=S3 Staging Bucket,description=Name of the S3 bucket to use for staging data loads. Must not contain dots (.)" jsonschema_extras:"order=0,pattern=^[^.]*$"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for the S3 staging bucket." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for the S3 staging bucket." jsonschema_extras:"secret=true,order=2"`
	Region             string `json:"region" jsonschema:"title=S3 Bucket Region,description=Region of the S3 staging bucket." jsonschema_extras:"order=3"`
	BucketPathS3       string `json:"bucketPathS3,omitempty" jsonschema:"title=Bucket Path,description=An optional prefix that will be used to store objects in the staging bucket." jsonschema_extras:"order=4"`
	Endpoint           string `json:"endpoint,omitempty" jsonschema:"title=Custom Endpoint,description=Custom endpoint for S3-compatible storage (e.g. Cloudflare R2). For R2 use the format: https://<account-id>.r2.cloudflarestorage.com" jsonschema_extras:"order=5"`
}

type stagingBucketGCSConfig struct {
	BucketGCS       string `json:"bucketGCS" jsonschema:"title=GCS Staging Bucket,description=Name of the GCS bucket to use for staging data loads." jsonschema_extras:"order=0"`
	CredentialsJSON string `json:"credentialsJSON" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization to the staging bucket." jsonschema_extras:"secret=true,multiline=true,order=1"`
	GCSHMACAccessID string `json:"gcsHMACAccessID" jsonschema:"title=HMAC Access ID,description=HMAC access ID for the service account." jsonschema_extras:"order=2"`
	GCSHMACSecret   string `json:"gcsHMACSecret" jsonschema:"title=HMAC Secret,description=HMAC secret for the service account." jsonschema_extras:"secret=true,order=3"`
	BucketPathGCS   string `json:"bucketPathGCS,omitempty" jsonschema:"title=Bucket Path,description=An optional prefix that will be used to store objects in the staging bucket." jsonschema_extras:"order=4"`
}

type stagingBucketAzureConfig struct {
	StorageAccountName string `json:"storageAccountName" jsonschema:"title=Storage Account Name,description=Name of the Azure storage account." jsonschema_extras:"order=0"`
	StorageAccountKey  string `json:"storageAccountKey" jsonschema:"title=Storage Account Key,description=Storage account key for authentication." jsonschema_extras:"secret=true,order=1"`
	ContainerName      string `json:"containerName" jsonschema:"title=Container Name,description=Name of the Azure Blob container to use for staging data loads." jsonschema_extras:"order=2"`
	BucketPathAzure    string `json:"bucketPathAzure,omitempty" jsonschema:"title=Bucket Path,description=An optional prefix that will be used to store objects in the staging container." jsonschema_extras:"order=3"`
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
	var checkTempSecret string
	var tempSecretName string
	switch c.StagingBucket.StagingBucketType {
	case stagingBucketTypeS3:
		if c.StagingBucket.Endpoint != "" {
            endpointURL, err := url.Parse(c.StagingBucket.Endpoint)
            if err != nil {
                return nil, err
            }
			
			// Custom endpoint for S3-compatible storage (e.g. Cloudflare R2).
			// Use path-style URLs since most S3-compatible services require this.
			createTempSecret = fmt.Sprintf(`CREATE SECRET IF NOT EXISTS (
				TYPE S3,
				KEY_ID '%s',
				SECRET '%s',
				REGION '%s',
				ENDPOINT '%s',
				URL_STYLE 'path',
				SCOPE 's3://%s'
			);`, c.StagingBucket.AWSAccessKeyID, c.StagingBucket.AWSSecretAccessKey, c.StagingBucket.Region, endpointURL.Host, c.StagingBucket.BucketS3)
		} else {
			createTempSecret = fmt.Sprintf(`CREATE SECRET IF NOT EXISTS (
				TYPE S3,
				KEY_ID '%s',
				SECRET '%s',
				REGION '%s',
				SCOPE 's3://%s'
			);`, c.StagingBucket.AWSAccessKeyID, c.StagingBucket.AWSSecretAccessKey, c.StagingBucket.Region, c.StagingBucket.BucketS3)
		}
		checkTempSecret = fmt.Sprintf(`SELECT name, persistent
			FROM which_secret('s3://%s', 's3'
		);`, c.StagingBucket.BucketS3)
		tempSecretName = "__default_s3"
	case stagingBucketTypeGCS:
		createTempSecret = fmt.Sprintf(`CREATE SECRET IF NOT EXISTS (
			TYPE GCS,
			KEY_ID '%s',
			SECRET '%s',
			SCOPE 'gs://%s'
		);`, c.StagingBucket.GCSHMACAccessID, c.StagingBucket.GCSHMACSecret, c.StagingBucket.BucketGCS)
		checkTempSecret = fmt.Sprintf(`SELECT name, persistent
			FROM which_secret('gs://%s', 'gcs'
		);`, c.StagingBucket.BucketGCS)
		tempSecretName = "__default_gcs"
	case stagingBucketTypeAzure:
		// Build Azure connection string from account name and key
		connectionString := fmt.Sprintf(
			"DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
			c.StagingBucket.StorageAccountName,
			c.StagingBucket.StorageAccountKey,
		)
		createTempSecret = fmt.Sprintf(`CREATE SECRET IF NOT EXISTS (
			TYPE AZURE,
			CONNECTION_STRING '%s',
			SCOPE 'az://%s'
		);`, connectionString, c.StagingBucket.ContainerName)
		checkTempSecret = fmt.Sprintf(`SELECT name, persistent
			FROM which_secret('az://%s', 'azure'
		);`, c.StagingBucket.ContainerName)
		tempSecretName = "__default_azure"
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

	// It is possible to have multiple secrets that match a scope.  If this is
	// the case then the one with the longest prefix will be used with temporary
	// credentials having higher priority than persistent credentials in the
	// case of ties.
	//
	// If Motherduck is unable to COPY to object store, there is no error.
	//
	// Using the wrong permission isn't necessarily a problem, so long as it
	// has the required permissions, but it can be a source of confusion.
	rows, err := db.QueryContext(ctx, checkTempSecret)
	if err != nil {
		return nil, fmt.Errorf("checking selected secret: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var persistent string
		if err := rows.Scan(&name, &persistent); err != nil {
			return nil, fmt.Errorf("scanning selected secret: %w", err)
		}

		// Only one temporary secret can be set per type, so we know if our
		// secret is being used by the name because temporary secrets cannot
		// have custom names.
		if name != tempSecretName || persistent != "TEMPORARY" {
			log.WithField("name", name).Warn("unexpected secret selected")
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
		var s3Opts []func(*s3.Options)
		if c.StagingBucket.Endpoint != "" {
			s3Opts = append(s3Opts, func(o *s3.Options) {
				o.BaseEndpoint = &c.StagingBucket.Endpoint
				o.UsePathStyle = true
				o.Region = c.StagingBucket.Region
			})
		}
		if bucket, err = blob.NewS3Bucket(ctx, c.StagingBucket.BucketS3, creds, s3Opts...); err != nil {
			return nil, "", fmt.Errorf("creating S3 bucket: %w", err)
		}
		path = c.StagingBucket.BucketPathS3
	case stagingBucketTypeGCS:
		auth := option.WithCredentialsJSON([]byte(c.StagingBucket.CredentialsJSON))
		if bucket, err = blob.NewGCSBucket(ctx, c.StagingBucket.BucketGCS, auth); err != nil {
			return nil, "", fmt.Errorf("creating GCS bucket: %w", err)
		}
		path = c.StagingBucket.BucketPathGCS
	case stagingBucketTypeAzure:
		azureBucket, err := blob.NewAzureBlobBucket(ctx,
			c.StagingBucket.ContainerName,
			c.StagingBucket.StorageAccountName,
			blob.WithAzureStorageAccountKey(c.StagingBucket.StorageAccountKey),
		)
		if err != nil {
			return nil, "", fmt.Errorf("creating Azure blob bucket: %w", err)
		}
		bucket = &azureBucketWrapper{
			AzureBlobBucket: azureBucket,
			container:       c.StagingBucket.ContainerName,
		}
		path = c.StagingBucket.BucketPathAzure
	}

	// If BucketPath starts with a /, trim it so we don't end up with repeated /
	// chars in the URI and so the object key does not start with a /.
	path = strings.TrimPrefix(path, "/")

	return bucket, path, nil
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)." jsonschema_extras:"x-schema-name=true"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"title=Delta Update,description=Should updates to this table be done via delta updates." jsonschema_extras:"x-delta-updates=true"`

	database string
}

func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}

	return nil
}

func (r tableConfig) WithDefaults(cfg config) tableConfig {
	if r.Schema == "" {
		r.Schema = cfg.Schema
	}
	r.database = cfg.Database

	return r
}

func (r tableConfig) Parameters() ([]string, bool, error) {
	return []string{r.database, r.Schema, r.Table}, r.Delta, nil
}
