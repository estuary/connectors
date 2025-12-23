package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/estuary/connectors/filesource"
	"github.com/estuary/connectors/go/auth/iam"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

type AuthType string

const (
	AWSAccessKey AuthType = "AWSAccessKey"
	AWSAnonymous AuthType = "AWSAnonymous"
	AWSIAM       AuthType = "AWSIAM"
)

type CredentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	AccessKeyCredentials
	AnonymousCredentials
	iam.IAMConfig
}

func (CredentialsConfig) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("Access Key", AccessKeyCredentials{}, string(AWSAccessKey)),
	}
	subSchemas = append(subSchemas,
		schemagen.OneOfSubSchema("Anonymous", AnonymousCredentials{}, string(AWSAnonymous)))
	subSchemas = append(subSchemas,
		schemagen.OneOfSubSchema("AWS IAM", iam.AWSConfig{}, string(AWSIAM)))

	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(AWSAccessKey), subSchemas...)
}

func (c *CredentialsConfig) Validate() error {
	switch c.AuthType {
	case AWSAccessKey:
		if c.AWSAccessKeyID == "" {
			return errors.New("missing 'aws_access_key_id'")
		}
		if c.AWSSecretAccessKey == "" {
			return errors.New("missing 'aws_secret_access_key'")
		}
		return nil
	case AWSAnonymous:
		return nil
	case AWSIAM:
		if err := c.ValidateIAM(); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("unknown 'auth_type'")
}

// Since the AccessKeyCredentials and IAMConfig have conflicting JSON field names, only parse
// the embedded struct of interest.
func (c *CredentialsConfig) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		AuthType AuthType `json:"auth_type"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	c.AuthType = discriminator.AuthType

	switch c.AuthType {
	case AWSAccessKey:
		return json.Unmarshal(data, &c.AccessKeyCredentials)
	case AWSAnonymous:
		return nil
	case AWSIAM:
		return json.Unmarshal(data, &c.IAMConfig)
	}
	return fmt.Errorf("unexpected auth_type: %s", c.AuthType)
}

type AccessKeyCredentials struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=Part of the AWS credentials that will be used to connect to S3." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=Part of the AWS credentials that will be used to connect to S3." jsonschema_extras:"secret=true,order=2"`
}

type AnonymousCredentials struct{}

type config struct {
	AWSAccessKeyID     string             `json:"awsAccessKeyId,omitempty" jsonschema:"-"`
	AWSSecretAccessKey string             `json:"awsSecretAccessKey,omitempty" jsonschema:"-"`
	Region             string             `json:"region"`
	Bucket             string             `json:"bucket"`
	Prefix             string             `json:"prefix"`
	MatchKeys          string             `json:"matchKeys"`
	Credentials        *CredentialsConfig `json:"credentials"`
	Parser             *parser.Config     `json:"parser"`
	Advanced           advancedConfig     `json:"advanced"`
}

type advancedConfig struct {
	AscendingKeys bool   `json:"ascendingKeys" jsonschema="title=Ascending Keys,description=Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. For more information see https://go.estuary.dev/xKswdo."`
	Endpoint      string `json:"endpoint" jsonschema="title=AWS Endpoint,description=The AWS endpoint URI to connect to. Use if you're capturing from a S3-compatible API that isn't provided by AWS`
}

func (c config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("missing region")
	}

	if c.Credentials != nil {
		if err := c.Credentials.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (c config) DiscoverRoot() string {
	return filesource.PartsToPath(c.Bucket, c.Prefix)
}

func (c config) RecommendedName() string {
	return strings.Trim(c.DiscoverRoot(), "/")
}

func (c config) FilesAreMonotonic() bool {
	return c.Advanced.AscendingKeys
}

func (c config) ParserConfig() *parser.Config {
	return c.Parser
}

func (c config) PathRegex() string {
	return c.MatchKeys
}

type s3Store struct {
	s3 *s3.Client
}

func (c config) CredentialsProvider(ctx context.Context) (aws.CredentialsProvider, error) {
	if c.Credentials == nil {
		if c.AWSSecretAccessKey != "" {
			return credentials.NewStaticCredentialsProvider(
				c.AWSAccessKeyID, c.AWSSecretAccessKey, ""), nil
		} else {
			return aws.AnonymousCredentials{}, nil
		}
	}

	switch c.Credentials.AuthType {
	case AWSAccessKey:
		return credentials.NewStaticCredentialsProvider(
			c.Credentials.AWSAccessKeyID, c.Credentials.AWSSecretAccessKey, ""), nil
	case AWSAnonymous:
		return aws.AnonymousCredentials{}, nil
	case AWSIAM:
		return c.Credentials.IAMTokens.AWSCredentialsProvider()
	}
	return nil, errors.New("unknown 'auth_type'")
}

func newS3Store(ctx context.Context, cfg config) (*s3Store, error) {
	credProvider, err := cfg.CredentialsProvider(ctx)
	if err != nil {
		return nil, err
	}

	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(credProvider),
	}

	if cfg.Region != "" {
		opts = append(opts, awsConfig.WithRegion(cfg.Region))
	}

	if cfg.Advanced.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: cfg.Advanced.Endpoint,
				HostnameImmutable: true,
				Source:            aws.EndpointSourceCustom,
			}, nil
		})
		opts = append(opts, awsConfig.WithEndpointResolverWithOptions(customResolver))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg)

	if err := validateBucket(ctx, cfg, s3Client); err != nil {
		return nil, err
	}

	return &s3Store{s3: s3Client}, nil
}

// validateBucket verifies that we can list objects in the bucket and potentially read an object in
// the bucket. This is done in a way that requires only s3:ListBucket and s3:GetObject permissions,
// since these are the permissions required by the connector.
func validateBucket(ctx context.Context, cfg config, s3Client *s3.Client) error {
	// All we care about is a successful listing rather than iterating on all objects, so MaxKeys =
	// 1 in this query.
	_, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(cfg.Bucket),
		Prefix:  aws.String(cfg.Prefix),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		// Note that a listing with zero objects does not return an error here, so we don't have to
		// account for that case.
		var noSuchBucket *s3Types.NoSuchBucket
		var apiErr smithy.APIError
		switch {
		case errors.As(err, &noSuchBucket):
			return cerrors.NewUserError(err, fmt.Sprintf("bucket %q does not exist", cfg.Bucket))
		case errors.As(err, &apiErr):
			switch apiErr.ErrorCode() {
			case "AccessDenied":
				return cerrors.NewUserError(err, fmt.Sprintf("bucket %q does not exist", cfg.Bucket))
			case "InvalidAccessKeyId":
				return cerrors.NewUserError(err, "configured AWS Access Key ID does not exist")
			case "SignatureDoesNotMatch":
				return cerrors.NewUserError(err, "configured AWS Secret Access Key is not valid")
			}
		}

		return fmt.Errorf("unable to list objects in bucket %q: %w", cfg.Bucket, err)
	}

	// s3:GetObject allows reading object data as well as object metadata. The name of the object is
	// not important here. We have verified our ability to list objects in this bucket (bucket
	// exists & correct access) via the list operation, so we can interpret a "not found" as a
	// successful outcome.
	objectKey := strings.TrimPrefix(path.Join(cfg.Prefix, uuid.NewString()), "/")
	if _, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &cfg.Bucket, Key: aws.String(objectKey)}); err != nil {
		var notFoundErr *s3Types.NotFound
		if !errors.As(err, &notFoundErr) {
			return fmt.Errorf("unable to read objects in bucket %q: %w", cfg.Bucket, err)
		}
	}

	return nil
}

func (s *s3Store) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	var bucket, prefix = filesource.PathToParts(query.Prefix)

	var input = s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	if !query.Recursive {
		input.Delimiter = aws.String(s3Delimiter)
	}

	if query.StartAt == "" {
		// Pass.
	} else {
		// Trim last character to map StartAt into StartAfter.
		_, startAt := filesource.PathToParts(query.StartAt)
		input.StartAfter = aws.String(startAt[:len(startAt)-1])
	}

	return &s3Listing{
		ctx:   ctx,
		s3:    s.s3,
		input: input,
	}, nil
}

func (s *s3Store) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	var bucket, key = filesource.PathToParts(obj.Path)

	resp, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		IfUnmodifiedSince: aws.Time(obj.ModTime),
	})
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	obj.ContentType = aws.ToString(resp.ContentType)
	obj.ContentEncoding = aws.ToString(resp.ContentEncoding)

	return resp.Body, obj, nil
}

type s3Listing struct {
	ctx    context.Context
	s3     *s3.Client
	input  s3.ListObjectsV2Input
	output s3.ListObjectsV2Output
}

func (l *s3Listing) Next() (filesource.ObjectInfo, error) {
	var bucket = aws.ToString(l.input.Bucket)

	for {
		if len(l.output.CommonPrefixes) != 0 {
			var prefix = l.output.CommonPrefixes[0]
			l.output.CommonPrefixes = l.output.CommonPrefixes[1:]

			return filesource.ObjectInfo{
				Path:     filesource.PartsToPath(bucket, aws.ToString(prefix.Prefix)),
				IsPrefix: true,
			}, nil
		}

		for len(l.output.Contents) != 0 {
			var obj = l.output.Contents[0]
			l.output.Contents = l.output.Contents[1:]

			// Filter out any objects with a "glacier" storage class because those objects could
			// take minutes or even hours to retrieve. This behavior matches that of other popular
			// tool that ingest from S3.
			if obj.StorageClass == s3Types.ObjectStorageClassGlacier || obj.StorageClass == s3Types.ObjectStorageClassDeepArchive {
				continue
			}

			return filesource.ObjectInfo{
				Path:       filesource.PartsToPath(bucket, aws.ToString(obj.Key)),
				ContentSum: aws.ToString(obj.ETag),
				Size:       aws.ToInt64(obj.Size),
				ModTime:    aws.ToTime(obj.LastModified),
			}, nil
		}

		if err := l.poll(); err != nil {
			return filesource.ObjectInfo{}, err
		}
	}
}

func (l *s3Listing) poll() error {
	var input = l.input

	if l.output.Prefix == nil {
		// Very first query.
	} else if !aws.ToBool(l.output.IsTruncated) {
		return io.EOF
	} else {
		input.StartAfter = nil
		input.ContinuationToken = l.output.NextContinuationToken
	}

	if out, err := l.s3.ListObjectsV2(l.ctx, &input); err != nil {
		return err
	} else {
		l.output = *out
	}

	return nil
}

var src = filesource.Source{
	NewConfig: func(raw json.RawMessage) (filesource.Config, error) {
		var cfg config
		if err := pf.UnmarshalStrict(raw, &cfg); err != nil {
			return nil, fmt.Errorf("parsing config json: %w", err)
		}
		return cfg, nil
	},
	Connect: func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
		return newS3Store(ctx, cfg.(config))
	},
	ConfigSchema: func(parserSchema json.RawMessage) json.RawMessage {
		authSchema := schemagen.GenerateSchema("Authentication", &CredentialsConfig{})
		authSchema.Extras["order"] = 4
		authSchema.Extras["x-iam-auth"] = true

		var parserOrderSchema map[string]any
		err := json.Unmarshal(parserSchema, &parserOrderSchema)
		if err != nil {
			log.Errorf("unable to parse parser schema: %v", err)
		}
		parserOrderSchema["order"] = 5

		schema := map[string]any{
			"$schema": "https://json-schema.org/draft/2020-12/schema",
			"$id":     "https://github.com/estuary/connectors/source-s3/config",
			"title":   "S3 Source",
			"type":    "object",
			"required": []string{
				"bucket",
				"region",
				"credentials",
			},
			"properties": map[string]any{
				"region": map[string]any{
					"type":        "string",
					"title":       "AWS Region",
					"description": "The name of the AWS region where the S3 bucket is located.",
					"order":       0,
				},
				"bucket": map[string]any{
					"type":        "string",
					"title":       "Bucket",
					"description": "Name of the S3 bucket",
					"order":       1,
				},
				"prefix": map[string]any{
					"type":        "string",
					"title":       "Prefix",
					"description": "Prefix within the bucket to capture from.",
					"order":       2,
				},
				"matchKeys": map[string]any{
					"type":        "string",
					"title":       "Match Keys",
					"format":      "regex",
					"description": "Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files.",
					"order":       3,
				},
				"credentials": authSchema,
				"parser":      parserOrderSchema,
				"advanced": map[string]any{
					"properties": map[string]any{
						"ascendingKeys": map[string]any{
							"type":        "boolean",
							"title":       "Ascending Keys",
							"description": "Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. For more information see https://go.estuary.dev/xKswdo.",
							"default":     false,
						},
						"endpoint": map[string]any{
							"type":        "string",
							"title":       "AWS Endpoint",
							"description": "The AWS endpoint URI to connect to. Use if you're capturing from a S3-compatible API that isn't provided by AWS",
						},
					},
					"additionalProperties": false,
					"type":                 "object",
					"title":                "Advanced",
					"description":          "Options for advanced users. You should not typically need to modify these.",
					"order":                6,
					"advanced":             true,
				},
			},
		}

		schemaBytes, err := json.Marshal(schema)
		if err != nil {
			log.Errorf("generating schema: %v", err)
		}

		return json.RawMessage(schemaBytes)
	},
	DocumentationURL: "https://go.estuary.dev/source-s3",
	// Set the delta to 30 seconds in the past, to guard against new files appearing with a
	// timestamp that's equal to the `MinBound` in the state.
	TimeHorizonDelta: time.Second * -30,
}

func main() {
	src.Main()
}

const s3Delimiter = "/"
