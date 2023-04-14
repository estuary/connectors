package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	"github.com/google/uuid"
)

type config struct {
	AWSAccessKeyID     string         `json:"awsAccessKeyId"`
	AWSSecretAccessKey string         `json:"awsSecretAccessKey"`
	Bucket             string         `json:"bucket"`
	MatchKeys          string         `json:"matchKeys"`
	Parser             *parser.Config `json:"parser"`
	Prefix             string         `json:"prefix"`
	Region             string         `json:"region"`
	Advanced           advancedConfig `json:"advanced"`
}

type advancedConfig struct {
	AscendingKeys bool   `json:"ascendingKeys"`
	Endpoint      string `json:"endpoint"`
}

func (c *config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("missing region")
	}
	if c.AWSAccessKeyID == "" && c.AWSSecretAccessKey != "" {
		return fmt.Errorf("missing awsAccessKeyID")
	}
	if c.AWSAccessKeyID != "" && c.AWSSecretAccessKey == "" {
		return fmt.Errorf("missing awsSecretAccessKey")
	}
	return nil
}

func (c *config) DiscoverRoot() string {
	return filesource.PartsToPath(c.Bucket, c.Prefix)
}

func (c *config) FilesAreMonotonic() bool {
	return c.Advanced.AscendingKeys
}

func (c *config) ParserConfig() *parser.Config {
	return c.Parser
}

func (c *config) PathRegex() string {
	return c.MatchKeys
}

type s3Store struct {
	s3 *s3.S3
}

func newS3Store(ctx context.Context, cfg *config) (*s3Store, error) {
	var c = aws.NewConfig()

	if cfg.AWSSecretAccessKey != "" {
		var creds = credentials.NewStaticCredentials(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, "")
		c = c.WithCredentials(creds)
	} else {
		c = c.WithCredentials(credentials.AnonymousCredentials)
	}
	c = c.WithCredentialsChainVerboseErrors(true)

	if cfg.Region != "" {
		c = c.WithRegion(cfg.Region)
	}
	if cfg.Advanced.Endpoint != "" {
		c = c.WithEndpoint(cfg.Advanced.Endpoint).WithS3ForcePathStyle(true)
	}

	awsSession, err := session.NewSession(c)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	s3Sess := s3.New(awsSession)
	if err := validateBucket(ctx, cfg, s3Sess); err != nil {
		return nil, err
	}

	return &s3Store{s3: s3Sess}, nil
}

// validateBucket verifies that we can list objects in the bucket and potentially read an object in
// the bucket. This is done in a way that requires only s3:ListBucket and s3:GetObject permissions,
// since these are the permissions required by the connector.
func validateBucket(ctx context.Context, cfg *config, s3Sess *s3.S3) error {
	// All we care about is a succesful listing rather than iterating on all objects, so MaxKeys = 1
	// in this query.
	_, err := s3Sess.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(cfg.Bucket),
		Prefix:  aws.String(cfg.Prefix),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		// Note that a listing with zero objects does not return an error here, so we don't have to
		// account for that case.
		return fmt.Errorf("unable to list objects in bucket %q: %w", cfg.Bucket, err)
	}

	// s3:GetObject allows reading object data as well as object metadata. The name of the object is
	// not important here. We have verified our ability to list objects in this bucket (bucket
	// exists & correct access) via the list operation, so we can interpret a "not found" as a
	// successful outcome.
	objectKey := strings.TrimPrefix(path.Join(cfg.Prefix, uuid.NewString()), "/")
	if _, err := s3Sess.HeadObjectWithContext(ctx, &s3.HeadObjectInput{Bucket: &cfg.Bucket, Key: aws.String(objectKey)}); err != nil {
		if e, ok := err.(awserr.RequestFailure); ok && e.StatusCode() != http.StatusNotFound {
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

	var getInput = s3.GetObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		IfUnmodifiedSince: &obj.ModTime,
	}
	resp, err := s.s3.GetObjectWithContext(ctx, &getInput)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	obj.ContentType = aws.StringValue(resp.ContentType)
	obj.ContentEncoding = aws.StringValue(resp.ContentEncoding)

	return resp.Body, obj, nil
}

type s3Listing struct {
	ctx    context.Context
	s3     *s3.S3
	input  s3.ListObjectsV2Input
	output s3.ListObjectsV2Output
}

func (l *s3Listing) Next() (filesource.ObjectInfo, error) {
	var bucket = aws.StringValue(l.input.Bucket)

	for {
		if len(l.output.CommonPrefixes) != 0 {
			var prefix = l.output.CommonPrefixes[0]
			l.output.CommonPrefixes = l.output.CommonPrefixes[1:]

			return filesource.ObjectInfo{
				Path:     filesource.PartsToPath(bucket, aws.StringValue(prefix.Prefix)),
				IsPrefix: true,
			}, nil
		}

		for len(l.output.Contents) != 0 {
			var obj = l.output.Contents[0]
			l.output.Contents = l.output.Contents[1:]

			// Filter out any objects with a "glacier" storage class because those objects could
			// take minutes or even hours to retrieve. This behavior matches that of other popular
			// tool that ingest from S3.
			if sc := aws.StringValue(obj.StorageClass); sc == s3.StorageClassGlacier || sc == s3.StorageClassDeepArchive {
				continue
			}

			return filesource.ObjectInfo{
				Path:       filesource.PartsToPath(bucket, aws.StringValue(obj.Key)),
				ContentSum: aws.StringValue(obj.ETag),
				Size:       aws.Int64Value(obj.Size),
				ModTime:    aws.TimeValue(obj.LastModified),
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
	} else if !aws.BoolValue(l.output.IsTruncated) {
		return io.EOF
	} else {
		input.StartAfter = nil
		input.ContinuationToken = l.output.NextContinuationToken
	}

	if out, err := l.s3.ListObjectsV2WithContext(l.ctx, &input); err != nil {
		return err
	} else {
		l.output = *out
	}

	return nil
}

func main() {

	var src = filesource.Source{
		NewConfig: func() filesource.Config { return new(config) },
		Connect: func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
			return newS3Store(ctx, cfg.(*config))
		},
		ConfigSchema: func(parserSchema json.RawMessage) json.RawMessage {
			return json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "S3 Source",
		"type":    "object",
		"required": [
			"bucket",
			"region"
		],
		"properties": {
			"awsAccessKeyId": {
				"type":        "string",
				"title":       "AWS Access Key ID",
				"description": "Part of the AWS credentials that will be used to connect to S3. Required unless the bucket is public and allows anonymous listings and reads.",
				"order": 0
			},
			"awsSecretAccessKey": {
				"type":        "string",
				"title":       "AWS Secret Access Key",
				"description": "Part of the AWS credentials that will be used to connect to S3. Required unless the bucket is public and allows anonymous listings and reads.",
				"secret":      true,
				"order":       1
			},
			"region": {
				"type":        "string",
				"title":       "AWS Region",
				"description": "The name of the AWS region where the S3 bucket is located.",
				"order":       2
			},
			"bucket": {
				"type":        "string",
				"title":       "Bucket",
				"description": "Name of the S3 bucket",
				"order":       3
			},
			"prefix": {
				"type":        "string",
				"title":       "Prefix",
				"description": "Prefix within the bucket to capture from.",
				"order":       4
			},
			"matchKeys": {
				"type":        "string",
				"title":       "Match Keys",
				"format":      "regex",
				"description": "Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files.",
				"order":       5
			},
			"advanced": {
				"properties": {
				  "ascendingKeys": {
					"type":        "boolean",
					"title":       "Ascending Keys",
					"description": "Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. For more information see https://go.estuary.dev/xKswdo.",
					"default":     false
				  },
				  "endpoint": {
					"type":        "string",
					"title":       "AWS Endpoint",
					"description": "The AWS endpoint URI to connect to. Use if you're capturing from a S3-compatible API that isn't provided by AWS"
				  }
				},
				"additionalProperties": false,
				"type": "object",
				"description": "Options for advanced users. You should not typically need to modify these.",
				"advanced": true
			},
			"parser": ` + string(parserSchema) + `
		}
    }`)
		},
		DocumentationURL: "https://go.estuary.dev/source-s3",
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
	}

	src.Main()
}

const s3Delimiter = "/"
