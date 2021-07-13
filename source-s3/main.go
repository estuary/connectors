package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/estuary/connectors/go-types/filesource"
	"github.com/estuary/connectors/go-types/parser"
)

type config struct {
	AWSAccessKeyID     string         `json:"awsAccessKeyId"`
	AWSSecretAccessKey string         `json:"awsSecretAccessKey"`
	Bucket             string         `json:"bucket"`
	Endpoint           string         `json:"endpoint"`
	MatchKeys          string         `json:"matchKeys"`
	Parser             *parser.Config `json:"parser"`
	Prefix             string         `json:"prefix"`
	Region             string         `json:"region"`
}

func (c *config) Validate() error {
	if c.Region == "" && c.Endpoint == "" {
		return fmt.Errorf("must supply one of 'region' or 'endpoint'")
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
	return partsToPath(c.Bucket, c.Prefix)
}

func (c *config) FilesAreMonotonic() bool {
	return false
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
	}
	c = c.WithCredentialsChainVerboseErrors(true)

	if cfg.Region != "" {
		c = c.WithRegion(cfg.Region)
	}
	if cfg.Endpoint != "" {
		c = c.WithEndpoint(cfg.Endpoint)
	}

	awsSession, err := session.NewSession(c)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	return &s3Store{s3: s3.New(awsSession)}, nil
}

func (s *s3Store) List(_ context.Context, query filesource.Query) (filesource.Listing, error) {
	var bucket, prefix, err = pathToParts(query.Prefix)
	if err != nil {
		return nil, err
	}

	var input = s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	if !query.Recursive {
		input.Delimiter = aws.String(s3Delimiter)
	}

	if query.StartAt == "" {
		// Pass.
	} else if _, startAt, err := pathToParts(query.StartAt); err != nil {
		return nil, err
	} else {
		// Trim last character to map StartAt into StartAfter.
		input.StartAfter = aws.String(startAt[:len(startAt)-1])
	}

	return &s3Listing{
		s3:    s.s3,
		input: input,
	}, nil
}

func (s *s3Store) Read(_ context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	var bucket, key, err = pathToParts(obj.Path)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	var getInput = s3.GetObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		IfUnmodifiedSince: &obj.ModTime,
	}
	resp, err := s.s3.GetObject(&getInput)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	obj.ContentType = aws.StringValue(resp.ContentType)
	obj.ContentEncoding = aws.StringValue(resp.ContentEncoding)

	return resp.Body, obj, nil
}

type s3Listing struct {
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
				Path:     partsToPath(bucket, aws.StringValue(prefix.Prefix)),
				IsPrefix: true,
			}, nil
		}

		for len(l.output.Contents) != 0 {
			var obj = l.output.Contents[0]
			l.output.Contents = l.output.Contents[1:]

			// returns true if the object is one that may be readable. This function filters out any objects
			// with a "glacier" storage class because those objects could take minutes or even hours to
			// retrieve. This behavior matches that of other popular tool that ingest from S3.
			if sc := aws.StringValue(obj.StorageClass); sc == s3.StorageClassGlacier || sc == s3.StorageClassDeepArchive {
				continue
			}

			return filesource.ObjectInfo{
				Path:       partsToPath(bucket, aws.StringValue(obj.Key)),
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

	if out, err := l.s3.ListObjectsV2(&input); err != nil {
		return err
	} else {
		l.output = *out
	}

	return nil
}

// Parses a path into its constituent bucket and prefix. The prefix may be empty.
func pathToParts(path string) (bucket, key string, _ error) {
	if ind := strings.IndexByte(path, ':'); ind == -1 {
		return "", "", fmt.Errorf("%q is missing `bucket:` prefix", path)
	} else {
		return path[:ind], path[ind+1:], nil
	}
}

// Forms a path from a bucket and key.
func partsToPath(bucket, key string) string {
	return bucket + ":" + key
}

func main() {

	var src = filesource.Source{
		NewConfig: func() filesource.Config { return new(config) },
		Connect: func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
			return newS3Store(ctx, cfg.(*config))
		},
		ConfigSchema: func(parserSchema json.RawMessage) json.RawMessage {
			return json.RawMessage(fmt.Sprintf(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "S3 Source Spec",
		"type":    "object",
		"required": [
			"awsAccessKeyId",
			"awsSecretAccessKey"
		],
		"properties": {
			"bucket": {
				"type":        "string",
				"title":       "Bucket",
				"description": "Name of the S3 bucket"
			},
			"prefix": {
				"type":        "string",
				"title":       "Prefix",
				"description": "Prefix within the bucket to capture from."
			},
			"matchKeys": {
				"type":        "string",
				"title":       "Match Keys",
				"format":      "regex",
				"description": "Filter applied to all object keys under the prefix. If provided, only objects whose key (relative to the prefix) matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files."
			},
			"parser": %s,
			"region": {
				"type":        "string",
				"title":       "AWS Region",
				"description": "The name of the AWS region where the S3 stream is located",
				"default":     "us-east-1"
			},
			"endpoint": {
				"type":        "string",
				"title":       "AWS Endpoint",
				"description": "The AWS endpoint URI to connect to, useful if you're capturing from a S3-compatible API that isn't provided by AWS"
			},
			"awsAccessKeyId": {
				"type":        "string",
				"title":       "AWS Access Key ID",
				"description": "Part of the AWS credentials that will be used to connect to S3",
				"default":     "example-aws-access-key-id"
			},
			"awsSecretAccessKey": {
				"type":        "string",
				"title":       "AWS Secret Access Key",
				"description": "Part of the AWS credentials that will be used to connect to S3",
				"default":     "example-aws-secret-access-key"
			}
		}
    }`, string(parserSchema)))
		},
	}

	src.Main()
}

const s3Delimiter = "/"
