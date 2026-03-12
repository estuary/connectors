package filesink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsHttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/estuary/connectors/go/auth/iam"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/invopop/jsonschema"
)

const (
	PartSize int = 15 * 1024 * 1024 // 15 MiB
)

type AuthType string

const (
	AWSAccessKey AuthType = "AWSAccessKey"
	AWSIAM       AuthType = "AWSIAM"
)

type AccessKeyCredentials struct {
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=AWS Access Key ID,description=Access Key ID for writing data to the bucket." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=AWS Secret Access key,description=Secret Access Key for writing data to the bucket." jsonschema_extras:"secret=true,order=2"`
}

type CredentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	AccessKeyCredentials
	iam.IAMConfig
}

func (CredentialsConfig) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("Access Key", AccessKeyCredentials{}, string(AWSAccessKey)),
	}
	subSchemas = append(subSchemas,
		schemagen.OneOfSubSchema("AWS IAM", iam.AWSConfig{}, string(AWSIAM)))

	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(AWSAccessKey), subSchemas...)
}

func (c *CredentialsConfig) Validate() error {
	switch c.AuthType {
	case AWSAccessKey:
		if c.AWSAccessKeyID == "" {
			return errors.New("missing 'awsAccessKeyId'")
		}
		if c.AWSSecretAccessKey == "" {
			return errors.New("missing 'awsSecretAccessKey'")
		}
		return nil
	case AWSIAM:
		if err := c.ValidateIAM(); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("unknown 'auth_type'")
}

type S3StoreConfig struct {
	Bucket             string `json:"bucket" jsonschema:"title=Bucket,description=Bucket to store materialized objects." jsonschema_extras:"order=0"`
	AWSAccessKeyID     string `json:"awsAccessKeyId,omitempty" jsonschema:"-"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey,omitempty" jsonschema:"-"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region of the bucket to write to." jsonschema_extras:"order=3"`

	Credentials *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=4"`

	UploadInterval string `json:"uploadInterval" jsonschema:"title=Upload Interval,description=Frequency at which files will be uploaded. Must be a valid Go duration string.,enum=1s,enum=5m,enum=15m,enum=30m,enum=1h,default=5m" jsonschema_extras:"order=5"`
	Prefix         string `json:"prefix,omitempty" jsonschema:"title=Prefix,description=Optional prefix that will be used to store objects. May contain date patterns." jsonschema_extras:"order=6"`
	FileSizeLimit  int    `json:"fileSizeLimit,omitempty" jsonschema:"title=File Size Limit,description=Approximate maximum size of materialized files in bytes. Defaults to 10737418240 (10 GiB) if blank." jsonschema_extras:"order=7"`

	Endpoint     string `json:"endpoint,omitempty" jsonschema:"title=Custom S3 Endpoint,description=The S3 endpoint URI to connect to. Use if you're materializing to a compatible API that isn't provided by AWS. Should normally be left blank." jsonschema_extras:"order=8"`
	UsePathStyle bool   `json:"use_path_style,omitempty" jsonschema:"-"`
}

func (c S3StoreConfig) Validate() error {
	var requiredProperties = [][]string{
		{"bucket", c.Bucket},
		{"region", c.Region},
		{"uploadInterval", c.UploadInterval},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Credentials == nil {
		if c.AWSAccessKeyID == "" {
			return errors.New("missing 'awsAccessKeyId'")
		}
		if c.AWSSecretAccessKey == "" {
			return errors.New("missing 'awsSecretAccessKey'")
		}
	} else if err := c.Credentials.Validate(); err != nil {
		return err
	}

	if _, err := time.ParseDuration(c.UploadInterval); err != nil {
		return fmt.Errorf("parsing upload interval %q: %w", c.UploadInterval, err)
	} else if c.Prefix != "" {
		if strings.HasPrefix(c.Prefix, "/") {
			return fmt.Errorf("prefix %q cannot start with /", c.Prefix)
		}
	} else if c.FileSizeLimit < 0 {
		return fmt.Errorf("fileSizeLimit '%d' cannot be negative", c.FileSizeLimit)
	}

	return nil
}

func (c S3StoreConfig) CredentialsProvider(ctx context.Context) (aws.CredentialsProvider, error) {
	if c.Credentials == nil {
		return credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""), nil
	}

	switch c.Credentials.AuthType {
	case AWSAccessKey:
		return credentials.NewStaticCredentialsProvider(
			c.Credentials.AWSAccessKeyID, c.Credentials.AWSSecretAccessKey, ""), nil
	case AWSIAM:
		return c.Credentials.IAMTokens.AWSCredentialsProvider()
	}
	return nil, errors.New("unknown 'auth_type'")
}

type S3Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func NewS3Store(ctx context.Context, cfg S3StoreConfig) (*S3Store, error) {
	credProvider, err := cfg.CredentialsProvider(ctx)
	if err != nil {
		return nil, err
	}

	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(credProvider),
		awsConfig.WithRegion(cfg.Region),
		awsConfig.WithHTTPClient(awsHttp.NewBuildableClient().WithTimeout(uploadTimeout)),
	}

	if cfg.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: cfg.Endpoint}, nil
		})

		opts = append(opts, awsConfig.WithEndpointResolverWithOptions(customResolver))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.UsePathStyle
	})

	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.Concurrency = 1
		u.PartSize = manager.MinUploadPartSize
	})

	return &S3Store{
		client:   s3Client,
		uploader: uploader,
		bucket:   cfg.Bucket,
	}, nil
}

func (s *S3Store) PutStream(ctx context.Context, r io.Reader, key string) error {
	uploadCtx, cancel := context.WithTimeout(ctx, uploadTimeout)
	defer cancel()

	_, err := s.uploader.Upload(uploadCtx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	return err
}

func (s *S3Store) Client() *s3.Client {
	return s.client
}

type Part struct {
	PartNumber int32
	ETag       string
}

// This is also basically the info needed to complete.
type S3MultipartUpload struct {
	Key      string
	Parts    []Part
	UploadID string
}

func (u *S3MultipartUpload) FileKey() string {
	return u.Key
}

func (s *S3Store) SupportsPathPatternExpansion() bool {
	return true
}

// StageObject writes a file as a multipart upload but does not complete the
// upload.  This prevents it from being visible to normal operations until it
// is completed.
func (s *S3Store) StageObject(ctx context.Context, r io.Reader, key string) (*S3MultipartUpload, error) {
	createResp, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	var uploadID = *createResp.UploadId
	var parts []Part
	var buf = make([]byte, PartSize)

	// Each part in a multipart upload must have a Content-Length, unlike with
	// PutObject which can use Transfer-Encoding: chunked.  To avoid using too
	// much memory we write multiple parts.
	for partNumber := int32(1); ; partNumber++ {
		n, ioErr := io.ReadFull(r, buf)
		if ioErr != nil && ioErr != io.ErrUnexpectedEOF {
			return nil, ioErr
		}

		uploadPartResp, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(s.bucket),
			Key:        aws.String(key),
			Body:       bytes.NewReader(buf[:n]),
			PartNumber: aws.Int32(partNumber),
			UploadId:   &uploadID,
		})
		if err != nil {
			return nil, err
		}

		parts = append(parts, Part{
			ETag:       *uploadPartResp.ETag,
			PartNumber: partNumber,
		})

		if ioErr != nil {
			break
		}
	}

	info := S3MultipartUpload{
		Key:      key,
		UploadID: uploadID,
		Parts:    parts,
	}
	return &info, nil
}

// CompleteObject completes a multipart upload making it visible to normal
// operations.
func (s *S3Store) CompleteObject(ctx context.Context, info *S3MultipartUpload) error {
	var parts []types.CompletedPart
	for _, part := range info.Parts {
		completed := types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(part.PartNumber),
		}
		parts = append(parts, completed)
	}

	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(info.Key),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
		UploadId: &info.UploadID,
	})
	if err != nil {
		return err
	}

	return nil
}
