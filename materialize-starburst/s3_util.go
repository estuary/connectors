package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
)

type s3config struct {
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID" jsonschema_extras:"order=0"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key" jsonschema_extras:"secret=true,order=1"`
	Region             string `json:"region" jsonschema:"title=Region" jsonschema_extras:"order=2"`
	Bucket             string `json:"bucket" jsonschema:"title=Bucket" jsonschema_extras:"order=3"`
}

// CloudOperator is the interface of the object responsible for uploading local files to the cloud.
type CloudOperator interface {
	Upload(key, localFileName string, contextType string) error
	Delete(key string) error
}

// S3Operator implements the CloudOperator interface for uploading files to S3.
type S3Operator struct {
	bucket      string
	uploaderImp *s3manager.Uploader
	client      *s3.S3
}

// Upload implements the S3Operator interface.
func (u *S3Operator) Upload(key, localFileName string, contentType string) error {
	f, err := os.Open(localFileName)
	if err != nil {
		return fmt.Errorf("opening local file: %w", err)
	}
	defer f.Close()

	upParams := &s3manager.UploadInput{
		Bucket:      &u.bucket,
		ContentType: &contentType,
		Key:         &key,
		Body:        f,
	}

	_, err = u.uploaderImp.Upload(upParams)

	return err
}

func (u *S3Operator) Delete(prefix string) error {

	input := &s3.ListObjectsInput{Bucket: &u.bucket,
		Prefix: &prefix}
	objects, err := u.client.ListObjects(input)
	if err != nil {
		return err
	}
	for _, obj := range objects.Contents {
		toDelete := &s3.DeleteObjectInput{
			Bucket: &u.bucket,
			Key:    obj.Key,
		}
		_, err = u.client.DeleteObject(toDelete)
		if err != nil {
			return fmt.Errorf("deleting file: %w", err)
		}
	}

	return err
}

// NewS3Operator creates an uploader for s3 given input configs.
func NewS3Operator(cfg s3config) (*S3Operator, error) {
	var c = aws.NewConfig()
	// We use localstack on a docker network for testing this connector, and so we
	// need to use path style addressing of S3
	c.S3ForcePathStyle = aws.Bool(true)

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

	awsSession, err := session.NewSession(c)
	if err != nil {
		return nil, fmt.Errorf("creating aws session: %w", err)
	}

	return &S3Operator{
		bucket:      cfg.Bucket,
		uploaderImp: s3manager.NewUploaderWithClient(s3.New(awsSession)),
		client:      s3.New(awsSession),
	}, nil
}
