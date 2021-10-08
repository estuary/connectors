package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Uploader is the interface of the object responsible for uploading local files to the cloud.
type Uploader interface {
	Upload(bucket, key, localFileName string, contextType string) error
}

// S3Uploader implements the Uploader interface for uploading files to S3.
type S3Uploader struct {
	uploaderImp *s3manager.Uploader
}

// Upload implements the S3Uploader interface.
func (u *S3Uploader) Upload(bucket, key, localFileName string, contentType string) error {
	f, err := os.Open(localFileName)
	if err != nil {
		return fmt.Errorf("opening local file: %w", err)
	}
	defer f.Close()

	upParams := &s3manager.UploadInput{
		Bucket:      &bucket,
		ContentType: &contentType,
		Key:         &key,
		Body:        f,
	}

	_, err = u.uploaderImp.Upload(upParams)

	return err
}

// NewS3Uploader creates an uploader for s3 given input configs.
func NewS3Uploader(cfg config) (*S3Uploader, error) {
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

	if cfg.Endpoint != "" {
		c = c.WithEndpoint(cfg.Endpoint)
	}

	awsSession, err := session.NewSession(c)
	if err != nil {
		return nil, fmt.Errorf("creating aws session: %w", err)
	}

	return &S3Uploader{
		uploaderImp: s3manager.NewUploaderWithClient(s3.New(awsSession)),
	}, nil
}
