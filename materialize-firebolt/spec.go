package main

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	pf "github.com/estuary/flow/go/protocols/flow"
	proto "github.com/gogo/protobuf/proto"
)

// Load existing spec from S3 and create a map of it by table name
func LoadSpec(cfg config, materialization string) (bool, map[string]*pf.MaterializationSpec_Binding, error) {
	awsConfig := aws.Config{
		Credentials: credentials.NewStaticCredentials(cfg.AWSKeyId, cfg.AWSSecretKey, ""),
		Region:      &cfg.AWSRegion,
	}
	sess := session.Must(session.NewSession(&awsConfig))
	downloader := s3manager.NewDownloader(sess)
	buf := aws.NewWriteAtBuffer([]byte{})
	existingSpecKey := fmt.Sprintf("%s%s.flow.materialization_spec", cfg.S3Prefix, materialization)

	var existing pf.MaterializationSpec
	haveExisting := false

	_, err := downloader.Download(buf, &s3.GetObjectInput{
		Bucket: &cfg.S3Bucket,
		Key:    &existingSpecKey,
	})

	if err != nil {
		// The file not existing means this is the first validate, otherwise it's an actual error
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() != s3.ErrCodeNoSuchKey {
			return false, nil, fmt.Errorf("downloading existing spec failed: %w", err)
		}
	} else {
		err = proto.Unmarshal(buf.Bytes(), &existing)
		if err != nil {
			return false, nil, fmt.Errorf("parsing existing materialization spec: %w", err)
		}
		haveExisting = true
	}

	if !haveExisting {
		return false, nil, nil
	}

	var bindingsByTable map[string]*pf.MaterializationSpec_Binding

	for _, binding := range existing.Bindings {
		var r resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &r); err != nil {
			return false, nil, fmt.Errorf("parsing resource config: %w", err)
		}
		bindingsByTable[r.Table] = binding
	}

	return true, bindingsByTable, nil
}

func WriteSpec(cfg config, materialization *pf.MaterializationSpec) error {
	awsConfig := aws.Config{
		Credentials: credentials.NewStaticCredentials(cfg.AWSKeyId, cfg.AWSSecretKey, ""),
		Region:      &cfg.AWSRegion,
	}
	sess := session.Must(session.NewSession(&awsConfig))
	uploader := s3manager.NewUploader(sess)
	materializationBytes, err := proto.Marshal(materialization)
	if err != nil {
		return fmt.Errorf("marshalling materialization spec: %w", err)
	}
	// TODO: Do we need the version here at all? Is it useful?
	specKey := fmt.Sprintf("%s%s.flow.materialization_spec", cfg.S3Prefix, materialization.Materialization)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: &cfg.S3Bucket,
		Key:    &specKey,
		Body:   bytes.NewReader(materializationBytes),
	})

	if err != nil {
		return fmt.Errorf("uploading materialization spec %s: %w", specKey, err)
	}

	return nil
}

func CleanSpec(cfg config, materialization string) error {
	awsConfig := aws.Config{
		Credentials: credentials.NewStaticCredentials(cfg.AWSKeyId, cfg.AWSSecretKey, ""),
		Region:      &cfg.AWSRegion,
	}
	sess := session.Must(session.NewSession(&awsConfig))
	svc := s3.New(sess)
	specKey := fmt.Sprintf("%s%s.flow.materialization_spec", cfg.S3Prefix, materialization)
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &cfg.S3Bucket,
		Key:    &specKey,
	})

	if err != nil {
		return fmt.Errorf("deleting materialization spec %s: %w", specKey, err)
	}

	return nil
}
