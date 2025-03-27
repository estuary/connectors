package connector

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
)

const fileSizeLimit = 250 * 1024 * 1024

type stagedFileClient struct {
	bucket   string
	s3Client *s3.Client
	uploader *manager.Uploader
}

func newFileClient(s3Client *s3.Client, bucket string) *stagedFileClient {
	return &stagedFileClient{
		bucket:   bucket,
		s3Client: s3Client,
		uploader: manager.NewUploader(s3Client, func(u *manager.Uploader) {
			u.Concurrency = 1
			u.PartSize = manager.MinUploadPartSize
		}),
	}
}

func (f *stagedFileClient) NewEncoder(w io.WriteCloser, fields []string) boilerplate.Encoder {
	return enc.NewCsvEncoder(w, fields, enc.WithCsvSkipHeaders(), enc.WithCsvQuoteChar('`'))
}

func (f *stagedFileClient) NewKey(keyParts []string) string {
	return path.Join(keyParts...) + ".csv.gz"
}

func (f *stagedFileClient) URI(key string) string {
	return "s3://" + path.Join(f.bucket, key)
}

func (f *stagedFileClient) UploadStream(ctx context.Context, key string, r io.Reader) error {
	if _, err := f.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
		Body:   r,
	}); err != nil {
		return err
	}

	return nil
}

func (f *stagedFileClient) Delete(ctx context.Context, uris []string) error {
	var batches [][]types.ObjectIdentifier
	var batch []types.ObjectIdentifier
	for _, uri := range uris {
		if len(batch) == 1000 {
			batches = append(batches, batch)
			batch = nil
		}

		_, key := s3UriToParts(uri)

		batch = append(batch, types.ObjectIdentifier{Key: aws.String(key)})
	}

	if len(batch) > 0 {
		batches = append(batches, batch)
	}

	for _, batch := range batches {
		if _, err := f.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(f.bucket),
			Delete: &types.Delete{
				Objects: batch,
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func s3UriToParts(uri string) (bucket, key string) {
	uri = strings.TrimPrefix(uri, "s3://")
	split := strings.SplitN(uri, "/", 2)
	return split[0], split[1]
}
