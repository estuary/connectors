package main

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type stagedFile struct {
	fields   []string
	client   *s3.Client
	uploader *manager.Uploader

	// The AWS S3 bucket configured for the materialization.
	bucket string

	// The prefix for files stored in the bucket. This includes the optional `bucketPath` if
	// configured, and the randomly generated UUID of this stagedFile for this connector invocation.
	prefix string

	encoder *enc.JsonEncoder
	group   *errgroup.Group

	// List of file names uploaded during the current transaction for transaction data. These data
	// file names are randomly generated UUIDs.
	uploaded []string

	// Indicates if the stagedFile has been initialized for this transaction yet. Set `true` by
	// start() and `false` by flush(). Useful for the transactor to know if a binding has any data
	// for the current transaction.
	started bool
}

func newStagedFile(client *s3.Client, bucket string, bucketPath string, fields []string) *stagedFile {
	return &stagedFile{
		fields: fields,
		client: client,
		uploader: manager.NewUploader(client, func(u *manager.Uploader) {
			// The default concurrency is 5, which will potentially start up 5 separate goroutines
			// with a 5MB buffer if data is being pushed to the writer faster than it can be sent to
			// S3. This can end up using quite a lot of memory, so instead the concurrency is set to
			// 1 to allow back pressure from the S3 upload to slow the rate of message consumption
			// while using a single 5MB buffer.
			u.Concurrency = 1

			// The minimum upload part size is 5MB, due to this being the minimum size for a part of
			// a multipart upload for all but the last part. It is not really possible to truly
			// "stream" files to S3, since the content-length for any object being put must be known
			// ahead of time. The upload manager configured in this way approximates streaming
			// behavior by uploading relatively small parts sequentially as part of a multipart
			// upload of a larger, unbounded stream of data.
			u.PartSize = manager.MinUploadPartSize
		}),
		bucket: bucket,
		prefix: path.Join(bucketPath, uuid.NewString()),
	}
}

func (f *stagedFile) start() {
	if f.started {
		return
	}

	f.uploaded = []string{}
	f.started = true
}

func (f *stagedFile) newFile(ctx context.Context) {
	r, w := io.Pipe()

	f.encoder = enc.NewJsonEncoder(w, f.fields)

	group, groupCtx := errgroup.WithContext(ctx)
	f.group = group
	fName := uuid.NewString()
	f.uploaded = append(f.uploaded, fName)

	f.group.Go(func() error {
		_, err := f.uploader.Upload(groupCtx, &s3.PutObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(path.Join(f.prefix, fName)),
			Body:   r,
		})
		if err != nil {
			// Closing the read half of the pipe will cause subsequent writes to fail, with the
			// error received here propagated.
			r.CloseWithError(err)
			return fmt.Errorf("uploading file: %w", err)
		}

		return nil
	})
}

func (f *stagedFile) flushFile() error {
	if f.encoder == nil {
		return nil
	}

	if err := f.encoder.Close(); err != nil {
		return fmt.Errorf("closing encoder: %w", err)
	} else if err := f.group.Wait(); err != nil {
		return err
	}

	f.encoder = nil
	return nil
}

func (f *stagedFile) encodeRow(ctx context.Context, row []interface{}) error {
	// May not have an encoder set yet if the previous encodeRow() resulted in flushing the current
	// file, or for the very first call to encodeRow().
	if f.encoder == nil {
		f.newFile(ctx)
	}

	if err := f.encoder.Encode(row); err != nil {
		return fmt.Errorf("encoding row: %w", err)
	}

	if f.encoder.Written() >= enc.DefaultJsonFileSizeLimit {
		if err := f.flushFile(); err != nil {
			return err
		}
	}

	return nil
}

func (f *stagedFile) fileURI(file string) string {
	return "s3://" + path.Join(f.bucket, f.fileKey(file))
}

func (f *stagedFile) fileKey(file string) string {
	return path.Join(f.prefix, file)
}

func (f *stagedFile) allFiles() []string {
	out := make([]string, 0, len(f.uploaded))

	for _, up := range f.uploaded {
		out = append(out, f.fileURI(up))
	}

	return out
}

func (f *stagedFile) flush() (func(context.Context), error) {
	if err := f.flushFile(); err != nil {
		return nil, err
	}

	toDelete := []types.ObjectIdentifier{}

	for _, u := range f.uploaded {
		toDelete = append(toDelete, types.ObjectIdentifier{
			Key: aws.String(f.fileKey(u)),
		})
	}

	// A single DeleteObjects call can delete up to 1000 objects. At 250 MB per file, that would be
	// 250 GB of compressed data. We do not currently expect a single transaction to be nearly that
	// large, but will sanity check here just in case.
	if len(toDelete) > 1000 {
		return nil, fmt.Errorf("cannot process transaction having more than 1000 files: had %d files", len(toDelete))
	}

	// Reset for next round.
	f.started = false

	return func(ctx context.Context) {
		d, err := f.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(f.bucket),
			Delete: &types.Delete{
				Objects: toDelete,
			},
		})
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Warn("deleteObjects failed")
			return
		}

		for _, err := range d.Errors {
			log.WithFields(log.Fields{
				"key":     err.Key,
				"code":    err.Code,
				"message": err.Message,
				"err":     err,
			}).Warn("failed to delete staged object file")
		}
	}, nil
}
