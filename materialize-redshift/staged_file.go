package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/estuary/connectors/go/writer"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	manifestFile = "files.manifest"
)

// s3Store is the S3 client configured with the user bucket.
type s3Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func newS3Store(client *s3.Client, bucket string) *s3Store {
	return &s3Store{
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
	}
}

// objectURI renders the s3:// URI of an object key.
func (s *s3Store) objectURI(key string) string {
	return "s3://" + path.Join(s.bucket, key)
}

// upload streams body to key as a single multipart upload.
func (s *s3Store) upload(ctx context.Context, key string, body io.Reader) error {
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

type copyManifest struct {
	Entries []manifestEntry `json:"entries"`
}

type manifestEntry struct {
	URL string `json:"url"`
	// The "mandatory" flag means the copy job will fail if the file can't be found. This should
	// always be set to `true`. See https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html
	Mandatory bool `json:"mandatory"`
}

// putManifest writes a COPY manifest at manifestKey listing the given data
// files, each an object key.
func (s *s3Store) putManifest(ctx context.Context, manifestKey string, files []string) error {
	manifest := copyManifest{Entries: make([]manifestEntry, 0, len(files))}
	for _, key := range files {
		manifest.Entries = append(manifest.Entries, manifestEntry{
			URL:       s.objectURI(key),
			Mandatory: true,
		})
	}

	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshalling manifest file: %w", err)
	}

	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(manifestKey),
		Body:   bytes.NewReader(manifestBytes),
	}); err != nil {
		return fmt.Errorf("putting manifest file: %w", err)
	}
	return nil
}

// deleteObjects deletes staged objects, in the 1000 per call that S3 allows. Failures are logged:
// a leftover object costs storage but nothing else.
func (s *s3Store) deleteObjects(ctx context.Context, keys []string) {
	for chunk := range slices.Chunk(keys, 1000) {
		toDelete := make([]types.ObjectIdentifier, 0, len(chunk))
		for _, key := range chunk {
			toDelete = append(toDelete, types.ObjectIdentifier{Key: aws.String(key)})
		}

		d, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{Objects: toDelete},
		})
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Warn("deleteObjects failed")
			continue
		}

		for _, err := range d.Errors {
			log.WithFields(log.Fields{
				"key":     err.Key,
				"code":    err.Code,
				"message": err.Message,
				"err":     err,
			}).Warn("failed to delete staged object file")
		}
	}
}

// stagedFile streams rows to newline-delimited JSON files in S3, acting as a single sink for
// writes during a transaction and automatically splitting into multiple files. The same
// stagedFile should not be used concurrently across multiple goroutines, but multiple concurrent
// processes can each create their own stagedFile.
//
// The lifecycle of a stagedFile is as follows:
//
// - start: Initializes values for a new transaction. Can be called repeatedly until flush.
//
// - writeRow: Writes a slice of values as JSON and outputs a JSON map with keys corresponding to
// the columns the stagedFile was initialized with. If the current file size has exceeded
// fileSizeLimit, the current file will be flushed to S3 and a new one started the next time
// writeRow is called.
//
// - flush: Closes out the last file that was started (if any) and returns the keys of the files
// written for the current transaction, for a manifest that Redshift can load them from.
type stagedFile struct {
	fields []string
	store  *s3Store

	// The optional `bucketPath` configured for the materialization.
	bucketPath string

	// The prefix for objects staged in the current transaction: bucketPath and a UUID new to
	// each transaction, so that the manifest key is unique per transaction.
	prefix string

	writer *writer.JsonWriter
	group  *errgroup.Group

	// List of file names uploaded during the current transaction for transaction data, not
	// including the manifest file name itself. These data file names randomly generated UUIDs.
	uploaded []string

	// Indicates if the stagedFile has been initialized for this transaction yet. Set `true` by
	// start() and `false` by flush(). Useful for the transactor to know if a binding has any data
	// for the current transaction.
	started bool
}

func newStagedFile(store *s3Store, bucketPath string, fields []string) *stagedFile {
	return &stagedFile{
		fields:     fields,
		store:      store,
		bucketPath: bucketPath,
	}
}

func (f *stagedFile) start() {
	if f.started {
		return
	}

	f.prefix = path.Join(f.bucketPath, uuid.NewString())
	f.uploaded = []string{}
	f.started = true
}

func (f *stagedFile) newFile(ctx context.Context) {
	r, w := io.Pipe()

	f.writer = writer.NewJsonWriter(w, f.fields)

	group, groupCtx := errgroup.WithContext(ctx)
	f.group = group
	fName := uuid.NewString()
	f.uploaded = append(f.uploaded, fName)

	f.group.Go(func() error {
		if err := f.store.upload(groupCtx, path.Join(f.prefix, fName), r); err != nil {
			// Closing the read half of the pipe will cause subsequent writes to fail, with the
			// error received here propagated.
			r.CloseWithError(err)
			return fmt.Errorf("uploading file: %w", err)
		}
		return nil
	})
}

func (f *stagedFile) flushFile() error {
	if f.writer == nil {
		return nil
	}

	if err := f.writer.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	} else if err := f.group.Wait(); err != nil {
		return err
	}

	f.writer = nil
	return nil
}

func (f *stagedFile) writeRow(ctx context.Context, row []interface{}) error {
	// May not have a writer set yet if the previous writeRow() resulted in flushing the current
	// file, or for the very first call to writeRow().
	if f.writer == nil {
		f.newFile(ctx)
	}

	if err := f.writer.Write(row); err != nil {
		return fmt.Errorf("writing row: %w", err)
	}

	if f.writer.Written() >= writer.DefaultJsonFileSizeLimit {
		if err := f.flushFile(); err != nil {
			return err
		}
	}

	return nil
}

// flush closes the last file and returns the keys of the transaction's files.
func (f *stagedFile) flush() ([]string, error) {
	if err := f.flushFile(); err != nil {
		return nil, err
	}

	files := make([]string, 0, len(f.uploaded))
	for _, u := range f.uploaded {
		files = append(files, path.Join(f.prefix, u))
	}

	// Reset for next round.
	f.started = false

	return files, nil
}
