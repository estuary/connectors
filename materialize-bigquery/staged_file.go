package connector

import (
	"context"
	"fmt"
	"path"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/storage"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type stagedFile struct {
	schema []*bigquery.FieldSchema
	client *storage.Client

	// The GCS bucket configured for the materialization.
	bucket string

	// The prefix for files stored in the bucket. This includes the optional `bucketPath` if
	// configured, and the randomly generated UUID of this stagedFile for this connector invocation.
	prefix string

	encoder *sql.CountingEncoder

	// List of file names uploaded during the current transaction. The data file names are randomly
	// generated UUIDs.
	uploaded []string

	// Indicates if the stagedFile has been initialized for this transaction yet. Set `true` by
	// start() and `false` by flush(). Useful for the transactor to know if a binding has any data
	// for the current transaction.
	started bool
}

func newStagedFile(client *storage.Client, bucket string, bucketPath string, schema []*bigquery.FieldSchema) *stagedFile {
	return &stagedFile{
		schema: schema,
		client: client,
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
	fName := uuid.NewString()
	writer := f.client.Bucket(f.bucket).Object(path.Join(f.prefix, fName)).NewWriter(ctx)
	f.encoder = sql.NewCountingEncoder(writer)
	f.uploaded = append(f.uploaded, fName)
}

func (f *stagedFile) flushFile() error {
	if f.encoder == nil {
		return nil
	} else if err := f.encoder.Close(); err != nil {
		return fmt.Errorf("closing encoder: %w", err)
	}

	f.encoder = nil
	return nil
}

func (f *stagedFile) encodeRow(ctx context.Context, row []interface{}) error {
	if len(row) != len(f.schema) { // Sanity check
		return fmt.Errorf("number of values in row to encode (%d) differs from number of configured fields (%d)", len(row), len(f.schema))
	}

	d := make(map[string]interface{})
	for idx := range row {
		d[f.schema[idx].Name] = row[idx]
	}

	// May not have an encoder set yet if the previous encodeRow() resulted in flushing the current
	// file, or for the very first call to encodeRow().
	if f.encoder == nil {
		f.newFile(ctx)
	}

	if err := f.encoder.Encode(d); err != nil {
		return fmt.Errorf("encoding row: %w", err)
	}

	if f.encoder.Written() >= sql.DefaultFileSizeLimit {
		if err := f.flushFile(); err != nil {
			return err
		}
	}

	return nil
}

func (f *stagedFile) fileURI(file string) string {
	return "gs://" + path.Join(f.bucket, f.fileKey(file))
}

func (f *stagedFile) fileKey(file string) string {
	return path.Join(f.prefix, file)
}

func (f *stagedFile) flush(ctx context.Context) (func(context.Context), error) {
	if err := f.flushFile(); err != nil {
		return nil, err
	}

	// Reset for next round.
	f.started = false

	return func(ctx context.Context) {
		for _, fName := range f.uploaded {
			if err := f.client.Bucket(f.bucket).Object(f.fileKey(fName)).Delete(ctx); err != nil {
				log.WithFields(log.Fields{
					"key": f.fileKey(fName),
					"err": err,
				}).Warn("failed to delete staged object file")
			}
		}
	}, nil
}

func (f *stagedFile) edc() *bigquery.ExternalDataConfig {
	uris := make([]string, 0, len(f.uploaded))
	for _, fName := range f.uploaded {
		uris = append(uris, f.fileURI(fName))
	}

	return &bigquery.ExternalDataConfig{
		SourceFormat: bigquery.JSON,
		SourceURIs:   uris,
		Schema:       f.schema,
		Compression:  bigquery.Gzip,
	}
}
