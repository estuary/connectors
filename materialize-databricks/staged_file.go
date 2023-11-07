package main

import (
	"context"
	"fmt"
	"io"
	"path"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"github.com/databricks/databricks-sdk-go/service/files"
)

const fileSizeLimit = 128 * 1024 * 1024
// stagedFile is a wrapper around the databricks Files API for streaming file uploads to databricks volumes.
// The same stagedFile should not be used concurrently across multiple goroutines, but multiple
// concurrent processes can create their own stagedFile and use them. stagedFile acts as a single
// sink for writes during a transaction, but will automatically split files into multiple parts.
//
// The lifecycle of a stagedFile is as follows:
//
// - start: Initializes values for a new transaction. Can be called repeatedly until flush.
//
// - encodeRow: Encodes a slice of values as JSON and outputs a JSON map with keys corresponding to
// the columns the stagedFile was initialized with. If the current file size has exceeded
// fileSizeLimit, the current file will be flushed to Databricks and a new one started the next time
// encodeRow is called.
//
// - flush: Closes out the last file that was started (if any). Returns a list of
// files written to the remote volume that can be used to construct COPY INTO queries, and a
// function that will delete all stored files.
type stagedFile struct {
	fields     []string
  filesAPI *files.FilesAPI

	// The Databricks volume and catalog root path to use as a base for uploading files
	root string

	encoder *sql.CountingEncoder
	group   *errgroup.Group

	// List of file names uploaded during the current transaction for transaction data.
  // These data file names are randomly generated UUIDs.
	uploaded []string

	// Indicates if the stagedFile has been initialized for this transaction yet. Set `true` by
	// start() and `false` by flush(). Useful for the transactor to know if a binding has any data
	// for the current transaction.
	started bool
}

func newStagedFile(filesAPI *files.FilesAPI, root string, fields []string) *stagedFile {
	return &stagedFile{
		fields: fields,
    filesAPI: filesAPI,
    root: root,
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

	f.encoder = sql.NewCountingEncoder(w, false, f.fields)

	group, groupCtx := errgroup.WithContext(ctx)
	f.group = group
	fName := fmt.Sprintf("%s.json", uuid.NewString())
	f.uploaded = append(f.uploaded, fName)

	f.group.Go(func() error {
		err := f.filesAPI.Upload(groupCtx, files.UploadRequest{
      Contents: r,
      FilePath: f.filePath(fName),
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

	if f.encoder.Written() >= fileSizeLimit {
		if err := f.flushFile(); err != nil {
			return err
		}
	}

	return nil
}

func (f *stagedFile) filePath(file string) string {
  return path.Join(f.root, f.fileKey(file))
}

func (f *stagedFile) fileKey(file string) string {
	return file
}

func (f *stagedFile) flush(ctx context.Context) ([]string, []string, error) {
	if err := f.flushFile(); err != nil {
		return nil, nil, err
	}

  var toDelete = make([]string, len(f.uploaded))
  var toCopy = make([]string, len(f.uploaded))
  for i, u := range f.uploaded {
    toCopy[i] = f.fileKey(u)
    toDelete[i] = f.filePath(u)
  }

	// Reset for next round.
	f.started = false

	return toCopy, toDelete, nil
}
