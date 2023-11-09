package main

import (
	"encoding/json"
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/databricks/databricks-sdk-go/service/files"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	log "github.com/sirupsen/logrus"
)

// fileBuffer provides Close() for a *bufio.Writer writing to an *os.File. Close() will flush the
// buffer and close the underlying file.
type fileBuffer struct {
	buf  *bufio.Writer
	file *os.File
}

func (f *fileBuffer) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}

func (f *fileBuffer) Close() error {
	if err := f.buf.Flush(); err != nil {
		return err
	} else if err := f.file.Close(); err != nil {
		return err
	}
	return nil
}

// stagedFile manages uploading a sequence of local files produced by reading from Load/Store
// iterators to an internal Databricks volume. The same stagedFile should not be used concurrently
// across multiple goroutines, but multiple concurrent processes can create their own stagedFile and
// use them.
//
// The data to be staged is split into multiple files for a few reasons:
//   - To allow for parallel processing within Databricks
//   - To enable some amount of concurrency between the network operations for PUTs and the encoding
//     & writing of JSON data locally
//   - To prevent any individual file from becoming excessively large, which seems to bog down the
//     encryption process used within the go-snowflake driver
//
// Ideally we would stream directly to the Databricks internal stage rather than reading from a local
// disk file, but streaming PUTs do not currently work well and we have not been able to stream more than
// 60MB of data for each upload, which is very small and leads to performance limitations
//
// The lifecycle of a staged file for a transaction is as follows:
//
// - start: Initializes the local directory for local disk files and starts a worker that will concurrently send files
// to Databricks via FilesAPI.Upload RPC as local files are finished.
//
// - encodeRow: Encodes a slice of values as JSON and writes to the current local file. If the local
// file has reached a size threshold a new file will be started. Finished files are sent to the
// worker for staging in Databricks.
//
// - flush: Sends the current & final local file to the worker for staging and waits for the worker
// to complete before returning.
type stagedFile struct {
	cols     []*sql.Column

	// Random string that will serve as the directory for local files of this binding.
	uuid string

	// The full directory path of local files for this binding formed by joining tempdir and uuid.
	dir string

	// The remote root directory for uploading files
	root string

	filesAPI *files.FilesAPI

	// Indicates if the stagedFile has been initialized for this transaction yet. Set `true` by
	// start() and `false` by flush().
	started bool

	// Index of the current file for this transaction. Starts at 0 and is incremented by 1 for each
	// new file that is created.
	fileIdx int

	// References to the current file being written.
	buf     *fileBuffer
	encoder *sql.CountingEncoder

	// List of file names uploaded during the current transaction for transaction data, not
	// including the manifest file name itself. These data file names randomly generated UUIDs.
	uploaded []string

	// Per-transaction coordination.
	putFiles chan string
	group    *errgroup.Group
	groupCtx context.Context // Used to check for group cancellation upon the worker returning an error.
}

func newStagedFile(filesAPI *files.FilesAPI, root string, cols []*sql.Column) *stagedFile {
	uuid := uuid.NewString()
	var tempdir = os.TempDir()

	return &stagedFile{
		cols: cols,
		uuid: uuid,
		dir:  filepath.Join(tempdir, uuid),
		root: root,
		filesAPI: filesAPI,
	}
}

func (f *stagedFile) start(ctx context.Context) error {
	if f.started {
		return nil
	}
	f.started = true

	// Create the local working directory for this binding. As a simplification we will always
	// remove and re-create the directory since it will already exist for transactions beyond the
	// first one.
	if err := os.RemoveAll(f.dir); err != nil {
		return fmt.Errorf("clearing temp dir: %w", err)
	} else if err := os.Mkdir(f.dir, 0700); err != nil {
		return fmt.Errorf("creating temp dir: %w", err)
	}

	// Reset values used per-transaction.
	f.uploaded = []string{}
	f.group, f.groupCtx = errgroup.WithContext(ctx)
	f.putFiles = make(chan string)

	// Start the putWorker for this transaction.
	f.group.Go(func() error {
		return f.putWorker(f.groupCtx, f.putFiles)
	})

	return nil
}

func (f *stagedFile) encodeRow(row []interface{}) error {
	// May not have an encoder set yet if the previous encodeRow() resulted in flushing the current
	// file, or for the very first call to encodeRow().
	if f.encoder == nil {
		if err := f.newFile(); err != nil {
			return err
		}
	}

	if len(row) != len(f.cols) { // Sanity check
		return fmt.Errorf("number of headers in row to encode (%d) differs from number of configured headers (%d)", len(row), len(f.cols))
	}

	d := make(map[string]interface{})
	for idx := range row {
		if v, ok := row[idx].(json.RawMessage); ok {
			d[f.cols[idx].Field] = string(v)
		} else {
			d[f.cols[idx].Field] = row[idx]
		}
	}

	if err := f.encoder.Encode(d); err != nil {
		return fmt.Errorf("encoding row: %w", err)
	}

	// Concurrently start the PUT process for this file if the current file has reached
	// fileSizeLimit.
	if f.encoder.Written() >= sql.DefaultFileSizeLimit {
		if err := f.putFile(); err != nil {
			return fmt.Errorf("encodeRow putFile: %w", err)
		}
	}

	return nil
}

func (f *stagedFile) flush() ([]string, []string, error) {
	if err := f.putFile(); err != nil {
		return nil, nil, fmt.Errorf("flush putFile: %w", err)
	}

	close(f.putFiles)
	f.started = false

	var toDelete = make([]string, len(f.uploaded))
	var toCopy = make([]string, len(f.uploaded))
	for i, u := range f.uploaded {
		toCopy[i] = u
		toDelete[i] = f.remoteFilePath(u)
	}

	// Wait for all outstanding PUT requests to complete.
	return toCopy, toDelete, f.group.Wait()
}

func (f *stagedFile) remoteFilePath(file string) string {
	return filepath.Join(f.root, file)
}

func (f *stagedFile) putWorker(ctx context.Context, filePaths <-chan string) error {
	for {
		var file string

		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-filePaths:
			if !ok {
				return nil
			}
			file = f
		}

		var fName = filepath.Base(file)
		log.WithField("filepath", f.remoteFilePath(fName)).Info("staged file: uploading")
		if r, err := os.Open(file); err != nil {
			return fmt.Errorf("opening file: %w", err)
		} else if err := f.filesAPI.Upload(ctx, files.UploadRequest{Contents: r, FilePath: f.remoteFilePath(fName)}); err != nil {
			return fmt.Errorf("uploading file: %w", err)
		}
		log.WithField("filepath", f.remoteFilePath(fName)).Info("staged file: upload done")

		// Once the file has been staged to Databricks we don't need it locally anymore and can
		// remove the local copy to manage disk usage.
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("putWorker removing local file: %w", err)
		}
	}
}

func (f *stagedFile) newFile() error {
	var fName = fmt.Sprintf("%s.json", uuid.NewString())
	filePath := filepath.Join(f.dir, fName)

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	f.buf = &fileBuffer{
		buf:  bufio.NewWriter(file),
		file: file,
	}
	f.encoder = sql.NewCountingEncoder(f.buf, false)
	f.uploaded = append(f.uploaded, fName)
	f.fileIdx += 1

	return nil
}

func (f *stagedFile) putFile() error {
	if f.encoder == nil {
		return nil
	}

	if err := f.encoder.Close(); err != nil {
		return fmt.Errorf("closing encoder: %w", err)
	}
	f.encoder = nil

	select {
	case <-f.groupCtx.Done():
		// If the group worker has returned an error and cancelled the group context, return that
		// rather than the general "context cancelled" error.
		return f.group.Wait()
	case f.putFiles <- f.buf.file.Name():
		return nil
	}
}
