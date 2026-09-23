package connector

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	stdsql "database/sql"

	driverctx "github.com/databricks/databricks-sql-go/driverctx"
	"github.com/estuary/connectors/go/writer"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const fileSizeLimit = 128 * 1024 * 1024
const uploadConcurrency = 3

// stagedSchemaDDL is the schema of a staged file as the DDL string read_files
// takes, so that reading the file infers nothing. Integer and boolean columns
// are written as JSON numbers and booleans and read as such; every other
// column is written as a JSON string and cast by the query, as the string
// forms of doubles include NaN and the infinities.
func stagedSchemaDDL(cols []*sql.Column, withDeleteFlag bool) string {
	var out = make([]string, 0, len(cols)+1)
	for _, col := range cols {
		var ddl = "STRING"
		switch strings.Fields(col.DDL)[0] {
		case "LONG":
			ddl = "BIGINT"
		case "BOOLEAN":
			ddl = "BOOLEAN"
		}
		out = append(out, "`"+strings.ReplaceAll(translateFlowField(col.Field), "`", "``")+"` "+ddl)
	}
	if withDeleteFlag {
		out = append(out, "`_flow_delete` BOOLEAN")
	}
	return strings.Join(out, ", ")
}

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
//   - To prevent any individual file from becoming excessively large. We aim for 128MB for files as per
//     recommendation by Databricks: https://docs.databricks.com/en/_extras/documents/best-practices-ingestion-partner-volumes.pdf
//
// Ideally we would stream directly to the Databricks internal stage rather than reading from a local
// disk file, but streaming PUTs do not currently work well and we have not been able to stream more than
// 60MB of data for each upload, which is very small and leads to performance limitations
//
// Each transaction's files are uploaded into a directory of their own under
// the root, so that a query can read the transaction's files as one relation.
// Uploaded names are relative to the root and include that directory.
//
// The lifecycle of a staged file for a transaction is as follows:
//
// - start: Initializes the local directory for local disk files, creates the transaction's remote
// directory, and starts a worker that will concurrently send files to Databricks via
// FilesAPI.Upload RPC as local files are finished.
//
// - writeRow: Writes a slice of values as JSON and writes to the current local file. If the local
// file has reached a size threshold a new file will be started. Finished files are sent to the
// worker for staging in Databricks.
//
// - flush: Sends the current & final local file to the worker for staging and waits for the worker
// to complete before returning.
type stagedFile struct {
	fields []string

	// The full directory path of local files for this binding formed by joining tempdir and uuid.
	dir string

	// The remote root directory for uploading files, and the current
	// transaction's directory under it.
	root   string
	txnDir string
	// mkdir creates a remote directory.
	mkdir func(ctx context.Context, path string) error

	// Indicates if the stagedFile has been initialized for this transaction yet. Set `true` by
	// start() and `false` by flush().
	started bool

	cfg config

	// References to the current file being written.
	buf    *fileBuffer
	writer *writer.JsonWriter

	// List of file names uploaded during the current transaction for transaction data, not
	// including the manifest file name itself. These data file names randomly generated UUIDs.
	uploaded []string

	// Per-transaction coordination.
	putFiles chan string
	group    *errgroup.Group
	groupCtx context.Context // Used to check for group cancellation upon the worker returning an error.
}

func newStagedFile(cfg config, root string, fields []string, mkdir func(ctx context.Context, path string) error) *stagedFile {
	uuid := uuid.NewString()
	var tempdir = os.TempDir()

	return &stagedFile{
		fields: fields,
		dir:    filepath.Join(tempdir, uuid),
		root:   root,
		mkdir:  mkdir,
		cfg:    cfg,
	}
}

// remoteDir is the directory of the current transaction's uploads.
func (f *stagedFile) remoteDir() string {
	return filepath.Join(f.root, f.txnDir)
}

func (f *stagedFile) start(ctx context.Context, db *stdsql.DB) error {
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
	f.txnDir = uuid.NewString()
	if err := f.mkdir(ctx, f.remoteDir()); err != nil {
		return fmt.Errorf("creating staging directory %q: %w", f.remoteDir(), err)
	}
	f.group, f.groupCtx = errgroup.WithContext(ctx)
	f.putFiles = make(chan string)

	for i := 0; i < uploadConcurrency; i++ {
		// Start the putWorker for this transaction.
		f.group.Go(func() error {
			return f.putWorker(f.groupCtx, db, f.putFiles)
		})
	}

	return nil
}

func (f *stagedFile) writeRow(row []interface{}) error {
	// May not have a writer set yet if the previous writeRow() resulted in flushing the current
	// file, or for the very first call to writeRow().
	if f.writer == nil {
		if err := f.newFile(); err != nil {
			return err
		}
	}

	if err := f.writer.Write(row); err != nil {
		return fmt.Errorf("writing row: %w", err)
	}

	// Concurrently start the PUT process for this file if the current file has reached
	// fileSizeLimit.
	if f.writer.Written() >= fileSizeLimit {
		if err := f.putFile(); err != nil {
			return fmt.Errorf("writeRow putFile: %w", err)
		}
	}

	return nil
}

func (f *stagedFile) flush() ([]string, error) {
	if err := f.putFile(); err != nil {
		return nil, fmt.Errorf("flush putFile: %w", err)
	}

	close(f.putFiles)
	f.started = false

	// Wait for all outstanding PUT requests to complete.
	return f.uploaded, f.group.Wait()
}

func (f *stagedFile) putWorker(ctx context.Context, db *stdsql.DB, filePaths <-chan string) error {
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
		log.WithField("filepath", filepath.Join(f.remoteDir(), fName)).Debug("staged file: uploading")

		ctx = driverctx.NewContextWithStagingInfo(ctx, []string{f.dir})

		// This query fails sometimes even in low load, we retry this query to avoid a full restart
		var maxAttempts = 3
		var attempt = 0
		for {
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`PUT '%s' INTO '%s' OVERWRITE`, file, filepath.Join(f.remoteDir(), fName))); err != nil {
				if attempt < maxAttempts {
					attempt++
					continue
				}
				return fmt.Errorf("put file: %w", err)
			}
			break
		}

		log.WithField("filepath", filepath.Join(f.remoteDir(), fName)).Debug("staged file: upload done")

		// Once the file has been staged to Databricks we don't need it locally anymore and can
		// remove the local copy to manage disk usage.
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("putWorker removing local file: %w", err)
		}
	}
}

func (f *stagedFile) newFile() error {
	// Databricks infers the codec of a staged file from its extension when reading it back. The
	// uploaded name is relative to root and so includes the transaction's directory; the local
	// file keeps only the base name.
	var fName = filepath.Join(f.txnDir, uuid.NewString()+".json.gz")
	filePath := filepath.Join(f.dir, filepath.Base(fName))

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	f.buf = &fileBuffer{
		buf:  bufio.NewWriter(file),
		file: file,
	}
	f.writer = writer.NewJsonWriter(f.buf, f.fields)
	f.uploaded = append(f.uploaded, fName)

	return nil
}

func (f *stagedFile) putFile() error {
	if f.writer == nil {
		return nil
	}

	if err := f.writer.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}
	f.writer = nil

	select {
	case <-f.groupCtx.Done():
		// If the group worker has returned an error and cancelled the group context, return that
		// rather than the general "context cancelled" error.
		return f.group.Wait()
	case f.putFiles <- f.buf.file.Name():
		return nil
	}
}
