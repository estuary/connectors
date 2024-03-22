package main

import (
	"bufio"
	"context"
	stdsql "database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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

// we keep track of files we upload so that we can send them to Snowpipe in an
// insertFiles RPC
// https://docs.snowflake.com/user-guide/data-load-snowpipe-rest-apis#label-rest-api-insertfiles
type fileRecord struct {
	Path string
	Size int
}

// stagedFile manages uploading a sequence of local files produced by reading from Load/Store
// iterators to an internal Snowflake stage. The same stagedFile should not be used concurrently
// across multiple goroutines, but multiple concurrent processes can create their own stagedFile and
// use them.
//
// The data to be staged is split into multiple files for a few reasons:
//   - To allow for parallel processing within Snowflake
//   - To enable some amount of concurrency between the network operations for PUTs and the encoding
//     & writing of JSON data locally
//   - To prevent any individual file from becoming excessively large, which seems to bog down the
//     encryption process used within the go-snowflake driver
//
// Ideally we would stream directly to the Snowflake internal stage rather than reading from a local
// disk file, but streaming PUTs do not currently work well and buffer excessive amounts of data
// in-memory; see https://github.com/estuary/connectors/issues/50.
//
// The lifecycle of a staged file for a transaction is as follows:
//
// - start: Initializes the local directory for local disk files and clears the Snowflake stage of
// any staged files from a previous transaction. Starts a worker that will concurrently send files
// to Snowflake via PUT commands as local files are finished.
//
// - encodeRow: Encodes a slice of values as JSON and writes to the current local file. If the local
// file has reached a size threshold a new file will be started. Finished files are sent to the
// worker for staging in Snowflake.
//
// - flush: Sends the current & final local file to the worker for staging and waits for the worker
// to complete before returning.
type stagedFile struct {
	// Random string that will serve as the directory for local and remote files of this binding.
	uuid string

	// temporary directory to store local files
	tempdir string

	// The full directory path of local files for this binding formed by joining tempdir and uuid.
	dir string

	// Indicates if the stagedFile has been initialized for this transaction yet. Set `true` by
	// start() and `false` by flush().
	started bool

	// References to the current file being written.
	buf     *fileBuffer
	encoder *sql.CountingEncoder

	// list of uploaded files
	uploaded []fileRecord

	// Per-transaction coordination.
	putFiles chan string
	group    *errgroup.Group
	groupCtx context.Context // Used to check for group cancellation upon the worker returning an error.
}

func newStagedFile(tempdir string) *stagedFile {
	return &stagedFile{
		tempdir: tempdir,
	}
}

const MaxConcurrentUploads = 5

func (f *stagedFile) start(ctx context.Context, db *stdsql.DB) error {
	if f.started {
		return nil
	}
	f.started = true
	f.uuid = uuid.NewString()
	f.dir = filepath.Join(f.tempdir, f.uuid)
	f.uploaded = nil

	// Create the local working directory for this binding. As a simplification we will always
	// remove and re-create the directory since it will already exist for transactions beyond the
	// first one.
	if err := os.RemoveAll(f.dir); err != nil {
		return fmt.Errorf("clearing temp dir: %w", err)
	} else if err := os.Mkdir(f.dir, 0700); err != nil {
		return fmt.Errorf("creating temp dir: %w", err)
	}

	// Clear the Snowflake stage directory of any existing files leftover from the last txn.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`REMOVE @flow_v1/%s;`, f.uuid)); err != nil {
		return fmt.Errorf("clearing stage: %w", err)
	}

	// Sanity check: Make sure the Snowflake stage directory is really empty. As far as I can tell
	// if the REMOVE command is successful it means that all files really were removed so
	// theoretically this is a superfluous check, but the consequences of any lingering files in the
	// directory are incorrect loaded data and this is a cheap check to be extra sure.
	if rows, err := db.QueryContext(ctx, fmt.Sprintf(`LIST @flow_v1/%s;`, f.uuid)); err != nil {
		return fmt.Errorf("verifying stage empty: %w", err)
	} else if rows.Next() {
		return fmt.Errorf("unexpected existing file from LIST @flow_v1/%s", f.uuid)
	} else {
		rows.Close()
	}

	f.group, f.groupCtx = errgroup.WithContext(ctx)
	f.putFiles = make(chan string)

	for i := 0; i < MaxConcurrentUploads; i++ {
		// Start the putWorker for this transaction.
		f.group.Go(func() error {
			return f.putWorker(f.groupCtx, db, f.putFiles)
		})
	}

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

	if err := f.encoder.Encode(row); err != nil {
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

func (f *stagedFile) flush() (string, error) {
	if err := f.putFile(); err != nil {
		return "", fmt.Errorf("flush putFile: %w", err)
	}
	close(f.putFiles)
	f.started = false

	// Wait for all outstanding PUT requests to complete.
	return fmt.Sprintf("@flow_v1/%s", f.uuid), f.group.Wait()
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

		query := fmt.Sprintf(
			// OVERWRITE=TRUE is set here not because we intend to overwrite anything, but rather to
			// avoid an extra LIST query that setting OVERWRITE=FALSE incurs.
			`PUT file://%s @flow_v1/%s AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE;`,
			file, f.uuid,
		)
		var source, target, sourceSize, targetSize, sourceCompression, targetCompression, status, message string
		if err := db.QueryRowContext(ctx, query).Scan(&source, &target, &sourceSize, &targetSize, &sourceCompression, &targetCompression, &status, &message); err != nil {
			return fmt.Errorf("putWorker PUT to stage: %w", err)
		} else if !strings.EqualFold("uploaded", status) {
			return fmt.Errorf("putWorker PUT to stage unexpected upload status: %s", status)
		}

		log.WithFields(log.Fields{
			"source":     source,
			"target":     target,
			"targetSize": targetSize,
			"file":       file,
			"uuid":       f.uuid,
		}).Debug("uploading file")

		if size, err := strconv.Atoi(targetSize); err != nil {
			return fmt.Errorf("parsing targetSize: %w", err)
		} else {
			f.uploaded = append(f.uploaded, fileRecord{
				Path: fmt.Sprintf("%s/%s", f.uuid, target),
				Size: size,
			})
		}

		// Once the file has been staged to Snowflake we don't need it locally anymore and can
		// remove the local copy to manage disk usage.
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("putWorker removing local file: %w", err)
		}
	}
}

func (f *stagedFile) newFile() error {
	var fName = uuid.NewString()
	filePath := filepath.Join(f.dir, fName)

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	f.buf = &fileBuffer{
		buf:  bufio.NewWriter(file),
		file: file,
	}
	f.encoder = sql.NewCountingEncoder(f.buf, true, nil)

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
