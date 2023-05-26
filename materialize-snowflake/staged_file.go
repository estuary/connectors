package main

import (
	"bufio"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

const (
	// Per https://docs.snowflake.com/en/sql-reference/sql/put#usage-notes data file sizes are
	// recommended to be in the 100-250 MB range for compressed data. We aren't currently using
	// compression, but experimentally 250 MB seems to work well enough with uncompressed JSON.
	fileSizeLimit = 250 * 1024 * 1024
)

// stagedFile manages uploading a sequence of local files produced by reading from Load/Store
// iterators to an internal Snowflake stage. The same stagedFile should not be used concurrently
// across multiple goroutines, but multiple concurrent processes can create their own stagedFile and
// use them.
//
// The data to be staged is split into multiple files for a few reasons:
//  - To allow for parallel processing within Snowflake
//  - To enable some amount of concurrency between the network operations for PUTs and the encoding
//    & writing of JSON data locally
//  - To prevent any individual file from becoming excessively large, which seems to bog down the
//    encryption process used within the go-snowflake driver
//
// Ideally we would stream directly to the Snowflake internal stage rather than reading from a local
// disk file, but streaming PUTs do not currently work well and buffer exessive amounts of data
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
	// Random string that will serve as the directory for local files of this binding.
	uuid string

	// The full directory path of local files for this binding formed by joining tempdir and uuid.
	dir string

	// Indicates if the stagedFile has been initialized for this transaction yet. Set `true` by
	// start() and `false` by flush().
	started bool

	// Index of the current file for this transaction. Starts at 0 and is incremented by 1 for each
	// new file that is created.
	fileIdx int

	// References to the current file being written.
	currentFile *os.File
	writer      *bufio.Writer

	// Amount of data written to the current file. A new file will be started when
	// currentFileWritten > fileSizeLimit.
	currentFileWritten int

	// Per-transaction coordination.
	putFiles chan string
	flushed  chan struct{}
	group    *errgroup.Group
	groupCtx context.Context // Used to check for group cancellation upon the worker returning an error.
}

func newStagedFile(tempdir string, suffix string) *stagedFile {
	uuid := uuid.NewString() + "_" + suffix

	return &stagedFile{
		uuid: uuid,
		dir:  filepath.Join(tempdir, uuid),
	}
}

func (f *stagedFile) start(ctx context.Context, conn *stdsql.Conn) error {
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

	// Clear the Snowflake stage directory of any existing files leftover from the last txn.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`REMOVE @flow_v1/%s;`, f.uuid)); err != nil {
		return fmt.Errorf("clearing stage: %w", err)
	}

	// Sanity check: Make sure the Snowflake stage directory is really empty. As far as I can tell
	// if the REMOVE command is successful it means that all files really were removed so
	// theoretically this is a superfluous check, but the consequences of any lingering files in the
	// directory are incorrect loaded data and this is a cheap check to be extra sure.
	if rows, err := conn.QueryContext(ctx, fmt.Sprintf(`LIST @flow_v1/%s;`, f.uuid)); err != nil {
		return fmt.Errorf("verifying stage empty: %w", err)
	} else if rows.Next() {
		return fmt.Errorf("unexpected existing file from LIST @flow_v1/%s", f.uuid)
	} else {
		rows.Close()
	}

	f.fileIdx = 0
	if err := f.newFile(); err != nil {
		return fmt.Errorf("creating first local staged file: %w", err)
	}

	// Reset values used per-transaction.
	f.currentFileWritten = 0
	f.flushed = make(chan struct{})
	f.group, f.groupCtx = errgroup.WithContext(ctx)
	f.putFiles = make(chan string)

	// Start the putWorker for this transaction.
	f.group.Go(func() error {
		return f.putWorker(f.groupCtx, conn, f.putFiles)
	})

	return nil
}

func (f *stagedFile) encodeRow(row []interface{}) error {
	jsonBytes, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("marshaling row to json: %w", err)
	}

	// Concurrently start the PUT process for this file and start writing to a new file if the
	// current file has reached fileSizeLimit.
	if f.currentFileWritten > 0 && f.currentFileWritten+len(jsonBytes) > fileSizeLimit {
		if err := f.putFile(); err != nil {
			return fmt.Errorf("encodeRow putFile: %w", err)
		} else if err := f.newFile(); err != nil {
			return fmt.Errorf("encodeRow newFile: %w", err)
		}
	}

	written, err := f.writer.Write(jsonBytes)
	if err != nil {
		return fmt.Errorf("encodeRow writing bytes: %w", err)
	}
	f.currentFileWritten += written

	return nil
}

func (f *stagedFile) flush() error {
	if err := f.putFile(); err != nil {
		return fmt.Errorf("flush putFile: %w", err)
	}

	close(f.flushed)
	f.started = false

	// Wait for all outstanding PUT requests to complete.
	return f.group.Wait()
}

func (f *stagedFile) putWorker(ctx context.Context, conn *stdsql.Conn, filePaths <-chan string) error {
	for {
		var file string

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.flushed:
			return nil
		case f := <-filePaths:
			file = f
		}

		query := fmt.Sprintf(
			// OVERWRITE=TRUE is set here not because we intend to overwrite anything, but rather to
			// avoid an extra LIST query that setting OVERWRITE=FALSE incurs.
			`PUT file://%s @flow_v1/%s AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=NONE OVERWRITE=TRUE;`,
			file, f.uuid,
		)
		var source, target, sourceSize, targetSize, sourceCompression, targetCompression, status, message string
		if err := conn.QueryRowContext(ctx, query).Scan(&source, &target, &sourceSize, &targetSize, &sourceCompression, &targetCompression, &status, &message); err != nil {
			return fmt.Errorf("putWorker PUT to stage: %w", err)
		} else if !strings.EqualFold("uploaded", status) {
			return fmt.Errorf("putWorker PUT to stage unexpected upload status: %s", status)
		}

		// Once the file has been staged to Snowflake we don't need it locally anymore and can
		// remove the local copy to manage disk usage.
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("putWorker removing local file: %w", err)
		}
	}
}

func (f *stagedFile) newFile() error {
	filePath := filepath.Join(f.dir, fmt.Sprintf("%d", f.fileIdx))

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	f.currentFileWritten = 0
	f.currentFile = file
	f.writer = bufio.NewWriter(file)

	f.fileIdx += 1

	return nil
}

func (f *stagedFile) putFile() error {
	if err := f.writer.Flush(); err != nil {
		return fmt.Errorf("flushing writer: %w", err)
	}

	fname := f.currentFile.Name()
	if err := f.currentFile.Close(); err != nil {
		return fmt.Errorf("closing local file: %w", err)
	}

	select {
	case <-f.groupCtx.Done():
		// If the group worker has returned an error and cancelled the group context, return that
		// rather than the general "context cancelled" error.
		return f.group.Wait()
	case f.putFiles <- fname:
		return nil
	}
}
