package main

import (
	"bufio"
	"compress/gzip"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// streamV2SpillFileSizeLimit bounds the uncompressed size of a single spill
// file. Files are the unit of upload and recovery download, not of appends, so
// this only needs to keep local disk and GET transfers reasonable.
const streamV2SpillFileSizeLimit = 128 * 1024 * 1024

// spillV2File describes one uploaded spill file, in write order. Row counts
// are retained in the driver checkpoint so recovery can account for batch
// boundaries without reading ahead.
type spillV2File struct {
	Name string
	Rows int
}

// streamV2Spill spills column-name-keyed row objects for a single binding to
// local gzip NDJSON files, concurrently PUTing finished files to the @flow_v1
// internal stage so they are durable before the recovery-log commit. Unlike
// stagedFile, local files are retained after upload: the happy path of
// Acknowledge streams them from local disk, and only recovery after a restart
// needs to GET them back from the stage. The spilled bytes are exactly the
// bytes later sent to the sidecar, so rows are serialized once.
type streamV2Spill struct {
	tempdir string
	uuid    string
	dir     string
	started bool

	seq     int
	buf     *fileBuffer
	gz      *gzip.Writer
	curPath string
	curRows int
	written int64

	files []spillV2File

	putFiles chan string
	group    *errgroup.Group
	groupCtx context.Context
}

func newStreamV2Spill(tempdir string) *streamV2Spill {
	return &streamV2Spill{tempdir: tempdir}
}

func (f *streamV2Spill) start(ctx context.Context, db *stdsql.DB) error {
	if f.started {
		return nil
	}
	f.started = true
	f.uuid = uuid.NewString()
	f.dir = filepath.Join(f.tempdir, f.uuid)
	f.seq = 0
	f.files = nil

	if err := os.RemoveAll(f.dir); err != nil {
		return fmt.Errorf("clearing spill dir: %w", err)
	} else if err := os.Mkdir(f.dir, 0700); err != nil {
		return fmt.Errorf("creating spill dir: %w", err)
	}

	f.group, f.groupCtx = errgroup.WithContext(ctx)
	f.putFiles = make(chan string)
	for i := 0; i < MaxConcurrentUploads; i++ {
		f.group.Go(func() error {
			return f.putWorker(f.groupCtx, db, f.putFiles)
		})
	}

	return nil
}

// writeRow appends one row object. The serialized form is written verbatim,
// one JSON object per line.
func (f *streamV2Spill) writeRow(row map[string]any) error {
	if f.buf == nil {
		if err := f.newFile(); err != nil {
			return err
		}
	}

	var data, err = json.Marshal(row)
	if err != nil {
		return fmt.Errorf("encoding spill row: %w", err)
	} else if _, err = f.gz.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("writing spill row: %w", err)
	}
	f.curRows++
	f.written += int64(len(data)) + 1

	if f.written >= streamV2SpillFileSizeLimit {
		if err := f.finishFile(); err != nil {
			return err
		}
	}
	return nil
}

// flush finishes the current file, waits for all uploads, and returns the
// stage directory, the local directory, and the ordered file manifest.
func (f *streamV2Spill) flush() (string, string, []spillV2File, error) {
	if f.buf != nil {
		if err := f.finishFile(); err != nil {
			return "", "", nil, err
		}
	}
	close(f.putFiles)
	f.started = false

	if err := f.group.Wait(); err != nil {
		return "", "", nil, err
	}
	return fmt.Sprintf("@flow_v1/%s", f.uuid), f.dir, f.files, nil
}

func (f *streamV2Spill) newFile() error {
	f.curPath = filepath.Join(f.dir, fmt.Sprintf("%08d.jsonl.gz", f.seq))
	f.seq++
	f.curRows = 0
	f.written = 0

	file, err := os.Create(f.curPath)
	if err != nil {
		return err
	}
	f.buf = &fileBuffer{buf: bufio.NewWriter(file), file: file}
	f.gz = gzip.NewWriter(f.buf)
	return nil
}

func (f *streamV2Spill) finishFile() error {
	if err := f.gz.Close(); err != nil {
		return fmt.Errorf("closing spill gzip: %w", err)
	} else if err := f.buf.Close(); err != nil {
		return fmt.Errorf("closing spill file: %w", err)
	}
	f.files = append(f.files, spillV2File{Name: filepath.Base(f.curPath), Rows: f.curRows})

	select {
	case <-f.groupCtx.Done():
		return f.group.Wait()
	case f.putFiles <- f.curPath:
	}

	f.buf = nil
	f.gz = nil
	return nil
}

func (f *streamV2Spill) putWorker(ctx context.Context, db *stdsql.DB, filePaths <-chan string) error {
	for {
		var file string
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p, ok := <-filePaths:
			if !ok {
				return nil
			}
			file = p
		}

		var query = fmt.Sprintf(
			`PUT file://%s @flow_v1/%s AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE;`,
			file, f.uuid,
		)
		var status string
		for attempt := 1; ; attempt++ {
			// NB: Not using QueryRowContext here since the Go Snowflake driver
			// retains contexts internally, and this worker's context is
			// cancelled after group.Wait() returns.
			// Columns: source, target, sourceSize, targetSize, sourceCompression, targetCompression, status, message.
			var discard [7]stdsql.NullString
			var err = db.QueryRow(query).Scan(&discard[0], &discard[1], &discard[2], &discard[3], &discard[4], &discard[5], &status, &discard[6])
			if err == nil {
				break
			} else if attempt > 3 {
				return fmt.Errorf("spill PUT to stage: %w", err)
			}
			var delay = time.Duration(attempt) * time.Second
			log.WithFields(log.Fields{"attempt": attempt, "delay": delay}).WithError(err).Info("retrying spill PUT to stage")
			time.Sleep(delay)
		}
		if !strings.EqualFold("uploaded", status) {
			return fmt.Errorf("spill PUT to stage unexpected upload status: %s", status)
		}
	}
}
