package connector

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/estuary/connectors/go/writer"
	"github.com/google/uuid"
)

// streamV2Batch accumulates the rows of one Snowpipe Streaming append in a local
// file of gzip-compressed NDJSON, one row per line.
//
// The lifecycle of a batch is as follows:
//
// - writeRow: Writes a row of column values as JSON to the current local file,
// creating the file first if there is none.
//
// - finish: Closes the file, flushing and releasing its compressor and buffer, and
// returns it.
type streamV2Batch struct {
	// columns names the values of each row.
	columns []string

	// References to the current file being written.
	path   string
	writer *writer.JsonWriter

	rows        int
	firstOffset int64
}

// streamV2BatchFile is the local file of a finished batch.
type streamV2BatchFile struct {
	path              string
	rows              int
	uncompressedBytes int
	compressedBytes   int
}

func (b *streamV2Batch) empty() bool {
	return b.rows == 0
}

// size is the number of compressed bytes written to the current file.
func (b *streamV2Batch) size() int {
	if b.writer == nil {
		return 0
	}
	return b.writer.Written()
}

func (b *streamV2Batch) writeRow(row []any) error {
	if b.writer == nil {
		if err := b.newFile(); err != nil {
			return err
		}
	}

	if err := b.writer.Write(row); err != nil {
		return fmt.Errorf("writing row: %w", err)
	}
	b.rows++

	return nil
}

// finish closes the current file and returns it, leaving the batch empty.
func (b *streamV2Batch) finish() (streamV2BatchFile, error) {
	if err := b.writer.Close(); err != nil {
		return streamV2BatchFile{}, fmt.Errorf("closing writer: %w", err)
	}
	var file = streamV2BatchFile{
		path:              b.path,
		rows:              b.rows,
		uncompressedBytes: b.writer.WrittenUncompressed(),
		compressedBytes:   b.writer.Written(),
	}

	b.path, b.writer, b.rows, b.firstOffset = "", nil, 0, 0
	return file, nil
}

func (b *streamV2Batch) newFile() error {
	var path = filepath.Join(os.TempDir(), uuid.NewString())

	file, err := os.Create(path)
	if err != nil {
		return err
	}

	b.path = path
	b.writer = writer.NewJsonWriter(
		&fileBuffer{buf: bufio.NewWriter(file), file: file},
		b.columns,
		writer.WithJsonSkipNulls(),
	)

	return nil
}
