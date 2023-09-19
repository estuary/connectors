package sql

import (
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
)

const (
	// JSON generally compresses well at minimum compression levels. Higher levels of compression
	// will usually take a lot more CPU while not providing much space savings.
	compressionLevel = flate.BestSpeed

	// Snowflake docs (https://docs.snowflake.com/en/sql-reference/sql/put#usage-notes) recommend
	// data file sizes be in the range of 100-250MB for compressed data. Redshift docs
	// (https://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html) suggest files
	// for compressed data be in the range of 1MB to 1GB, and all be about the same size. Bigquery
	// docs don't mention anything other than the files must be less than 4GB. So, a 250MB file size
	// seems reasonable to use across the board given the current materializations that use this.
	DefaultFileSizeLimit = 250 * 1024 * 1024
)

// CountingEncoder provides access to a count of gzip'd bytes that have been written by a
// json.Encoder to an io.WriterCloser.
type CountingEncoder struct {
	enc *json.Encoder
	cwc *countingWriteCloser
	gz  *gzip.Writer
}

// NewCountingEncoder creates a CountingEncoder from w. w is closed when CountingEncoder is closed.
func NewCountingEncoder(w io.WriteCloser) *CountingEncoder {
	cwc := &countingWriteCloser{w: w}
	gz, err := gzip.NewWriterLevel(cwc, compressionLevel)
	if err != nil {
		// Only possible if compressionLevel is not valid.
		panic("invalid compression level for gzip.NewWriterLevel")
	}

	enc := json.NewEncoder(gz)
	enc.SetIndent("", "")

	// We certainly don't want to escape <, >, &, etc., and also setting this to false might be
	// beneficial to throughput.
	enc.SetEscapeHTML(false)

	return &CountingEncoder{
		enc: enc,
		cwc: cwc,
		gz:  gz,
	}
}

func (e *CountingEncoder) Encode(v any) error {
	if err := e.enc.Encode(v); err != nil {
		return fmt.Errorf("countingEncoder encoding to enc: %w", err)
	}
	return nil
}

func (e *CountingEncoder) Written() int {
	return e.cwc.written
}

// Close closes the underlying gzip writer, flushing its data and writing the GZIP footer. It also
// closes io.WriteCloser that was used to initialize the counting encoder.
func (e *CountingEncoder) Close() error {
	if err := e.gz.Close(); err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	} else if err := e.cwc.Close(); err != nil {
		return fmt.Errorf("closing counting writer: %w", err)
	}
	return nil
}

// countingWriteCloser is used internally by CountingEncoder to access the count of compressed bytes
// written to the writer.
type countingWriteCloser struct {
	written int
	w       io.WriteCloser
}

func (c *countingWriteCloser) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if err != nil {
		return 0, fmt.Errorf("countingWriteCloser writing to w: %w", err)
	}
	c.written += n

	return n, nil
}

func (c *countingWriteCloser) Close() error {
	if err := c.w.Close(); err != nil {
		return fmt.Errorf("countingWriteCloser closing w: %w", err)
	}
	return nil
}
