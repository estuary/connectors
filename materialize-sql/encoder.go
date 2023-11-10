package sql

import (
	"compress/flate"
	"fmt"
	"io"

	"github.com/estuary/connectors/go/encrow"
	"github.com/klauspost/compress/gzip"
	"github.com/segmentio/encoding/json"
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

// CountingEncoder provides access to a count of optionally gzip'd bytes that have been written by a
// json.Encoder to an io.WriterCloser.
type CountingEncoder struct {
	w     io.Writer // will be set to `gz` for compressed writes or `cwc` if compression is disabled
	cwc   *countingWriteCloser
	gz    *gzip.Writer
	shape *encrow.Shape
	buf   []byte
}

// NewCountingEncoder creates a CountingEncoder from w. w is closed when CountingEncoder is closed.
// If `fields` is nil, values will be encoded as a JSON array rather than as an object.
func NewCountingEncoder(w io.WriteCloser, gzipCompression bool, fields []string) *CountingEncoder {
	enc := &CountingEncoder{
		cwc: &countingWriteCloser{w: w},
	}

	if gzipCompression {
		gz, err := gzip.NewWriterLevel(enc.cwc, compressionLevel)
		if err != nil {
			// Only possible if compressionLevel is not valid.
			panic("invalid compression level for gzip.NewWriterLevel")
		}
		enc.gz = gz
		enc.w = gz
	} else {
		enc.w = enc.cwc
	}

	if fields != nil {
		enc.shape = encrow.NewShape(fields)
		// Setting TrustRawMessage here prevents unnecessary validation of pre-serialized JSON
		// received from the runtime, which we can assume to be valid (flow_document for example).
		// Note that we are also not setting SortMapKeys or EscapeHTML: Sorting keys is not needed
		// because encrow.Shape already sorts the top-level keys and any object values are already
		// serialized as JSON, and escaping HTML is not desired so as to avoid escaping values like
		// <, >, &, etc. if they are present in the materialized collection's data.
		enc.shape.SetFlags(json.TrustRawMessage)
	}

	return enc
}

func (e *CountingEncoder) Encode(vals []any) (err error) {
	if e.shape == nil {
		// Serialize as a JSON array of values.
		e.buf = e.buf[:0]
		e.buf = append(e.buf, '[')
		for idx, v := range vals {
			if e.buf, err = json.Append(e.buf, v, json.TrustRawMessage); err != nil {
				return fmt.Errorf("encoding JSON array value: %w", err)
			}
			if idx != len(vals)-1 {
				e.buf = append(e.buf, ',')
			}
		}
		e.buf = append(e.buf, ']')
	} else {
		// Serialize as a JSON object.
		if e.buf, err = e.shape.Encode(e.buf, vals); err != nil {
			return fmt.Errorf("encoding shape: %w", err)
		}
	}

	e.buf = append(e.buf, '\n')

	if _, err := e.w.Write(e.buf); err != nil {
		return fmt.Errorf("writing gzip bytes: %w", err)
	}

	return nil
}

func (e *CountingEncoder) Written() int {
	return e.cwc.written
}

// Close closes the underlying gzip writer if compression is enabled, flushing its data and writing
// the GZIP footer. It also closes the underlying io.WriteCloser that was used to initialize the
// counting encoder.
func (e *CountingEncoder) Close() error {
	if e.gz != nil {
		if err := e.gz.Close(); err != nil {
			return fmt.Errorf("closing gzip writer: %w", err)
		}
	}

	if err := e.cwc.Close(); err != nil {
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
