package writer

import (
	"bytes"
	"compress/flate"
	stdjson "encoding/json"
	"fmt"
	"io"

	"github.com/estuary/connectors/go/encrow"
	"github.com/klauspost/pgzip"
	"github.com/segmentio/encoding/json"
)

const (
	// JSON generally compresses well at minimum compression levels. Higher levels of compression
	// will usually take a lot more CPU while not providing much space savings.
	jsonCompressionlevel = flate.BestSpeed

	// Snowflake docs (https://docs.snowflake.com/en/sql-reference/sql/put#usage-notes) recommend
	// data file sizes be in the range of 100-250MB for compressed data. Redshift docs
	// (https://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html) suggest files
	// for compressed data be in the range of 1MB to 1GB, and all be about the same size. Bigquery
	// docs don't mention anything other than the files must be less than 4GB. So, a 250MB file size
	// seems reasonable to use across the board given the current materializations that use this.
	DefaultJsonFileSizeLimit = 250 * 1024 * 1024
)

type jsonConfig struct {
	disableCompression bool
	skipNulls          bool
}

type JsonOption func(*jsonConfig)

func WithJsonDisableCompression() JsonOption {
	return func(cfg *jsonConfig) {
		cfg.disableCompression = true
	}
}

func WithJsonSkipNulls() JsonOption {
	return func(cfg *jsonConfig) {
		cfg.skipNulls = true
	}
}

type JsonWriter struct {
	w            io.Writer // will be set to `gz` for compressed writes or `cwc` if compression is disabled
	cwc          *countingWriteCloser
	gz           *pgzip.Writer
	shape        *encrow.Shape
	buf          []byte
	uncompressed int

	// fields names the values of a row, for errors about a single value.
	fields []string
}

// NewJsonWriter creates a JsonWriter from w. w is closed when JsonWriter is closed. If `fields`
// is nil, values will be encoded as a JSON array rather than as an object.
func NewJsonWriter(w io.WriteCloser, fields []string, opts ...JsonOption) *JsonWriter {
	var cfg jsonConfig
	for _, o := range opts {
		o(&cfg)
	}

	jw := &JsonWriter{
		cwc:    &countingWriteCloser{w: w},
		fields: fields,
	}

	if !cfg.disableCompression {
		gz, err := pgzip.NewWriterLevel(jw.cwc, jsonCompressionlevel)
		if err != nil {
			// Only possible if compressionLevel is not valid.
			panic("invalid compression level for gzip.NewWriterLevel")
		}
		jw.gz = gz
		jw.w = gz
	} else {
		jw.w = jw.cwc
	}

	if fields != nil {
		jw.shape = encrow.NewShape(fields)
		// Setting TrustRawMessage here prevents unnecessary validation of pre-serialized JSON
		// received from the runtime, which we can assume to be valid (flow_document for example).
		// Note that we are also not setting SortMapKeys or EscapeHTML: Sorting keys is not needed
		// because encrow.Shape already sorts the top-level keys and any object values are already
		// serialized as JSON, and escaping HTML is not desired so as to avoid escaping values like
		// <, >, &, etc. if they are present in the materialized collection's data.
		jw.shape.SetFlags(json.TrustRawMessage)
		jw.shape.SetSkipNulls(cfg.skipNulls)
	}

	return jw
}

func (w *JsonWriter) Write(vals []any) (err error) {
	if vals, err = w.compactLineBreaks(vals); err != nil {
		return err
	}

	w.buf = w.buf[:0]
	if w.shape == nil {
		// Serialize as a JSON array of values.
		w.buf = append(w.buf, '[')
		for idx, v := range vals {
			if w.buf, err = json.Append(w.buf, v, json.TrustRawMessage); err != nil {
				return fmt.Errorf("encoding JSON array value: %w", err)
			}
			if idx != len(vals)-1 {
				w.buf = append(w.buf, ',')
			}
		}
		w.buf = append(w.buf, ']')
	} else {
		// Serialize as a JSON object.
		if w.buf, err = w.shape.Encode(w.buf, vals); err != nil {
			return fmt.Errorf("encoding shape: %w", err)
		}
	}

	w.buf = append(w.buf, '\n')

	if _, err := w.w.Write(w.buf); err != nil {
		return fmt.Errorf("writing gzip bytes: %w", err)
	}
	w.uncompressed += len(w.buf)

	return nil
}

// compactLineBreaks returns vals with each pre-serialized JSON value that contains a
// line break replaced by its compacted form, so that every row is written on a single
// line. vals itself is left unchanged, and an empty value is an error.
func (w *JsonWriter) compactLineBreaks(vals []any) ([]any, error) {
	var out, copied = vals, false
	for i, v := range vals {
		raw, ok := v.(stdjson.RawMessage)
		if !ok || raw == nil {
			continue
		} else if len(raw) == 0 {
			return nil, fmt.Errorf("field %q: empty pre-serialized JSON value", w.fieldName(i))
		} else if bytes.IndexByte(raw, '\n') < 0 && bytes.IndexByte(raw, '\r') < 0 {
			continue
		}

		var compacted bytes.Buffer
		if err := stdjson.Compact(&compacted, raw); err != nil {
			return nil, fmt.Errorf("field %q: %w", w.fieldName(i), err)
		}
		if !copied {
			out, copied = append([]any(nil), vals...), true
		}
		out[i] = stdjson.RawMessage(compacted.Bytes())
	}
	return out, nil
}

func (w *JsonWriter) fieldName(i int) string {
	if i < len(w.fields) {
		return w.fields[i]
	}
	return fmt.Sprintf("%d", i)
}

// Written returns the number of bytes written to the underlying writer, after compression.
func (w *JsonWriter) Written() int {
	return int(w.cwc.written.Load())
}

// WrittenUncompressed returns the number of bytes of rows written, before compression.
func (w *JsonWriter) WrittenUncompressed() int {
	return w.uncompressed
}

// Close closes the underlying gzip writer if compression is enabled, flushing its data and writing
// the GZIP footer. It also closes the underlying io.WriteCloser that was used to initialize the
// counting writer.
func (w *JsonWriter) Close() error {
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			return fmt.Errorf("closing gzip writer: %w", err)
		}
	}

	if err := w.cwc.Close(); err != nil {
		return fmt.Errorf("closing counting writer: %w", err)
	}
	return nil
}
