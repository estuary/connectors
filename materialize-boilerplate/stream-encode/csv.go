package stream_encode

import (
	"compress/flate"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/klauspost/compress/gzip"
)

const csvCompressionlevel = flate.BestSpeed

type csvConfig struct {
	skipHeaders bool
}

type CsvEncoder struct {
	cfg    csvConfig
	fields []string
	csv    *csvWriter
	cwc    *countingWriteCloser
	gz     *gzip.Writer
}

type CsvOption func(*csvConfig)

func WithCsvSkipHeaders() CsvOption {
	return func(cfg *csvConfig) {
		cfg.skipHeaders = true
	}
}

func NewCsvEncoder(w io.WriteCloser, fields []string, opts ...CsvOption) *CsvEncoder {
	var cfg csvConfig
	for _, o := range opts {
		o(&cfg)
	}

	cwc := &countingWriteCloser{w: w}
	gz, err := gzip.NewWriterLevel(cwc, csvCompressionlevel)
	if err != nil {
		// Only possible if compressionLevel is not valid.
		panic("invalid compression level for gzip.NewWriterLevel")
	}

	return &CsvEncoder{
		cfg:    cfg,
		csv:    newCsvWriter(gz),
		cwc:    cwc,
		gz:     gz,
		fields: fields,
	}
}

func (e *CsvEncoder) Encode(row []any) error {
	if !e.cfg.skipHeaders {
		headerRow := make([]any, len(e.fields))
		for i, f := range e.fields {
			headerRow[i] = f
		}
		if err := e.csv.writeRow(headerRow); err != nil {
			return fmt.Errorf("writing header: %w", err)
		}
		e.cfg.skipHeaders = true
	}

	return e.csv.writeRow(row)
}

func (e *CsvEncoder) Written() int {
	return e.cwc.written
}

func (e *CsvEncoder) Close() error {
	if err := e.gz.Close(); err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	} else if err := e.cwc.Close(); err != nil {
		return fmt.Errorf("closing counting writer: %w", err)
	}

	return nil
}

type csvWriter struct {
	w io.Writer
}

func newCsvWriter(w io.Writer) *csvWriter {
	return &csvWriter{w: w}
}

func (w *csvWriter) writeRow(row []any) error {
	for n, v := range row {
		if n > 0 {
			if _, err := w.w.Write([]byte(",")); err != nil {
				return err
			}
		}

		var field string
		switch value := v.(type) {
		case json.RawMessage:
			field = string(value)
		case []byte:
			field = string(value)
		case string:
			field = value
		case bool:
			field = strconv.FormatBool(value)
		case int64:
			field = strconv.Itoa(int(value))
		case int:
			field = strconv.Itoa(value)
		case float64:
			field = strconv.FormatFloat(value, 'f', -1, 64)
		case float32:
			field = strconv.FormatFloat(float64(value), 'f', -1, 64)
		case nil:
			continue
		default:
			field = fmt.Sprintf("%v", value)
		}

		if err := w.writeField(field); err != nil {
			return err
		}
	}

	if _, err := w.w.Write([]byte("\n")); err != nil {
		return err
	}

	return nil
}

func (w *csvWriter) writeField(field string) error {
	if !w.fieldNeedsQuotes(field) {
		if _, err := w.w.Write([]byte(field)); err != nil {
			return err
		}
	} else {
		if _, err := w.w.Write([]byte(`"`)); err != nil {
			return err
		}
		for len(field) > 0 {
			// Escape quote characters present in the string by replacing them
			// with double quotes.
			i := strings.Index(field, `"`)
			if i < 0 {
				i = len(field)
			}

			if _, err := w.w.Write([]byte(field[:i])); err != nil {
				return err
			}

			field = field[i:]
			if len(field) > 0 {
				if _, err := w.w.Write([]byte(`""`)); err != nil {
					return err
				}
				field = field[1:]
			}
		}
		if _, err := w.w.Write([]byte(`"`)); err != nil {
			return err
		}
	}

	return nil
}

func (w *csvWriter) fieldNeedsQuotes(field string) bool {
	if field == "" {
		return true
	}

	for i := 0; i < len(field); i++ {
		c := field[i]
		if c == '\n' || c == '\r' || c == '"' || c == ',' {
			return true
		}
	}

	r1, _ := utf8.DecodeRuneInString(field)
	return unicode.IsSpace(r1)
}
