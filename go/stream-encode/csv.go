package stream_encode

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/klauspost/pgzip"
)

const csvCompressionlevel = flate.BestSpeed

type csvConfig struct {
	skipHeaders bool
	quoteChar   rune
}

type CsvEncoder struct {
	cfg    csvConfig
	fields []string
	csv    *csvWriter
	cwc    *countingWriteCloser
	gz     *pgzip.Writer
}

type CsvOption func(*csvConfig)

func WithCsvSkipHeaders() CsvOption {
	return func(cfg *csvConfig) {
		cfg.skipHeaders = true
	}
}

func WithCsvQuoteChar(char rune) CsvOption {
	return func(cfg *csvConfig) {
		cfg.quoteChar = char
	}
}

func NewCsvEncoder(w io.WriteCloser, fields []string, opts ...CsvOption) *CsvEncoder {
	var cfg csvConfig
	for _, o := range opts {
		o(&cfg)
	}

	cwc := &countingWriteCloser{w: w}
	gz, err := pgzip.NewWriterLevel(cwc, csvCompressionlevel)
	if err != nil {
		// Only possible if compressionLevel is not valid.
		panic("invalid compression level for gzip.NewWriterLevel")
	}

	quoteChar := '"'
	if cfg.quoteChar != 0 {
		quoteChar = cfg.quoteChar
	}

	return &CsvEncoder{
		cfg:    cfg,
		csv:    newCsvWriter(gz, byte(quoteChar)),
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
	w         io.Writer
	buf       []byte
	quoteChar byte
}

func newCsvWriter(w io.Writer, quoteChar byte) *csvWriter {
	return &csvWriter{
		w:         w,
		quoteChar: quoteChar,
	}
}

func (w *csvWriter) writeRow(row []any) error {
	w.buf = w.buf[:0]

	for n, v := range row {
		if n > 0 {
			w.buf = append(w.buf, ',')
		}

		switch value := v.(type) {
		case json.RawMessage:
			w.buf = w.appendString(w.buf, value)
		case []byte:
			w.buf = w.appendString(w.buf, value)
		case string:
			// Safety: This value is immediately written to the output and never
			// modified.
			w.buf = w.appendString(w.buf, unsafe.Slice(unsafe.StringData(value), len(value)))
		case bool:
			w.buf = strconv.AppendBool(w.buf, value)
		case int64:
			w.buf = strconv.AppendInt(w.buf, value, 10)
		case int:
			w.buf = strconv.AppendInt(w.buf, int64(value), 10)
		case uint64:
			w.buf = strconv.AppendUint(w.buf, value, 10)
		case float64:
			w.buf = strconv.AppendFloat(w.buf, value, 'f', -1, 64)
		case float32:
			w.buf = strconv.AppendFloat(w.buf, float64(value), 'f', -1, 64)
		case *big.Int:
			w.buf = append(w.buf, value.String()...)
		case nil:
			continue
		default:
			return fmt.Errorf("unsupported value type: %T of value %#v", value, value)
		}
	}

	w.buf = append(w.buf, '\n')
	if _, err := w.w.Write(w.buf); err != nil {
		return err
	}

	return nil
}

func (w *csvWriter) appendString(buf []byte, field []byte) []byte {
	if !w.stringNeedsQuotes(field) {
		buf = append(buf, field...)
	} else {
		buf = append(buf, w.quoteChar)
		for len(field) > 0 {
			// Escape quote characters present in the string by replacing them
			// with double quotes.
			i := bytes.IndexByte(field, w.quoteChar)
			if i < 0 {
				i = len(field)
			}

			buf = append(buf, field[:i]...)
			field = field[i:]
			if len(field) > 0 {
				buf = append(buf, w.quoteChar, w.quoteChar)
				field = field[1:]
			}
		}
		buf = append(buf, w.quoteChar)
	}

	return buf
}

func (w *csvWriter) stringNeedsQuotes(field []byte) bool {
	if len(field) == 0 {
		return true
	}

	for i := 0; i < len(field); i++ {
		c := field[i]
		if c == w.quoteChar || c == '\n' || c == '\r' || c == ',' {
			return true
		}
	}

	r1, _ := utf8.DecodeRune(field)
	return unicode.IsSpace(r1)
}
