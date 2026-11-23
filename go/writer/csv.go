package writer

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/klauspost/pgzip"
)

const csvCompressionlevel = flate.BestSpeed

type csvConfig struct {
	skipHeaders    bool
	quoteChar      rune
	escapeFormulas bool
}

type CsvWriter struct {
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

// WithCsvEscapeFormulas prefixes string values and headers that a spreadsheet
// application would evaluate as a formula with a single quote. This changes
// the written values, so it is not suitable for files read back as data.
func WithCsvEscapeFormulas() CsvOption {
	return func(cfg *csvConfig) {
		cfg.escapeFormulas = true
	}
}

func NewCsvWriter(w io.WriteCloser, fields []string, opts ...CsvOption) *CsvWriter {
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

	return &CsvWriter{
		cfg:    cfg,
		csv:    newCsvWriter(gz, byte(quoteChar), cfg.escapeFormulas),
		cwc:    cwc,
		gz:     gz,
		fields: fields,
	}
}

func (w *CsvWriter) Write(row []any) error {
	if !w.cfg.skipHeaders {
		headerRow := make([]any, len(w.fields))
		for i, f := range w.fields {
			headerRow[i] = f
		}
		if err := w.csv.writeRow(headerRow); err != nil {
			return fmt.Errorf("writing header: %w", err)
		}
		w.cfg.skipHeaders = true
	}

	return w.csv.writeRow(row)
}

func (w *CsvWriter) Written() int {
	return int(w.cwc.written.Load())
}

func (w *CsvWriter) Close() error {
	if err := w.gz.Close(); err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	} else if err := w.cwc.Close(); err != nil {
		return fmt.Errorf("closing counting writer: %w", err)
	}

	return nil
}

type csvWriter struct {
	w              io.Writer
	buf            []byte
	escaped        []byte
	quoteChar      byte
	escapeFormulas bool
}

func newCsvWriter(w io.Writer, quoteChar byte, escapeFormulas bool) *csvWriter {
	return &csvWriter{
		w:              w,
		quoteChar:      quoteChar,
		escapeFormulas: escapeFormulas,
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
	if w.escapeFormulas && startsFormula(field) {
		w.escaped = append(append(w.escaped[:0], '\''), field...)
		field = w.escaped
	}

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

var csvSignedNumber = regexp.MustCompile(`^[+-][0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// startsFormula reports whether a spreadsheet application would evaluate the
// field as a formula. Signed numbers are only ever read as numbers.
func startsFormula(field []byte) bool {
	if len(field) == 0 {
		return false
	}

	switch field[0] {
	case '=', '@', '\t', '\r', '\n':
		return true
	case '+', '-':
		return !csvSignedNumber.Match(field)
	}

	return false
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
