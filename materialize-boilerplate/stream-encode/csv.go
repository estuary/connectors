package stream_encode

import (
	"compress/flate"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/klauspost/compress/gzip"
)

const csvCompressionlevel = flate.BestSpeed

type csvConfig struct {
	skipHeaders bool
	nullStr     string
	delimiter   rune
}

type CsvEncoder struct {
	cfg    csvConfig
	fields []string
	csv    *csv.Writer
	cwc    *countingWriteCloser
	gz     *gzip.Writer
}

type CsvOption func(*csvConfig)

func WithCsvSkipHeaders() CsvOption {
	return func(cfg *csvConfig) {
		cfg.skipHeaders = true
	}
}

func WithCsvNullString(str string) CsvOption {
	return func(cfg *csvConfig) {
		cfg.nullStr = str
	}
}

func WithCsvDelimiter(r rune) CsvOption {
	return func(cfg *csvConfig) {
		cfg.delimiter = r
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

	csvw := csv.NewWriter(gz)
	if cfg.delimiter != 0 {
		csvw.Comma = cfg.delimiter
	}

	return &CsvEncoder{
		cfg:    cfg,
		csv:    csvw,
		cwc:    cwc,
		gz:     gz,
		fields: fields,
	}
}

func (e *CsvEncoder) Encode(row []any) error {
	if !e.cfg.skipHeaders {
		if err := e.csv.Write(e.fields); err != nil {
			return fmt.Errorf("writing header: %w", err)
		}
		e.cfg.skipHeaders = true
	}

	record := make([]string, 0, len(row))

	for _, v := range row {
		switch value := v.(type) {
		case json.RawMessage:
			record = append(record, string(value))
		case []byte:
			record = append(record, string(value))
		case string:
			record = append(record, value)
		case bool:
			record = append(record, strconv.FormatBool(value))
		case int64:
			record = append(record, strconv.Itoa(int(value)))
		case int:
			record = append(record, strconv.Itoa(value))
		case float64:
			record = append(record, strconv.FormatFloat(value, 'f', -1, 64))
		case float32:
			record = append(record, strconv.FormatFloat(float64(value), 'f', -1, 64))
		case nil:
			record = append(record, e.cfg.nullStr)
		default:
			record = append(record, fmt.Sprintf("%v", value))
		}
	}

	return e.csv.Write(record)
}

func (e *CsvEncoder) Written() int {
	return e.cwc.written
}

func (e *CsvEncoder) Close() error {
	e.csv.Flush()

	if err := e.csv.Error(); err != nil {
		return fmt.Errorf("flushing csv writer: %w", err)
	} else if err := e.gz.Close(); err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	} else if err := e.cwc.Close(); err != nil {
		return fmt.Errorf("closing counting writer: %w", err)
	}

	return nil
}
