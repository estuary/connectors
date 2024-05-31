package filesink

import (
	"fmt"
	"io"

	enc "github.com/estuary/connectors/go/stream-encode"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// This file contains helpers for building StreamEncoder implementations for common file formats.

type ParquetConfig struct {
	RowGroupRowLimit  int `json:"rowGroupRowLimit,omitempty" jsonschema:"title=Row Group Row Limit,description=Maximum number of rows in a row group. Defaults to 1000000 if blank." jsonschema_extras:"order=0"`
	RowGroupByteLimit int `json:"rowGroupByteLimit,omitempty" jsonschema:"title=Row Group Byte Limit,description=Approximate maximum number of bytes in a row group. Defaults to 536870912 (512 MiB) if blank." jsonschema_extras:"order=1"`
}

func (c ParquetConfig) Validate() error {
	if c.RowGroupRowLimit < 0 {
		return fmt.Errorf("rowGroupRowLimit cannot be negative: got %d", c.RowGroupRowLimit)
	} else if c.RowGroupByteLimit < 0 {
		return fmt.Errorf("rowGroupByteLimit cannot be negative: got %d", c.RowGroupByteLimit)
	}

	return nil
}

func NewParquetStreamEncoder(cfg ParquetConfig, b *pf.MaterializationSpec_Binding, w io.WriteCloser) StreamEncoder {
	sch := enc.FieldsToParquetSchema(b.FieldSelection.AllFields(), b.Collection)

	var opts []enc.ParquetOption

	if cfg.RowGroupRowLimit != 0 {
		opts = append(opts, enc.WithParquetRowGroupRowLimit(cfg.RowGroupRowLimit))
	}
	if cfg.RowGroupByteLimit != 0 {
		opts = append(opts, enc.WithParquetRowGroupByteLimit(cfg.RowGroupByteLimit))
	}

	// For now, we'll always use Snappy compression, as it is by far the most commonly recommended
	// compression.
	opts = append(opts, enc.WithParquetCompression(enc.Snappy))

	return enc.NewParquetEncoder(w, sch, opts...)
}

type CsvConfig struct {
	Delimiter   string `json:"delimiter,omitempty" jsonschema:"title=Delimiter,description=Character to separate columns within a row. Defaults to a comma if blank. Must be a single character with a byte length of 1." jsonschema_extras:"order=0"`
	NullString  string `json:"nullString,omitempty" jsonschema:"title=Null String,description=String to use to represent NULL values. Defaults to an empty string if blank." jsonschema_extras:"order=1"`
	SkipHeaders bool   `json:"skipHeaders,omitempty" jsonschema:"title=Skip Headers,description=Do not write headers to files." jsonschema_extras:"order=2"`
}

func (c CsvConfig) Validate() error {
	if r := []rune(c.Delimiter); len(r) > 1 {
		return fmt.Errorf("delimiter %q must be a single rune (byte length of 1): got byte length of %d", c.Delimiter, len(r))
	}

	return nil
}

func NewCsvStreamEncoder(cfg CsvConfig, b *pf.MaterializationSpec_Binding, w io.WriteCloser) StreamEncoder {
	var opts []enc.CsvOption

	if cfg.Delimiter != "" {
		opts = append(opts, enc.WithCsvDelimiter([]rune(cfg.Delimiter)[0])) // already validated to be 1 byte in length
	}
	if cfg.NullString != "" {
		opts = append(opts, enc.WithCsvNullString(cfg.NullString))
	}
	if cfg.SkipHeaders {
		opts = append(opts, enc.WithCsvSkipHeaders())
	}

	return enc.NewCsvEncoder(w, b.FieldSelection.AllFields(), opts...)
}
