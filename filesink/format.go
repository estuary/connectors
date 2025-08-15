package filesink

import (
	"fmt"
	"io"

	"github.com/estuary/connectors/go/writer"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// This file contains helpers for building StreamWriter implementations for common file formats.

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

func NewParquetWriter(cfg ParquetConfig, b *pf.MaterializationSpec_Binding, w io.WriteCloser, uuidLogicalType bool) (StreamWriter, error) {
	var schemaOpts []writer.ParquetSchemaOption
	if !uuidLogicalType {
		schemaOpts = append(schemaOpts, writer.WithParquetUUIDAsString())
	}

	sch := make(writer.ParquetSchema, 0, len(b.FieldSelection.AllFields()))
	for _, f := range b.FieldSelection.AllFields() {
		p := b.Collection.GetProjection(f)

		var fc fieldConfig
		if raw, ok := b.FieldSelection.FieldConfigJsonMap[f]; ok {
			if err := pf.UnmarshalStrict(raw, &fc); err != nil {
				return nil, fmt.Errorf("unmarshaling field config for field %q: %w", f, err)
			}
		}

		sch = append(sch, writer.ProjectionToParquetSchemaElement(*p, fc.CastToString, schemaOpts...))
	}

	var opts []writer.ParquetOption

	if cfg.RowGroupRowLimit != 0 {
		opts = append(opts, writer.WithParquetRowGroupRowLimit(cfg.RowGroupRowLimit))
	}
	if cfg.RowGroupByteLimit != 0 {
		opts = append(opts, writer.WithParquetRowGroupByteLimit(cfg.RowGroupByteLimit))
	}

	// For now, we'll always use Snappy compression, as it is by far the most commonly recommended
	// compression.
	opts = append(opts, writer.WithParquetCompression(writer.Snappy))

	return writer.NewParquetWriter(w, sch, opts...), nil
}

type CsvConfig struct {
	SkipHeaders bool `json:"skipHeaders,omitempty" jsonschema:"title=Skip Headers,description=Do not write headers to files." jsonschema_extras:"order=2"`
}

func (c CsvConfig) Validate() error {
	return nil
}

func NewCsvStreamWriter(cfg CsvConfig, b *pf.MaterializationSpec_Binding, w io.WriteCloser) StreamWriter {
	var opts []writer.CsvOption

	if cfg.SkipHeaders {
		opts = append(opts, writer.WithCsvSkipHeaders())
	}

	return writer.NewCsvWriter(w, b.FieldSelection.AllFields(), opts...)
}
