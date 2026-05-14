package writer

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	iso8601 "github.com/senseyeio/duration"
	log "github.com/sirupsen/logrus"
)

const (
	// Sets an approximate limit on the number of document "rows" to buffer in memory before writing
	// them out as a row group to the scratch file. A larger buffer will use more connector memory,
	// and perhaps afford slightly less overhead when seeking through the scratch file to read
	// columns from its individual row groups. A smaller buffer here means more row groups written
	// to the scratch file, and this interacts with the maxScratchColumnChunkCount value as well in
	// terms of how much metadata is written to the scratch file.
	maxBufferSize = 25 * 1024 * 1024

	// Each row group that is written to the scratch file has a column chunk for each column it
	// contains. In extreme cases of very large numbers of columns (1000+) where the values in the
	// columns are small (example: all booleans), we can end up with a truly enormous amount of
	// scratch file metadata and potentially run out of memory when trying to read the metadata to
	// transfer the values out of the file. This value sets a limit on how much metadata is
	// generated in the scratch file by only allowing this many column chunks to be written.
	maxScratchColumnChunkCount = 5_000

	// The default number of rows per row group in the generated parquet file. Configurable via
	// WithRowGroupRowsLimit.
	defaultRowGroupRowLimit = 1_000_000

	// The default approximate upper limit for row group byte sizes, after compression. Configurable
	// via WithRowGroupByteLimit.
	defaultRowGroupByteLimit = 512 * 1024 * 1024
)

// rowSize is used to get a rough estimate of how much memory a row of values will take up when
// buffered in the ParquetWriter's `buffer` slice. These estimates are used to bound how often a
// buffer is written as a row group to the scratch file. The maximum allowed buffer size based on
// these estimates should be << the connector limits since they are at best proportional wild
// guesses.
type rowSize struct {
	// fixed is the constant component of a row's size as determined from a nominal number of bytes
	// consumed by its scalar types and slice/string headers.
	fixed int
	// calcLen is the indices within a row where we should additionally consider the length of a
	// byte slice or string in the memory estimate, on a per-row basis.
	calcLen []int
}

func newRowSizing(sch ParquetSchema) rowSize {
	rs := rowSize{
		fixed: 24, // slice header for the row itself; it will use this much memory even for an empty row
	}

	for idx, e := range sch {
		// Assume 16 bytes of overhead for any value due to the interface{} that contains it.
		rs.fixed += 16

		switch e.DataType {
		case PrimitiveTypeInteger, PrimitiveTypeNumber:
			rs.fixed += 8
		case PrimitiveTypeBoolean:
			rs.fixed += 1
		case PrimitiveTypeBinary, LogicalTypeJson, LogicalTypeDecimal:
			rs.fixed += 24 // slice header
			rs.calcLen = append(rs.calcLen, idx)
		case LogicalTypeString, LogicalTypeUuid, LogicalTypeDate, LogicalTypeTime, LogicalTypeTimestamp, LogicalTypeInterval:
			rs.fixed += 16 // string header
			rs.calcLen = append(rs.calcLen, idx)
		default:
			panic(fmt.Sprintf("newRowSizing unknown type: %d", e.DataType))
		}
	}

	return rs
}

func (rs rowSize) estSize(row []any) int {
	out := rs.fixed

	for _, pos := range rs.calcLen {
		switch v := row[pos].(type) {
		case string:
			out += len(v)
		case []byte:
			out += len(v)
		case json.RawMessage:
			out += len(v)
		case nil:
			// No additional overhead is assumed for nil values.
		default:
			// All other values are assumed to have a fixed overhead of 16
			// bytes. This is applicable to fields which have multiple types
			// where the Parquet type is a string or byte array but the field's
			// value is some other scalar type.
			out += 16
		}
	}

	return out
}

type ParquetCompression int

const (
	Uncompressed ParquetCompression = iota
	Snappy
	Gzip
)

type parquetConfig struct {
	compression               ParquetCompression
	disableDictionaryEncoding bool
	rowGroupRowLimit          int
	rowGroupByteLimit         int
	metadata                  map[string]string
}

type ParquetWriter struct {
	cfg parquetConfig

	schema        ParquetSchema
	schemaRoot    *schema.GroupNode
	sinkWriter    *mergeWriter
	cwc           *countingWriteCloser
	rs            rowSize
	rowGroupCount int

	// Scratch values are re-initialized as scratch files are transposed into the output stream.
	scratch struct {
		file             *os.File
		writer           *file.Writer
		sizeBytes        int
		rowCount         int
		columnChunkCount int
	}

	// buffer holds column-oriented values pending to be written as a row group to the scratch file
	// when bufferSizeBytes exceeds the threshold. Each entry is a strongly-typed slice (e.g.
	// []int64, []parquet.ByteArray) matching the column's parquet physical type, and contains only
	// non-null values for that column. defLevels[colIdx] runs parallel to the row order with 0 for
	// null and 1 for present, so its length always equals bufferRowCount.
	buffer          []any
	defLevels       [][]int16
	bufferRowCount  int
	bufferSizeBytes int
}

type ParquetOption func(*parquetConfig)

func WithParquetCompression(c ParquetCompression) ParquetOption {
	return func(cfg *parquetConfig) {
		cfg.compression = c
	}
}

func WithDisableDictionaryEncoding() ParquetOption {
	return func(cfg *parquetConfig) {
		cfg.disableDictionaryEncoding = true
	}
}

func WithParquetRowGroupRowLimit(n int) ParquetOption {
	return func(cfg *parquetConfig) {
		cfg.rowGroupRowLimit = n
	}
}

func WithParquetRowGroupByteLimit(n int) ParquetOption {
	return func(cfg *parquetConfig) {
		cfg.rowGroupByteLimit = n
	}
}

func WithParquetMetadata(meta map[string]string) ParquetOption {
	return func(cfg *parquetConfig) {
		cfg.metadata = meta
	}
}

func NewParquetWriter(w io.WriteCloser, sch ParquetSchema, opts ...ParquetOption) *ParquetWriter {
	cfg := parquetConfig{
		compression:       Uncompressed,
		rowGroupRowLimit:  defaultRowGroupRowLimit,
		rowGroupByteLimit: defaultRowGroupByteLimit,
	}
	for _, o := range opts {
		o(&cfg)
	}

	fields := make(schema.FieldList, 0, len(sch))
	for _, e := range sch {
		fields = append(fields, makeNode(e))
	}

	schemaRoot := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1))

	cwc := &countingWriteCloser{w: w}
	props, kvmeta := sinkWriterProperties(cfg)
	// Matching arrow-go's *file.Writer constructor, we panic on the magic-bytes write failure
	// rather than threading an error through NewParquetWriter — a sink that can't accept 4 bytes
	// at construction time will fail again immediately on the first real write.
	sinkWriter, err := newMergeWriter(cwc, schemaRoot, props, kvmeta)
	if err != nil {
		panic(fmt.Sprintf("creating sink writer: %s", err))
	}

	buffer := make([]any, len(sch))
	defLevels := make([][]int16, len(sch))
	const initialCap = 1000
	for i, e := range sch {
		buffer[i] = makeColumnBuffer(e.DataType, initialCap)
		defLevels[i] = make([]int16, 0, initialCap)
	}

	return &ParquetWriter{
		cfg:        cfg,
		schema:     sch,
		schemaRoot: schemaRoot,
		sinkWriter: sinkWriter,
		cwc:        cwc,
		rs:         newRowSizing(sch),
		buffer:     buffer,
		defLevels:  defLevels,
	}
}

// makeColumnBuffer returns a pointer to an empty strongly-typed slice for the given column type.
// Storing a pointer (one word) in the []any buffer avoids a heap allocation on every append,
// since the interface data word holds the pointer directly rather than boxing a 3-word slice header.
func makeColumnBuffer(dt ParquetDataType, initialCap int) any {
	switch dt {
	case PrimitiveTypeInteger, LogicalTypeTime, LogicalTypeTimestamp:
		s := make([]int64, 0, initialCap)
		return &s
	case PrimitiveTypeNumber:
		s := make([]float64, 0, initialCap)
		return &s
	case PrimitiveTypeBoolean:
		s := make([]bool, 0, initialCap)
		return &s
	case PrimitiveTypeBinary, LogicalTypeJson, LogicalTypeString:
		s := make([]parquet.ByteArray, 0, initialCap)
		return &s
	case LogicalTypeUuid, LogicalTypeDecimal, LogicalTypeInterval:
		s := make([]parquet.FixedLenByteArray, 0, initialCap)
		return &s
	case LogicalTypeDate:
		s := make([]int32, 0, initialCap)
		return &s
	default:
		panic(fmt.Sprintf("makeColumnBuffer unknown type: %d", dt))
	}
}

// resetColumnBuffer truncates the slice pointed to by s to length 0 while preserving its
// underlying capacity so subsequent buffer cycles can reuse the allocation.
func resetColumnBuffer(s any) {
	switch sp := s.(type) {
	case *[]int64:
		*sp = (*sp)[:0]
	case *[]int32:
		*sp = (*sp)[:0]
	case *[]float64:
		*sp = (*sp)[:0]
	case *[]bool:
		*sp = (*sp)[:0]
	case *[]parquet.ByteArray:
		*sp = (*sp)[:0]
	case *[]parquet.FixedLenByteArray:
		*sp = (*sp)[:0]
	default:
		panic(fmt.Sprintf("resetColumnBuffer unknown type: %T", s))
	}
}

func compressionCodec(c ParquetCompression) compress.Compression {
	switch c {
	case Uncompressed:
		return compress.Codecs.Uncompressed
	case Snappy:
		return compress.Codecs.Snappy
	case Gzip:
		return compress.Codecs.Gzip
	default:
		panic(fmt.Sprintf("unknown compression setting: %d", c))
	}
}

// sinkWriterProperties builds the WriterProperties used by the sink mergeWriter and the
// KeyValueMetadata to embed in the file. Dictionary encoding is unconditionally disabled because
// transferColumnValues copies pages verbatim from scratch into the sink, and the sink cannot
// retroactively build a dictionary from already-encoded pages. The `disableDictionaryEncoding`
// config field is therefore now a no-op kept only for API compatibility.
func sinkWriterProperties(cfg parquetConfig) (*parquet.WriterProperties, metadata.KeyValueMetadata) {
	propOpts := []parquet.WriterProperty{
		parquet.WithCompression(compressionCodec(cfg.compression)),
		parquet.WithDictionaryDefault(false),
	}
	props := parquet.NewWriterProperties(propOpts...)

	var kv metadata.KeyValueMetadata
	if len(cfg.metadata) > 0 {
		kv = metadata.NewKeyValueMetadata()
		for k, v := range cfg.metadata {
			if err := kv.Append(k, v); err != nil {
				panic(fmt.Sprintf("invalid metadata: %s", err)) // only possible if the metadata keys/values contain invalid UTF-8
			}
		}
	}
	return props, kv
}

// Write a row of data by buffering it in the writers's buffer, and if thresholds are
// exceed writing the buffer as a row group to the scratch file, and potentially flushing the row
// groups from the scratch file collectively as a single row group to the output.
func (w *ParquetWriter) Write(row []any) error {
	var err error

	if len(row) != len(w.schema) {
		return fmt.Errorf("write: row length (%d) does not match schema length (%d)", len(row), len(w.schema))
	}

	if w.scratch.writer == nil {
		// Either the very first row, or the first one after flushing the scratch file.
		if w.scratch.file, err = os.CreateTemp("", "parquet-scratch-*"); err != nil {
			return fmt.Errorf("write creating scratch file: %w", err)
		}

		scratchOpts := []parquet.WriterProperty{
			// Don't use dictionary encoding for the scratch file, since it is
			// much slower to write than plain encoding with the small row
			// groups of the scratch file. Also avoids the dictionary-page
			// splice problem when copying pages verbatim into the sink.
			parquet.WithDictionaryDefault(false),
			// Match the sink codec so transferColumnValues can copy page bytes
			// without re-compressing. Stats default on so per-page min/max
			// propagate via copied pages to the sink output.
			parquet.WithCompression(compressionCodec(w.cfg.compression)),
		}
		w.scratch.writer = file.NewParquetWriter(w.scratch.file, w.schemaRoot, file.WithWriterProps(parquet.NewWriterProperties(scratchOpts...)))
	}

	for colIdx, f := range w.schema {
		v := row[colIdx]
		if v == nil {
			w.defLevels[colIdx] = append(w.defLevels[colIdx], 0)
			continue
		}
		w.defLevels[colIdx] = append(w.defLevels[colIdx], 1)

		var convErr error
		switch f.DataType {
		case PrimitiveTypeInteger:
			convErr = appendVal(w, colIdx, v, getIntVal)
		case PrimitiveTypeNumber:
			convErr = appendVal(w, colIdx, v, getNumberVal)
		case PrimitiveTypeBoolean:
			convErr = appendVal(w, colIdx, v, getBooleanVal)
		case PrimitiveTypeBinary:
			convErr = appendVal(w, colIdx, v, getBinaryVal)
		case LogicalTypeJson:
			convErr = appendVal(w, colIdx, v, getJsonVal)
		case LogicalTypeString:
			convErr = appendVal(w, colIdx, v, getStringVal)
		case LogicalTypeUuid:
			convErr = appendVal(w, colIdx, v, getUuidVal)
		case LogicalTypeDate:
			convErr = appendVal(w, colIdx, v, getDateVal)
		case LogicalTypeTime:
			convErr = appendVal(w, colIdx, v, getTimeVal)
		case LogicalTypeTimestamp:
			convErr = appendVal(w, colIdx, v, getTimestampVal)
		case LogicalTypeDecimal:
			convErr = appendVal(w, colIdx, v, getDecimalVal)
		case LogicalTypeInterval:
			convErr = appendVal(w, colIdx, v, getIntervalVal)
		default:
			panic(fmt.Sprintf("attempted to write unknown type of column '%s': %d", f.Name, f.DataType))
		}
		if convErr != nil {
			return fmt.Errorf("converting value for column '%s': %w (type %T)", f.Name, convErr, v)
		}
	}
	w.bufferSizeBytes += w.rs.estSize(row)
	w.bufferRowCount += 1
	w.scratch.rowCount += 1

	if w.bufferSizeBytes >= maxBufferSize {
		// Write out the buffer as a single row group to the scratch file.
		if err := w.flushBuffer(); err != nil {
			return fmt.Errorf("write flushing buffer based on buffer size: %w", err)
		}
	}

	if w.scratch.sizeBytes >= w.cfg.rowGroupByteLimit ||
		w.scratch.rowCount >= w.cfg.rowGroupRowLimit ||
		w.scratch.columnChunkCount >= maxScratchColumnChunkCount {
		if w.scratch.columnChunkCount >= maxScratchColumnChunkCount {
			log.WithFields(log.Fields{
				"scratchSizeBytes":        w.scratch.sizeBytes,
				"scratchRowCount":         w.scratch.rowCount,
				"scratchColumnChunkCount": w.scratch.columnChunkCount,
			}).Debug("flushing scratch file based on column chunk count")
		}

		if err := w.flushBuffer(); err != nil {
			// Still might need to flush the buffer based on row count.
			return fmt.Errorf("write flushing buffer based on scratch file size: %w", err)
		} else if err := w.flushScratchFile(); err != nil {
			return fmt.Errorf("write flushing scratch file based on scratch file size: %w", err)
		}
	}

	return nil
}

// Written returns the number of bytes written to the output writer. This value increases only as
// row groups from the scratch file are flushed to the output, which happens whenever there is
// enough data (either by rows or bytes) in the scratch file to fill a complete row group.
func (w *ParquetWriter) Written() int {
	return w.cwc.written
}

// Close flushes the buffered rows and scratch file, and closes the output writer.
func (w *ParquetWriter) Close() error {
	if err := w.flushBuffer(); err != nil {
		return fmt.Errorf("flushing buffer: %w", err)
	} else if err := w.flushScratchFile(); err != nil {
		return fmt.Errorf("flushing scratch file: %w", err)
	} else if err := w.sinkWriter.Close(); err != nil { // also closes the underlying io.WriteCloser
		return fmt.Errorf("closing sink: %w", err)
	}
	return nil
}

// FileMetadata returns the current state of the FileMetadata that would be
// written if this file were to be closed. If the file has already been closed,
// then this will return the FileMetaData which was written to the file. This is
// a proxy for the parquet (*file).Writer.FileMetadata() method.
func (w *ParquetWriter) FileMetadata() (*metadata.FileMetaData, error) {
	return w.sinkWriter.FileMetadata()
}

// RowGroupsWritten returns the number of row groups that have been written to
// the output so far, which is equivalent to how many times the scratch file has
// been flushed.
func (w *ParquetWriter) RowGroupsWritten() int {
	return w.rowGroupCount
}

func (w *ParquetWriter) flushScratchFile() error {
	if w.scratch.writer == nil {
		return nil
	}

	if err := w.scratch.writer.Close(); err != nil { // also closes the underlying io.WriteCloser
		return fmt.Errorf("closing scratch writer: %w", err)
	} else if sr, err := os.Open(w.scratch.file.Name()); err != nil {
		return fmt.Errorf("opening scratch file to transfer values: %w", err)
	} else if scratchReader, err := file.NewParquetReader(sr); err != nil {
		return fmt.Errorf("creating scratch reader: %w", err)
	} else if err := transferColumnValues(scratchReader, w.sinkWriter); err != nil {
		return fmt.Errorf("transferring column values: %w", err)
	} else if err := sr.Close(); err != nil {
		return fmt.Errorf("closing scratch file after reading: %w", err)
	} else if err := os.Remove(w.scratch.file.Name()); err != nil {
		return fmt.Errorf("removing scratch file: %w", err)
	}

	w.scratch.file = nil
	w.scratch.writer = nil
	w.scratch.sizeBytes = 0
	w.scratch.columnChunkCount = 0
	w.scratch.rowCount = 0
	w.rowGroupCount += 1

	return nil
}

// flushBuffer writes the current buffered rows as a single row group to the scratch file.
func (w *ParquetWriter) flushBuffer() error {
	if w.bufferRowCount == 0 {
		return nil
	}

	rgWriter := w.scratch.writer.AppendRowGroup()

	for colIdx, f := range w.schema {
		cw, err := rgWriter.NextColumn()
		if err != nil {
			return fmt.Errorf("getting next column: %w", err)
		}

		defLvls := w.defLevels[colIdx]

		switch f.DataType {
		case PrimitiveTypeInteger:
			if err := writeColumn(cw.(*file.Int64ColumnChunkWriter), *w.buffer[colIdx].(*[]int64), defLvls); err != nil {
				return fmt.Errorf("writing integer column '%s': %w", f.Name, err)
			}
		case PrimitiveTypeNumber:
			if err := writeColumn(cw.(*file.Float64ColumnChunkWriter), *w.buffer[colIdx].(*[]float64), defLvls); err != nil {
				return fmt.Errorf("writing number column '%s': %w", f.Name, err)
			}
		case PrimitiveTypeBoolean:
			if err := writeColumn(cw.(*file.BooleanColumnChunkWriter), *w.buffer[colIdx].(*[]bool), defLvls); err != nil {
				return fmt.Errorf("writing boolean column '%s': %w", f.Name, err)
			}
		case PrimitiveTypeBinary:
			if err := writeColumn(cw.(*file.ByteArrayColumnChunkWriter), *w.buffer[colIdx].(*[]parquet.ByteArray), defLvls); err != nil {
				return fmt.Errorf("writing byte array column '%s': %w", f.Name, err)
			}
		case LogicalTypeJson:
			if err := writeColumn(cw.(*file.ByteArrayColumnChunkWriter), *w.buffer[colIdx].(*[]parquet.ByteArray), defLvls); err != nil {
				return fmt.Errorf("writing byte array (json) column '%s': %w", f.Name, err)
			}
		case LogicalTypeString:
			if err := writeColumn(cw.(*file.ByteArrayColumnChunkWriter), *w.buffer[colIdx].(*[]parquet.ByteArray), defLvls); err != nil {
				return fmt.Errorf("writing byte array (string) column '%s': %w", f.Name, err)
			}
		case LogicalTypeUuid:
			if err := writeColumn(cw.(*file.FixedLenByteArrayColumnChunkWriter), *w.buffer[colIdx].(*[]parquet.FixedLenByteArray), defLvls); err != nil {
				return fmt.Errorf("writing uuid column '%s': %w", f.Name, err)
			}
		case LogicalTypeDate:
			if err := writeColumn(cw.(*file.Int32ColumnChunkWriter), *w.buffer[colIdx].(*[]int32), defLvls); err != nil {
				return fmt.Errorf("writing date column '%s': %w", f.Name, err)
			}
		case LogicalTypeTime:
			if err := writeColumn(cw.(*file.Int64ColumnChunkWriter), *w.buffer[colIdx].(*[]int64), defLvls); err != nil {
				return fmt.Errorf("writing time column '%s': %w", f.Name, err)
			}
		case LogicalTypeTimestamp:
			if err := writeColumn(cw.(*file.Int64ColumnChunkWriter), *w.buffer[colIdx].(*[]int64), defLvls); err != nil {
				return fmt.Errorf("writing timestamp column '%s': %w", f.Name, err)
			}
		case LogicalTypeDecimal:
			if err := writeColumn(cw.(*file.FixedLenByteArrayColumnChunkWriter), *w.buffer[colIdx].(*[]parquet.FixedLenByteArray), defLvls); err != nil {
				return fmt.Errorf("writing decimal column '%s': %w", f.Name, err)
			}
		case LogicalTypeInterval:
			if err := writeColumn(cw.(*file.FixedLenByteArrayColumnChunkWriter), *w.buffer[colIdx].(*[]parquet.FixedLenByteArray), defLvls); err != nil {
				return fmt.Errorf("writing interval column '%s': %w", f.Name, err)
			}
		default:
			panic(fmt.Sprintf("attempted to write unknown type of column '%s': %d", f.Name, f.DataType))
		}

		if err := cw.Close(); err != nil {
			return fmt.Errorf("closing column writer: %w", err)
		}
	}

	if err := rgWriter.Close(); err != nil {
		return fmt.Errorf("closing row group writer: %w", err)
	}

	w.scratch.sizeBytes += int(rgWriter.TotalBytesWritten())
	w.scratch.columnChunkCount += len(w.schema)
	for i := range w.buffer {
		resetColumnBuffer(w.buffer[i])
		w.defLevels[i] = w.defLevels[i][:0]
	}
	w.bufferRowCount = 0
	w.bufferSizeBytes = 0

	return nil
}

type parquetValue interface {
	int64 | int32 | float64 | bool | parquet.FixedLenByteArray | parquet.ByteArray
}

// columnBatchWriter is a generic wrapper for writing a batch of values having type T to a column.
// This interface is satisfied by any of the typed column writers from the Apache parquet package.
type columnBatchWriter[T parquetValue] interface {
	// WriteBatch writes a batch of repetition levels, definition levels, and values to the column.
	// We don't currently support nested data structures (typed arrays being the only ones we
	// reasonably could), so repLvls is always nil. The number of values in defLvls must equal the
	// number of conceptual rows that are being written. The vals slice may contain a number of
	// values less than or equal to the number of rows, as null values are omitted. Since we don't
	// currently support nested data structures, a defLvl of 0 means the row at that corresponding
	// position is null, and a defLvl of 1 means that value is not null and its value should be
	// taken from the next value of vals. The returned valuesOffset indicates the number of physical
	// values that were written, and it may be smaller than the number of conceptual rows written.
	WriteBatch(vals []T, defLvls []int16, repLvls []int16) (valueOffset int64, err error)
}

type getValFn[T parquetValue] func(v any) (got T, err error)

// appendVal converts v with getVal and appends the result to the column's typed buffer slice. The
// caller is responsible for handling nil values and maintaining defLevels.
func appendVal[T parquetValue](w *ParquetWriter, colIdx int, v any, getVal getValFn[T]) error {
	got, err := getVal(v)
	if err != nil {
		return err
	}
	sp := w.buffer[colIdx].(*[]T)
	*sp = append(*sp, got)
	return nil
}

func writeColumn[T parquetValue](w columnBatchWriter[T], vals []T, defLevels []int16) error {
	if valuesWritten, err := w.WriteBatch(vals, defLevels, nil); err != nil {
		return fmt.Errorf("writing batch of values: %w", err)
	} else if int(valuesWritten) != len(vals) {
		return fmt.Errorf("written %d values vs. %d values in vals", valuesWritten, len(vals))
	}

	return nil
}

// transferColumnValues merges the row groups of scratch file r into a single row group written to
// w by copying compressed page bytes verbatim. This avoids the per-value decode/re-encode that the
// previous implementation performed, including codec re-compression. Preconditions, all enforced
// at scratch-writer construction in (*ParquetWriter).Write:
//
//   - The scratch writer disables dictionary encoding so we never see a DictionaryPage to splice
//     (arrow-go's column writer cannot import a foreign dictionary page).
//   - The scratch writer's compression codec equals the sink's, so copied page bytes are valid in
//     the sink's column chunks.
//
// w is our own *mergeWriter, which natively accepts pre-encoded data pages and tracks row counts
// explicitly via SetNumRows — no need to bypass arrow-go's *file.Writer with reflect/unsafe.
func transferColumnValues(r *file.Reader, w *mergeWriter) error {
	sch := r.MetaData().Schema
	rgWriter, err := w.AppendRowGroup()
	if err != nil {
		return fmt.Errorf("appending row group: %w", err)
	}

	totalRows := 0
	for rgIdx := 0; rgIdx < r.NumRowGroups(); rgIdx++ {
		totalRows += int(r.RowGroup(rgIdx).NumRows())
	}
	rgWriter.SetNumRows(totalRows)

	for i := 0; i < sch.NumColumns(); i++ {
		cw, err := rgWriter.NextColumn()
		if err != nil {
			return fmt.Errorf("getting next column writer: %w", err)
		}

		for rgIdx := 0; rgIdx < r.NumRowGroups(); rgIdx++ {
			pr, err := r.RowGroup(rgIdx).GetColumnPageReader(i)
			if err != nil {
				return fmt.Errorf("getting page reader for col %d rg %d: %w", i, rgIdx, err)
			}
			for pr.Next() {
				dp, ok := pr.Page().(file.DataPage)
				if !ok {
					return fmt.Errorf("unexpected non-data page in scratch col %d rg %d (%T)", i, rgIdx, pr.Page())
				}
				if err := cw.WriteDataPage(dp); err != nil {
					return fmt.Errorf("writing data page: %w", err)
				}
			}
			if err := pr.Err(); err != nil {
				return fmt.Errorf("page reader error col %d rg %d: %w", i, rgIdx, err)
			}
		}
	}

	if err := rgWriter.Close(); err != nil {
		return fmt.Errorf("closing row group writer: %w", err)
	}

	return nil
}

// The remaining "getXVal" functions are for getting a specifically typed value from the provided
// "any" values, as well as performing any processing necessary on that value to make it suitable
// for storing in a parquet file.

var (
	// Used to verify no overflow when receiving integers encoded as 0
	// fractional part floats.
	minInt64 = big.NewFloat(float64(math.MinInt64))
	maxInt64 = big.NewFloat(float64(math.MaxInt64))
)

func getIntVal(val any) (got int64, err error) {
	switch v := val.(type) {
	case int64:
		got = v
	case int:
		got = int64(v)
	case string:
		// Strings ending in a 0 decimal part like "1.0" or "3.00" are
		// considered valid as integers per JSON specification so we must handle
		// this possibility here. Anything after the decimal is discarded on the
		// assumption that Flow has validated the data and verified that the
		// decimal component is all 0's.
		if idx := strings.Index(v, "."); idx != -1 {
			v = v[:idx]
		}

		if p, parseErr := strconv.Atoi(v); parseErr != nil {
			err = fmt.Errorf("unable to parse string %q as integer: %w", v, parseErr)
		} else {
			got = int64(p)
		}
	case float64:
		if f := big.NewFloat(v); f.Cmp(minInt64) < 0 || f.Cmp(maxInt64) > 0 {
			err = fmt.Errorf("float64 value %f is out of range for int64", v)
		} else {
			got, _ = f.Int64()
		}
	default:
		err = fmt.Errorf("getIntVal unhandled type: %T", v)
	}

	return
}

func getNumberVal(val any) (got float64, err error) {
	switch v := val.(type) {
	case float64:
		got = v
	case float32:
		got = float64(v)
	case int64:
		got = float64(v)
	case string:
		if p, parseErr := strconv.ParseFloat(v, 64); parseErr != nil {
			err = fmt.Errorf("unable to parse string %q as float64: %w", v, parseErr)
		} else {
			got = p
		}
	default:
		err = fmt.Errorf("getNumberVal unhandled type: %T", v)
	}

	return
}

func getBooleanVal(val any) (got bool, err error) {
	switch v := val.(type) {
	case bool:
		got = v
	default:
		err = fmt.Errorf("getBooleanVal unhandled type: %T", v)
	}

	return
}

func getBinaryVal(val any) (got parquet.ByteArray, err error) {
	switch v := val.(type) {
	case string:
		if got, err = base64.StdEncoding.DecodeString(v); err != nil {
			err = fmt.Errorf("unable to parse string %q as binary: %w", v, err)
		}
	default:
		err = fmt.Errorf("getBinaryVal unhandled type: %T", v)
	}

	return
}

func getJsonVal(val any) (got parquet.ByteArray, err error) {
	switch v := val.(type) {
	case []byte:
		got = v
	case json.RawMessage:
		got = []byte(v)
	default:
		got, err = json.Marshal(v)
	}

	return
}

func getStringVal(val any) (got parquet.ByteArray, err error) {
	switch v := val.(type) {
	case string:
		// Safety: This value is immediately written to the output and never
		// modified.
		got = unsafe.Slice(unsafe.StringData(v), len(v))
	case []byte:
		got = v
	case json.RawMessage:
		got = []byte(v)
	case bool:
		got = []byte(strconv.FormatBool(v))
	case int64:
		got = []byte(strconv.Itoa(int(v)))
	case uint64:
		got = []byte(strconv.FormatUint(v, 10))
	case float64:
		got = []byte(strconv.FormatFloat(v, 'f', -1, 64))
	default:
		err = fmt.Errorf("getStringVal unhandled type: %T", v)
	}

	return
}

func getUuidVal(val any) (got parquet.FixedLenByteArray, err error) {
	switch v := val.(type) {
	case string:
		if p, parseErr := uuid.Parse(v); parseErr != nil {
			err = fmt.Errorf("unable to parse string %q as UUID: %w", v, parseErr)
		} else if b, marshalErr := p.MarshalBinary(); marshalErr != nil {
			err = fmt.Errorf("failed to marshal string %q as binary: %w", v, marshalErr)
		} else {
			got = b
		}
	default:
		err = fmt.Errorf("getUuidVal unhandled type: %T", v)
	}

	return
}

func getDateVal(val any) (got int32, err error) {
	switch v := val.(type) {
	case string:
		if d, parseErr := time.Parse(time.DateOnly, v); parseErr != nil {
			err = fmt.Errorf("unable to parse string %q as time.DateOnly: %w", v, parseErr)
		} else {
			unixSeconds := d.Unix()
			unixDays := unixSeconds / 60 / 60 / 24
			got = int32(unixDays)
		}
	default:
		err = fmt.Errorf("getDateVal unhandled type: %T", v)
	}

	return
}

func getTimeVal(val any) (got int64, err error) {
	switch v := val.(type) {
	case string:
		v = strings.Replace(v, "z", "Z", 1)
		if parsed, parseErr := time.Parse("15:04:05.999999999Z07:00", v); parseErr != nil {
			err = fmt.Errorf("unable to parse string %q as time: %w", v, parseErr)
		} else {
			year, month, day := parsed.UTC().Date()
			midnight := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
			got = parsed.UnixMicro() - midnight.UnixMicro()
		}
	default:
		err = fmt.Errorf("getTimeVal unhandled type: %T", v)
	}

	return
}

func getTimestampVal(val any) (got int64, err error) {
	switch v := val.(type) {
	case string:
		v = strings.Replace(v, "z", "Z", 1)
		if d, parseErr := time.Parse(time.RFC3339Nano, v); parseErr != nil {
			err = fmt.Errorf("unable to parse string %q as timestamp: %w", v, parseErr)
		} else {
			got = d.UnixMicro()
		}
	default:
		err = fmt.Errorf("getTimestampVal unhandled type: %T", v)
	}

	return
}

func getIntervalVal(val any) (got parquet.FixedLenByteArray, err error) {
	switch v := val.(type) {
	case string:
		if d, parseErr := iso8601.ParseISO8601(v); parseErr != nil {
			err = fmt.Errorf("unable to parse string %q as ISO8601 duration string: %w", v, parseErr)
		} else {
			months := uint32(d.Y*12 + d.M)
			days := uint32(d.W*7 + d.D)
			millis := uint32(d.TH*60*60*1000 + d.TM*60*1000 + d.TS*1000)

			val := make([]byte, 0, 12)
			val = binary.LittleEndian.AppendUint32(val, months)
			val = binary.LittleEndian.AppendUint32(val, days)
			val = binary.LittleEndian.AppendUint32(val, millis)

			got = val
		}
	default:
		err = fmt.Errorf("getIntervalVal unhandled type: %T", v)
	}

	return
}

func getDecimalVal(val any) (got parquet.FixedLenByteArray, err error) {
	switch v := val.(type) {
	case decimal128.Num:
		got = slices.Grow(got, 16)[:16]
		// Big Endian, 2's compliment encoding.
		binary.BigEndian.PutUint64(got, uint64(v.HighBits()))
		binary.BigEndian.PutUint64(got[8:], v.LowBits())
	default:
		err = fmt.Errorf("getDecimalVal unhandled type: %T", v)
	}

	return
}
