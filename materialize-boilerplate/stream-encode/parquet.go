package stream_encode

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/google/uuid"
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
// buffered in the ParquetEncoder's `buffer` slice. These estimates are used to bound how often a
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
		case PrimitiveTypeBinary, LogicalTypeJson:
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
			panic(fmt.Sprintf("estSize unknown type: %T", v))
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
	compression       ParquetCompression
	rowGroupRowLimit  int
	rowGroupByteLimit int
}

type ParquetEncoder struct {
	cfg parquetConfig

	schema     ParquetSchema
	schemaRoot *schema.GroupNode
	sinkWriter *file.Writer
	cwc        *countingWriteCloser
	rs         rowSize

	// Scratch values are re-initialized as scratch files are transposed into the output stream.
	scratch struct {
		file             *os.File
		writer           *file.Writer
		sizeBytes        int
		rowCount         int
		columnChunkCount int
	}

	// buffer contains rows of values that are pending to be written as a row group to the scratch
	// file when bufferSizeBytes exceeds the threshold.
	buffer          [][]any
	bufferSizeBytes int
}

type ParquetOption func(*parquetConfig)

func WithParquetCompression(c ParquetCompression) ParquetOption {
	return func(cfg *parquetConfig) {
		cfg.compression = c
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

func NewParquetEncoder(w io.WriteCloser, sch ParquetSchema, opts ...ParquetOption) *ParquetEncoder {
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

	return &ParquetEncoder{
		cfg:        cfg,
		schema:     sch,
		schemaRoot: schemaRoot,
		sinkWriter: file.NewParquetWriter(cwc, schemaRoot, writerOpts(cfg)),
		cwc:        cwc,
		rs:         newRowSizing(sch),
	}
}

func writerOpts(cfg parquetConfig) file.WriteOption {
	propOpts := []parquet.WriterProperty{}

	switch cfg.compression {
	case Uncompressed:
		propOpts = append(propOpts, parquet.WithCompression(compress.Codecs.Uncompressed))
	case Snappy:
		propOpts = append(propOpts, parquet.WithCompression(compress.Codecs.Snappy))
	case Gzip:
		propOpts = append(propOpts, parquet.WithCompression(compress.Codecs.Gzip))
	default:
		panic(fmt.Sprintf("unknown compression setting: %d", cfg.compression))
	}

	return file.WithWriterProps(parquet.NewWriterProperties(propOpts...))
}

// Encode encodes a row of data by buffering it in the encoder'ss buffer, and if thresholds are
// exceed writing the buffer as a row group to the scratch file, and potentially flushing the row
// groups from the scratch file collectively as a single row group to the output.
func (e *ParquetEncoder) Encode(row []any) error {
	var err error

	if e.scratch.writer == nil {
		// Either the very first row, or the first one after flushing the scratch file.
		if e.scratch.file, err = os.CreateTemp("", "parquet-scratch-*"); err != nil {
			return fmt.Errorf("encode creating scratch file: %W", err)
		}
		e.scratch.writer = file.NewParquetWriter(e.scratch.file, e.schemaRoot)
	}

	e.buffer = append(e.buffer, row)
	e.bufferSizeBytes += e.rs.estSize(row)
	e.scratch.rowCount += 1

	if e.bufferSizeBytes >= maxBufferSize {
		// Write out the buffer as a single row group to the scratch file.
		if err := e.flushBuffer(); err != nil {
			return fmt.Errorf("encode flushing buffer based on buffer size: %W", err)
		}
	}

	if e.scratch.sizeBytes >= e.cfg.rowGroupByteLimit ||
		e.scratch.rowCount >= e.cfg.rowGroupRowLimit ||
		e.scratch.columnChunkCount >= maxScratchColumnChunkCount {
		if e.scratch.columnChunkCount >= maxScratchColumnChunkCount {
			log.WithFields(log.Fields{
				"scratchSizeBytes":        e.scratch.sizeBytes,
				"scratchRowCount":         e.scratch.rowCount,
				"scratchColumnChunkCount": e.scratch.columnChunkCount,
			}).Debug("flushing scratch file based on column chunk count")
		}

		if err := e.flushBuffer(); err != nil {
			// Still might need to flush the buffer based on row count.
			return fmt.Errorf("encode flushing buffer based on scratch file size: %w", err)
		} else if err := e.flushScratchFile(); err != nil {
			return fmt.Errorf("encode flushing scratch file based on scratch file size: %w", err)
		}
	}

	return nil
}

// Written returns the number of bytes written to the output writer. This value increases only as
// row groups from the scratch file are flushed to the output, which happens whenever there is
// enough data (either by rows or bytes) in the scratch file to fill a complete row group.
func (e *ParquetEncoder) Written() int {
	return e.cwc.written
}

// Close flushes the buffered rows and scratch file, and closes the output writer.
func (e *ParquetEncoder) Close() error {
	if err := e.flushBuffer(); err != nil {
		return fmt.Errorf("flushing buffer: %w", err)
	} else if err := e.flushScratchFile(); err != nil {
		return fmt.Errorf("flushing scratch file: %w", err)
	} else if err := e.sinkWriter.Close(); err != nil { // also closes the underlying io.WriteCloser
		return fmt.Errorf("closing sink: %w", err)
	}
	return nil
}

func (e *ParquetEncoder) flushScratchFile() error {
	if e.scratch.writer == nil {
		return nil
	}

	if err := e.scratch.writer.Close(); err != nil { // also closes the underlying io.WriteCloser
		return fmt.Errorf("closing scratch writer: %w", err)
	} else if sr, err := os.Open(e.scratch.file.Name()); err != nil {
		return fmt.Errorf("opening scratch file to transfer values: %w", err)
	} else if scratchReader, err := file.NewParquetReader(sr); err != nil {
		return fmt.Errorf("creating scratch reader: %w", err)
	} else if err := transferColumnValues(scratchReader, e.sinkWriter); err != nil {
		return fmt.Errorf("transferring column values: %w", err)
	} else if err := sr.Close(); err != nil {
		return fmt.Errorf("closing scratch file after reading: %w", err)
	} else if err := os.Remove(e.scratch.file.Name()); err != nil {
		return fmt.Errorf("removing scratch file: %w", err)
	}

	e.scratch.file = nil
	e.scratch.writer = nil
	e.scratch.sizeBytes = 0
	e.scratch.columnChunkCount = 0
	e.scratch.rowCount = 0

	return nil
}

// flushBuffer writes the current buffered rows as a single row group to the scratch file.
func (e *ParquetEncoder) flushBuffer() error {
	if len(e.buffer) == 0 {
		return nil
	}

	rgWriter := e.scratch.writer.AppendRowGroup()

	// Transpose the buffered rows into the scratch file row group by writing them column-by-column.
	for colIdx, f := range e.schema {
		cw, err := rgWriter.NextColumn()
		if err != nil {
			return fmt.Errorf("getting next column: %w", err)
		}

		switch f.DataType {
		case PrimitiveTypeInteger:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.Int64ColumnChunkWriter), getIntVal); err != nil {
				return fmt.Errorf("writing integer column: %w", err)
			}
		case PrimitiveTypeNumber:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.Float64ColumnChunkWriter), getNumberVal); err != nil {
				return fmt.Errorf("writing number column: %w", err)
			}
		case PrimitiveTypeBoolean:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.BooleanColumnChunkWriter), getBooleanVal); err != nil {
				return fmt.Errorf("writing boolean column: %w", err)
			}
		case PrimitiveTypeBinary:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.ByteArrayColumnChunkWriter), getBinaryVal); err != nil {
				return fmt.Errorf("writing byte array column: %w", err)
			}
		case LogicalTypeJson:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.ByteArrayColumnChunkWriter), getJsonVal); err != nil {
				return fmt.Errorf("writing byte array (json) column: %w", err)
			}
		case LogicalTypeString:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.ByteArrayColumnChunkWriter), getStringVal); err != nil {
				return fmt.Errorf("writing byte array (string) column: %w", err)
			}
		case LogicalTypeUuid:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.FixedLenByteArrayColumnChunkWriter), getUuidVal); err != nil {
				return fmt.Errorf("writing uuid column: %w", err)
			}
		case LogicalTypeDate:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.Int32ColumnChunkWriter), getDateVal); err != nil {
				return fmt.Errorf("writing date column: %w", err)
			}
		case LogicalTypeTime:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.Int64ColumnChunkWriter), getTimeVal); err != nil {
				return fmt.Errorf("writing time column: %w", err)
			}
		case LogicalTypeTimestamp:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.Int64ColumnChunkWriter), getTimestampVal); err != nil {
				return fmt.Errorf("writing timestamp column: %w", err)
			}
		case LogicalTypeInterval:
			if err := writeColumn(colIdx, e.buffer, cw.(*file.FixedLenByteArrayColumnChunkWriter), getIntervalVal); err != nil {
				return fmt.Errorf("writing interval column: %w", err)
			}
		default:
			panic(fmt.Sprintf("attempted to encode unknown type: %d", f.DataType))
		}

		if err := cw.Close(); err != nil {
			return fmt.Errorf("closing column writer: %w", err)
		}
	}

	if err := rgWriter.Close(); err != nil {
		return fmt.Errorf("closing row group writer: %w", err)
	}

	e.scratch.sizeBytes += int(rgWriter.TotalBytesWritten())
	e.scratch.columnChunkCount += len(e.schema)
	e.buffer = e.buffer[:0]
	e.bufferSizeBytes = 0

	return nil
}

type parquetValue interface {
	int64 | int32 | float64 | bool | parquet.FixedLenByteArray | parquet.ByteArray
}

// columnBatchReader is a generic wrapper for reading a batch of values having type T from a column.
// This interface is satisfied by any of the typed column readers from the Apache parquet package.
type columnBatchReader[T parquetValue] interface {
	// ReadBatch reads batchSize values from the column. values must be large enough to hold the
	// number of values that will be read. defLvls and repLvls will be populated if not nil; they
	// are not inputs to the operation but their populated values are necessary for interpreting the
	// data read into values. total is the number of rows that were read; valuesRead is the actual
	// number of physical values that were read excluding nulls.
	ReadBatch(batchSize int64, values []T, defLvls []int16, repLvls []int16) (total int64, valuesRead int, err error)
}

// columnBatchWriter is a generic wrapper for writing a batch of values having type T to a column.
// This interface is satisfied by any of the typed column writers from the Apache parquet package.
type columnBatchWriter[T parquetValue] interface {
	// WriteBatch writes a batch of repetition levels, definition levels, and values to the column.
	// We don't currently support nested data structures (typed arrays being the only ones we
	// reasonably could), so repLvls is always nil. The number of values in defLvls must equal the
	// number of conceptual rows that are being written. The vals slice may contain a number of
	// values less than or equal to the number of rows, as null values are omitted. Since we don't
	// currently support nested data structures, a repLvl of 0 means the row at that corresponding
	// position is null, and a repLvl of 1 means that value is not null and its value should be
	// taken from the next value of vals. The returned valuesOffset indicates the number of physical
	// values that were written, and it may be smaller than the number of conceptual rows written.
	WriteBatch(vals []T, defLvls []int16, repLvls []int16) (valueOffset int64, err error)
}

type getValFn[T parquetValue] func(v any) (got T, err error)

func writeColumn[T parquetValue](
	colIdx int,
	buf [][]any,
	w columnBatchWriter[T],
	getVal getValFn[T],
) error {
	var vals []T
	var defLevels []int16

	for _, row := range buf {
		v := row[colIdx]
		switch tv := v.(type) {
		case nil:
			defLevels = append(defLevels, 0)
		default:
			got, err := getVal(tv)
			if err != nil {
				return fmt.Errorf("getting typed value for value: %w (type %T)", err, tv)
			}

			vals = append(vals, got)
			defLevels = append(defLevels, 1)
		}
	}

	if valuesWritten, err := w.WriteBatch(vals, defLevels, nil); err != nil {
		return fmt.Errorf("writing batch of values: %w", err)
	} else if int(valuesWritten) != len(vals) {
		return fmt.Errorf("written %d values vs. %d values in vals", valuesWritten, len(vals))
	}

	return nil
}

// transferColumnValues reads columns from the scratch file r and writes the values to w. The
// scratch file may contain many row groups, and the corresponding column from each row group is
// read and written in order to combine all of the row groups from the scratch file into a single
// row group written to w.
func transferColumnValues(r *file.Reader, w *file.Writer) error {
	sch := r.MetaData().Schema
	rgWriter := w.AppendRowGroup()

	for c := 0; c < sch.NumColumns(); c++ {
		cw, err := rgWriter.NextColumn()
		if err != nil {
			return fmt.Errorf("getting next column writer: %w", err)
		}

		for rgIdx := 0; rgIdx < r.NumRowGroups(); rgIdx++ {
			rgReader := r.RowGroup(rgIdx)
			n := int(rgReader.NumRows())

			cr, err := rgReader.Column(c)
			if err != nil {
				return fmt.Errorf("getting next column reader: %w", err)
			}

			switch cw := cw.(type) {
			case *file.FixedLenByteArrayColumnChunkWriter:
				if err := doTransfer(n, cr.(*file.FixedLenByteArrayColumnChunkReader), cw); err != nil {
					return fmt.Errorf("transferring fixed length byte array column: %w", err)
				}
			case *file.Float64ColumnChunkWriter:
				if err := doTransfer(n, cr.(*file.Float64ColumnChunkReader), cw); err != nil {
					return fmt.Errorf("transferring float64 column: %w", err)
				}
			case *file.ByteArrayColumnChunkWriter:
				if err := doTransfer(n, cr.(*file.ByteArrayColumnChunkReader), cw); err != nil {
					return fmt.Errorf("transferring byte array column: %w", err)
				}
			case *file.Int32ColumnChunkWriter:
				if err := doTransfer(n, cr.(*file.Int32ColumnChunkReader), cw); err != nil {
					return fmt.Errorf("transferring int32 column: %w", err)
				}
			case *file.Int64ColumnChunkWriter:
				if err := doTransfer(n, cr.(*file.Int64ColumnChunkReader), cw); err != nil {
					return fmt.Errorf("transferring int64 column: %w", err)
				}
			case *file.BooleanColumnChunkWriter:
				if err := doTransfer(n, cr.(*file.BooleanColumnChunkReader), cw); err != nil {
					return fmt.Errorf("transferring boolean column: %w", err)
				}
			default:
				return fmt.Errorf("unhandled physical type %q (writer type %T)", sch.Column(c).PhysicalType(), cw)
			}
		}
	}

	if err := rgWriter.Close(); err != nil {
		return fmt.Errorf("closing row group writer: %w", err)
	}

	return nil
}

func doTransfer[T parquetValue](numRows int, r columnBatchReader[T], w columnBatchWriter[T]) error {
	vals := make([]T, numRows)
	defLvls := make([]int16, numRows)
	rowsRead, valuesRead, err := r.ReadBatch(int64(numRows), vals, defLvls, nil)
	if err != nil {
		return fmt.Errorf("reading batch: %w", err)
	}

	vals = vals[:valuesRead]

	if int(rowsRead) != numRows {
		return fmt.Errorf("read %d rows vs. expected %d", rowsRead, numRows)
	} else if valuesWritten, err := w.WriteBatch(vals, defLvls, nil); err != nil {
		return fmt.Errorf("writing batch of values: %w", err)
	} else if int(valuesWritten) != len(vals) {
		return fmt.Errorf("written %d values vs. %d values in vals", valuesWritten, len(vals))
	}

	return nil
}

// The remaining "getXVal" functions are for getting a specifically typed value from the provided
// "any" values, as well as performing any processing necessary on that value to make it suitable
// for storing in a parquet file.

func getIntVal(val any) (got int64, err error) {
	switch v := val.(type) {
	case int64:
		got = v
	case int:
		got = int64(v)
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
	default:
		got, err = json.Marshal(v)
	}

	return
}

func getStringVal(val any) (got parquet.ByteArray, err error) {
	switch v := val.(type) {
	case string:
		got = []byte(v)
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
