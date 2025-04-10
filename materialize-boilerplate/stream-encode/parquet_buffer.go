package stream_encode

import (
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
)

type parquetValueBuffer interface {
	add(value any) error

	writeInt64(*file.Int64ColumnChunkWriter) error
	writeInt32(*file.Int32ColumnChunkWriter) error
	writeFloat(*file.Float64ColumnChunkWriter) error
	writeBool(*file.BooleanColumnChunkWriter) error
	writeBytes(*file.ByteArrayColumnChunkWriter) error
	writeFixedBytes(*file.FixedLenByteArrayColumnChunkWriter) error
}

var _ parquetValueBuffer = (*parquetByteArrayBuffer)(nil)

type parquetByteArrayBuffer struct {
	valueBufferBase

	buf           []byte
	vals          []parquet.ByteArray
	defLevels     []int16
	appendBytesFn appendBytesFn
}

func (b *parquetByteArrayBuffer) add(value any) error {
	if value == nil {
		b.defLevels = append(b.defLevels, 0)
		return nil
	}
	b.defLevels = append(b.defLevels, 1)

	var start = len(b.buf)
	var err error
	if b.buf, err = b.appendBytesFn(b.buf, value); err != nil {
		return err
	}
	b.vals = append(b.vals, b.buf[start:])

	return nil
}

func (b *parquetByteArrayBuffer) writeBytes(w *file.ByteArrayColumnChunkWriter) error {
	if valuesWritten, err := w.WriteBatch(b.vals, b.defLevels, nil); err != nil {
		return fmt.Errorf("writing batch of values: %w", err)
	} else if int(valuesWritten) != len(b.vals) {
		return fmt.Errorf("written %d values vs. %d values in vals", valuesWritten, len(b.vals))
	}
	b.buf = b.buf[:0]
	b.vals = b.vals[:0]
	b.defLevels = b.defLevels[:0]

	return nil
}

var _ parquetValueBuffer = (*parquetFixedLenByteArrayBuffer)(nil)

type parquetFixedLenByteArrayBuffer struct {
	valueBufferBase

	buf           []byte
	vals          []parquet.FixedLenByteArray
	defLevels     []int16
	appendBytesFn appendBytesFn
}

func (b *parquetFixedLenByteArrayBuffer) add(value any) error {
	if value == nil {
		b.defLevels = append(b.defLevels, 0)
		return nil
	}
	b.defLevels = append(b.defLevels, 1)

	var start = len(b.buf)
	var err error
	if b.buf, err = b.appendBytesFn(b.buf, value); err != nil {
		return err
	}
	b.vals = append(b.vals, b.buf[start:])

	return nil
}

func (b *parquetFixedLenByteArrayBuffer) writeFixedBytes(w *file.FixedLenByteArrayColumnChunkWriter) error {
	if valuesWritten, err := w.WriteBatch(b.vals, b.defLevels, nil); err != nil {
		return fmt.Errorf("writing batch of values: %w", err)
	} else if int(valuesWritten) != len(b.vals) {
		return fmt.Errorf("written %d values vs. %d values in vals", valuesWritten, len(b.vals))
	}
	b.buf = b.buf[:0]
	b.vals = b.vals[:0]
	b.defLevels = b.defLevels[:0]

	return nil
}

var _ parquetValueBuffer = (*parquetInt64Buffer)(nil)

type parquetInt64Buffer struct {
	valueBufferBase

	vals           []int64
	defLevels      []int16
	getScalarValFn getScalarValFn[int64]
}

func (b *parquetInt64Buffer) add(value any) error {
	if value == nil {
		b.defLevels = append(b.defLevels, 0)
		return nil
	}
	b.defLevels = append(b.defLevels, 1)

	got, err := b.getScalarValFn(value)
	if err != nil {
		return err
	}
	b.vals = append(b.vals, got)

	return nil
}

func (b *parquetInt64Buffer) writeInt64(w *file.Int64ColumnChunkWriter) error {
	if valuesWritten, err := w.WriteBatch(b.vals, b.defLevels, nil); err != nil {
		return fmt.Errorf("writing batch of values: %w", err)
	} else if int(valuesWritten) != len(b.vals) {
		return fmt.Errorf("written %d values vs. %d values in vals", valuesWritten, len(b.vals))
	}
	b.vals = b.vals[:0]
	b.defLevels = b.defLevels[:0]

	return nil
}

var _ parquetValueBuffer = (*parquetInt32Buffer)(nil)

type parquetInt32Buffer struct {
	valueBufferBase

	vals           []int32
	defLevels      []int16
	getScalarValFn getScalarValFn[int32]
}

func (b *parquetInt32Buffer) add(value any) error {
	if value == nil {
		b.defLevels = append(b.defLevels, 0)
		return nil
	}
	b.defLevels = append(b.defLevels, 1)

	got, err := b.getScalarValFn(value)
	if err != nil {
		return err
	}
	b.vals = append(b.vals, got)

	return nil
}

func (b *parquetInt32Buffer) writeInt32(w *file.Int32ColumnChunkWriter) error {
	if valuesWritten, err := w.WriteBatch(b.vals, b.defLevels, nil); err != nil {
		return fmt.Errorf("writing batch of values: %w", err)
	} else if int(valuesWritten) != len(b.vals) {
		return fmt.Errorf("written %d values vs. %d values in vals", valuesWritten, len(b.vals))
	}
	b.vals = b.vals[:0]
	b.defLevels = b.defLevels[:0]

	return nil
}

var _ parquetValueBuffer = (*parquetFloatBuffer)(nil)

type parquetFloatBuffer struct {
	valueBufferBase

	vals           []float64
	defLevels      []int16
	getScalarValFn getScalarValFn[float64]
}

func (b *parquetFloatBuffer) add(value any) error {
	if value == nil {
		b.defLevels = append(b.defLevels, 0)
		return nil
	}
	b.defLevels = append(b.defLevels, 1)

	got, err := b.getScalarValFn(value)
	if err != nil {
		return err
	}
	b.vals = append(b.vals, got)

	return nil
}

func (b *parquetFloatBuffer) writeFloat(w *file.Float64ColumnChunkWriter) error {
	if valuesWritten, err := w.WriteBatch(b.vals, b.defLevels, nil); err != nil {
		return fmt.Errorf("writing batch of values: %w", err)
	} else if int(valuesWritten) != len(b.vals) {
		return fmt.Errorf("written %d values vs. %d values in vals", valuesWritten, len(b.vals))
	}
	b.vals = b.vals[:0]
	b.defLevels = b.defLevels[:0]

	return nil
}

var _ parquetValueBuffer = (*parquetBoolBuffer)(nil)

type parquetBoolBuffer struct {
	valueBufferBase

	vals           []bool
	defLevels      []int16
	getScalarValFn getScalarValFn[bool]
}

func (b *parquetBoolBuffer) add(value any) error {
	if value == nil {
		b.defLevels = append(b.defLevels, 0)
		return nil
	}
	b.defLevels = append(b.defLevels, 1)

	got, err := b.getScalarValFn(value)
	if err != nil {
		return err
	}
	b.vals = append(b.vals, got)

	return nil
}

func (b *parquetBoolBuffer) writeBool(w *file.BooleanColumnChunkWriter) error {
	if valuesWritten, err := w.WriteBatch(b.vals, b.defLevels, nil); err != nil {
		return fmt.Errorf("writing batch of values: %w", err)
	} else if int(valuesWritten) != len(b.vals) {
		return fmt.Errorf("written %d values vs. %d values in vals", valuesWritten, len(b.vals))
	}
	b.vals = b.vals[:0]
	b.defLevels = b.defLevels[:0]

	return nil
}

func makeBuffer(e ParquetSchemaElement) parquetValueBuffer {
	var out parquetValueBuffer

	switch e.DataType {
	case PrimitiveTypeInteger:
		out = &parquetInt64Buffer{getScalarValFn: getIntVal}
	case PrimitiveTypeNumber:
		out = &parquetFloatBuffer{getScalarValFn: getNumberVal}
	case PrimitiveTypeBoolean:
		out = &parquetBoolBuffer{getScalarValFn: getBooleanVal}
	case PrimitiveTypeBinary:
		out = &parquetByteArrayBuffer{appendBytesFn: appendBinaryVal}
	case LogicalTypeString:
		out = &parquetByteArrayBuffer{appendBytesFn: appendStringVal}
	case LogicalTypeJson:
		out = &parquetByteArrayBuffer{appendBytesFn: appendJsonVal}
	case LogicalTypeUuid:
		out = &parquetFixedLenByteArrayBuffer{appendBytesFn: appendUuidVal}
	case LogicalTypeDate:
		out = &parquetInt32Buffer{getScalarValFn: getDateVal}
	case LogicalTypeTime:
		out = &parquetInt64Buffer{getScalarValFn: getTimeVal}
	case LogicalTypeTimestamp:
		out = &parquetInt64Buffer{getScalarValFn: getTimestampVal}
	case LogicalTypeInterval:
		out = &parquetFixedLenByteArrayBuffer{appendBytesFn: appendIntervalVal}
	default:
		panic(fmt.Sprintf("makeBuffer unknown type: %d", e.DataType))
	}

	return out
}

var _ parquetValueBuffer = (*valueBufferBase)(nil)

type valueBufferBase struct{}

func (v *valueBufferBase) add(any) error                                     { panic("unimplemented") }
func (v *valueBufferBase) writeBool(*file.BooleanColumnChunkWriter) error    { panic("unimplemented") }
func (v *valueBufferBase) writeBytes(*file.ByteArrayColumnChunkWriter) error { panic("unimplemented") }
func (v *valueBufferBase) writeFixedBytes(*file.FixedLenByteArrayColumnChunkWriter) error {
	panic("unimplemented")
}
func (v *valueBufferBase) writeFloat(*file.Float64ColumnChunkWriter) error { panic("unimplemented") }
func (v *valueBufferBase) writeInt32(*file.Int32ColumnChunkWriter) error   { panic("unimplemented") }
func (v *valueBufferBase) writeInt64(*file.Int64ColumnChunkWriter) error   { panic("unimplemented") }
