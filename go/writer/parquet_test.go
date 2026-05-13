package writer

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestParquetWriter(t *testing.T) {
	tests := []struct {
		name  string
		nulls bool
		opts  []ParquetOption
	}{
		{
			name:  "required values",
			nulls: false,
			opts:  nil,
		},
		{
			name:  "optional values",
			nulls: true,
			opts:  nil,
		},
		{
			name:  "small row groups",
			nulls: false,
			opts:  []ParquetOption{WithParquetRowGroupRowLimit(1)},
		},
		{
			name:  "dictionary encoding disabled",
			nulls: true,
			opts:  []ParquetOption{WithDisableDictionaryEncoding()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			sink, err := os.CreateTemp(dir, "*.parquet")
			require.NoError(t, err)

			w := NewParquetWriter(sink, makeTestParquetSchema(!tt.nulls), tt.opts...)
			for i := range 10 {
				row := makeTestRow(t, i)
				if tt.nulls {
					row[i] = nil
				}
				require.NoError(t, w.Write(row))
			}
			require.NoError(t, w.Close())

			cupaloy.SnapshotT(t, duckdbReadFile(t, sink.Name(), "JSON"))
		})
	}
}

func TestParquetWriterTimestampNanos(t *testing.T) {
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "tsField", DataType: LogicalTypeTimestampNanos, Required: false},
	}

	tsStrings := []string{
		"2024-01-02T03:04:05.123456789Z",
		"1970-01-01T00:00:00.000000001Z",
		"", // null
	}

	w := NewParquetWriter(sink, sch)
	for _, ts := range tsStrings {
		var v any
		if ts != "" {
			v = ts
		}
		require.NoError(t, w.Write([]any{v}))
	}
	require.NoError(t, w.Close())

	f, err := file.OpenParquetFile(sink.Name(), false)
	require.NoError(t, err)
	defer f.Close()

	require.Equal(t, 1, f.MetaData().Schema.NumColumns())
	col := f.MetaData().Schema.Column(0)
	require.Equal(t, parquet.Types.Int64, col.PhysicalType())
	lt, ok := col.LogicalType().(schema.TimestampLogicalType)
	require.True(t, ok, "expected TimestampLogicalType, got %T", col.LogicalType())
	require.Equal(t, schema.TimeUnitNanos, lt.TimeUnit())

	rg := f.RowGroup(0)
	require.EqualValues(t, len(tsStrings), rg.NumRows())

	r, err := rg.Column(0)
	require.NoError(t, err)
	vals := make([]int64, len(tsStrings))
	def := make([]int16, len(tsStrings))
	_, _, err = r.(*file.Int64ColumnChunkReader).ReadBatch(int64(len(tsStrings)), vals, def, nil)
	require.NoError(t, err)

	require.Equal(t, []int16{1, 1, 0}, def)
	want0, err := time.Parse(time.RFC3339Nano, tsStrings[0])
	require.NoError(t, err)
	require.Equal(t, want0.UnixNano(), vals[0])
	want1, err := time.Parse(time.RFC3339Nano, tsStrings[1])
	require.NoError(t, err)
	require.Equal(t, want1.UnixNano(), vals[1])
}

func TestParquetWriterVariant(t *testing.T) {
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "variantField", DataType: LogicalTypeVariant, Required: false},
	}

	v0, err := variant.New([]byte{0x01, 0x00, 0x00}, []byte{0x0a})
	require.NoError(t, err)
	v1, err := variant.New([]byte{0x01, 0x00, 0x00}, []byte{0x0b, 0x0c})
	require.NoError(t, err)
	rows := []any{v0, v1, nil}

	w := NewParquetWriter(sink, sch)
	for _, v := range rows {
		require.NoError(t, w.Write([]any{v}))
	}
	require.NoError(t, w.Close())

	f, err := file.OpenParquetFile(sink.Name(), false)
	require.NoError(t, err)
	defer f.Close()

	require.Equal(t, 2, f.MetaData().Schema.NumColumns())
	valueCol := f.MetaData().Schema.Column(0)
	require.Equal(t, "value", valueCol.Name())
	require.Equal(t, parquet.Types.ByteArray, valueCol.PhysicalType())
	metaCol := f.MetaData().Schema.Column(1)
	require.Equal(t, "metadata", metaCol.Name())
	require.Equal(t, parquet.Types.ByteArray, metaCol.PhysicalType())

	rg := f.RowGroup(0)
	require.EqualValues(t, len(rows), rg.NumRows())

	readByteArrays := func(col int) ([]parquet.ByteArray, []int16) {
		r, err := rg.Column(col)
		require.NoError(t, err)
		vals := make([]parquet.ByteArray, len(rows))
		def := make([]int16, len(rows))
		_, _, err = r.(*file.ByteArrayColumnChunkReader).ReadBatch(int64(len(rows)), vals, def, nil)
		require.NoError(t, err)
		return vals, def
	}

	valueVals, valueDef := readByteArrays(0)
	metaVals, metaDef := readByteArrays(1)

	require.Equal(t, []int16{1, 1, 0}, valueDef)
	require.Equal(t, []int16{1, 1, 0}, metaDef)
	require.Equal(t, v0.Bytes(), []byte(valueVals[0]))
	require.Equal(t, v1.Bytes(), []byte(valueVals[1]))
	require.Equal(t, v0.Metadata().Bytes(), []byte(metaVals[0]))
	require.Equal(t, v1.Metadata().Bytes(), []byte(metaVals[1]))
}

// This is a regression test for a bug in arrow-go 18.5.2, added test so we
// don't accidentally reintroduce it.  It appears to be fixed in
// 3ad38d03dbc85283f61d60ba2bd8ae8492f0972a.
func TestParquetEOFReadingByteArrayRegression(t *testing.T) {
	field := schema.NewByteArrayNode("binaryField", parquet.Repetitions.Optional, -1)
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{field}, -1)
	require.NoError(t, err)
	sc := schema.NewSchema(root)

	writer := &bytes.Buffer{}

	scratchOpts := []parquet.WriterProperty{
		parquet.WithDictionaryDefault(false),
		parquet.WithStats(false),
	}
	w := file.NewParquetWriter(writer, sc.Root(), file.WithWriterProps(parquet.NewWriterProperties(scratchOpts...)))
	rg := w.AppendRowGroup()

	col, err := rg.NextColumn()
	require.NoError(t, err)

	cw := col.(*file.ByteArrayColumnChunkWriter)

	_, err = cw.WriteBatch(
		[]parquet.ByteArray{
			make([]byte, 1024*1023),
			make([]byte, 1024*1024),
		},
		[]int16{1, 1},
		nil,
	)
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)

	reader := bytes.NewReader(writer.Bytes())
	r, err := file.NewParquetReader(reader)
	require.NoError(t, err)
	rgReader := r.RowGroup(0)
	cr, err := rgReader.Column(0)
	require.NoError(t, err)

	numRows := 2
	vals := make([]parquet.ByteArray, numRows)
	defLvls := make([]int16, numRows)
	_, _, err = cr.(*file.ByteArrayColumnChunkReader).ReadBatch(int64(numRows), vals, defLvls, nil)
	require.NoError(t, err)
}
