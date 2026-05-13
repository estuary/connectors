package writer

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
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

func TestParquetWriterTimestampNanosAndVariant(t *testing.T) {
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "tsField", DataType: LogicalTypeTimestampNanos, Required: false},
		{Name: "variantField", DataType: LogicalTypeVariant, Required: false},
	}

	rows := []struct {
		ts      string
		variant *VariantValue
	}{
		{"2024-01-02T03:04:05.123456789Z", &VariantValue{Value: []byte{0x0a}, Metadata: []byte{0x01, 0x00, 0x00}}},
		{"1970-01-01T00:00:00.000000001Z", &VariantValue{Value: []byte{0x0b, 0x0c}, Metadata: []byte{0x01, 0x00, 0x00}}},
		{"", nil},
	}

	w := NewParquetWriter(sink, sch)
	for _, r := range rows {
		var tsVal any
		if r.ts != "" {
			tsVal = r.ts
		}
		var varVal any
		if r.variant != nil {
			varVal = *r.variant
		}
		require.NoError(t, w.Write([]any{tsVal, varVal}))
	}
	require.NoError(t, w.Close())

	f, err := file.OpenParquetFile(sink.Name(), false)
	require.NoError(t, err)
	defer f.Close()

	// The variant group adds a column; the schema root contains both leaf columns of the variant
	// plus the timestamp.
	require.Equal(t, 3, f.MetaData().Schema.NumColumns())

	tsCol := f.MetaData().Schema.Column(0)
	require.Equal(t, parquet.Types.Int64, tsCol.PhysicalType())
	tsLogical, ok := tsCol.LogicalType().(schema.TimestampLogicalType)
	require.True(t, ok, "expected TimestampLogicalType, got %T", tsCol.LogicalType())
	require.Equal(t, schema.TimeUnitNanos, tsLogical.TimeUnit())

	// Variant group is reached via its leaf columns; the parent is the variant group node.
	variantValueCol := f.MetaData().Schema.Column(1)
	require.Equal(t, "value", variantValueCol.Name())
	require.Equal(t, parquet.Types.ByteArray, variantValueCol.PhysicalType())
	variantMetaCol := f.MetaData().Schema.Column(2)
	require.Equal(t, "metadata", variantMetaCol.Name())
	require.Equal(t, parquet.Types.ByteArray, variantMetaCol.PhysicalType())

	rg := f.RowGroup(0)
	require.EqualValues(t, len(rows), rg.NumRows())

	tsReader, err := rg.Column(0)
	require.NoError(t, err)
	tsVals := make([]int64, len(rows))
	tsDef := make([]int16, len(rows))
	_, _, err = tsReader.(*file.Int64ColumnChunkReader).ReadBatch(int64(len(rows)), tsVals, tsDef, nil)
	require.NoError(t, err)

	// First two rows have non-null timestamps; third row is null.
	require.Equal(t, int16(1), tsDef[0])
	require.Equal(t, int16(1), tsDef[1])
	require.Equal(t, int16(0), tsDef[2])

	wantTs0, err := time.Parse(time.RFC3339Nano, rows[0].ts)
	require.NoError(t, err)
	require.Equal(t, wantTs0.UnixNano(), tsVals[0])
	wantTs1, err := time.Parse(time.RFC3339Nano, rows[1].ts)
	require.NoError(t, err)
	require.Equal(t, wantTs1.UnixNano(), tsVals[1])

	readByteArrays := func(col int) ([]parquet.ByteArray, []int16) {
		r, err := rg.Column(col)
		require.NoError(t, err)
		vals := make([]parquet.ByteArray, len(rows))
		def := make([]int16, len(rows))
		_, _, err = r.(*file.ByteArrayColumnChunkReader).ReadBatch(int64(len(rows)), vals, def, nil)
		require.NoError(t, err)
		return vals, def
	}

	valueVals, valueDef := readByteArrays(1)
	metaVals, metaDef := readByteArrays(2)

	require.Equal(t, []int16{1, 1, 0}, valueDef)
	require.Equal(t, []int16{1, 1, 0}, metaDef)
	require.Equal(t, []byte(rows[0].variant.Value), []byte(valueVals[0]))
	require.Equal(t, []byte(rows[1].variant.Value), []byte(valueVals[1]))
	require.Equal(t, []byte(rows[0].variant.Metadata), []byte(metaVals[0]))
	require.Equal(t, []byte(rows[1].variant.Metadata), []byte(metaVals[1]))
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
