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

func TestParquetWriterTimestampNanos(t *testing.T) {
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "tsField", DataType: LogicalTypeTimestampNanos, Required: false},
	}

	rows := []string{"2024-01-02T03:04:05.123456789Z", "1970-01-01T00:00:00.000000001Z", ""}

	w := NewParquetWriter(sink, sch)
	for _, ts := range rows {
		var val any
		if ts != "" {
			val = ts
		}
		require.NoError(t, w.Write([]any{val}))
	}
	require.NoError(t, w.Close())

	f, err := file.OpenParquetFile(sink.Name(), false)
	require.NoError(t, err)
	defer f.Close()

	require.Equal(t, 1, f.MetaData().Schema.NumColumns())

	col := f.MetaData().Schema.Column(0)
	require.Equal(t, parquet.Types.Int64, col.PhysicalType())
	tsLogical, ok := col.LogicalType().(schema.TimestampLogicalType)
	require.True(t, ok)
	require.Equal(t, schema.TimeUnitNanos, tsLogical.TimeUnit())

	rg := f.RowGroup(0)
	require.EqualValues(t, len(rows), rg.NumRows())

	cr, err := rg.Column(0)
	require.NoError(t, err)
	vals := make([]int64, len(rows))
	def := make([]int16, len(rows))
	_, _, err = cr.(*file.Int64ColumnChunkReader).ReadBatch(int64(len(rows)), vals, def, nil)
	require.NoError(t, err)

	require.Equal(t, int16(1), def[0])
	require.Equal(t, int16(1), def[1])
	require.Equal(t, int16(0), def[2])

	wantTs0, err := time.Parse(time.RFC3339Nano, rows[0])
	require.NoError(t, err)
	require.Equal(t, wantTs0.UnixNano(), vals[0])

	wantTs1, err := time.Parse(time.RFC3339Nano, rows[1])
	require.NoError(t, err)
	require.Equal(t, wantTs1.UnixNano(), vals[1])
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
