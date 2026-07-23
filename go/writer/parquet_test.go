package writer

import (
	"bytes"
	"fmt"
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

			w, err := NewParquetWriter(sink, makeTestParquetSchema(!tt.nulls), tt.opts...)
			require.NoError(t, err)
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

// errOnFirstWriteCloser fails its very first Write, simulating a transient
// failure on the sink (e.g. a broken pipe to a cloud-storage upload) at the
// point where the parquet magic header is written.
type errOnFirstWriteCloser struct {
	wrote bool
}

func (w *errOnFirstWriteCloser) Write(p []byte) (int, error) {
	if !w.wrote {
		w.wrote = true
		return 0, fmt.Errorf("simulated transient write failure")
	}
	return len(p), nil
}

func (w *errOnFirstWriteCloser) Close() error { return nil }

// A sink that fails its initial write (writing the parquet magic header) must
// surface a returned error rather than panicking, so callers can handle it as
// a normal, retryable failure. Regression test for the "failed to write magic
// number" panic.
func TestParquetWriterSinkWriteErrorReturnsError(t *testing.T) {
	sch := ParquetSchema{
		{Name: "field", DataType: LogicalTypeString, Required: false},
	}

	w, err := NewParquetWriter(&errOnFirstWriteCloser{}, sch)
	require.Error(t, err)
	require.Nil(t, w)
}

func TestParquetWriterTimestampNanos(t *testing.T) {
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "tsField", DataType: LogicalTypeTimestampNanos, Required: false},
	}

	rows := []string{"2024-01-02T03:04:05.123456789Z", "1970-01-01T00:00:00.000000001Z", ""}

	w, err := NewParquetWriter(sink, sch)
	require.NoError(t, err)
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

func TestParquetWriterVariant(t *testing.T) {
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "idField", DataType: PrimitiveTypeInteger, Required: true, FieldId: ptrTo(int32(1))},
		{Name: "variantField", DataType: LogicalTypeVariant, Required: false, FieldId: ptrTo(int32(2))},
		{Name: "reqVariantField", DataType: LogicalTypeVariant, Required: true, FieldId: ptrTo(int32(3))},
		{Name: "strField", DataType: LogicalTypeString, Required: false, FieldId: ptrTo(int32(4))},
	}

	variantVals := []any{
		[]byte(`{"a": 1, "b": {"c": [1, 2, "three"], "d": null}}`),
		[]byte(`[1, "two", 3.5, false, null, {"nested": true}]`),
		[]byte(`"just a string"`),
		[]byte(`12345.678`),
		[]byte(`true`),
		[]byte(`null`),
		nil,
		[]byte(`9223372036854775807`),
		map[string]any{"marshalled": true},
	}

	// A small row group limit exercises the scratch file flush and transfer
	// paths with multiple row groups.
	w, err := NewParquetWriter(sink, sch, WithParquetRowGroupRowLimit(4))
	require.NoError(t, err)
	for i, v := range variantVals {
		row := []any{i, v, fmt.Appendf(nil, `{"seq": %d}`, i), fmt.Sprintf("str_%d", i)}
		require.NoError(t, w.Write(row))
	}
	require.NoError(t, w.Close())

	f, err := file.OpenParquetFile(sink.Name(), false)
	require.NoError(t, err)
	defer f.Close()

	fileSchema := f.MetaData().Schema
	require.Equal(t, 4, fileSchema.Root().NumFields())
	require.Equal(t, 6, fileSchema.NumColumns())

	for idx, want := range []struct {
		name    string
		rep     parquet.Repetition
		fieldId int32
	}{
		{name: "variantField", rep: parquet.Repetitions.Optional, fieldId: 2},
		{name: "reqVariantField", rep: parquet.Repetitions.Required, fieldId: 3},
	} {
		node := fileSchema.Root().Field(idx + 1)
		gn, ok := node.(*schema.GroupNode)
		require.True(t, ok, "expected *schema.GroupNode for %q, got %T", want.name, node)
		require.Equal(t, want.name, gn.Name())
		require.Equal(t, want.rep, gn.RepetitionType())
		require.Equal(t, want.fieldId, gn.FieldID())
		_, ok = gn.LogicalType().(schema.VariantLogicalType)
		require.True(t, ok, "expected VariantLogicalType, got %T", gn.LogicalType())
	}

	require.Equal(t, "variantField.metadata", fileSchema.Column(1).Path())
	require.Equal(t, "variantField.value", fileSchema.Column(2).Path())
	require.Equal(t, "reqVariantField.metadata", fileSchema.Column(3).Path())
	require.Equal(t, "reqVariantField.value", fileSchema.Column(4).Path())

	for rgIdx := 0; rgIdx < f.NumRowGroups(); rgIdx++ {
		rgMeta := f.MetaData().RowGroup(rgIdx)
		for _, colIdx := range []int{1, 2, 3, 4} {
			cc, err := rgMeta.ColumnChunk(colIdx)
			require.NoError(t, err)
			statsSet, err := cc.StatsSet()
			require.NoError(t, err)
			require.False(t, statsSet, "variant leaf column %d must not embed binary column statistics", colIdx)
		}

		cc, err := rgMeta.ColumnChunk(5)
		require.NoError(t, err)
		statsSet, err := cc.StatsSet()
		require.NoError(t, err)
		require.True(t, statsSet, "statistics must remain enabled for non-variant columns")
	}

	// Read the file back with an independent parquet variant reader to prove
	// the encoding is accepted outside of arrow-go.
	cupaloy.SnapshotT(t, duckdbDockerQueryFile(t, sink.Name(),
		"SELECT idField, variantField::JSON AS variantField, reqVariantField::JSON AS reqVariantField, strField FROM read_parquet('%s') ORDER BY idField"))
}

func TestParquetWriterVariantInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "variantField", DataType: LogicalTypeVariant, Required: false},
	}

	w, err := NewParquetWriter(sink, sch)
	require.NoError(t, err)
	require.NoError(t, w.Write([]any{[]byte(`{"unterminated": `)}))

	// The row is buffered on Write, so the encoding error surfaces when the
	// buffer is flushed on Close.
	err = w.Close()
	require.Error(t, err)
	require.ErrorContains(t, err, "variantField")
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
