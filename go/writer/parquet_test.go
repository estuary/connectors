package writer

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/segmentio/encoding/json"
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

func TestParquetWriterVariantRequired(t *testing.T) {
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "variantField", DataType: LogicalTypeVariant, Required: true},
	}

	v0, err := variant.Of("hello")
	require.NoError(t, err)
	// Use 1<<40 to exceed int32 range, forcing Int64 encoding.
	v1, err := variant.Of(int64(1 << 40))
	require.NoError(t, err)
	rows := []any{v0, v1}

	w := NewParquetWriter(sink, sch)
	for _, v := range rows {
		require.NoError(t, w.Write([]any{v}))
	}
	require.NoError(t, w.Close())

	f, err := file.OpenParquetFile(sink.Name(), false)
	require.NoError(t, err)
	defer f.Close()

	require.Equal(t, 2, f.MetaData().Schema.NumColumns())

	rg := f.RowGroup(0)
	require.EqualValues(t, len(rows), rg.NumRows())

	readByteArrays := func(col int) []parquet.ByteArray {
		r, err := rg.Column(col)
		require.NoError(t, err)
		vals := make([]parquet.ByteArray, len(rows))
		_, _, err = r.(*file.ByteArrayColumnChunkReader).ReadBatch(int64(len(rows)), vals, nil, nil)
		require.NoError(t, err)
		return vals
	}

	valueVals := readByteArrays(0)
	metaVals := readByteArrays(1)

	got0, err := variant.New(metaVals[0], valueVals[0])
	require.NoError(t, err)
	require.Equal(t, variant.String, got0.Type())
	require.Equal(t, "hello", got0.Value())

	got1, err := variant.New(metaVals[1], valueVals[1])
	require.NoError(t, err)
	require.Equal(t, variant.Int64, got1.Type())
	require.Equal(t, int64(1<<40), got1.Value())
}

func TestGetVariantVal(t *testing.T) {
	decNum := decimal128.New(0, 42)
	// Use 1<<40 to exceed int32 range, forcing Int64 encoding.
	prebuilt, err := variant.Of(int64(1 << 40))
	require.NoError(t, err)

	tests := []struct {
		name     string
		input    any
		wantType variant.Type
		wantVal  any
	}{
		{
			name:     "variant.Value passthrough",
			input:    prebuilt,
			wantType: variant.Int64,
			wantVal:  int64(1 << 40),
		},
		{
			name:     "int64",
			input:    int64(1 << 40),
			wantType: variant.Int64,
			wantVal:  int64(1 << 40),
		},
		{
			name:     "float64",
			input:    float64(2.5),
			wantType: variant.Double,
			wantVal:  float64(2.5),
		},
		{
			name:     "string",
			input:    "hello",
			wantType: variant.String,
			wantVal:  "hello",
		},
		{
			name:     "bool true",
			input:    true,
			wantType: variant.Bool,
			wantVal:  true,
		},
		{
			name:     "bool false",
			input:    false,
			wantType: variant.Bool,
			wantVal:  false,
		},
		{
			name:     "[]byte",
			input:    []byte("bin"),
			wantType: variant.Binary,
			wantVal:  []byte("bin"),
		},
		{
			// json.RawMessage is parsed as a Variant value, not stored as opaque bytes.
			name:     "json.RawMessage",
			input:    json.RawMessage(`42`),
			wantType: variant.Int8,
			wantVal:  int8(42),
		},
		{
			name:     "decimal128.Num",
			input:    decNum,
			wantType: variant.Decimal16,
			wantVal:  variant.DecimalValue[decimal128.Num]{Scale: 0, Value: decNum},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getVariantVal(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.wantType, got.Type())
			require.Equal(t, tt.wantVal, got.Value())
		})
	}
}

func TestGetVariantValError(t *testing.T) {
	_, err := getVariantVal(struct{}{})
	require.ErrorContains(t, err, "unhandled variant type: struct {}")
}

func TestParquetWriterVariantTypes(t *testing.T) {
	decNum := decimal128.New(0, 100)
	// Use 1<<40 to exceed int32 range, forcing Int64 encoding.
	prebuilt, err := variant.Of(int64(1 << 40))
	require.NoError(t, err)

	inputs := []any{
		int64(1 << 40),
		float64(1.5),
		"hello world",
		true,
		false,
		[]byte("binary data"),
		json.RawMessage(`{"key":"value"}`),
		decNum,
		prebuilt,
		nil,
	}

	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "v", DataType: LogicalTypeVariant, Required: false},
	}

	w := NewParquetWriter(sink, sch)
	for _, inp := range inputs {
		require.NoError(t, w.Write([]any{inp}))
	}
	require.NoError(t, w.Close())

	f, err := file.OpenParquetFile(sink.Name(), false)
	require.NoError(t, err)
	defer f.Close()

	rg := f.RowGroup(0)
	require.EqualValues(t, len(inputs), rg.NumRows())

	readByteArrays := func(col int) ([]parquet.ByteArray, []int16) {
		r, err := rg.Column(col)
		require.NoError(t, err)
		vals := make([]parquet.ByteArray, len(inputs))
		def := make([]int16, len(inputs))
		_, _, err = r.(*file.ByteArrayColumnChunkReader).ReadBatch(int64(len(inputs)), vals, def, nil)
		require.NoError(t, err)
		return vals, def
	}

	valueVals, valueDef := readByteArrays(0)
	metaVals, metaDef := readByteArrays(1)

	n := len(inputs)
	require.Equal(t, int16(0), valueDef[n-1], "nil row should have def=0")
	require.Equal(t, int16(0), metaDef[n-1], "nil row should have def=0")

	wantTypes := []variant.Type{
		variant.Int64,
		variant.Double,
		variant.String,
		variant.Bool,
		variant.Bool,
		variant.Binary,
		variant.Object, // json.RawMessage is parsed as a Variant object
		variant.Decimal16,
		variant.Int64, // variant.Value passthrough
	}

	for i, wantType := range wantTypes {
		require.Equal(t, int16(1), valueDef[i], "row %d def level", i)
		v, err := variant.New(metaVals[i], valueVals[i])
		require.NoError(t, err, "row %d reconstruct", i)
		require.Equal(t, wantType, v.Type(), "row %d type", i)
	}
}

func TestParquetWriterVariantMixedSchema(t *testing.T) {
	// Variant consumes two NextColumn() calls in flushBuffer. This test verifies that
	// columns placed after a variant column in the schema are written to the correct
	// physical column, i.e. the cursor does not desync.
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "s", DataType: LogicalTypeString, Required: true},
		{Name: "v", DataType: LogicalTypeVariant, Required: false},
		{Name: "n", DataType: PrimitiveTypeInteger, Required: true},
	}

	v0, err := variant.Of("hello")
	require.NoError(t, err)
	// Use 1<<40 to exceed int32 range, forcing Int64 encoding.
	v2, err := variant.Of(int64(1 << 40))
	require.NoError(t, err)

	rows := [][]any{
		{"foo", v0, int64(1)},
		{"bar", nil, int64(2)}, // nil variant row tests optional null path
		{"baz", v2, int64(3)},
	}

	w := NewParquetWriter(sink, sch)
	for _, r := range rows {
		require.NoError(t, w.Write(r))
	}
	require.NoError(t, w.Close())

	f, err := file.OpenParquetFile(sink.Name(), false)
	require.NoError(t, err)
	defer f.Close()

	// 4 physical columns: s, v.value, v.metadata, n
	require.Equal(t, 4, f.MetaData().Schema.NumColumns())

	rg := f.RowGroup(0)
	require.EqualValues(t, len(rows), rg.NumRows())

	readStrings := func(col int) []string {
		r, err := rg.Column(col)
		require.NoError(t, err)
		vals := make([]parquet.ByteArray, len(rows))
		_, _, err = r.(*file.ByteArrayColumnChunkReader).ReadBatch(int64(len(rows)), vals, nil, nil)
		require.NoError(t, err)
		out := make([]string, len(vals))
		for i, v := range vals {
			out[i] = string(v)
		}
		return out
	}
	readByteArrays := func(col int) ([]parquet.ByteArray, []int16) {
		r, err := rg.Column(col)
		require.NoError(t, err)
		vals := make([]parquet.ByteArray, len(rows))
		def := make([]int16, len(rows))
		_, _, err = r.(*file.ByteArrayColumnChunkReader).ReadBatch(int64(len(rows)), vals, def, nil)
		require.NoError(t, err)
		return vals, def
	}
	readInts := func(col int) []int64 {
		r, err := rg.Column(col)
		require.NoError(t, err)
		vals := make([]int64, len(rows))
		_, _, err = r.(*file.Int64ColumnChunkReader).ReadBatch(int64(len(rows)), vals, nil, nil)
		require.NoError(t, err)
		return vals
	}

	require.Equal(t, []string{"foo", "bar", "baz"}, readStrings(0))

	valueVals, valueDef := readByteArrays(1)
	metaVals, metaDef := readByteArrays(2)
	require.Equal(t, []int16{1, 0, 1}, valueDef)
	require.Equal(t, []int16{1, 0, 1}, metaDef)

	// ReadBatch packs non-null values densely: row 0 is at index 0, row 2 (the second
	// non-null row) is at index 1 — there is no slot for the null row 1.
	got0, err := variant.New(metaVals[0], valueVals[0])
	require.NoError(t, err)
	require.Equal(t, variant.String, got0.Type())
	require.Equal(t, "hello", got0.Value())

	got2, err := variant.New(metaVals[1], valueVals[1])
	require.NoError(t, err)
	require.Equal(t, variant.Int64, got2.Type())
	require.Equal(t, int64(1<<40), got2.Value())

	require.Equal(t, []int64{1, 2, 3}, readInts(3))
}

func TestParquetWriterVariantNilInRequiredColumn(t *testing.T) {
	// Writing nil to a required column is a caller contract violation. Verify it surfaces
	// as an error rather than silently producing a corrupt file.
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.parquet")
	require.NoError(t, err)

	sch := ParquetSchema{
		{Name: "v", DataType: LogicalTypeVariant, Required: true},
	}

	w := NewParquetWriter(sink, sch)
	require.NoError(t, w.Write([]any{nil}))
	require.Error(t, w.Close())
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
