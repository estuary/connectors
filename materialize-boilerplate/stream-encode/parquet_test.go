package stream_encode

import (
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestParquetEncoder(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			sink, err := os.CreateTemp(dir, "*.parquet")
			require.NoError(t, err)

			enc := NewParquetEncoder(sink, makeTestParquetSchema(!tt.nulls), tt.opts...)
			for i := 0; i < 10; i++ {
				row := makeTestRow(t, i)
				if tt.nulls {
					row[i] = nil
				}
				require.NoError(t, enc.Encode(row))
			}
			require.NoError(t, enc.Close())

			cupaloy.SnapshotT(t, duckdbReadFile(t, sink.Name(), "JSON"))
		})
	}
}

// func TestWriteColumnBytes(t *testing.T) {
// 	var buf []byte
// 	var err error

// 	variableWriter := &testColumnBatchWriter[parquet.FixedLenByteArray]{}
// 	fixedWriter := &testColumnBatchWriter[parquet.FixedLenByteArray]{}

// 	for _, tt := range []struct {
// 		name   string
// 		fn     appendBytesFn
// 		writer any
// 		input  []any
// 		want   [][]byte
// 	}{
// 		{
// 			name:   "appendBinaryVal",
// 			fn:     appendBinaryVal,
// 			writer: variableWriter,
// 			input: []any{
// 				base64.StdEncoding.EncodeToString([]byte("first")),
// 				base64.StdEncoding.EncodeToString([]byte("second")),
// 				base64.StdEncoding.EncodeToString([]byte("third")),
// 			},
// 			want: [][]byte{
// 				[]byte("first"),
// 				[]byte("second"),
// 				[]byte("third"),
// 			},
// 		},
// 		{
// 			name:   "appendJsonVal",
// 			fn:     appendJsonVal,
// 			writer: variableWriter,
// 			input: []any{
// 				[]byte(`{"isAlreadyJson": true}`),
// 				json.RawMessage(`{"isAlsoAlreadyJson": true}`),
// 				map[string]string{"isSomethingElse": "yes"},
// 			},
// 			want: [][]byte{
// 				[]byte(`{"isAlreadyJson": true}`),
// 				[]byte(`{"isAlsoAlreadyJson": true}`),
// 				[]byte(`{"isSomethingElse":"yes"}`),
// 			},
// 		},
// 		{
// 			name:   "appendStringVal",
// 			fn:     appendStringVal,
// 			writer: variableWriter,
// 			input: []any{
// 				"some existing string",
// 				int64(1234),
// 				uint64(9999),
// 				true,
// 				12.32,
// 				json.RawMessage(`{"some":"json"}`),
// 				[]byte("string as bytes"),
// 			},
// 			want: [][]byte{
// 				[]byte("some existing string"),
// 				[]byte("1234"),
// 				[]byte("9999"),
// 				[]byte("true"),
// 				[]byte("12.32"),
// 				[]byte(`{"some":"json"}`),
// 				[]byte("string as bytes"),
// 			},
// 		},
// 		{
// 			name:   "appendUuidVal",
// 			fn:     appendUuidVal,
// 			writer: fixedWriter,
// 			input: []any{
// 				"38373433-3437-6136-6339-636264386136",
// 				"66643364-3036-3934-6335-303634316664",
// 				"36333263-3361-3830-3762-336566393933",
// 			},
// 			want: [][]byte{
// 				{56, 55, 52, 51, 52, 55, 97, 54, 99, 57, 99, 98, 100, 56, 97, 54},
// 				{102, 100, 51, 100, 48, 54, 57, 52, 99, 53, 48, 54, 52, 49, 102, 100},
// 				{54, 51, 50, 99, 51, 97, 56, 48, 55, 98, 51, 101, 102, 57, 57, 51},
// 			},
// 		},
// 		{
// 			name:   "appendIntervalVal",
// 			fn:     appendIntervalVal,
// 			writer: fixedWriter,
// 			input: []any{
// 				"P1Y1M1W1DT1H1M1S",
// 				"P500Y500M100W500DT5H5M5S",
// 				"P9000Y9000M1000W900DT9H9M9S",
// 			},
// 			want: [][]byte{
// 				{13, 0, 0, 0, 8, 0, 0, 0, 200, 220, 55, 0},
// 				{100, 25, 0, 0, 176, 4, 0, 0, 232, 79, 23, 1},
// 				{8, 201, 1, 0, 220, 30, 0, 0, 8, 195, 246, 1},
// 			},
// 		},
// 	} {
// 		t.Run(tt.name, func(t *testing.T) {
// 			var rowsWithNulls [][]any
// 			var defLevels []int16
// 			for i, v := range tt.input {
// 				rowsWithNulls = append(rowsWithNulls, []any{v})
// 				defLevels = append(defLevels, 1)
// 				if i%2 == 0 {
// 					rowsWithNulls = append(rowsWithNulls, []any{nil})
// 					defLevels = append(defLevels, 0)
// 				}
// 			}

// 			var gotDefLevels []int16
// 			if w, ok := tt.writer.(*testColumnBatchWriter[parquet.ByteArray]); ok {
// 				buf, err = writeColumnBytes(buf, 0, rowsWithNulls, w, tt.fn)
// 				require.NoError(t, err)

// 				var gotVals []parquet.ByteArray
// 				gotVals, gotDefLevels = w.get()
// 				for i, v := range gotVals {
// 					require.Equal(t, tt.want[i], []byte(v))
// 				}
// 			} else if w, ok := tt.writer.(*testColumnBatchWriter[parquet.FixedLenByteArray]); ok {
// 				buf, err = writeColumnBytes(buf, 0, rowsWithNulls, w, tt.fn)
// 				require.NoError(t, err)

// 				var gotVals []parquet.FixedLenByteArray
// 				gotVals, gotDefLevels = w.get()
// 				for i, v := range gotVals {
// 					require.Equal(t, tt.want[i], []byte(v))
// 				}
// 			} else {
// 				t.Fatalf("unexpected writer type %T", tt.writer)
// 			}
// 			require.Equal(t, defLevels, gotDefLevels)
// 		})
// 	}
// }

// type testColumnBatchWriter[T parquetValueBytes] struct {
// 	gotVals []T
// 	gotDefs []int16
// }

// func (w *testColumnBatchWriter[T]) WriteBatch(vals []T, defLevels []int16, repLevels []int16) (int64, error) {
// 	w.gotVals = append(w.gotVals, vals...)
// 	w.gotDefs = append(w.gotDefs, defLevels...)
// 	return int64(len(vals)), nil
// }

// func (w *testColumnBatchWriter[T]) get() ([]T, []int16) {
// 	outVals := w.gotVals
// 	outDefs := w.gotDefs
// 	w.gotVals = nil
// 	w.gotDefs = nil
// 	return outVals, outDefs
// }

// func BenchmarkWriteColumnBytes(b *testing.B) {
// 	var buf = make([]byte, 10_000_000)
// 	var err error

// 	variableWriter := &noopColumnBatchWriter[parquet.ByteArray]{}
// 	// fixedWriter := &testColumnBatchWriter[parquet.FixedLenByteArray]{}

// 	fn := appendStringVal

// 	numVals := 100_000
// 	rows := make([][]any, numVals)
// 	for i := 0; i < numVals; i++ {
// 		rows[i] = []any{strings.Repeat("a", 10)}
// 	}

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		buf, err = writeColumnBytes(buf, 0, rows, variableWriter, fn)
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 	}
// }

// type noopColumnBatchWriter[T parquetValueBytes] struct{}

// func (w *noopColumnBatchWriter[T]) WriteBatch(vals []T, defLevels []int16, repLevels []int16) (int64, error) {
// 	return int64(len(vals)), nil
// }
