package stream_encode

import (
	"bytes"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestCsvEncoder(t *testing.T) {
	tests := []struct {
		name  string
		nulls bool
		opts  []CsvOption
	}{
		{
			name:  "with headers",
			nulls: false,
			opts:  nil,
		},
		{
			name:  "without headers",
			nulls: false,
			opts:  []CsvOption{WithCsvSkipHeaders()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			sink, err := os.CreateTemp(dir, "*.csv.gz")
			require.NoError(t, err)

			enc := NewCsvEncoder(sink, makeTestFields(), tt.opts...)
			for i := 0; i < 10; i++ {
				row := makeTestRow(t, i)
				if tt.nulls {
					row[i] = nil
				}
				require.NoError(t, enc.Encode(row))
			}
			require.NoError(t, enc.Close())

			cupaloy.SnapshotT(t, duckdbReadFile(t, sink.Name(), "CSV"))
		})
	}
}

func TestCsvWriter(t *testing.T) {
	for _, tt := range []struct {
		name string
		row  []any
		want string
	}{
		{
			name: "empty",
			row:  nil,
			want: "\n",
		},
		{
			name: "basic",
			row:  []any{"first", "second", "third"},
			want: "first,second,third\n",
		},
		{
			name: "empty string and null",
			row:  []any{"first", "", nil},
			want: "first,\"\",\n",
		},
		{
			name: "special characters",
			row:  []any{"has\nnewline", " startsWithSpace", "\tstartsWithTab", "has\"quote", "has,comma", "has\rreturn"},
			want: "\"has\nnewline\",\" startsWithSpace\",\"\tstartsWithTab\",\"has\"\"quote\",\"has,comma\",\"has\rreturn\"\n",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			csvw := newCsvWriter(&buf, '"')
			require.NoError(t, csvw.writeRow(tt.row))
			require.Equal(t, tt.want, buf.String())
		})
	}
}
