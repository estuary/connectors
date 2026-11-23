package writer

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/klauspost/pgzip"
	"github.com/stretchr/testify/require"
)

func TestCsvWriter(t *testing.T) {
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

			w := NewCsvWriter(sink, makeTestFields(), tt.opts...)
			for i := range 10 {
				row := makeTestRow(t, i)
				if tt.nulls {
					row[i] = nil
				}
				require.NoError(t, w.Write(row))
			}
			require.NoError(t, w.Close())

			cupaloy.SnapshotT(t, duckdbReadFile(t, sink.Name(), "CSV"))
		})
	}
}

func TestCsvWriterIsolated(t *testing.T) {
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
			csvw := newCsvWriter(&buf, '"', false)
			require.NoError(t, csvw.writeRow(tt.row))
			require.Equal(t, tt.want, buf.String())
		})
	}
}

func TestCsvWriterEscapeFormulas(t *testing.T) {
	row := []any{
		"=1+1", "+1+1", "-1+1", "@a", "\t=1+1", "\r=1+1", "\n=1+1",
		"=a,\"b\"", []byte("=1+1"),
		"-12.5", "+3", "-1e-9", "-", "+1.", "a=1", "", int64(-5), -1.5,
	}

	for _, tt := range []struct {
		name   string
		escape bool
		want   string
	}{
		{
			name:   "disabled",
			escape: false,
			want: "=1+1,+1+1,-1+1,@a,\"\t=1+1\",\"\r=1+1\",\"\n=1+1\"," +
				"\"=a,\"\"b\"\"\",=1+1," +
				"-12.5,+3,-1e-9,-,+1.,a=1,\"\",-5,-1.5\n",
		},
		{
			name:   "enabled",
			escape: true,
			want: "'=1+1,'+1+1,'-1+1,'@a,'\t=1+1,\"'\r=1+1\",\"'\n=1+1\"," +
				"\"'=a,\"\"b\"\"\",'=1+1," +
				"-12.5,+3,-1e-9,'-,'+1.,a=1,\"\",-5,-1.5\n",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			csvw := newCsvWriter(&buf, '"', tt.escape)
			require.NoError(t, csvw.writeRow(row))
			require.Equal(t, tt.want, buf.String())
		})
	}

	t.Run("headers", func(t *testing.T) {
		var buf bytes.Buffer
		w := NewCsvWriter(&nopWriteCloser{w: &buf}, []string{"=1+1", "ok"}, WithCsvEscapeFormulas())
		require.NoError(t, w.Write([]any{"a", "b"}))
		require.NoError(t, w.Close())

		gz, err := pgzip.NewReader(&buf)
		require.NoError(t, err)
		got, err := io.ReadAll(gz)
		require.NoError(t, err)
		require.Equal(t, "'=1+1,ok\na,b\n", string(got))
	})
}
