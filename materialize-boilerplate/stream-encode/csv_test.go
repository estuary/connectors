package stream_encode

import (
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
		{
			name:  "with default null",
			nulls: true,
			opts:  nil,
		},
		{
			name:  "with custom null",
			nulls: true,
			opts:  []CsvOption{WithCsvNullString("MyNullString")},
		},
		{
			name:  "with custom delimiter",
			nulls: true,
			opts:  []CsvOption{WithCsvDelimiter([]rune("|")[0])},
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
