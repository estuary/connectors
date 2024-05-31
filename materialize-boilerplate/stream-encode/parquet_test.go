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
