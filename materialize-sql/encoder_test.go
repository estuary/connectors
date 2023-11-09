package sql

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountingEncoder(t *testing.T) {
	for _, tt := range []struct {
		name      string
		fields    []string
		input     []any
		wantBytes []byte
	}{
		{
			name:      "object",
			fields:    []string{"str", "int", "bool", "num"},
			input:     []any{"testing", 1, true, 2.3},
			wantBytes: append([]byte(`{"bool":true,"int":1,"num":2.3,"str":"testing"}`), '\n'),
		},
		{
			name:      "array",
			fields:    nil,
			input:     []any{"testing", 1, true, 2.3},
			wantBytes: append([]byte(`["testing",1,true,2.3]`), '\n'),
		},
		{
			name:      "single field object",
			fields:    []string{"str"},
			input:     []any{"testing"},
			wantBytes: append([]byte(`{"str":"testing"}`), '\n'),
		},
		{
			name:      "single field array",
			fields:    nil,
			input:     []any{"testing"},
			wantBytes: append([]byte(`["testing"]`), '\n'),
		},
		{
			name:      "empty object",
			fields:    []string{},
			input:     []any{},
			wantBytes: append([]byte(`{}`), '\n'),
		},
		{
			name:      "empty array",
			fields:    nil,
			input:     []any{},
			wantBytes: append([]byte(`[]`), '\n'),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var wantGzipBytes bytes.Buffer
			gzw, err := gzip.NewWriterLevel(&wantGzipBytes, compressionLevel)
			require.NoError(t, err)

			_, err = gzw.Write(tt.wantBytes)
			require.NoError(t, err)
			require.NoError(t, gzw.Close())

			var buf bytes.Buffer
			tw := &testWriter{
				w: &buf,
			}

			enc := NewCountingEncoder(tw, tt.fields)
			require.NoError(t, enc.Encode(tt.input))
			require.NoError(t, enc.Close())

			// The provided writer is closed when enc is closed.
			require.True(t, tw.closed)

			// The written bytes can be unzip'd correctly.
			r, err := gzip.NewReader(&buf)
			require.NoError(t, err)
			gotBytes, err := io.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, string(tt.wantBytes), string(gotBytes))
		})
	}
}

type testWriter struct {
	w      io.Writer
	closed bool
}

func (t *testWriter) Write(p []byte) (int, error) {
	return t.w.Write(p)
}

func (t *testWriter) Close() error {
	t.closed = true
	return nil
}
