package stream_encode

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestJsonEncoder(t *testing.T) {
	dir := t.TempDir()
	sink, err := os.CreateTemp(dir, "*.json.gz")
	require.NoError(t, err)

	enc := NewJsonEncoder(sink, makeTestFields())

	for i := 0; i < 10; i++ {
		require.NoError(t, enc.Encode(makeTestRow(t, i)))
	}

	require.NoError(t, enc.Close())

	cupaloy.SnapshotT(t, duckdbReadFile(t, sink.Name(), "JSON"))
}

func TestJsonEncoderInputs(t *testing.T) {
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
			gzw, err := gzip.NewWriterLevel(&wantGzipBytes, jsonCompressionlevel)
			require.NoError(t, err)

			_, err = gzw.Write(tt.wantBytes)
			require.NoError(t, err)
			require.NoError(t, gzw.Close())

			for _, compress := range []bool{true, false} {
				var buf bytes.Buffer
				tw := &testWriter{
					w: &buf,
				}

				enc := NewJsonEncoder(tw, tt.fields)
				if !compress {
					enc = NewJsonEncoder(tw, tt.fields, WithJsonDisableCompression())
				}
				require.NoError(t, enc.Encode(tt.input))
				require.NoError(t, enc.Close())

				// The provided writer is closed when enc is closed.
				require.True(t, tw.closed)

				if compress {
					// The written bytes can be unzip'd correctly.
					r, err := gzip.NewReader(&buf)
					require.NoError(t, err)
					gotBytes, err := io.ReadAll(r)
					require.NoError(t, err)
					require.Equal(t, string(tt.wantBytes), string(gotBytes))
				} else {
					require.Equal(t, string(tt.wantBytes), buf.String())
				}
			}
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

const benchmarkDatasetSize = 1000

func BenchmarkEncodingObjects(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var names, values = benchmarkDataset(b, benchmarkDatasetSize)
		b.StartTimer()

		enc := NewJsonEncoder(&nopWriteCloser{w: io.Discard}, names)
		for _, row := range values {
			require.NoError(b, enc.Encode(row))
		}
		require.NoError(b, enc.Close())
	}
}

func BenchmarkEncodingArrays(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var _, values = benchmarkDataset(b, benchmarkDatasetSize)
		b.StartTimer()

		enc := NewJsonEncoder(&nopWriteCloser{w: io.Discard}, nil)
		for _, row := range values {
			require.NoError(b, enc.Encode(row))
		}
		require.NoError(b, enc.Close())
	}
}

type nopWriteCloser struct {
	w io.Writer
}

func (w *nopWriteCloser) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

func (w *nopWriteCloser) Close() error {
	return nil
}

func benchmarkDataset(b *testing.B, size int) ([]string, [][]any) {
	var names = []string{"id", "type", "ctime", "mtime", "name", "description", "sequenceNumber", "state", "version", "intParamA", "intParamB", "intParamC", "intParamD", "flow_document"}
	var values [][]any
	for i := 0; i < size; i++ {
		var row = []any{
			uuid.New().String(),
			[]string{"event", "action", "item", "unknown"}[rand.Intn(4)],
			time.Now().Add(-time.Duration(rand.Intn(10000000)+50000000) * time.Second),
			time.Now().Add(-time.Duration(rand.Intn(10000000)+10000000) * time.Second),
			fmt.Sprintf("Row Number %d", i),
			fmt.Sprintf("An object representing some row in a synthetic dataset. This one is row number %d.", i),
			i,
			[]string{"new", "in-progress", "completed"}[rand.Intn(3)],
			rand.Intn(3),
			rand.Intn(1000000000),
			rand.Intn(1000000000),
			rand.Intn(1000000000),
			rand.Intn(1000000000),
		}

		doc := make(map[string]any)
		for idx, n := range names[:len(names)-1] {
			doc[n] = row[idx]
		}
		docJson, err := json.Marshal(doc)
		require.NoError(b, err)
		row = append(row, json.RawMessage(docJson))

		values = append(values, row)
	}
	return names, values
}
