package writer

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

func TestJsonWriter(t *testing.T) {
	var dir = t.TempDir()
	var sink, err = os.CreateTemp(dir, "*.json.gz")
	require.NoError(t, err)

	var w = NewJsonWriter(sink, makeTestFields())

	for i := range 10 {
		require.NoError(t, w.Write(makeTestRow(t, i)))
	}

	require.NoError(t, w.Close())

	cupaloy.SnapshotT(t, duckdbReadFile(t, sink.Name(), "JSON"))
}

func TestJsonWriterInputs(t *testing.T) {
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
			var gzw, err = gzip.NewWriterLevel(&wantGzipBytes, jsonCompressionlevel)
			require.NoError(t, err)

			_, err = gzw.Write(tt.wantBytes)
			require.NoError(t, err)
			require.NoError(t, gzw.Close())

			for _, compress := range []bool{true, false} {
				var buf bytes.Buffer
				var tw = &testWriter{
					w: &buf,
				}

				var w = NewJsonWriter(tw, tt.fields)
				if !compress {
					w = NewJsonWriter(tw, tt.fields, WithJsonDisableCompression())
				}
				require.NoError(t, w.Write(tt.input))
				require.NoError(t, w.Close())

				// The provided writer is closed when w is closed.
				require.True(t, tw.closed)

				if compress {
					// The written bytes can be unzip'd correctly.
					var r, err = gzip.NewReader(&buf)
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

func TestJsonWriterWrittenUncompressed(t *testing.T) {
	var fields = []string{"str", "int"}
	var rows = [][]any{{"one", 1}, {"two", 2}, {"three", 3}}

	var plain, compressed bytes.Buffer
	var pw = NewJsonWriter(&testWriter{w: &plain}, fields, WithJsonDisableCompression())
	var zw = NewJsonWriter(&testWriter{w: &compressed}, fields)
	for _, row := range rows {
		require.NoError(t, pw.Write(row))
		require.NoError(t, zw.Write(row))
	}
	require.NoError(t, pw.Close())
	require.NoError(t, zw.Close())

	var r, err = gzip.NewReader(&compressed)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, plain.String(), string(got))

	// Both counts are of the bytes that reached their own side of the compressor.
	require.Equal(t, plain.Len(), zw.WrittenUncompressed())
	require.Equal(t, plain.Len(), pw.WrittenUncompressed())
}

func TestJsonWriterCompactRawLineBreaks(t *testing.T) {
	for _, tt := range []struct {
		name    string
		input   any
		want    string
		wantErr string
	}{
		{name: "line breaks are compacted", input: json.RawMessage("{\"a\": 1,\n\"b\": \"x\\ny\"\r\n}"), want: `{"doc":{"a":1,"b":"x\ny"}}`},
		{name: "a value without one is written as given", input: json.RawMessage(`{"a": 1,  "b": [ 2 ]}`), want: `{"doc":{"a": 1,  "b": [ 2 ]}}`},
		{name: "a nil value is null", input: json.RawMessage(nil), want: `{"doc":null}`},
		{name: "an empty value is an error", input: json.RawMessage{}, wantErr: `field "doc": empty pre-serialized JSON value`},
		{name: "an invalid value with a line break is an error", input: json.RawMessage("{\n"), wantErr: `field "doc": unexpected end of JSON input`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			var w = NewJsonWriter(&testWriter{w: &buf}, []string{"doc"}, WithJsonDisableCompression())
			var row = []any{tt.input}

			var err = w.Write(row)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.Zero(t, buf.Len())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want+"\n", buf.String())
			// The caller's row is left as it was.
			require.Equal(t, tt.input, row[0])
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
	for b.Loop() {
		b.StopTimer()
		var names, values = benchmarkDataset(b, benchmarkDatasetSize)
		b.StartTimer()

		var w = NewJsonWriter(&nopWriteCloser{w: io.Discard}, names)
		for _, row := range values {
			require.NoError(b, w.Write(row))
		}
		require.NoError(b, w.Close())
	}
}

func BenchmarkEncodingArrays(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		var _, values = benchmarkDataset(b, benchmarkDatasetSize)
		b.StartTimer()

		var w = NewJsonWriter(&nopWriteCloser{w: io.Discard}, nil)
		for _, row := range values {
			require.NoError(b, w.Write(row))
		}
		require.NoError(b, w.Close())
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
	for i := range size {
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

		var doc = make(map[string]any)
		for idx, n := range names[:len(names)-1] {
			doc[n] = row[idx]
		}
		var docJson, err = json.Marshal(doc)
		require.NoError(b, err)
		row = append(row, json.RawMessage(docJson))

		values = append(values, row)
	}
	return names, values
}
