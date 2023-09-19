package sql

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountingEncoder(t *testing.T) {
	testData := "this is a test"

	var wantBytes bytes.Buffer
	require.NoError(t, json.NewEncoder(&wantBytes).Encode(testData))

	var wantGzipBytes bytes.Buffer
	gzw, err := gzip.NewWriterLevel(&wantGzipBytes, compressionLevel)
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(gzw).Encode(testData))
	require.NoError(t, gzw.Close())

	var buf bytes.Buffer
	tw := &testWriter{
		w: &buf,
	}

	enc := NewCountingEncoder(tw)
	require.NoError(t, enc.Encode(testData))
	require.NoError(t, enc.Close())

	// The provided writer is closed when enc is closed.
	require.True(t, tw.closed)

	// The count of bytes written is for the GZIP'd bytes.
	require.Equal(t, wantGzipBytes.Len(), enc.Written())

	// The written bytes can be unzip'd correctly.
	r, err := gzip.NewReader(&buf)
	require.NoError(t, err)
	gotBytes, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, wantBytes.Bytes(), gotBytes)
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
