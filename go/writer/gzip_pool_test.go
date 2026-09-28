package writer

import (
	"bytes"
	"compress/gzip"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(b))
	require.NoError(t, err)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return out
}

func wordText(seed int64, n int) []byte {
	r := rand.New(rand.NewSource(seed))
	words := []string{"alpha", "beta", "gamma", "delta", "flow", "estuary", "json", "row", "value"}
	var buf bytes.Buffer
	for buf.Len() < n {
		buf.WriteString(words[r.Intn(len(words))])
		buf.WriteByte(' ')
		if r.Intn(20) == 0 {
			buf.WriteString(string(rune('a' + r.Intn(26))))
		}
	}
	return buf.Bytes()[:n]
}

func TestGzipPoolRoundTrip(t *testing.T) {
	pool := NewGzipPool(2, 1024)
	for _, size := range []int{0, 1, 1000, 1024, 1025, 2048, 100_000, 300_000} {
		var out bytes.Buffer
		s := pool.NewStream(&out)
		in := wordText(int64(size), size)
		// Write in uneven pieces so chunk boundaries fall mid-write.
		for i := 0; i < len(in); {
			n := min(len(in)-i, 1+(i%700))
			_, err := s.Write(in[i : i+n])
			require.NoError(t, err)
			i += n
		}
		require.NoError(t, s.Close())
		require.Equal(t, in, gunzip(t, out.Bytes()), "size %d", size)
	}
}

func TestGzipPoolDictionaryCarriesAcrossChunks(t *testing.T) {
	// The dictionary tail lets a chunk reference the previous chunk, so a
	// repeating stream should compress far better than chunk size alone.
	pool := NewGzipPool(1, 4096)
	in := bytes.Repeat(wordText(7, 3000), 200)
	var out bytes.Buffer
	s := pool.NewStream(&out)
	_, err := s.Write(in)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.Equal(t, in, gunzip(t, out.Bytes()))
	require.Less(t, out.Len(), len(in)/20)
}

func TestGzipPoolManyStreamsConcurrently(t *testing.T) {
	pool := NewGzipPool(3, 8192)
	const streams = 200
	inputs := make([][]byte, streams)
	outputs := make([]bytes.Buffer, streams)
	writers := make([]*PooledGzipWriter, streams)
	for i := range writers {
		inputs[i] = wordText(int64(i), 50_000+i*37)
		writers[i] = pool.NewStream(&outputs[i])
	}
	// Interleave writes across streams the way a Store phase does.
	for off := 0; off < 60_000; off += 1500 {
		for i, w := range writers {
			if off < len(inputs[i]) {
				_, err := w.Write(inputs[i][off:min(off+1500, len(inputs[i]))])
				require.NoError(t, err)
			}
		}
	}
	var wg sync.WaitGroup
	for _, w := range writers {
		wg.Add(1)
		go func(w *PooledGzipWriter) {
			defer wg.Done()
			require.NoError(t, w.Close())
		}(w)
	}
	wg.Wait()
	for i := range writers {
		require.Equal(t, inputs[i], gunzip(t, outputs[i].Bytes()), "stream %d", i)
	}
}

func TestJsonWriterWithGzipPool(t *testing.T) {
	pool := NewGzipPool(2, 4096)
	var out bytes.Buffer
	w := NewJsonWriter(nopCloser{&out}, []string{"id", "v"}, WithJsonGzipPool(pool))
	for i := 0; i < 5000; i++ {
		require.NoError(t, w.Write([]any{int64(i), "value"}))
	}
	require.NoError(t, w.Close())
	require.Equal(t, out.Len(), w.Written())
	plain := gunzip(t, out.Bytes())
	require.Equal(t, 5000, bytes.Count(plain, []byte("\n")))
	require.True(t, bytes.HasPrefix(plain, []byte(`{"id":0,"v":"value"}`)))
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }
