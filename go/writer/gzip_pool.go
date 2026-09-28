package writer

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/klauspost/compress/flate"
	"golang.org/x/sync/semaphore"
)

// GzipPool compresses many concurrently open gzip streams with a fixed number
// of flate compressors. A stream owns only a small chunk buffer and the last
// 32 KiB of its plain data; when the chunk fills, a pooled compressor is
// re-seeded with that tail as a preset dictionary, appends the chunk to the
// stream's deflate output, and is returned to the pool. The output is one
// ordinary gzip member, so memory is bounded by the pool size rather than by
// the number of open streams, and streams compress in parallel across the
// pool.
type GzipPool struct {
	sem       *semaphore.Weighted
	level     int
	chunkSize int
	comps     sync.Pool // *flate.Writer
	chunks    sync.Pool // *[]byte of cap chunkSize
}

// NewGzipPool returns a pool of `compressors` flate compressors. Each stream
// buffers up to chunkSize of plain data before using one.
func NewGzipPool(compressors, chunkSize int) *GzipPool {
	p := &GzipPool{
		sem:       semaphore.NewWeighted(int64(compressors)),
		level:     jsonCompressionlevel,
		chunkSize: chunkSize,
	}
	p.comps.New = func() any {
		w, err := flate.NewWriter(io.Discard, p.level)
		if err != nil {
			panic(fmt.Sprintf("invalid compression level: %v", err))
		}
		return w
	}
	p.chunks.New = func() any {
		b := make([]byte, 0, chunkSize)
		return &b
	}
	return p
}

// NewStream returns a writer producing a gzip member on dst. Bytes reach dst
// only from the stream's own goroutine, in order, and Close waits for the
// trailer to be written.
func (p *GzipPool) NewStream(dst io.Writer) *PooledGzipWriter {
	s := &PooledGzipWriter{
		pool:    p,
		dst:     dst,
		pending: p.chunks.Get().(*[]byte),
		jobs:    make(chan gzipJob, 1),
		done:    make(chan struct{}),
	}
	go s.run()
	return s
}

const dictSize = 32 << 10 // deflate window

type gzipJob struct {
	chunk *[]byte
	final bool
}

// PooledGzipWriter is one gzip stream served by a GzipPool.
type PooledGzipWriter struct {
	pool    *GzipPool
	dst     io.Writer
	pending *[]byte
	jobs    chan gzipJob
	done    chan struct{}

	// Owned by the stream goroutine.
	header bool
	tail   []byte
	crc    uint32
	size   uint32
	err    error
}

func (s *PooledGzipWriter) Write(b []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	n := len(b)
	for len(b) > 0 {
		room := cap(*s.pending) - len(*s.pending)
		if room == 0 {
			if err := s.submit(false); err != nil {
				return 0, err
			}
			continue
		}
		take := min(room, len(b))
		*s.pending = append(*s.pending, b[:take]...)
		b = b[take:]
	}
	return n, nil
}

// submit hands the pending chunk to the stream goroutine and takes a fresh
// buffer. It blocks while the previous chunk is still queued, which bounds
// each stream to two chunks in flight.
func (s *PooledGzipWriter) submit(final bool) error {
	select {
	case s.jobs <- gzipJob{chunk: s.pending, final: final}:
	case <-s.done:
		return s.err
	}
	if !final {
		s.pending = s.pool.chunks.Get().(*[]byte)
	}
	return nil
}

func (s *PooledGzipWriter) Close() error {
	if s.err != nil {
		return s.err
	}
	if err := s.submit(true); err != nil {
		return err
	}
	s.pending = nil
	<-s.done
	return s.err
}

func (s *PooledGzipWriter) run() {
	defer close(s.done)
	for job := range s.jobs {
		if s.err == nil {
			s.err = s.compress(*job.chunk, job.final)
		}
		*job.chunk = (*job.chunk)[:0]
		s.pool.chunks.Put(job.chunk)
		if job.final {
			return
		}
	}
}

var gzipHeader = []byte{0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff}

// compress appends one chunk to the stream's deflate output using a pooled
// compressor seeded with the stream's tail, then sync-flushes so the next
// chunk can continue from any compressor. The final chunk closes the deflate
// stream and writes the gzip trailer.
func (s *PooledGzipWriter) compress(chunk []byte, final bool) error {
	p := s.pool
	if err := p.sem.Acquire(context.Background(), 1); err != nil {
		return err
	}
	defer p.sem.Release(1)

	if !s.header {
		if _, err := s.dst.Write(gzipHeader); err != nil {
			return err
		}
		s.header = true
	}

	fw := p.comps.Get().(*flate.Writer)
	defer p.comps.Put(fw)
	fw.ResetDict(s.dst, s.tail)
	if _, err := fw.Write(chunk); err != nil {
		return err
	}
	s.crc = crc32.Update(s.crc, crc32.IEEETable, chunk)
	s.size += uint32(len(chunk))
	s.tail = appendTail(s.tail, chunk)

	if !final {
		return fw.Flush()
	}
	if err := fw.Close(); err != nil {
		return err
	}
	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[:4], s.crc)
	binary.LittleEndian.PutUint32(trailer[4:], s.size)
	_, err := s.dst.Write(trailer[:])
	return err
}

// appendTail keeps the last dictSize bytes of the plain stream in a buffer the
// stream owns, since the compressor holds a reference to its dictionary.
func appendTail(tail, chunk []byte) []byte {
	if len(chunk) >= dictSize {
		return append(tail[:0], chunk[len(chunk)-dictSize:]...)
	}
	if len(tail)+len(chunk) > dictSize {
		drop := len(tail) + len(chunk) - dictSize
		tail = append(tail[:0], tail[drop:]...)
	}
	return append(tail, chunk...)
}
