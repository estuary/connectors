//go:build linux

package writer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// residentBytes reports how much of the file at path is currently resident in
// the page cache, via mincore on a PROT_NONE mapping. It returns 0 for files
// that vanish mid-measurement, since the scratch file is removed when flushed.
func residentBytes(path string) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil || fi.Size() == 0 {
		return 0
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(fi.Size()), unix.PROT_NONE, unix.MAP_SHARED)
	if err != nil {
		return 0
	}
	defer unix.Munmap(data)

	// x/sys/unix has no Mincore wrapper; invoke the raw syscall.
	pageSize := int64(os.Getpagesize())
	vec := make([]byte, (fi.Size()+pageSize-1)/pageSize)
	if _, _, errno := unix.Syscall(unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&vec[0]))); errno != 0 {
		return 0
	}

	var n int64
	for _, v := range vec {
		n += int64(v&1) * pageSize
	}
	return n
}

type discardCloser struct{}

func (discardCloser) Write(p []byte) (int, error) { return len(p), nil }
func (discardCloser) Close() error                { return nil }

// BenchmarkScratchPageCache measures the peak page-cache residency of the
// parquet writer's scratch file, which is charged against the connector's
// cgroup memory limit the same as heap memory. Standard benchmark metrics
// only see the Go heap, so residency is sampled with mincore by a background
// goroutine while rows are written, and reported as peak-pagecache-B alongside
// the peak observed scratch file size for reference. Run with -benchtime=1x
// (or a small fixed count); one op is a full write-and-close cycle.
func BenchmarkScratchPageCache(b *testing.B) {
	scratchDir, err := os.MkdirTemp("", "pagecache-bench-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(scratchDir)

	var stat unix.Statfs_t
	if err := unix.Statfs(scratchDir, &stat); err != nil {
		b.Fatal(err)
	} else if stat.Type == unix.TMPFS_MAGIC {
		b.Skip("scratch dir is tmpfs: pages are the backing store, fadvise cannot drop them")
	}

	// os.CreateTemp("", ...) consults TMPDIR on every call, so the writer's
	// scratch files land somewhere this benchmark can find them.
	b.Setenv("TMPDIR", scratchDir)

	sch := ParquetSchema{
		{Name: "id", DataType: PrimitiveTypeInteger, Required: true},
		{Name: "payload", DataType: LogicalTypeString, Required: true},
	}

	// ~1 KiB per row, ~200k rows: roughly 200 MiB through the scratch file per
	// op, flushed from the in-memory buffer in ~25 MiB row groups, without
	// reaching the 512 MiB rowGroupByteLimit until Close.
	const numRows = 200_000
	payload := strings.Repeat("x", 1024)

	// The write phase (Write calls) and the close phase (flushScratchFile's read-back of the
	// scratch file) stress the page cache differently, so their peaks are tracked separately.
	var inClose atomic.Bool
	var peakWrite, peakClose, peakSize atomic.Int64

	sample := func() {
		matches, _ := filepath.Glob(filepath.Join(scratchDir, "parquet-scratch-*"))
		for _, m := range matches {
			peak := &peakWrite
			if inClose.Load() {
				peak = &peakClose
			}
			if r := residentBytes(m); r > peak.Load() {
				peak.Store(r)
			}
			if fi, err := os.Stat(m); err == nil && fi.Size() > peakSize.Load() {
				peakSize.Store(fi.Size())
			}
		}
	}

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
				sample()
			}
		}
	}()
	defer close(done)

	b.SetBytes(int64(numRows * len(payload)))
	b.ResetTimer()

	for b.Loop() {
		w := NewParquetWriter(discardCloser{}, sch)
		inClose.Store(false)
		for i := range numRows {
			if err := w.Write([]any{int64(i), fmt.Sprintf("%08d-%s", i, payload)}); err != nil {
				b.Fatal(err)
			}
		}
		inClose.Store(true)
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(peakWrite.Load())/(1024*1024), "write-peak-pagecache-MiB")
	b.ReportMetric(float64(peakClose.Load())/(1024*1024), "close-peak-pagecache-MiB")
	b.ReportMetric(float64(peakSize.Load())/(1024*1024), "peak-scratch-MiB")
}
