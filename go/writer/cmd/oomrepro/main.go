// Command oomrepro reproduces the materialize-s3-parquet OOM seen on wide-row tables (e.g.
// base64-gzipped-XML columns around 181KB/row) by driving writer.ParquetWriter directly with
// synthetic high-entropy rows, using the same GOMEMLIMIT/cgroup setup as production
// (common.ConfigureMemoryLimit). Run it inside a container with a real memory limit to observe
// the same SIGKILL the reactor sees:
//
//	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/oomrepro ./go/writer/cmd/oomrepro
//	docker run --rm --name oomrepro --memory=1g --memory-swap=1g \
//	  -v /tmp/oomrepro:/oomrepro:ro golang:1.25 /oomrepro -rows=0 -log-every=25
//
// Swap GOARCH to amd64 if the Docker daemon's VM/host is x86_64.
package main

import (
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/estuary/connectors/go/common"
	"github.com/estuary/connectors/go/writer"
)

type discardWriteCloser struct{}

func (discardWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (discardWriteCloser) Close() error                { return nil }

func main() {
	rows := flag.Int("rows", 0, "total rows to write before closing (0 = run until killed)")
	rowBytes := flag.Int("row-bytes", 181*1024, "raw byte size of the wide column's payload, pre-base64")
	logEvery := flag.Int("log-every", 50, "log memory stats every N rows")
	rowGroupByteLimitMiB := flag.Int("row-group-byte-limit-mib", 512, "ParquetWriter row group byte limit, MiB")
	rowGroupRowLimit := flag.Int("row-group-row-limit", 1_000_000, "ParquetWriter row group row limit")
	bufferSizeMiB := flag.Int("buffer-size-mib", 25, "ParquetWriter in-heap row buffer size, MiB")
	flag.Parse()

	common.ConfigureMemoryLimit()
	fmt.Printf("effective GOMEMLIMIT = %d MiB\n", debug.SetMemoryLimit(-1)/1024/1024)

	sch := writer.ParquetSchema{
		{Name: "id", DataType: writer.PrimitiveTypeInteger, Required: true},
		{Name: "payload", DataType: writer.LogicalTypeString, Required: true},
	}

	w := writer.NewParquetWriter(discardWriteCloser{}, sch,
		writer.WithParquetRowGroupByteLimit(*rowGroupByteLimitMiB*1024*1024),
		writer.WithParquetRowGroupRowLimit(*rowGroupRowLimit),
		writer.WithParquetBufferSize(*bufferSizeMiB*1024*1024),
	)

	// High-entropy payload, matching the base64-gzipped-XML column that triggers the OOM:
	// incompressible, so parquet dictionary encoding falls back immediately for every row.
	raw := make([]byte, *rowBytes)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	payload := base64.StdEncoding.EncodeToString(raw)

	logStats := func(i int) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		cur, _ := os.ReadFile("/sys/fs/cgroup/memory.current")
		peak, _ := os.ReadFile("/sys/fs/cgroup/memory.peak")
		stat := readMemoryStat()
		fmt.Printf("rows=%d heapAlloc=%dMiB heapSys=%dMiB cgroupCurrent=%sMiB cgroupPeak=%sMiB anon=%dMiB file=%dMiB activeFile=%dMiB inactiveFile=%dMiB\n",
			i, m.HeapAlloc/1024/1024, m.HeapSys/1024/1024,
			mib(cur), mib(peak),
			stat["anon"]/1024/1024, stat["file"]/1024/1024,
			stat["active_file"]/1024/1024, stat["inactive_file"]/1024/1024)
	}

	start := time.Now()
	for i := 0; *rows == 0 || i < *rows; i++ {
		if err := w.Write([]any{int64(i), payload}); err != nil {
			panic(fmt.Sprintf("write failed at row %d: %v", i, err))
		}
		if i%*logEvery == 0 {
			logStats(i)
		}
	}

	fmt.Println("closing writer (this is where FinalizeTxn crashes in production)...")
	if err := w.Close(); err != nil {
		panic(fmt.Sprintf("close failed: %v", err))
	}
	fmt.Printf("done in %s\n", time.Since(start))
}

// mib converts a cgroup stat file's raw byte count to a MiB string for logging.
func mib(b []byte) string {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "?"
	}
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return "?"
	}
	return fmt.Sprintf("%d", n/1024/1024)
}

// readMemoryStat parses /sys/fs/cgroup/memory.stat (cgroup v2), returning byte values keyed by
// field name (e.g. "anon", "file", "active_file", "inactive_file"). Used to distinguish Go heap
// (anon) growth from page-cache (file) growth charged against the same cgroup memory.max.
func readMemoryStat() map[string]int64 {
	out := make(map[string]int64)
	data, err := os.ReadFile("/sys/fs/cgroup/memory.stat")
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		var n int64
		if _, err := fmt.Sscanf(fields[1], "%d", &n); err == nil {
			out[fields[0]] = n
		}
	}
	return out
}
