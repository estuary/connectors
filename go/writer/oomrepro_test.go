// TestOOMRepro reproduces the materialize-s3-parquet OOM seen on wide-row tables (e.g.
// base64-gzipped-XML columns around 181KB/row) by driving ParquetWriter with synthetic
// high-entropy rows, using the same GOMEMLIMIT/cgroup setup as production
// (common.ConfigureMemoryLimit). It's a one-shot memory-stress reproduction, not a benchmark and
// not a correctness check, so it skips itself unless OOMREPRO is set -- a plain `go test` never
// runs it. Run it inside a container with a real memory limit to observe the same SIGKILL the
// reactor sees:
//
//	GOOS=linux GOARCH=arm64 go test -c -o oomrepro ./go/writer
//	docker run --rm --name oomrepro --memory=1g --memory-swap=1g -e OOMREPRO=1 \
//	  -v $(pwd)/oomrepro:/oomrepro:ro golang:1.25 \
//	  /oomrepro -test.run=TestOOMRepro -test.v -rows=0 -log-every=25
//
// Swap GOARCH to amd64 if the Docker daemon's VM/host is x86_64.
package writer

import (
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
)

var (
	oomReproRows                 = flag.Int("rows", 0, "total rows to write before closing (0 = run until killed)")
	oomReproRowBytes             = flag.Int("row-bytes", 181*1024, "raw byte size of the wide column's payload, pre-base64 (ignored if -row-bytes-min/-avg/-max are all set)")
	oomReproRowBytesMin          = flag.Int("row-bytes-min", 0, "minimum raw row byte size for variable-size mode")
	oomReproRowBytesAvg          = flag.Int("row-bytes-avg", 0, "target average raw row byte size for variable-size mode")
	oomReproRowBytesMax          = flag.Int("row-bytes-max", 0, "maximum raw row byte size for variable-size mode (occurs rarely)")
	oomReproLogEvery             = flag.Int("log-every", 50, "log memory stats every N rows")
	oomReproRowGroupByteLimitMiB = flag.Int("row-group-byte-limit-mib", 512, "ParquetWriter row group byte limit, MiB")
	oomReproRowGroupRowLimit     = flag.Int("row-group-row-limit", 1_000_000, "ParquetWriter row group row limit")
	oomReproBufferSizeMiB        = flag.Int("buffer-size-mib", 25, "ParquetWriter in-heap row buffer size, MiB")
)

type oomReproDiscardCloser struct{}

func (oomReproDiscardCloser) Write(p []byte) (int, error) { return len(p), nil }
func (oomReproDiscardCloser) Close() error                { return nil }

func TestOOMRepro(t *testing.T) {
	if os.Getenv("OOMREPRO") == "" {
		t.Skip("set OOMREPRO=1 to run this manual memory-stress reproduction (see doc comment)")
	}

	common.ConfigureMemoryLimit()
	fmt.Printf("effective GOMEMLIMIT = %d MiB\n", debug.SetMemoryLimit(-1)/1024/1024)

	sch := ParquetSchema{
		{Name: "id", DataType: PrimitiveTypeInteger, Required: true},
		{Name: "payload", DataType: LogicalTypeString, Required: true},
	}

	variableSize := *oomReproRowBytesMin != 0 && *oomReproRowBytesAvg != 0 && *oomReproRowBytesMax != 0

	// nextSize and fixedPayload are mutually exclusive, chosen by variableSize below.
	var nextSize func() int
	var fixedPayload string

	if variableSize {
		nextSize = newOOMReproRowSizer(*oomReproRowBytesMin, *oomReproRowBytesAvg, *oomReproRowBytesMax)
	} else {
		// High-entropy payload, matching the base64-gzipped-XML column that triggers the OOM:
		// incompressible, so parquet dictionary encoding falls back immediately for every row.
		raw := make([]byte, *oomReproRowBytes)
		if _, err := rand.Read(raw); err != nil {
			t.Fatal(err)
		}
		fixedPayload = base64.StdEncoding.EncodeToString(raw)
	}

	logStats := func(i int) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		cur, _ := os.ReadFile("/sys/fs/cgroup/memory.current")
		peak, _ := os.ReadFile("/sys/fs/cgroup/memory.peak")
		stat := readOOMReproMemoryStat()
		fmt.Printf("rows=%d heapAlloc=%dMiB heapSys=%dMiB cgroupCurrent=%sMiB cgroupPeak=%sMiB anon=%dMiB file=%dMiB activeFile=%dMiB inactiveFile=%dMiB\n",
			i, m.HeapAlloc/1024/1024, m.HeapSys/1024/1024,
			oomReproMiB(cur), oomReproMiB(peak),
			stat["anon"]/1024/1024, stat["file"]/1024/1024,
			stat["active_file"]/1024/1024, stat["inactive_file"]/1024/1024)
	}

	w := NewParquetWriter(oomReproDiscardCloser{}, sch,
		WithParquetRowGroupByteLimit(*oomReproRowGroupByteLimitMiB*1024*1024),
		WithParquetRowGroupRowLimit(*oomReproRowGroupRowLimit),
		WithParquetBufferSize(*oomReproBufferSizeMiB*1024*1024),
	)

	var totalBytes, maxRowsSeen int64
	start := time.Now()
	for i := 0; *oomReproRows == 0 || i < *oomReproRows; i++ {
		payload := fixedPayload
		if variableSize {
			size := nextSize()
			totalBytes += int64(size)
			if size == *oomReproRowBytesMax {
				maxRowsSeen++
			}
			payload = oomReproRandomPayload(size)
		}

		if err := w.Write([]any{int64(i), payload}); err != nil {
			t.Fatalf("write failed at row %d: %v", i, err)
		}
		if i%*oomReproLogEvery == 0 {
			logStats(i)
			if variableSize && i > 0 {
				fmt.Printf("  (variable-size: %d rows, %d at max, actual avg raw bytes=%d)\n",
					i+1, maxRowsSeen, totalBytes/int64(i+1))
			}
		}
	}

	fmt.Println("closing writer (this is where FinalizeTxn crashes in production)...")
	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	fmt.Printf("done in %s\n", time.Since(start))
	if variableSize {
		fmt.Printf("final: max-sized rows=%d, actual avg raw bytes=%d\n", maxRowsSeen, totalBytes/int64(max(*oomReproRows, 1)))
	}
}

// newOOMReproRowSizer returns a closure generating row byte sizes whose long-run average
// approaches avg, mixing frequent small-bucket rows (uniform over [min, avg]) with a rare
// max-sized row. The small bucket's own mean is (min+avg)/2, so a small fraction p of max-sized
// rows is enough to pull the overall mean up to avg: p = (avg - smallAvg) / (max - smallAvg).
func newOOMReproRowSizer(min, avg, max int) func() int {
	smallAvg := float64(min+avg) / 2
	p := (float64(avg) - smallAvg) / (float64(max) - smallAvg)

	return func() int {
		if mrand.Float64() < p {
			return max
		}
		return min + mrand.IntN(avg-min+1)
	}
}

// oomReproRandomPayload returns a base64-encoded string of n raw random bytes. Uses math/rand/v2
// rather than crypto/rand for speed at the volumes generated here; the goal is
// incompressible-looking data to defeat parquet dictionary/RLE encoding, not cryptographic
// randomness.
func oomReproRandomPayload(n int) string {
	raw := make([]byte, n)
	for i := 0; i < n; i += 8 {
		v := mrand.Uint64()
		for j := 0; j < 8 && i+j < n; j++ {
			raw[i+j] = byte(v >> (8 * j))
		}
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// oomReproMiB converts a cgroup stat file's raw byte count to a MiB string for logging.
func oomReproMiB(b []byte) string {
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

// readOOMReproMemoryStat parses /sys/fs/cgroup/memory.stat (cgroup v2), returning byte values
// keyed by field name (e.g. "anon", "file", "active_file", "inactive_file"). Used to distinguish
// Go heap (anon) growth from page-cache (file) growth charged against the same cgroup memory.max.
func readOOMReproMemoryStat() map[string]int64 {
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
