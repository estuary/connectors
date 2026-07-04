package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

// TestStreamingMemoryClimbRepro reproduces the memory accumulation observed
// with Snowpipe Streaming during large transactions. It mimics exactly what
// streamManager does over the Store phase of a single transaction: rows are
// written to a bdecWriter, the blob is rotated when its single row group
// reaches MAX_CHUNK_SIZE_IN_BYTES_DEFAULT (256 MiB), and each finished blob's
// blobStatsTracker is retained until the transaction commits (flush).
//
// Memory that should be released (or never held) per finished blob is instead
// retained until commit:
//
//  1. blobStatsTracker.buf grows to the largest single Write from the parquet
//     writer -- roughly one Snappy-compressed page, which for large document
//     values is close to an entire ~25 MiB scratch row group column chunk --
//     and is kept per blob.
//  2. columnStatsTracker min/max string statistics alias the backing arrays of
//     full document values, pinning up to two whole documents per string or
//     variant column per blob.
//
// The test fails ("OOM") when the live heap after GC exceeds MEM_LIMIT_BYTES
// (default 1 GiB, matching the connector container limit). Run with:
//
//	RUN_MEM_REPRO=1 go test -v -run TestStreamingMemoryClimbRepro -timeout 30m ./materialize-snowflake
//
// Knobs: MEM_LIMIT_BYTES (default 1 GiB), DOC_KB (default 1024), MAX_BLOBS
// (default 48; 48 blobs is a ~12 GiB transaction).
func TestStreamingMemoryClimbRepro(t *testing.T) {
	if os.Getenv("RUN_MEM_REPRO") != "1" {
		t.Skip("set RUN_MEM_REPRO=1 to run this memory reproduction")
	}

	memLimit := envInt("MEM_LIMIT_BYTES", 1<<30)
	docKB := envInt("DOC_KB", 1024)
	maxBlobs := envInt("MAX_BLOBS", 48)

	zeroScale := 0
	mapped := []*sql.Column{{Identifier: "ID"}, {Identifier: "DOC"}}
	existing := []tableColumn{
		{Name: "ID", Type: "number", LogicalType: "fixed", PhysicalType: "SB16", Scale: &zeroScale, Nullable: false, Ordinal: 1},
		{Name: "DOC", Type: "variant", LogicalType: "variant", PhysicalType: "LOB", Nullable: true, Ordinal: 2},
	}

	encryptionKey := "aGVsbG8K"
	rng := rand.New(rand.NewSource(1))

	// Mimics streamManager.blobStats: one tracker per finished blob, all
	// retained until the transaction's StartCommit calls flush.
	var trackers []*blobStatsTracker
	var totalBytes int64
	var rowID int64

	logMem := func(blobs int) uint64 {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		var bufBytes int
		for _, tr := range trackers {
			bufBytes += cap(tr.buf)
		}
		t.Logf("blobs=%d dataWritten=%dMiB liveHeapAfterGC=%dMiB retainedXORBufs=%dMiB",
			blobs, totalBytes>>20, ms.HeapAlloc>>20, bufBytes>>20)
		return ms.HeapAlloc
	}

	for n := range maxBlobs {
		bw, err := newBdecWriter(
			discardWriteCloser{},
			mapped,
			existing,
			encryptionKey,
			blobFileName(fmt.Sprintf("blob_%d.bdec", n)),
		)
		require.NoError(t, err)

		// Write rows until the blob's single row group is full, exactly as
		// streamManager.writeRow does.
		for !bw.done {
			doc := makeDoc(rng, docKB<<10)
			totalBytes += int64(len(doc))
			require.NoError(t, bw.writeRow([]any{rowID, json.RawMessage(doc)}))
			rowID++
		}
		require.NoError(t, bw.close())
		trackers = append(trackers, bw.blobStats)

		if live := logMem(n + 1); live > uint64(memLimit) {
			t.Fatalf(
				"OOM: live heap %d MiB exceeds %d MiB limit after %d blobs (%d MiB of transaction data)",
				live>>20, memLimit>>20, n+1, totalBytes>>20,
			)
		}
	}

	// Optionally write a heap profile while the trackers are still retained,
	// to attribute any remaining per-blob memory growth.
	if profilePath := os.Getenv("HEAP_PROFILE"); profilePath != "" {
		runtime.GC()
		f, err := os.Create(profilePath)
		require.NoError(t, err)
		require.NoError(t, pprof.WriteHeapProfile(f))
		require.NoError(t, f.Close())
		t.Logf("wrote heap profile to %s", profilePath)
	}

	// Transaction commit: blob metadata is generated and the trackers are
	// released, as streamManager.flush does.
	ch := &channel{Database: "DB", Schema: "S", Table: "T", Channel: "C"}
	for i, tr := range trackers {
		_ = generateBlobMetadata(tr, ch, blobToken("basetoken", i))
	}
	trackers = nil
	logMem(-1)
}

// makeDoc produces a JSON document of roughly n bytes with incompressible
// content, approximating a real flow_document of that size.
func makeDoc(rng *rand.Rand, n int) []byte {
	raw := make([]byte, n*3/4)
	rng.Read(raw)
	return []byte(`{"data":"` + base64.StdEncoding.EncodeToString(raw) + `"}`)
}

func envInt(name string, fallback int) int {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Sprintf("invalid %s: %v", name, err))
		}
		return n
	}
	return fallback
}
