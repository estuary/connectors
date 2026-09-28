package connector

import (
	"github.com/estuary/connectors/go/common"
	"github.com/estuary/connectors/go/writer"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// The gosnowflake driver hands uploadPartSize to the S3 transfer manager as
// both the part size and the multipart threshold. The transfer manager then
// reads one part up front and keeps a pool of uploadParallel+1 parts, so every
// in-flight PUT to an S3-backed stage holds about uploadMemoryPerFile of heap.
// That figure was measured at 29-30 MiB per upload with 8 MiB parts (the
// growth of the first read adds to the three parts), and 205 MiB with the
// driver's 64 MiB default. GCS-backed stages stream the file and hold
// nothing.
//
// uploadBudgetPercent of the container memory limit is shared between the
// open staged file of every binding and the in-flight PUTs. Each open file
// holds a gzip writer: pgzip with 256 KiB blocks compresses off the writing
// goroutine at full speed (271 MB/s on 2 CPUs) for about 4 MiB, and serial
// gzip halves that throughput for under 2 MiB. pgzip is used while it leaves
// room for minUploadSlots uploads, serial beyond that, and whatever the
// writers don't need is divided into upload slots. The rest of the limit is
// left for row conversion, the driver, and the Go runtime.
const (
	uploadPartSize      = 8 << 20
	uploadParallel      = 1
	uploadMemoryPerFile = 32 << 20
	uploadBudgetPercent = 50

	parallelWriterMemory = 4 << 20
	serialWriterMemory   = 2 << 20
	minUploadSlots       = 4
)

type stagingBudget struct {
	writerOpts []writer.JsonOption
	uploads    *semaphore.Weighted
}

func newStagingBudget(bindings int) stagingBudget {
	budget := common.MemoryLimit() * uploadBudgetPercent / 100
	perBinding, opts := int64(parallelWriterMemory), []writer.JsonOption{writer.WithJsonCompressionBlocks(256<<10, 1)}
	if budget-int64(bindings)*perBinding < minUploadSlots*uploadMemoryPerFile {
		perBinding, opts = serialWriterMemory, []writer.JsonOption{writer.WithJsonSerialCompression()}
	}
	slots := max(1, (budget-int64(bindings)*perBinding)/uploadMemoryPerFile)
	log.WithFields(log.Fields{
		"bindings":          bindings,
		"serialCompression": perBinding == serialWriterMemory,
		"concurrentUploads": slots,
	}).Info("staging memory budget")
	return stagingBudget{writerOpts: opts, uploads: semaphore.NewWeighted(slots)}
}
