package connector

import (
	"github.com/estuary/connectors/go/common"
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
// per-binding cost of an open staged file (a serial gzip writer plus row
// buffers, about 1 MiB) and the in-flight PUTs; whatever the bindings don't
// need is divided by the per-upload cost to bound how many PUTs may run at
// once across all bindings. The rest of the limit is left for row conversion,
// the driver, and the Go runtime.
const (
	uploadPartSize       = 8 << 20
	uploadParallel       = 1
	uploadMemoryPerFile  = 32 << 20
	bindingMemoryReserve = 2 << 20
	uploadBudgetPercent  = 50
)

func newUploadLimiter(bindings int) *semaphore.Weighted {
	budget := common.MemoryLimit()*uploadBudgetPercent/100 - int64(bindings)*bindingMemoryReserve
	slots := max(1, budget/uploadMemoryPerFile)
	log.WithFields(log.Fields{
		"bindings":          bindings,
		"concurrentUploads": slots,
	}).Info("upload concurrency budget")
	return semaphore.NewWeighted(slots)
}
