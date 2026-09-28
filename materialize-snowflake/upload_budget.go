package connector

import (
	"github.com/estuary/connectors/go/common"
	"golang.org/x/sync/semaphore"
)

// The gosnowflake driver hands uploadPartSize to the S3 transfer manager as
// both the part size and the multipart threshold. The transfer manager then
// reads one part up front and keeps a pool of uploadParallel+1 parts, so every
// in-flight PUT to an S3-backed stage holds about uploadMemoryPerFile of heap.
// That figure was measured at 29-30 MiB per upload with 8 MiB parts (the
// growth of the first read adds to the three parts), and 205 MiB with the
// driver's 64 MiB default. GCS-backed stages stream the file and hold
// nothing. uploadBudgetPercent of the container memory limit is divided by
// the per-upload cost to bound how many PUTs may run at once across all
// bindings; the rest is left for the per-binding gzip writers (about 2.5 MiB
// each), row conversion, and the Go runtime.
const (
	uploadPartSize      = 8 << 20
	uploadParallel      = 1
	uploadMemoryPerFile = 32 << 20
	uploadBudgetPercent = 50
)

func newUploadLimiter() *semaphore.Weighted {
	slots := common.MemoryLimit() * uploadBudgetPercent / 100 / uploadMemoryPerFile
	return semaphore.NewWeighted(max(1, slots))
}
