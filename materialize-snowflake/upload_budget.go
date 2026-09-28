package connector

import (
	"github.com/estuary/connectors/go/common"
	"golang.org/x/sync/semaphore"
)

// The gosnowflake driver hands uploadPartSize to the S3 transfer manager as
// both the part size and the multipart threshold. The transfer manager then
// reads one part up front and keeps a pool of uploadParallel+1 parts, so every
// in-flight PUT holds about uploadMemoryPerFile of heap. uploadBudgetPercent of
// the container memory limit is divided by that cost to bound how many PUTs
// may run at once across all bindings.
const (
	uploadPartSize      = 8 << 20
	uploadParallel      = 1
	uploadMemoryPerFile = (uploadParallel + 2) * uploadPartSize
	uploadBudgetPercent = 80
)

func newUploadLimiter() *semaphore.Weighted {
	slots := common.MemoryLimit() * uploadBudgetPercent / 100 / uploadMemoryPerFile
	return semaphore.NewWeighted(max(1, slots))
}
