package common

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

// defaultMemoryLimit is the assumed container memory limit when the cgroup
// limit cannot be determined. This matches the standard 1GiB reactor allocation.
const defaultMemoryLimit int64 = 1 << 30 // 1 GiB

// memoryLimit holds the actual container memory limit in bytes, as determined
// by ConfigureMemoryLimit. It defaults to 1 GiB and is updated if the cgroup
// limit is successfully read.
var memoryLimit = defaultMemoryLimit

// ConfigureMemoryLimit reads the container memory limit from cgroup and
// configures the Go runtime's GC memory limit to 90% of that value. If the
// cgroup limit is unavailable, the GOMEMLIMIT environment variable (set in
// the base Docker image) applies as-is.
//
// This should be called once, early in process startup.
func ConfigureMemoryLimit() {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		log.WithField("err", err).Debug("unable to read cgroup memory limit, using default")
		return
	}

	var text = strings.TrimSpace(string(data))
	if text == "max" {
		log.Debug("cgroup memory limit is 'max' (unlimited), using default")
		return
	}

	limit, err := strconv.ParseInt(text, 10, 64)
	if err != nil || limit <= 0 {
		log.WithFields(log.Fields{"err": err, "value": text}).Debug("unable to parse cgroup memory limit, using default")
		return
	}

	memoryLimit = limit
	var goMemLimit = limit * 9 / 10
	debug.SetMemoryLimit(goMemLimit)

	log.WithFields(log.Fields{
		"cgroup_limit": fmt.Sprintf("%.0fMiB", float64(limit)/(1024*1024)),
		"go_mem_limit": fmt.Sprintf("%.0fMiB", float64(goMemLimit)/(1024*1024)),
	}).Debug("configured memory limit from cgroup")
}

// MemoryLimit returns the container memory limit in bytes. If ConfigureMemoryLimit
// was not called or could not determine the cgroup limit, this returns 1 GiB.
func MemoryLimit() int64 {
	return memoryLimit
}
