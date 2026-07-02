//go:build linux

package writer

import (
	"os"

	"golang.org/x/sys/unix"
)

// dropPageCache hints to the kernel that f's cached pages can be released immediately, rather
// than lingering as reclaimable-but-unreclaimed page cache. Page cache from scratch file I/O is
// charged against the process's cgroup memory limit the same as heap memory, but the kernel has
// no reason to prioritize reclaiming it until it's under memory pressure -- which can be too late
// against a hard cgroup memory.max with no throttling runway.
func dropPageCache(f *os.File) {
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
