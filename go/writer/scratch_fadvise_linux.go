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

// flushAndDropPageCache forces writeback of f's dirty pages and then drops them from the page
// cache. FADV_DONTNEED skips dirty pages, so the writeback must complete first for the drop to
// take effect on a freshly written file. Calling this after each row group written to the scratch
// file caps the file's resident page cache at roughly one buffer's worth, rather than letting it
// grow to the full scratch file size.
func flushAndDropPageCache(f *os.File) {
	_ = unix.SyncFileRange(int(f.Fd()), 0, 0, // whole file
		unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}

// dropPageCacheRange drops a byte range of f from the page cache. It only works on clean pages,
// so it is suited to ranges that were read back from a file already synced by
// flushAndDropPageCache. The kernel rounds the range inward to whole pages.
func dropPageCacheRange(f *os.File, off, length int64) {
	_ = unix.Fadvise(int(f.Fd()), off, length, unix.FADV_DONTNEED)
}
