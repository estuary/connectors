//go:build !linux

package writer

import "os"

// dropPageCache is a no-op outside of Linux; connectors only run in Linux containers in
// production, and fadvise has no portable equivalent worth pursuing for local dev/test builds.
func dropPageCache(f *os.File) {}

func flushAndDropPageCache(f *os.File) {}

func dropPageCacheRange(f *os.File, off, length int64) {}
