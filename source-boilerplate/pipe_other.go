//go:build !linux

package boilerplate

import "os"

// enlargeOutputPipe is a no-op on platforms without F_SETPIPE_SZ.
func enlargeOutputPipe(f *os.File, targetSize int) {}
