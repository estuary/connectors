//go:build linux

package boilerplate

import (
	"os"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

// enlargeOutputPipe grows the kernel buffer of the pipe behind f to at least
// targetSize bytes. It does nothing if f is not a pipe, and logs a warning
// if the pipe can't be enlarged.
func enlargeOutputPipe(f *os.File, targetSize int) {
	var fd = uintptr(f.Fd())
	var current, err = unix.FcntlInt(fd, unix.F_GETPIPE_SZ, 0)
	if err != nil || current >= targetSize {
		return
	}
	if _, err := unix.FcntlInt(fd, unix.F_SETPIPE_SZ, targetSize); err != nil {
		log.WithFields(log.Fields{
			"err":       err,
			"current":   current,
			"requested": targetSize,
		}).Warn("failed to enlarge output pipe buffer")
	}
}
