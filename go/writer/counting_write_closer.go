package writer

import (
	"fmt"
	"io"
	"sync/atomic"
)

// countingWriteCloser counts the bytes written to w. The count is atomic so that it
// can be read while another goroutine writes.
type countingWriteCloser struct {
	written atomic.Int64
	w       io.WriteCloser
}

func (c *countingWriteCloser) Write(p []byte) (int, error) {
	var n, err = c.w.Write(p)
	if err != nil {
		return 0, fmt.Errorf("countingWriteCloser writing to w: %w", err)
	}
	c.written.Add(int64(n))

	return n, nil
}

func (c *countingWriteCloser) Close() error {
	if err := c.w.Close(); err != nil {
		return fmt.Errorf("countingWriteCloser closing w: %w", err)
	}
	return nil
}
