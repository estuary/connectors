package stream_encode

import (
	"fmt"
	"io"
)

type countingWriteCloser struct {
	written int
	w       io.WriteCloser
}

func (c *countingWriteCloser) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if err != nil {
		return 0, fmt.Errorf("countingWriteCloser writing to w: %w", err)
	}
	c.written += n

	return n, nil
}

func (c *countingWriteCloser) Close() error {
	if err := c.w.Close(); err != nil {
		return fmt.Errorf("countingWriteCloser closing w: %w", err)
	}
	return nil
}
