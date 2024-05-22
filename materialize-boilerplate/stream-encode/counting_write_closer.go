package stream_encode

import (
	"fmt"
	"io"
)

type countingWriteCloser struct {
	Written int
	W       io.WriteCloser
}

func (c *countingWriteCloser) Write(p []byte) (int, error) {
	n, err := c.W.Write(p)
	if err != nil {
		return 0, fmt.Errorf("countingWriteCloser writing to w: %w", err)
	}
	c.Written += n

	return n, nil
}

func (c *countingWriteCloser) Close() error {
	if err := c.W.Close(); err != nil {
		return fmt.Errorf("countingWriteCloser closing w: %w", err)
	}
	return nil
}
