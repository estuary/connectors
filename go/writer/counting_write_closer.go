package writer

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

// Tell satisfies the parquet/internal/utils.WriterTell interface that arrow-go's
// file.NewPageWriter requires of its sink. We return the running byte count.
func (c *countingWriteCloser) Tell() int64 {
	return int64(c.written)
}
