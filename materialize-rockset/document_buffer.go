package materialize_rockset

import (
	"encoding/json"
	"fmt"
)

// Thin wrapper around a `[]json.RawMessage` for easy and efficient access patterns.
type DocumentBuffer struct {
	inner []json.RawMessage
}

// Creates a new buffer. This should be used sparingly, as the buffer is
// intended to be reused after being emptied with `SplitN`.
func NewDocumentBuffer() *DocumentBuffer {
	return &DocumentBuffer{inner: make([]json.RawMessage, 0, 1024)}
}

func (b *DocumentBuffer) Len() int {
	return len(b.inner)
}

func (b *DocumentBuffer) Push(item json.RawMessage) {
	b.inner = append(b.inner, item)
}

// Splits the buffer into N non-overlapping chunks.
func (b *DocumentBuffer) SplitN(n int) [][]json.RawMessage {
	if n < 1 {
		panic(fmt.Sprintf("cannot split into %v chunks. n must be a positive integer", n))
	}

	chunks := make([][]json.RawMessage, n)

	originalSize := b.Len()
	if originalSize > 0 {
		chunkSize := originalSize / n

		for i := 0; i < n-1; i++ {
			chunks[i] = b.inner[chunkSize*i : chunkSize*(i+1)]
		}
		// Grab any remainder of the slice that didn't divide evenly into n chunks.
		chunks[n-1] = b.inner[chunkSize*(n-1):]
	}

	return chunks
}

// Empty out the inner buffer.
func (b *DocumentBuffer) Clear() {
	b.inner = b.inner[:0]
}
