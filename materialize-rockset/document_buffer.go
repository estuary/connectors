package materialize_rockset

import (
	"fmt"
)

// Thin wrapper around a `[]json.RawMessage` for easy and efficient access patterns.
type DocumentBuffer struct {
	inner []map[string]interface{}
}

// Creates a new buffer. This should be used sparingly, as the buffer is
// intended to be reused after being emptied with `Clear`.
func NewDocumentBuffer() *DocumentBuffer {
	return &DocumentBuffer{inner: make([]map[string]interface{}, 0, 1024)}
}

func (b *DocumentBuffer) Len() int {
	return len(b.inner)
}

func (b *DocumentBuffer) Push(item map[string]interface{}) {
	b.inner = append(b.inner, item)
}

// Splits the buffer into N non-overlapping chunks. Once split, no more
// documents should be added until `Clear` is invoked. All chunks must be
// dropped before calling `Clear`.
func (b *DocumentBuffer) SplitN(n int) [][]map[string]interface{} {
	if n < 1 {
		panic(fmt.Sprintf("cannot split into %v chunks. n must be a positive integer", n))
	}

	chunks := make([][]map[string]interface{}, n)

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
