package boilerplate

import (
	"bufio"
	"bytes"
	"testing"
)

func TestPeekMessage(t *testing.T) {
	// Construct a buffered reader with 256kB buffer and a bunch of bytes available to read.
	const shortMessageSize = 1024
	const bufferSize = 256 * 1024
	const longMessageSize = 1024 * 1024
	const totalDataSize = 8 * longMessageSize
	var data = make([]byte, totalDataSize)
	var br = bufio.NewReaderSize(bytes.NewReader(data), bufferSize)

	// Make sure a mix of short and long reads works
	for i := 0; i < 4; i++ {
		var bs, err = peekMessage(br, shortMessageSize)
		if err != nil {
			t.Fatalf("short message: %v", err)
		}
		if len(bs) != shortMessageSize {
			t.Fatalf("short message: got %d bytes, expected %d", len(bs), shortMessageSize)
		}

		bs, err = peekMessage(br, longMessageSize)
		if err != nil {
			t.Fatalf("long message: %v", err)
		}
		if len(bs) != longMessageSize {
			t.Fatalf("long message: got %d bytes, expected %d", len(bs), longMessageSize)
		}
	}
}
