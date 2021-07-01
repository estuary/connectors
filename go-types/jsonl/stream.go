package jsonl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Stream is an io.Writer implementation that parses JSONL output, typically from the invocation
// of an external program. It invokes callbacks for each decoded JSON document or on an error.
type Stream struct {
	// OnNew returns a value to decode into
	OnNew func() interface{}
	// OnDecode is called with the decoded values
	OnDecode func(interface{}) error
	// OnError is called when an error is encountered either with decoding json, or if OnDecode
	// returns an error.
	OnError func(error)
	buffer  []byte
}

func (o *Stream) Write(b []byte) (int, error) {
	if len(o.buffer) == 0 {
		o.buffer = append([]byte(nil), b...) // Clone.
	} else {
		o.buffer = append(o.buffer, b...)
	}

	var ind = bytes.LastIndexByte(o.buffer, '\n') + 1
	var chunk = o.buffer[:ind]
	o.buffer = o.buffer[ind:]

	var dec = json.NewDecoder(bytes.NewReader(chunk))
	dec.DisallowUnknownFields()

	for {
		var rec = o.OnNew()

		if err := dec.Decode(rec); err == io.EOF {
			return len(b), nil
		} else if err != nil {
			o.OnError(fmt.Errorf("decoding connector record: %w", err))
			return len(b), nil
		} else if err = o.OnDecode(rec); err != nil {
			o.OnError(err)
			return len(b), nil
		}
	}
}

func (o *Stream) Close() error {
	if len(o.buffer) != 0 {
		o.OnError(fmt.Errorf("connector stdout closed without a final newline: %q", string(o.buffer)))
	}
	return nil
}
