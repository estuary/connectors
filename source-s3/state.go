package main

import (
	"time"
)

// state related to a single object
type objectState struct {
	LastModified time.Time `json:"mod"`
	ETag         string    `json:"etag"`
	RecordCount  uint64    `json:"n"`
	Complete     bool      `json:"done"`
}

// streamState is a map of the relative object key to the object state
type streamState map[string]*objectState

func copyStreamState(s streamState) streamState {
	var c = make(map[string]*objectState)
	for k, v := range s {
		var vCopy = *v
		c[k] = &vCopy
	}
	return c
}

// stateMap is a map of streamStates, keyed on stream.
type stateMap map[string]streamState

func (s stateMap) Validate() error {
	return nil
}
