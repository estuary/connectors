package main

import (
	"context"
	"sync"
)

type shardState int

const (
	stateReading shardState = iota
	stateFinished
)

// shardTracker tracks which shards are completed and currently being read.
// Shard splits or merges create situations where the capture must know that the
// parent shard(s) have been completely read, and in the case of a shard merge
type shardTracker struct {
	mu      sync.Mutex
	states  map[string]shardState
	waiters map[string]chan struct{}
}

func newShardTracker() *shardTracker {
	return &shardTracker{
		states:  make(map[string]shardState),
		waiters: make(map[string]chan struct{}),
	}
}

func (st *shardTracker) setReading(shardId string) bool {
	st.mu.Lock()
	defer st.mu.Unlock()

	if _, ok := st.states[shardId]; ok {
		// Already being read, or already finished.
		return false
	}

	st.states[shardId] = stateReading
	return true
}

func (st *shardTracker) setFinished(shardId string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.states[shardId] = stateFinished
	if ch, ok := st.waiters[shardId]; ok {
		close(ch)
		delete(st.waiters, shardId)
	}
}

func (st *shardTracker) waitForFinished(shardId string) func(context.Context) error {
	st.mu.Lock()
	if st.states[shardId] == stateFinished {
		st.mu.Unlock()
		return nil
	}

	ch, ok := st.waiters[shardId]
	if !ok {
		st.waiters[shardId] = make(chan struct{})
		ch = st.waiters[shardId]
	}
	st.mu.Unlock()

	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			return nil
		}
	}
}
