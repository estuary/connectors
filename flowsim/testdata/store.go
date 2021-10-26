package testdata

import (
	"sync"

	"github.com/estuary/protocols/fdb/tuple"
)

// TestStore is a simple store for TestData that supports the functions needed to test connectors.
type Store struct {
	store []TestData
	sync.Mutex
}

// NewTestStore creates a new TestStore.
func NewStore() *Store {
	return &Store{
		store: make([]TestData, 0),
	}
}

// Push adds one or more item to the end of a store.
func (s *Store) Push(item ...TestData) {
	s.Lock()
	s.store = append(s.store, item...)
	s.Unlock()
}

// Pop removes the last item from the store.
func (s *Store) Pop() TestData {
	s.Lock()
	defer s.Unlock()
	if len(s.store) == 0 {
		return nil
	}
	item := s.store[len(s.store)-1]
	s.store = s.store[:len(s.store)-1]
	return item
}

// Len returns the number of items in the store.
func (s *Store) Len() int {
	s.Lock()
	l := len(s.store)
	s.Unlock()
	return l
}

// Range returns a slice of all the items in the store.
func (s *Store) Range() []TestData {
	return s.store
}

// Clear blanks the store.
func (s *Store) Clear() {
	s.store = s.store[:0]
}

// Find looks up an item in the store by it keys returns it. It optionally removes the found item.
func (s *Store) Find(findKeys tuple.Tuple, remove bool) TestData {
	s.Lock()
	defer s.Unlock()
findLoop:
	for i, item := range s.store {
		itemKeys := item.Keys()
		if len(itemKeys) != len(findKeys) {
			continue
		}
		for j, key := range findKeys {
			if itemKeys[j] != key {
				continue findLoop
			}
		}
		if remove {
			s.store = append(s.store[:i], s.store[i+1:]...)
		}
		return item
	}
	return nil
}
