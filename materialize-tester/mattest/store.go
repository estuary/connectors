package mattest

import (
	"sync"

	"github.com/estuary/protocols/fdb/tuple"
)

type TestStore struct {
	store []StoreItem
	sync.Mutex
}

func NewTestStore() *TestStore {
	return &TestStore{
		store: make([]StoreItem, 0),
	}
}

type StoreItem interface {
	Keys() tuple.Tuple
	SetKeys(keys tuple.Tuple)
	Values() tuple.Tuple
	JSON() []byte
}

func (ts *TestStore) Push(item ...StoreItem) {
	ts.Lock()
	ts.store = append(ts.store, item...)
	ts.Unlock()
}

func (ts *TestStore) Pop() StoreItem {
	ts.Lock()
	defer ts.Unlock()
	if len(ts.store) == 0 {
		return nil
	}
	item := ts.store[len(ts.store)-1]
	ts.store = ts.store[:len(ts.store)-1]
	return item
}

func (ts *TestStore) Len() int {
	ts.Lock()
	l := len(ts.store)
	ts.Unlock()
	return l
}

func (ts *TestStore) Range() []StoreItem {
	return ts.store
}

func (ts *TestStore) Find(findKeys tuple.Tuple, remove bool) StoreItem {
	ts.Lock()
	defer ts.Unlock()
findLoop:
	for i, item := range ts.store {
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
			ts.store = append(ts.store[:i], ts.store[i+1:]...)
		}
		return item
	}
	return nil
}
