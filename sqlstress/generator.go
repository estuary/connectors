package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type changeEvent struct {
	op  string // Either "Insert", "Update", or "Delete"
	id  int
	seq int
}

// idWindow is the number of IDs which will be actively updated at any moment
const idWindow = 50

// trafficGenerator is an infinite source of changeEvents, which occur in random
// order but follow a well-defined sequence for each ID such that most classes of
// capture correctness violation can be detected solely from the capture output.
//
// The sequence for any individual ID goes:
//   Insert(id, 0)
//   Update(id, 1)
//   Update(id, 2)
//   Delete(id)
//   Insert(id, 4)
//   Update(id, 5)
//   Update(id, 6)
//   Delete(id)
//   Insert(id, 8)
//   Update(id, 9)
//
// And so we can verify the following properties of any captured change sequence:
//  * If the captured change is a Delete, the seq was previously 2 or 6
//  * If the captured change is an Insert, the row must not have previously existed
//  * If the captured change is an Update, the row must have existed with a seq exactly one less than the new one
//  * At the end of the entire capture, all rows from 0 up to some N must exist and have seq=9
//
// Together these properties are expected to turn up most likely forms of correctness bug
// in a capture connector.
type trafficGenerator struct {
	sync.Mutex
	rng *rand.Rand

	closed bool        // Indicates that Close() has been called and we should stop introducing new IDs
	nextID int         // The next ID that isn't yet in `ids`
	ids    []int       // A list of all IDs in a non-final state
	seqs   map[int]int // A mapping from ID to sequence number
}

func newTrafficGenerator() *trafficGenerator {
	var rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	return &trafficGenerator{
		rng:    rng,
		nextID: idWindow,
		ids:    rng.Perm(idWindow),
		seqs:   make(map[int]int),
	}
}

func (g *trafficGenerator) TableDef() string {
	return "(id INTEGER PRIMARY KEY, seq INTEGER)"
}

func (g *trafficGenerator) PrimaryKey() string {
	return "id"
}

func (g *trafficGenerator) Transaction() []changeEvent {
	g.Lock()
	defer g.Unlock()

	// Accumulate 1 to N events in a list which should be applied as a transaction.
	var size = 1 + g.rng.Intn(4)
	var evts []changeEvent
	for len(evts) < size {
		var evt = g.next()
		if evt == nil {
			return evts
		}
		evts = append(evts, *evt)
	}
	return evts
}

func (g *trafficGenerator) next() *changeEvent {
	// Maintain a window of `idWindow` IDs which are actively being updated
	// so long as the generator isn't in shutdown.
	for !g.closed && len(g.ids) < idWindow {
		g.ids = append(g.ids, g.nextID)
		g.nextID++
	}

	// Once we run out, shutdown must be complete
	if len(g.ids) == 0 {
		return nil
	}

	// Select a random ID in a non-final state and get its sequence number,
	// incrementing it by one.
	var ii = g.rng.Intn(len(g.ids))
	var id = g.ids[ii]
	var seq, ok = g.seqs[id]
	if !ok {
		seq = -1
	}
	seq++
	g.seqs[id] = seq

	switch seq {
	case 0, 4, 8:
		return &changeEvent{op: "Insert", id: id, seq: seq}
	case 1, 2, 5, 6:
		return &changeEvent{op: "Update", id: id, seq: seq}
	case 3, 7:
		return &changeEvent{op: "Delete", id: id}
	case 9:
		// Forget about this ID entirely after updating it to 9
		g.ids[ii] = g.ids[len(g.ids)-1]
		g.ids = g.ids[:len(g.ids)-1]
		delete(g.seqs, id)
		return &changeEvent{op: "Update", id: id, seq: seq}
	}
	panic(fmt.Sprintf("invalid state for id %d", id))
}

func (g *trafficGenerator) Close() {
	g.Lock()
	g.closed = true
	g.Unlock()
}
