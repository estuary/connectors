package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type getOpTimeFn func(context.Context) (primitive.Timestamp, error)

type streamBackfillCoordinator struct {
	mu               sync.Mutex
	opTimeWatermark  primitive.Timestamp
	streamsInCatchup map[string]bool
	catchupDone      chan struct{}
	getClusterOpTime getOpTimeFn
}

// newBatchStreamCoordinator returns an apparatus that is used for coordinating
// backfills of streamed collections, backfills of collections captured in
// "batch" mode, and the change streams themselves:
//   - Streamed collections must not concurrently emit change events from their
//     change stream and documents read from a backfill, as this could result in
//     races between backfilled documents being emitted after and clobbering more
//     recent change stream documents.
//   - Batch mode collections can be read concurrently with on-going change streams.
//     It is important for the change streams to remain reasonably caught up to stay
//     within the oplog retention window, so occasionally any on-going batch backfills
//     will be paused until all change streams are fully caught up to mitigate
//     potential resource contention issues.
func newBatchStreamCoordinator(changeStreamBindings []bindingInfo, getOpTime getOpTimeFn) *streamBackfillCoordinator {
	out := &streamBackfillCoordinator{
		opTimeWatermark:  primitive.Timestamp{},
		streamsInCatchup: map[string]bool{},
		catchupDone:      make(chan struct{}),
		getClusterOpTime: getOpTime,
	}

	// Initialized in an "all change streams are caught up" state.
	for _, db := range databasesForBindings(changeStreamBindings) {
		out.streamsInCatchup[db] = false
	}
	close(out.catchupDone)

	return out
}

func (b *streamBackfillCoordinator) startCatchingUp(ctx context.Context) error {
	if len(b.streamsInCatchup) == 0 {
		// No change streams included in the capture, so there is nothing to
		// catch up.
		return nil
	}

	latestOpTime, err := b.getClusterOpTime(ctx)
	if err != nil {
		return fmt.Errorf("getting cluster op time: %w", err)
	}
	log.WithField("lastWriteOpTime", latestOpTime).Info("catching up streams")

	b.mu.Lock()
	defer b.mu.Unlock()

	b.catchupDone = make(chan struct{})
	b.opTimeWatermark = latestOpTime
	for db := range b.streamsInCatchup {
		b.streamsInCatchup[db] = true
	}

	return nil
}

func (b *streamBackfillCoordinator) gotCaughtUp(db string, latest primitive.Timestamp) bool {
	// Op times are extracted from resume tokens, and should never be zero.
	if latest.IsZero() {
		panic("internal error: latest opTime is zero")
	}

	if b.opTimeWatermark.After(latest) {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.streamsInCatchup[db]; !ok {
		panic(fmt.Sprintf("interal error: db %q not recognized", db))
	}

	if !b.streamsInCatchup[db] {
		return false
	}

	// This stream is caught up. If all streams are now caught up, signal the
	// catchupDone channel.
	b.streamsInCatchup[db] = false
	allStreamsCaughtUp := true
	for _, stillCatchingUp := range b.streamsInCatchup {
		if stillCatchingUp {
			allStreamsCaughtUp = false
			break
		}
	}

	if allStreamsCaughtUp {
		close(b.catchupDone)
		log.Info("finished catching up streams")
	}

	return true
}

func (b *streamBackfillCoordinator) streamsCaughtUp() <-chan struct{} {
	b.mu.Lock()
	// This will either be a closed channel if no streams are catching up, or an
	// open channel that will eventually be closed in gotCaughtUp if streams are
	// catching up. It may be re-assigned in startCatchingUp, so a reference is
	// obtained here under lock.
	catchupDone := b.catchupDone
	b.mu.Unlock()

	return catchupDone
}

func (b *streamBackfillCoordinator) startRepeatingStreamCatchups(ctx context.Context) error {
	ticker := time.NewTicker(backfillFor)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			b.startCatchingUp(ctx)
		}
	}
}

func databasesForBindings(bindings []bindingInfo) []string {
	out := []string{}

	for _, b := range bindings {
		out = append(out, b.resource.Database)
	}

	slices.Sort(out)
	return slices.Compact(out)
}
