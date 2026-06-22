package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/segmentio/encoding/json"

	"github.com/estuary/connectors/go/schedule"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// backfillBinding is the information needed to backfill a binding.
type backfillBinding struct {
	binding      bindingInfo
	backfillDone chan struct{}
}

// backfillChangeStreamCollections backfills all capture collections until time runs out, or the backfill is
// complete. Each round of backfilling the bindings will be randomized to provide some degree of
// equality in how they are backfilled, but within a single round of backfilling a binding will be
// backfilled continuously until it finishes.
func (c *capture) backfillChangeStreamCollections(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	changeStreamBindings []bindingInfo,
	backfillFor time.Duration,
) error {
	ts := time.Now()
	log.WithField("backfillFor", backfillFor.String()).Info("backfilling collections")
	defer func() { log.WithField("took", time.Since(ts).String()).Info("finished backfill round") }()

	shuffledBindings := make([]bindingInfo, len(changeStreamBindings))
	copy(shuffledBindings, changeStreamBindings)
	rand.Shuffle(len(shuffledBindings), func(i, j int) {
		shuffledBindings[i], shuffledBindings[j] = shuffledBindings[j], shuffledBindings[i]
	})

	backfillBindings := make(chan backfillBinding)
	stopBackfill := make(chan struct{})

	go func() {
		<-time.After(backfillFor)
		close(stopBackfill)
	}()

	group, groupCtx := errgroup.WithContext(ctx)

	for idx := 0; idx < concurrentBackfillLimit; idx++ {
		group.Go(func() error {
			return c.backfillWorker(groupCtx, coordinator, backfillBindings, stopBackfill)
		})
	}

	// Push bindings to backfill to the backfill workers until time runs out. Each worker backfills
	// a single binding until it is done backfilling, and is then able to start working on another.
	for _, b := range shuffledBindings {
		c.mu.Lock()
		done := c.state.Resources[b.stateKey].Backfill.done()
		c.mu.Unlock()
		if done {
			continue
		}

		select {
		case backfillBindings <- backfillBinding{binding: b}:
		case <-ctx.Done():
			return ctx.Err()
		case <-groupCtx.Done():
			return group.Wait()
		case <-stopBackfill:
			return group.Wait()
		}
	}

	close(backfillBindings)
	return group.Wait()
}

// backfillBatchCollections repeatedly backfills all batch collections according
// to their schedules.
func (c *capture) backfillBatchCollections(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	batchBindings []bindingInfo,
) error {
	backfillBindings := make(chan backfillBinding)
	group, groupCtx := errgroup.WithContext(ctx)
	for idx := 0; idx < concurrentBackfillLimit; idx++ {
		group.Go(func() error {
			return c.backfillWorker(groupCtx, coordinator, backfillBindings, make(chan struct{}))
		})
	}

	for _, binding := range batchBindings {
		binding := binding

		group.Go(func() error {
			for {
				c.mu.Lock()
				backfillState := c.state.Resources[binding.stateKey].Backfill
				c.mu.Unlock()

				if backfillState.done() {
					next := binding.schedule.Next(*backfillState.LastPollStart)
					if time.Until(next) > 0 {
						log.WithFields(log.Fields{
							"database":   binding.resource.Database,
							"collection": binding.resource.Collection,
							"next":       next.UTC().String(),
							"mode":       binding.resource.getMode(),
						}).Info("waiting to backfill batch collection")
						if err := schedule.WaitForNext(groupCtx, binding.schedule, *backfillState.LastPollStart); err != nil {
							return err
						}
					}
				}

				doneCh := make(chan struct{})
				select {
				case backfillBindings <- backfillBinding{binding: binding, backfillDone: doneCh}:
					select {
					case <-doneCh:
					case <-groupCtx.Done():
						return groupCtx.Err()
					}
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
		})
	}

	return group.Wait()
}

func (c *capture) backfillWorker(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	backfillBindings <-chan backfillBinding,
	stopBackfill <-chan struct{},
) error {
	for {
		select {
		case bb, ok := <-backfillBindings:
			if !ok {
				return nil
			}

			if err := c.doBackfill(ctx, coordinator, bb.binding, bb.backfillDone, stopBackfill); err != nil {
				return fmt.Errorf(
					"backfilling collection %q in database %q: %w",
					bb.binding.resource.Collection,
					bb.binding.resource.Database,
					err,
				)
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-stopBackfill:
			return nil
		}
	}
}

// bsonBracket is one entry in MongoDB's canonical BSON type comparison order.
// min is the smallest possible value of the bracket.
type bsonBracket struct {
	types []bsontype.Type
	min   any
}

// bsonOrder lists the BSON type brackets a cursor value may fall into, in ascending
// canonical comparison order. The table is only consulted to enumerate the brackets that sort
// after a resume value. Types that cannot be meaningfully range-scanned are omitted: Array and
// the internal MinKey/MaxKey, plus Regex and the JavaScript/symbol/code types.
// See https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order
var bsonOrder = []bsonBracket{
	{types: []bsontype.Type{bson.TypeNull}, min: nil},
	{types: []bsontype.Type{bson.TypeDouble, bson.TypeInt32, bson.TypeInt64, bson.TypeDecimal128}, min: math.Inf(-1)},
	{types: []bsontype.Type{bson.TypeString}, min: ""},
	{types: []bsontype.Type{bson.TypeEmbeddedDocument}, min: bson.D{}},
	{types: []bsontype.Type{bson.TypeBinary}, min: primitive.Binary{Subtype: 0x00, Data: []byte{}}},
	{types: []bsontype.Type{bson.TypeObjectID}, min: primitive.ObjectID{}},
	{types: []bsontype.Type{bson.TypeBoolean}, min: false},
	{types: []bsontype.Type{bson.TypeDateTime}, min: primitive.DateTime(math.MinInt64)},
	{types: []bsontype.Type{bson.TypeTimestamp}, min: primitive.Timestamp{}},
}

// bracketIndex returns the index of t within bsonOrder, or -1 if t is not a type we can
// resume across with value comparisons.
func bracketIndex(t bsontype.Type) int {
	for i, b := range bsonOrder {
		if slices.Contains(b.types, t) {
			return i
		}
	}
	return -1
}

// buildResumeFilter constructs the query filter for a backfill scan ordered by cursorField.
// With no prior cursor value it returns an empty filter that scans the whole collection.
// Otherwise it must select every document that sorts after the last emitted value.
//
// A naive {cursorField: {$gt: last}} is insufficient when cursorField holds values of more
// than one BSON type. MongoDB's comparison operators are type-bracketed, so $gt only matches
// values of last's own type. A backfill interrupted partway through one type would resume,
// drain the rest of that type, observe an empty result, and incorrectly conclude the backfill
// is complete, silently skipping every document of a later-sorting type.
// Instead we express "everything after last in full BSON sort order" as a union of per-bracket
// intervals: the remainder of last's own bracket via $gt, plus the entirety of each bracket
// that sorts after it via {$gte: bracketMin}.
func buildResumeFilter(cursorField string, last *bson.RawValue) (bson.D, error) {
	if last == nil {
		return bson.D{}, nil
	}

	idx := bracketIndex(last.Type)
	if idx < 0 {
		// Regex, JavaScript/code/symbol, Array, and MinKey/MaxKey have no place in a value-range
		// resume: a regex operand is rejected by comparison operators, arrays carry multikey
		// semantics, and the rest are internal or deprecated. None of them can be an _id, so
		// reaching here means there's a bug or an unsupported custom cursor field.
		return nil, fmt.Errorf("cannot resume backfill on cursor field %q: unsupported BSON type %q for cursor value", cursorField, last.Type)
	}

	var v any
	if err := last.Unmarshal(&v); err != nil {
		return nil, fmt.Errorf("unmarshalling last_id: %w", err)
	}

	// The remainder of last's own bracket. This is the complete filter when last is already in
	// the highest bracket and there are no later brackets to union.
	gt := bson.D{{Key: cursorField, Value: bson.D{{Key: "$gt", Value: v}}}}
	later := bsonOrder[idx+1:]
	if len(later) == 0 {
		return gt, nil
	}

	clauses := bson.A{gt}
	for _, b := range later {
		clauses = append(clauses, bson.D{{Key: cursorField, Value: bson.D{{Key: "$gte", Value: b.min}}}})
	}
	return bson.D{{Key: "$or", Value: clauses}}, nil
}

// doBackfill runs a backfill for a binding to completion, or until signalled to
// stop by stopBackfill. The backfillDone channel will be closed when the
// backfill is completely done (cursor returns no more documents) if it is not
// `nil`.
func (c *capture) doBackfill(
	ctx context.Context,
	coordinator *streamBackfillCoordinator,
	binding bindingInfo,
	backfillDone chan struct{},
	stopBackfill <-chan struct{},
) error {
	c.mu.Lock()
	backfillState := c.state.Resources[binding.stateKey].Backfill
	c.mu.Unlock()

	lastCursorValue := backfillState.LastCursorValue
	initialDocsCaptured := backfillState.BackfilledDocs
	if backfillState.done() && binding.resource.getMode() == captureModeSnapshot {
		// Starting the snapshot backfill over.
		lastCursorValue = nil
		initialDocsCaptured = 0
	}
	additionalDocsCaptured := 0

	collection := c.client.Database(binding.resource.Database).Collection(binding.resource.Collection)

	logEntry := log.WithFields(log.Fields{
		"database":   binding.resource.Database,
		"collection": binding.resource.Collection,
	})

	var err error
	var estimatedTotalDocs int64
	if !binding.isTimeseries {
		// Not using the more precise `CountDocuments()` here since that
		// requires a full collection scan. Getting an estimate from the
		// collection's metadata is very fast and should be close enough for
		// useful logging. Timeseries collections do not have an efficient way
		// to get a quick estimate of document counts, since they will at a
		// minimum need to have every bucket's metadata queried, and also seem
		// to fall back to using the extremely inefficient `CountDocuments()`
		// even if you ask for an estimate.
		if estimatedTotalDocs, err = collection.EstimatedDocumentCount(ctx); err != nil {
			return fmt.Errorf("getting estimated document count: %w", err)
		}
		logEntry = logEntry.WithField("estimatedTotalDocs", estimatedTotalDocs)
	}

	cursorField := binding.resource.getCursorField()

	// Check if the collection is sharded. For sharded collections, we must use explicit sort
	// to ensure consistent ordering across rounds, as natural order is non-deterministic
	// when results are merged from multiple shards.
	isSharded, err := c.isShardedCollection(ctx, binding.resource.Database, binding.resource.Collection)
	if err != nil {
		return fmt.Errorf("checking if collection is sharded: %w", err)
	}
	logEntry = logEntry.WithField("sharded", isSharded)

	var opts *options.FindOptions
	if cursorField == idProperty && binding.resource.getMode() == captureModeChangeStream && !isSharded {
		// Sort by _id and hint the _id index (which should always exist) so the scan is served directly by
		// that index in ascending _id order. The sort is required for correctness, not just speed:
		// resuming with buildResumeFilter checkpoints a single monotonic high-water mark
		// (LastCursorValue), which is only valid if documents arrive in ascending _id order. Although
		// the hint pushes MongoDB to return results in _id BSON comparison order, MongoDB does not guarantee
		// result order without an explicit sort, so we request it explicitly. A { _id: 1 } sort is
		// satisfied by the _id index with no blocking in-memory sort (unlike { $natural: 1 }, which would
		// disregard all indices and force a collection scan).
		// See https://www.mongodb.com/docs/manual/reference/method/cursor.hint
		opts = options.Find().SetHint(bson.M{idProperty: 1}).SetSort(bson.D{{Key: idProperty, Value: 1}})
	} else {
		// Other cursor fields require a sort.
		opts = options.Find().SetSort(bson.D{{Key: cursorField, Value: 1}})
	}

	filter, err := buildResumeFilter(cursorField, lastCursorValue)
	if err != nil {
		return err
	}

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("collection.Find: %w", err)
	}
	defer cursor.Close(ctx)

	logEntry.WithField("resumeAfter", lastCursorValue).Info("starting backfill for collection")

	for {
		select {
		case <-stopBackfill:
			if additionalDocsCaptured != 0 {
				newTotal := initialDocsCaptured + additionalDocsCaptured

				ll := logEntry.WithFields(log.Fields{
					"docsCapturedThisRound": additionalDocsCaptured,
					"totalDocsCaptured":     newTotal,
				})
				if !binding.isTimeseries {
					complete := fmt.Sprintf("%.0f", float64(newTotal)/float64(estimatedTotalDocs)*100)
					ll = ll.WithField("percentComplete", complete)
				}
				ll.Info("progressed backfill for collection")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-coordinator.streamsCaughtUp():
			docCount, err := c.pullCursor(ctx, cursor, binding)
			if err != nil {
				return fmt.Errorf("pullCursor: %w", err)
			}

			if docCount == 0 {
				if backfillDone != nil {
					close(backfillDone)
				}
				logEntry.WithField("totalDocsCaptured", initialDocsCaptured+additionalDocsCaptured).Info("completed backfill for collection")
				return nil
			}

			additionalDocsCaptured += docCount
		}
	}
}

// pullCursor requests a batch of documents for the provided cursor. It returns
// when either the current batch of documents has been processed and another
// network call would be needed for more, or the cursor is exhausted and the
// collection scan is complete. Checkpointing and updating the in-memory state
// for processed documents is handled within this function call.
func (c *capture) pullCursor(
	ctx context.Context,
	cursor *mongo.Cursor,
	binding bindingInfo,
) (int, error) {
	var sk = binding.stateKey
	// LookupErr expects individual field components, unlike collection.Find filters
	// which use dot notation, so we split the cursor field path here.
	var cursorField = strings.Split(binding.resource.getCursorField(), ".")
	// Accumulate raw BSON documents for batch transcoding
	var rawDocBatch []RawDocumentInput
	var lastCursor bson.RawValue
	var batchBytes int
	var docsRead int

	c.mu.Lock()
	state := c.state.Resources[sk]
	if state.Backfill.LastPollStart == nil || state.Backfill.done() {
		// First time ever polling this collection, or doing a scheduled re-poll
		// of a previously completed collection.
		state.Backfill.LastPollStart = makePtr(time.Now().UTC())
	}
	if state.Backfill.done() && binding.resource.getMode() == captureModeSnapshot {
		state.Backfill.BackfilledDocs = 0
	}
	state.Backfill.Done = makePtr(false)
	c.state.Resources[sk] = state
	c.mu.Unlock()

	emitDocs := func() error {
		// Transcode all raw documents through the transcoder
		responses, err := c.transcoder.TranscodeRawDocuments(rawDocBatch)
		if err != nil {
			return fmt.Errorf("transcoding backfill documents: %w", err)
		}

		// Convert responses to json.RawMessage slice
		docBatch := make([]json.RawMessage, 0, len(responses))
		for _, resp := range responses {
			if !resp.IsSkip {
				docBatch = append(docBatch, resp.Document)
			}
		}

		c.mu.Lock()
		state := c.state.Resources[sk]
		state.Backfill.LastCursorValue = &lastCursor
		state.Backfill.BackfilledDocs += len(docBatch)
		c.state.Resources[sk] = state
		checkpoint := captureState{
			Resources: map[boilerplate.StateKey]resourceState{
				sk: {Backfill: state.Backfill},
			},
		}
		c.mu.Unlock()

		if cpJson, err := json.Marshal(checkpoint); err != nil {
			return fmt.Errorf("serializing checkpoint: %w", err)
		} else if err := c.output.DocumentsAndCheckpoint(cpJson, true, binding.index, docBatch...); err != nil {
			return fmt.Errorf("outputting documents and checkpoint: %w", err)
		}

		docsRead += len(docBatch)
		// Reset for the next batch.
		rawDocBatch = nil
		batchBytes = 0

		return nil
	}

	done := true
	for cursor.Next(ctx) {
		var err error
		if lastCursor, err = cursor.Current.LookupErr(cursorField...); err != nil {
			return 0, fmt.Errorf("looking up cursor field '%s': %w", strings.Join(cursorField, "."), err)
		}

		// Copy cursor.Current since it's reused by the cursor
		rawCopy := make(bson.Raw, len(cursor.Current))
		copy(rawCopy, cursor.Current)

		rawDocBatch = append(rawDocBatch, RawDocumentInput{
			Raw:        rawCopy,
			Database:   binding.resource.Database,
			Collection: binding.resource.Collection,
		})
		batchBytes += len(rawCopy)

		if len(rawDocBatch) > backfillCheckpointSize || batchBytes > backfillBytesSize {
			if err := emitDocs(); err != nil {
				return 0, fmt.Errorf("emitting backfill batch: %w", err)
			}
		}

		if cursor.RemainingBatchLength() == 0 {
			done = false
			break
		}
	}
	if err := cursor.Err(); err != nil {
		return 0, fmt.Errorf("backfill cursor error: %w", err)
	}

	if len(rawDocBatch) > 0 {
		if err := emitDocs(); err != nil {
			return 0, fmt.Errorf("emitting final backfill batch: %w", err)
		}
	}

	if done {
		c.mu.Lock()
		state := c.state.Resources[sk]
		state.Backfill.Done = makePtr(true)
		c.state.Resources[sk] = state
		c.mu.Unlock()

		checkpoint := captureState{
			Resources: map[boilerplate.StateKey]resourceState{sk: state},
		}

		if cp, err := json.Marshal(checkpoint); err != nil {
			return 0, fmt.Errorf("serializing checkpoint: %w", err)
		} else if err := c.output.Checkpoint(cp, true); err != nil {
			return 0, fmt.Errorf("outputting checkpoint: %w", err)
		}
	}

	return docsRead, nil
}

// isShardedCollection checks if a collection is sharded by running a $collStats aggregation.
// For sharded collections, $collStats returns one document per shard.
func (c *capture) isShardedCollection(ctx context.Context, database, collection string) (bool, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$collStats", Value: bson.D{{Key: "count", Value: bson.D{}}}}},
	}

	cursor, err := c.client.Database(database).Collection(collection).Aggregate(ctx, pipeline)
	if err != nil {
		var cmdErr mongo.CommandError
		if errors.As(err, &cmdErr) {
			// Check if this is a NamespaceNotFound error (code 26). This can occur for new/empty
			// collections that haven't been physically created yet. Treat these as non-sharded.
			if cmdErr.Code == 26 {
				log.WithFields(log.Fields{
					"database":   database,
					"collection": collection,
				}).Debug("collection not found, assuming not sharded")
				return false, nil
			}
			// Check if this is a CommandNotSupportedOnView error (code 166). Views can have _id
			// from multiple tables, so we treat them as sharded to ensure proper sorting.
			if cmdErr.Code == 166 {
				log.WithFields(log.Fields{
					"database":   database,
					"collection": collection,
				}).Debug("collection is a view, treating as sharded")
				return true, nil
			}
		}
		return false, fmt.Errorf("running $collStats aggregation: %w", err)
	}
	defer cursor.Close(ctx)

	// If there are two or more documents, the collection is sharded (one doc per shard)
	hasShards := cursor.Next(ctx) && cursor.Next(ctx)

	if err := cursor.Err(); err != nil {
		return false, fmt.Errorf("iterating $collStats results: %w", err)
	}

	return hasShards, nil
}
