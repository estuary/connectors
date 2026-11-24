package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"
	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	pc "go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
)

const (
	// Per-batch caps. Bytes limit bounds typical memory; size limit guards
	// against pathologically small elements where Go's per-element overhead
	// dominates.
	batchBytesLimit = 10 * 1024 * 1024
	batchSizeLimit  = 1000

	// Concurrent in-flight ReadRows / ApplyBulk calls. Pipelining keeps the
	// iterator advancing while previous batches are still streaming.
	concurrentWorkers = 5

	// publishedAtLayout is the format of the publication times the runtime
	// projects from /_meta/uuid: UTC with nanoseconds padded to nine digits,
	// so that lexicographic order matches temporal order.
	publishedAtLayout = "2006-01-02T15:04:05.000000000Z"
)

type binding struct {
	tableName string
	path      []string
	table     *bigtable.Table

	// fields are the non-document selected projections, ordered to match the
	// stream of (key | values) that a Store iterator produces.
	fields []mappedType

	// docField is the column qualifier holding the materialized root document.
	// Usually the default `flow_document` but a user may project the root
	// document under an alternate name.
	docField string

	// publishedAtField is the column qualifier of the selected field that
	// projects /_meta/uuid as a date-time, used to find rows published before
	// a completed backfill. Empty if the binding excludes that field, in which
	// case publishedAtErr explains why.
	publishedAtField string
	publishedAtErr   error
}

type state struct {
	// CommittedTimestamp is the last committed cell timestamp, in microseconds.
	// Bigtable truncates timestamps to millisecond granularity, so writes step by
	// 1000µs per transaction to keep each transaction on a distinct cell version.
	CommittedTimestamp int64 `json:"committed_timestamp"`
}

type transactor struct {
	bindings   []binding
	state      state
	hardDelete bool
	be         *m.BindingEvents

	// truncateAfterBackfill permits deleting the rows published before a
	// completed backfill.
	truncateAfterBackfill bool
	// truncations maps a binding index to the boundary of a completed
	// backfill, for deletion after this transaction's stores.
	truncations map[int]time.Time
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) UnmarshalState(raw json.RawMessage) error {
	if err := json.Unmarshal(raw, &t.state); err != nil {
		return fmt.Errorf("unmarshalling connector state: %w", err)
	}

	return nil
}

func (t *transactor) Flush(_ context.Context, completes map[int]time.Time) error {
	t.truncations = make(map[int]time.Time, len(completes))
	for binding, boundary := range completes {
		b := t.bindings[binding]
		if !t.truncateAfterBackfill {
			log.WithFields(log.Fields{
				"table":    b.path,
				"boundary": boundary,
			}).Info("rows published before the backfill were not deleted because feature flags disable it")
			continue
		} else if b.publishedAtField == "" {
			t.be.Warn(b.tableName, fmt.Sprintf("Rows published before the backfill of table %s were not deleted because %s.", b.tableName, b.publishedAtErr))
			continue
		}
		t.truncations[binding] = boundary
	}
	return nil
}

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}

type loadBatch struct {
	binding int
	keys    bigtable.RowList
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	ctx := it.Context()
	it.WaitForAcknowledged()

	var mu sync.Mutex
	lockedAndLoaded := func(binding int, doc json.RawMessage) error {
		mu.Lock()
		defer mu.Unlock()
		return loaded(binding, doc)
	}

	batches := make(chan loadBatch)
	group, groupCtx := errgroup.WithContext(ctx)
	txTS := bigtable.Timestamp(t.state.CommittedTimestamp + 1000)

	for range concurrentWorkers {
		group.Go(func() error {
			return t.loadWorker(groupCtx, txTS, lockedAndLoaded, batches)
		})
	}

	// Load keys are not strictly in binding order, but they tend to arrive in
	// runs for the same binding. Accumulate one batch at a time and send it
	// whenever the binding changes or the batch hits a limit.
	var (
		currentBatch bigtable.RowList
		currentBytes int
		lastBinding  int
	)

	sendBatch := func() error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case batches <- loadBatch{binding: lastBinding, keys: currentBatch}:
			currentBatch = nil
			currentBytes = 0
			return nil
		}
	}

	for it.Next() {
		if len(currentBatch) > 0 && lastBinding != it.Binding {
			if err := sendBatch(); err != nil {
				return fmt.Errorf("sending load batch: %w", err)
			}
		}

		lastBinding = it.Binding
		currentBatch = append(currentBatch, string(it.PackedKey))
		currentBytes += len(it.PackedKey)

		if len(currentBatch) >= batchSizeLimit || currentBytes >= batchBytesLimit {
			if err := sendBatch(); err != nil {
				return fmt.Errorf("sending load batch: %w", err)
			}
		}
	}
	if it.Err() != nil {
		return fmt.Errorf("iterating loads: %w", it.Err())
	}

	if len(currentBatch) > 0 {
		if err := sendBatch(); err != nil {
			return fmt.Errorf("sending final load batch: %w", err)
		}
	}

	close(batches)
	if err := group.Wait(); err != nil {
		return fmt.Errorf("draining load workers: %w", err)
	}

	return nil
}

func (t *transactor) loadWorker(
	ctx context.Context,
	txTS bigtable.Timestamp,
	loaded func(int, json.RawMessage) error,
	batches <-chan loadBatch,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-batches:
			if !ok {
				return nil
			}

			b := &t.bindings[batch.binding]

			filter := bigtable.ChainFilters(
				bigtable.FamilyFilter(columnFamily),
				bigtable.ColumnFilter(regexp.QuoteMeta(b.docField)),
				bigtable.LatestNFilter(2),
			)

			var loadErr error
			err := b.table.ReadRows(ctx, batch.keys, func(r bigtable.Row) bool {
				// The filter restricts items to document-cell versions, newest first.
				// Walk forward to the first non-dirty cell and surface it as the
				// committed value.
				for _, cell := range r[columnFamily] {
					if cell.Timestamp == txTS {
						continue
					}
					if err := loaded(batch.binding, json.RawMessage(cell.Value)); err != nil {
						loadErr = err
						return false
					}

					return true
				}

				return true
			}, bigtable.RowFilter(filter))
			if err != nil {
				return fmt.Errorf("ReadRows on table %q: %w", b.tableName, err)
			} else if loadErr != nil {
				return fmt.Errorf("loaded callback for table %q: %w", b.tableName, loadErr)
			}
		}
	}
}

type storeBatch struct {
	binding   int
	rowKeys   []string
	mutations []*bigtable.Mutation
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	batches := make(chan storeBatch)
	group, groupCtx := errgroup.WithContext(ctx)
	round := it.Round
	// stored counts the rows Bigtable acknowledged per binding, for the
	// transaction health report.
	stored := make([]atomic.Int64, len(t.bindings))

	for range concurrentWorkers {
		group.Go(func() error {
			return t.storeWorker(groupCtx, batches, stored)
		})
	}

	var (
		currentBatch storeBatch
		currentBytes int
		ts           = bigtable.Timestamp(t.state.CommittedTimestamp + 1000)
	)

	sendBatch := func() error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case batches <- currentBatch:
			currentBatch = storeBatch{}
			currentBytes = 0
			return nil
		}
	}

	for it.Next(t.hardDelete) {
		if len(currentBatch.rowKeys) > 0 && currentBatch.binding != it.Binding {
			if err := sendBatch(); err != nil {
				return nil, fmt.Errorf("sending store batch: %w", err)
			}
		}

		b := &t.bindings[it.Binding]

		mut := bigtable.NewMutation()
		if it.Delete && t.hardDelete {
			mut.DeleteRow()
		} else {
			allFields := append(it.Key, it.Values...)
			for i, te := range allFields {
				f := b.fields[i]
				val, err := f.encode(te)
				if err != nil {
					return nil, fmt.Errorf("encoding field %q on table %q: %w", f.field, b.tableName, err)
				}
				mut.Set(columnFamily, f.field, ts, val)
			}
			mut.Set(columnFamily, b.docField, ts, it.RawJSON)
		}

		currentBatch.binding = it.Binding
		currentBatch.rowKeys = append(currentBatch.rowKeys, string(it.PackedKey))
		currentBatch.mutations = append(currentBatch.mutations, mut)
		currentBytes += len(it.PackedKey) + len(it.RawJSON)

		if len(currentBatch.rowKeys) >= batchSizeLimit || currentBytes >= batchBytesLimit {
			if err := sendBatch(); err != nil {
				return nil, fmt.Errorf("sending store batch: %w", err)
			}
		}
	}
	if it.Err() != nil {
		return nil, fmt.Errorf("iterating stores: %w", it.Err())
	}

	if len(currentBatch.rowKeys) > 0 {
		if err := sendBatch(); err != nil {
			return nil, fmt.Errorf("sending final store batch: %w", err)
		}
	}
	close(batches)

	t.state.CommittedTimestamp = int64(ts)
	newState, err := json.Marshal(t.state)
	if err != nil {
		return nil, fmt.Errorf("marshalling state: %w", err)
	}

	if err := group.Wait(); err != nil {
		return nil, fmt.Errorf("draining store workers: %w", err)
	}
	for i, b := range t.bindings {
		if n := stored[i].Load(); n > 0 {
			t.be.ReportRowStats(round, b.path, m.TotalRowStats(n))
		}
	}

	// Truncations run after the stores, so the scan sees each row's
	// post-store publication time and doesn't delete rows this transaction
	// just refreshed.
	for binding, boundary := range t.truncations {
		b := &t.bindings[binding]
		deleted, err := truncateStaleRows(ctx, b, boundary)
		if err != nil {
			return nil, fmt.Errorf("truncating table %q after backfill: %w", b.tableName, err)
		}
		log.WithFields(log.Fields{
			"table":    b.path,
			"boundary": boundary,
			"deleted":  deleted,
		}).Info("truncated rows published before the backfill")
		t.be.ReportRowStats(round, b.path, m.TotalRowStats(0).WithTruncated(deleted))
	}
	clear(t.truncations)

	return func(_ context.Context, _ *pc.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		return &pf.ConnectorState{UpdatedJson: newState}, nil
	}, nil
}

func (t *transactor) storeWorker(ctx context.Context, batches <-chan storeBatch, stored []atomic.Int64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-batches:
			if !ok {
				return nil
			}

			b := &t.bindings[batch.binding]
			errs, err := b.table.ApplyBulk(ctx, batch.rowKeys, batch.mutations)
			if err != nil {
				return fmt.Errorf("ApplyBulk on table %q: %w", b.tableName, err)
			}
			for i, e := range errs {
				if e != nil {
					return fmt.Errorf("ApplyBulk row %q on table %q: %w", batch.rowKeys[i], b.tableName, e)
				}
			}
			stored[batch.binding].Add(int64(len(batch.rowKeys)))
		}
	}
}

func (t *transactor) Destroy() {}

// truncateStaleRows deletes the rows of b's table whose latest
// publishedAtField value predates boundary, returning how many it deleted. A
// row with no value for that field is left alone. It scans the whole table
// because Bigtable has no secondary index on the publication time.
func truncateStaleRows(ctx context.Context, b *binding, boundary time.Time) (int64, error) {
	// The runtime always formats publication times as UTC with nine
	// fractional digits, matching publishedAtLayout, so they sort
	// lexicographically in temporal order and the value range selects
	// exactly the stale rows. Its lower bound excludes empty (null) values,
	// and rows left with no cells by the filter are not returned. Only the
	// row keys are needed, so values are stripped.
	filter := bigtable.ChainFilters(
		bigtable.FamilyFilter(columnFamily),
		bigtable.ColumnFilter(regexp.QuoteMeta(b.publishedAtField)),
		bigtable.LatestNFilter(1),
		bigtable.ValueRangeFilter([]byte("0"), []byte(boundary.UTC().Format(publishedAtLayout))),
		bigtable.StripValueFilter(),
	)

	batches := make(chan []string)
	group, groupCtx := errgroup.WithContext(ctx)
	var deleted atomic.Int64

	for range concurrentWorkers {
		group.Go(func() error {
			return deleteWorker(groupCtx, b, batches, &deleted)
		})
	}

	group.Go(func() error {
		defer close(batches)

		var batch []string
		var sendErr error
		send := func() bool {
			select {
			case <-groupCtx.Done():
				sendErr = groupCtx.Err()
				return false
			case batches <- batch:
				batch = nil
				return true
			}
		}

		err := b.table.ReadRows(groupCtx, bigtable.InfiniteRange(""), func(r bigtable.Row) bool {
			batch = append(batch, r.Key())
			if len(batch) >= batchSizeLimit {
				return send()
			}
			return true
		}, bigtable.RowFilter(filter))
		if err != nil {
			return fmt.Errorf("scanning table %q: %w", b.tableName, err)
		} else if sendErr != nil {
			return sendErr
		}

		if len(batch) > 0 && !send() {
			return sendErr
		}
		return nil
	})

	err := group.Wait()
	return deleted.Load(), err
}

// deleteWorker deletes each batch of row keys read from batches.
func deleteWorker(ctx context.Context, b *binding, batches <-chan []string, deleted *atomic.Int64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rowKeys, ok := <-batches:
			if !ok {
				return nil
			}

			mutations := make([]*bigtable.Mutation, len(rowKeys))
			for i := range mutations {
				mutations[i] = bigtable.NewMutation()
				mutations[i].DeleteRow()
			}
			errs, err := b.table.ApplyBulk(ctx, rowKeys, mutations)
			if err != nil {
				return fmt.Errorf("ApplyBulk delete on table %q: %w", b.tableName, err)
			}
			for i, e := range errs {
				if e != nil {
					return fmt.Errorf("ApplyBulk delete row %q on table %q: %w", rowKeys[i], b.tableName, e)
				}
			}
			deleted.Add(int64(len(rowKeys)))
		}
	}
}
