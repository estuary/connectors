package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"

	"cloud.google.com/go/bigtable"
	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
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
)

type binding struct {
	tableName string
	table     *bigtable.Table

	// fields are the non-document selected projections, ordered to match the
	// stream of (key | values) that a Store iterator produces.
	fields []mappedType

	// docField is the column qualifier holding the materialized root document.
	// Usually the default `flow_document` but a user may project the root
	// document under an alternate name.
	docField string
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

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
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

	for range concurrentWorkers {
		group.Go(func() error {
			return t.storeWorker(groupCtx, batches)
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

	return func(_ context.Context, _ *pc.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		return &pf.ConnectorState{UpdatedJson: newState}, nil
	}, nil
}

func (t *transactor) storeWorker(ctx context.Context, batches <-chan storeBatch) error {
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
		}
	}
}

func (t *transactor) Destroy() {}
