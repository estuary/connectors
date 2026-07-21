package hubspot

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"

	"github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	MaxConcurrentRequests = 5
)

type binding struct {
	object     CRMObject
	properties map[string]*Property
	idProperty *Property
	fields     []*MappedField
	docField   *MappedField
}

func (b binding) convertKey(ts tuple.Tuple) (string, error) {
	if len(ts) != 1 {
		return "", fmt.Errorf("unexpected key tuple length: %v", len(ts))
	}

	mapped := b.fields[0]
	return mapped.ConvertString(ts[0], mapped.Property)
}

func (b binding) convert(ts tuple.Tuple, doc json.RawMessage) (map[string]any, error) {
	values := make(map[string]any, len(ts)+1)
	for idx, elem := range ts {
		mapped := b.fields[idx]

		value, err := mapped.Convert(elem, mapped.Property)
		if err != nil {
			return nil, err
		}

		if value != nil {
			values[mapped.Property.Name] = value
		}
	}

	if doc != nil && b.docField != nil {
		property := b.properties[b.docField.Property.Name]
		value, err := b.docField.Convert(doc, property)
		if err != nil {
			return nil, err
		}
		values[b.docField.Property.Name] = value
	}
	return values, nil
}

type transactor struct {
	client   *Client
	bindings []*binding
}

var _ materialize.Transactor = (*transactor)(nil)

func (t *transactor) RecoverCheckpoint(context.Context, pf.MaterializationSpec, pf.RangeSpec) (materialize.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error {
	return nil
}

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	return nil, nil
}

func (t *transactor) Destroy() {
	t.client.Close()
}

func (t *transactor) Load(it *materialize.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) storeUniqueProperty(ctx context.Context, b *binding, batch *Batch) error {
	inputs := make([]*BatchUpsertInput, 0, len(batch.Items))
	for _, item := range batch.Items {
		input := &BatchUpsertInput{
			IDProperty: b.idProperty.Name,
			ID:         item.ID,
			Properties: item.Properties,
		}
		inputs = append(inputs, input)
	}

	return t.client.BatchUpsert(ctx, b.object, inputs)
}

func (t *transactor) storeNonUniqueProperty(ctx context.Context, b *binding, batch *Batch) error {
	creates := []*BatchCreateInput{}
	updates := []*BatchUpdateInput{}

	ids := make([]any, 0, len(batch.Items))
	for _, item := range batch.Items {
		ids = append(ids, item.ID)
	}

	// Batches are no more than 100 items so this should never happen.  We
	// don't want to page because if the objects are deleted during paging
	// a result can be skipped.
	if len(batch.Items) > MaxSearchPageLimit {
		panic("search limit too large")
	}

	searchRequest := &SearchRequest{
		FilterGroups: NewFilterGroupsIn(b.idProperty.Name, ids),
		Properties:   []string{"id", b.idProperty.Name},
		Limit:        101, // One more than the largest batch; for duplicate detection below.
	}
	response, err := t.client.Search(ctx, b.object, searchRequest)
	if err != nil {
		return err
	}

	// If we received more results than we have ids, there are duplicate
	// records with this idProperty.  A user will need to go in and merge the
	// records to fix this, or better yet switch to a unique property.
	if len(response.Results) > len(ids) {
		log.WithFields(log.Fields{
			"object":      b.object,
			"id_property": b.idProperty.Name,
		}).Warn("duplicate records matched")
	}

	// Save the record ID for matches, all records not in the result set
	// are new.
	recordIDs := make(map[string]string, len(batch.Items))
	for _, record := range response.Results {
		idPropertyValue, ok := record.Properties[b.idProperty.Name]
		if !ok {
			return fmt.Errorf("idProperty not set")
		}

		// In search results, all values are strings or null regardless of
		// their property type.  For this query the idProperty should
		// always be set.
		switch v := idPropertyValue.(type) {
		case string:
			recordIDs[v] = record.ID
		default:
			return fmt.Errorf("unexpected idProperty type: %T", idPropertyValue)
		}
	}

	for _, item := range batch.Items {
		if id, ok := recordIDs[item.ID]; ok {
			updates = append(updates, &BatchUpdateInput{
				ID:         id,
				Properties: item.Properties,
			})
		} else {
			creates = append(creates, &BatchCreateInput{
				Properties: item.Properties,
			})
		}
	}

	if len(creates) != 0 {
		err := t.client.BatchCreate(ctx, b.object, creates)
		if err != nil {
			return err
		}
	}

	if len(updates) != 0 {
		err := t.client.BatchUpdate(ctx, b.object, updates)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *transactor) Store(it *materialize.StoreIterator) (materialize.StartCommitFunc, error) {
	ctx := it.Context()

	batches := make(chan *Batch)
	group, groupCtx := errgroup.WithContext(ctx)

	for range MaxConcurrentRequests {
		group.Go(func() error {
			for batch := range batches {
				b := t.bindings[batch.BindingIdx]

				if b.idProperty.HasUniqueValue {
					err := t.storeUniqueProperty(groupCtx, b, batch)
					if err != nil {
						return err
					}
				} else {
					err := t.storeNonUniqueProperty(groupCtx, b, batch)
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	group.Go(func() error {
		defer close(batches)

		for batch, err := range t.storeBatches(it, MaxBatchRecords) {
			if err != nil {
				return err
			}

			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case batches <- batch:
			}
		}
		return nil
	})

	err := group.Wait()
	return nil, err
}

// BatchItem is a record to send to the Batch API.
//
// ID is the stringified key/match-property value for the record.
type BatchItem struct {
	ID         string
	Properties map[string]any
}

// Batch is the records for a binding to send in a Batch API request.
//
// The Batch API operates on no more than MaxBatchRecords per request, so the
// length of Items will be no longer than this.
type Batch struct {
	BindingIdx int
	Items      []BatchItem
}

func (t *transactor) storeBatches(it *materialize.StoreIterator, size int) iter.Seq2[*Batch, error] {
	return func(yield func(*Batch, error) bool) {
		var activeKey string
		var haveActive bool
		activeBinding := -1
		activeProperties := make(map[string]any)
		items := make([]BatchItem, 0, MaxBatchRecords)

		for it.Next(true) {
			if it.Err() != nil {
				yield(nil, fmt.Errorf("unable to iterate: %w", it.Err()))
				return
			}

			b := t.bindings[it.Binding]

			key, err := b.convertKey(it.Key)
			if err != nil {
				yield(nil, fmt.Errorf("unable to convert key for object %q: %w", b.object, err))
				return
			}

			properties, err := b.convert(append(it.Key, it.Values...), it.RawJSON)
			if err != nil {
				yield(nil, fmt.Errorf("unable to convert values for object %q: %w", b.object, err))
				return
			}

			changedBinding := activeBinding != it.Binding

			// Flush items whenever the binding changes.
			if changedBinding {
				if haveActive {
					items = append(items, BatchItem{
						ID:         activeKey,
						Properties: activeProperties,
					})

					haveActive = false

					if len(items) != 0 {
						if !yield(&Batch{BindingIdx: activeBinding, Items: items}, nil) {
							return
						}
						items = make([]BatchItem, 0, MaxBatchRecords)
					}
				}
			}

			changedKey := !haveActive || activeKey != key

			// Merge changes.
			if !changedKey {
				for k, v := range properties {
					activeProperties[k] = v
				}
				continue
			}

			// Add the item unless first in the binding.
			if haveActive {
				items = append(items, BatchItem{
					ID:         activeKey,
					Properties: activeProperties,
				})

				haveActive = false

				if len(items) == size {
					if !yield(&Batch{BindingIdx: activeBinding, Items: items}, nil) {
						return
					}
					items = make([]BatchItem, 0, MaxBatchRecords)
				}
			}

			activeBinding = it.Binding
			activeKey = key
			activeProperties = properties
			haveActive = true
		}

		if haveActive {
			items = append(items, BatchItem{
				ID:         activeKey,
				Properties: activeProperties,
			})
		}

		if len(items) != 0 {
			if !yield(&Batch{BindingIdx: activeBinding, Items: items}, nil) {
				return
			}
		}
	}
}
