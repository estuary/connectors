package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"github.com/estuary/connectors/go/keyhash"
	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/sync/errgroup"
)

type transactor struct {
	bindings []*topicBinding
	be       *m.BindingEvents
}

type topicBinding struct {
	path       []string
	identifier string
	topic      *pubsub.Topic
}

func (t *transactor) UnmarshalState(state json.RawMessage) error { return nil }
func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}

// PubSub is delta-update only.
func (t *transactor) Load(it *m.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	errGroup, ctx := errgroup.WithContext(it.Context())
	round := it.Round
	published := make([]atomic.Int64, len(t.bindings))

	for it.Next(false) {
		binding := t.bindings[it.Binding]
		bindingIdx := it.Binding

		msg := &pubsub.Message{
			Data:        it.RawJSON,
			OrderingKey: fmt.Sprintf("%08x", keyhash.PackedKeyHash_HH64(it.PackedKey)),
		}
		// Only include an identifier attribute if an identifier has been configured.
		if binding.identifier != "" {
			msg.Attributes = map[string]string{IDENTIFIER_ATTRIBUTE_KEY: binding.identifier}
		}

		// Blocks if the maximum number of messages are queue'd, since
		// topic.PublishSettings.FlowControlSettings.LimitExceededBehavior = pubsub.FlowControlBlock
		res := binding.topic.Publish(ctx, msg)

		errGroup.Go(func() error {
			// This will block until the individual publish call is complete.
			if _, err := res.Get(ctx); err != nil {
				// An error here indicates a non-retryable error. Retrying retryable errors is handled
				// by the PubSub client. Returning an error from (*transactor).Store will result in the
				// transaction being cancelled. With ordering enabled, we would normally need to resume
				// publishing (see https://cloud.google.com/pubsub/docs/publisher#retry_ordering), but
				// since returning an error here will cause the connector to exit, we don't need to
				// worry about resuming publishing from the same client.
				return fmt.Errorf("error publishing document for binding [%d]: %w", bindingIdx, err)
			}
			published[bindingIdx].Add(1)

			return nil
		})
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	// Wait for all messages to be delivered.
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	for i, b := range t.bindings {
		if n := published[i].Load(); n > 0 {
			t.be.ReportRowStats(round, b.path, m.TotalRowStats(n))
		}
	}
	return nil, nil
}

func (t *transactor) Destroy() {
	for _, b := range t.bindings {
		// Wait for all async messages to finished sending for each topic.
		b.topic.Stop()
	}
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}
