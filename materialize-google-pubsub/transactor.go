package connector

import (
	"encoding/json"

	"cloud.google.com/go/pubsub"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"golang.org/x/sync/errgroup"
)

type transactor struct {
	bindings []*topicBinding
}

type topicBinding struct {
	identifier string
	topic      *pubsub.Topic
}

// PubSub is delta-update only.
func (t *transactor) Load(it *pm.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	errGroup, ctx := errgroup.WithContext(it.Context())

	for it.Next() {
		binding := t.bindings[it.Binding]

		msg := &pubsub.Message{
			Data:        it.RawJSON,
			OrderingKey: it.Key.String(), // Allows for reading of messages for the same key in order.
		}
		// Only include an identifier attribute if an identifier has been configured.
		if binding.identifier != "" {
			msg.Attributes = map[string]string{IDENTIFIER_ATTRIBUTE_KEY: binding.identifier}
		}

		res := binding.topic.Publish(ctx, msg)

		errGroup.Go(func() error {
			// This will block until the individual publish call is complete.
			_, err := res.Get(ctx)

			// An error here indicates a non-retryable error. Retrying retryable errors is handled
			// by the PubSub client. Returning an error from (*transactor).Store will result in the
			// transaction being cancelled. With ordering enabled, we would normally need to resume
			// publishing (see https://cloud.google.com/pubsub/docs/publisher#retry_ordering), but
			// since returning an error here will cause the connector to exit, we don't need to
			// worry about resuming publishing from the same client.
			return err
		})
	}

	// Wait for all messages to be delivered.
	return nil, errGroup.Wait()
}

func (t *transactor) Destroy() {
	for _, b := range t.bindings {
		// Wait for all async messages to finished sending for each topic.
		b.topic.Stop()
	}
}
