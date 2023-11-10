package connector

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/minio/highwayhash"
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

// The hash function and hash key below are copied directly from the Flow repo, go/flow/mapping.go.
// In the future if the hashed value of the packedKey is added to the materialization connector
// protocol, the PubSub materialization can be converted to using the hashed value directly instead
// of computing it separately.

// PackedKeyHash_HH64 builds a packed key hash from the top 32-bits of a
// HighwayHash 64-bit checksum computed using a fixed key.
func PackedKeyHash_HH64(packedKey []byte) uint32 {
	return uint32(highwayhash.Sum64(packedKey, highwayHashKey) >> 32)
}

// highwayHashKey is a fixed 32 bytes (as required by HighwayHash) read from /dev/random.
var highwayHashKey, _ = hex.DecodeString("ba737e89155238d47d8067c35aad4d25ecdd1c3488227e011ffa480c022bd3ba")

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	errGroup, ctx := errgroup.WithContext(it.Context())

	for it.Next() {
		binding := t.bindings[it.Binding]

		msg := &pubsub.Message{
			Data:        it.RawJSON,
			OrderingKey: fmt.Sprintf("%08x", PackedKeyHash_HH64(it.PackedKey)),
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
				return fmt.Errorf("error publishing document for binding [%d]: %w", it.Binding, err)
			}

			return nil
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
