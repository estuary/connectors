package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/minio/highwayhash"
	"golang.org/x/sync/errgroup"
)

// publishConcurrency caps in-flight Publish calls per Store invocation, bounding connector memory
// when the StoreIterator delivers documents faster than SNS can accept them.
const publishConcurrency = 64

// maxMessageBytes is the SNS per-message payload limit. We don't set Subject or
// MessageAttributes, so the message body is the only contributor to this budget.
// Pre-checking gives users a useful error instead of an opaque InvalidParameter from SNS.
const maxMessageBytes = 256 * 1024

type transactor struct {
	client   *sns.Client
	bindings []*topicBinding
}

type topicBinding struct {
	topicARN string
	isFifo   bool
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

// SNS is delta-update only.
func (t *transactor) Load(it *materialize.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

// PackedKeyHash_HH64 and highwayHashKey are copied verbatim from materialize-google-pubsub so SNS
// FIFO MessageGroupId values match Flow's internal key-hash ordering scheme.
func PackedKeyHash_HH64(packedKey []byte) uint32 {
	return uint32(highwayhash.Sum64(packedKey, highwayHashKey) >> 32)
}

var highwayHashKey, _ = hex.DecodeString("ba737e89155238d47d8067c35aad4d25ecdd1c3488227e011ffa480c022bd3ba")

func (t *transactor) Store(it *materialize.StoreIterator) (materialize.StartCommitFunc, error) {
	errGroup, ctx := errgroup.WithContext(it.Context())
	errGroup.SetLimit(publishConcurrency)

	for it.Next(false) {
		bindingIdx := it.Binding
		packedKey := it.PackedKey
		doc := it.RawJSON
		if err := t.publishOne(ctx, errGroup, bindingIdx, packedKey, doc); err != nil {
			return nil, err
		}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	return nil, errGroup.Wait()
}

// publishOne is the per-document publish path, extracted so integration tests can drive it without
// having to synthesize an unexported *materialize.StoreIterator. Synchronous validation errors
// (e.g. oversize document) are returned directly; the Publish RPC itself is dispatched on
// errGroup, so its result is observed via errGroup.Wait().
func (t *transactor) publishOne(ctx context.Context, errGroup *errgroup.Group, bindingIdx int, packedKey []byte, doc json.RawMessage) error {
	binding := t.bindings[bindingIdx]
	if len(doc) > maxMessageBytes {
		return fmt.Errorf(
			"document for binding [%d] is %d bytes, which exceeds the SNS %d byte per-message limit",
			bindingIdx, len(doc), maxMessageBytes,
		)
	}
	input := &sns.PublishInput{
		TopicArn: aws.String(binding.topicARN),
		Message:  aws.String(string(doc)),
	}
	if binding.isFifo {
		input.MessageGroupId = aws.String(fmt.Sprintf("%08x", PackedKeyHash_HH64(packedKey)))
		sum := sha256.Sum256(append(append([]byte{}, packedKey...), doc...))
		input.MessageDeduplicationId = aws.String(hex.EncodeToString(sum[:]))
	}

	errGroup.Go(func() error {
		if _, err := t.client.Publish(ctx, input); err != nil {
			return fmt.Errorf("publishing document for binding [%d]: %w", bindingIdx, err)
		}
		return nil
	})
	return nil
}

func (t *transactor) Destroy() {}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (materialize.RuntimeCheckpoint, error) {
	return nil, nil
}
