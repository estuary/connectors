package connector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tidwall/gjson"
)

// publishWindow bounds unconfirmed messages and the AMQP confirmation buffer.
const publishWindow = 256

// closeReasonWait bounds how long a failed publish waits for the broker's close reason.
const closeReasonWait = time.Second

type publishChannel interface {
	PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error
	IsClosed() bool
}

type transactor struct {
	client   *amqp.Connection
	channel  publishChannel
	confirms <-chan amqp.Confirmation
	closed   <-chan *amqp.Error
	bindings []*exchangeBinding
	be       *m.BindingEvents
}

type exchangeBinding struct {
	path            []string
	exchange        string
	routingKey      string
	routingKeyIndex int
	routingKeyIsKey bool
}

func (t *transactor) UnmarshalState(state json.RawMessage) error     { return nil }
func (t *transactor) Flush(context.Context, map[int]time.Time) error { return nil }

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}

// RabbitMQ is delta-update only.
func (t *transactor) Load(it *m.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return it.Err()
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	if t.client != nil {
		// PublishWithContext only checks cancellation before writing. Expire the
		// transport deadline to interrupt an in-flight write as well.
		stop := context.AfterFunc(it.Context(), func() { t.client.CloseDeadline(time.Now()) })
		defer stop()
	}
	pending := 0
	published := make([]int64, len(t.bindings))
	for it.Next(false) {
		if pending == publishWindow {
			if err := t.waitForConfirm(it.Context()); err != nil {
				return nil, err
			}
			pending--
		}
		binding := t.bindings[it.Binding]
		var routingValue any
		if binding.routingKeyIndex >= 0 {
			values := it.Values
			if binding.routingKeyIsKey {
				values = it.Key
			}
			if binding.routingKeyIndex >= len(values) {
				return nil, fmt.Errorf("missing selected routing key field for binding [%d]", it.Binding)
			}
			routingValue = values[binding.routingKeyIndex]
		}
		msg, routingKey, err := binding.message(it.RawJSON, it.PackedKey, routingValue)
		if err != nil {
			return nil, fmt.Errorf("building message for binding [%d]: %w", it.Binding, err)
		}
		if err := t.channel.PublishWithContext(it.Context(), binding.exchange, routingKey, false, false, msg); err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				// The broker's reason for closing the channel is delivered after the
				// channel is marked closed. Wait briefly for it.
				if reason := t.closeReason(it.Context(), closeReasonWait); reason != nil {
					err = fmt.Errorf("publish channel closed: %w", reason)
				}
			}
			return nil, fmt.Errorf("publishing document for binding [%d]: %w", it.Binding, err)
		}
		pending++
		published[it.Binding]++
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	for pending > 0 {
		if err := t.waitForConfirm(it.Context()); err != nil {
			return nil, err
		}
		pending--
	}
	if t.channel.IsClosed() {
		return nil, fmt.Errorf("publish channel closed: %w", amqp.ErrClosed)
	}
	for i, b := range t.bindings {
		if published[i] > 0 {
			t.be.ReportRowStats(it.Round, b.path, m.TotalRowStats(published[i]))
		}
	}
	return nil, nil
}

func (t *transactor) waitForConfirm(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-t.closed:
		if err != nil {
			return fmt.Errorf("publish channel closed: %w", err)
		}
		return fmt.Errorf("publish channel closed before confirmation: %w", amqp.ErrClosed)
	case confirmation, ok := <-t.confirms:
		if !ok {
			if reason := t.closeReason(ctx, 0); reason != nil {
				return fmt.Errorf("publish channel closed: %w", reason)
			}
			return fmt.Errorf("publish channel closed before confirmation: %w", amqp.ErrClosed)
		}
		if !confirmation.Ack {
			return fmt.Errorf("RabbitMQ nacked message %d", confirmation.DeliveryTag)
		}
		return nil
	}
}

// closeReason returns the broker's reason for closing the publish channel,
// waiting up to wait for it to arrive.
func (t *transactor) closeReason(ctx context.Context, wait time.Duration) *amqp.Error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case err := <-t.closed:
		return err
	default:
	}
	select {
	case err := <-t.closed:
		return err
	case <-ctx.Done():
	case <-timer.C:
	}
	return nil
}

func (b *exchangeBinding) message(raw json.RawMessage, packedKey []byte, routingValue any) (amqp.Publishing, string, error) {
	var body bytes.Buffer
	if err := json.Compact(&body, raw); err != nil {
		return amqp.Publishing{}, "", fmt.Errorf("compacting document: %w", err)
	}
	uuid := gjson.GetBytes(raw, "_meta.uuid")
	if uuid.Type != gjson.String || uuid.Str == "" {
		return amqp.Publishing{}, "", fmt.Errorf("document must have a string at /_meta/uuid")
	}
	hash := sha256.New()
	hash.Write(packedKey)
	hash.Write([]byte("|"))
	hash.Write([]byte(uuid.Str))

	// Fields with multiple types, objects, and arrays are packed as JSON.
	if raw, ok := routingValue.([]byte); ok {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&routingValue); err != nil {
			return amqp.Publishing{}, "", fmt.Errorf("decoding routing key field: %w", err)
		}
	}
	routingKey := b.routingKey
	switch v := routingValue.(type) {
	case string:
		routingKey = v
	case int64, uint64, float32, float64, json.Number, bool:
		encoded, err := json.Marshal(v)
		if err != nil {
			return amqp.Publishing{}, "", fmt.Errorf("encoding routing key field: %w", err)
		}
		routingKey = string(encoded)
	}
	if len(routingKey) > 255 {
		return amqp.Publishing{}, "", fmt.Errorf("routing key exceeds the AMQP limit of 255 bytes")
	}
	return amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    hex.EncodeToString(hash.Sum(nil)),
		Body:         body.Bytes(),
	}, routingKey, nil
}

func (t *transactor) Destroy() {
	if t.client != nil {
		t.client.Close()
	}
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}
