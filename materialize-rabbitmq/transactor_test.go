package connector

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

const testUUID = "13814000-1dd2-11b2-8000-071353030311"

func TestMessage(t *testing.T) {
	t.Parallel()
	binding := &exchangeBinding{routingKey: "orders"}
	packedKey := tuple.Tuple{int64(1)}.Pack()
	raw := json.RawMessage(`{ "id": 1, "status": "new", "_meta": { "uuid": "` + testUUID + `", "op": "d" } }`)
	msg, key, err := binding.message(raw, packedKey, "new")
	require.NoError(t, err)
	require.Equal(t, "new", key)
	require.Equal(t, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: 2,
		MessageId:    "e3b2388866528c644c4b2a3f198323fbd249ff7b8729997dcca427c401efd54d",
		Body:         []byte(`{"id":1,"status":"new","_meta":{"uuid":"` + testUUID + `","op":"d"}}`),
	}, msg)

	for _, tc := range []struct {
		name  string
		value any
		key   string
	}{
		{"string", "paid", "paid"},
		{"empty string", "", ""},
		{"escaped string", `a"b\c`, `a"b\c`},
		{"integer", int64(42), "42"},
		{"large integer", int64(9007199254740993), "9007199254740993"},
		{"unsigned integer", uint64(18446744073709551615), "18446744073709551615"},
		{"number", 1.25, "1.25"},
		{"true", true, "true"},
		{"false", false, "false"},
		{"missing", nil, "orders"},
		{"null", []byte(`null`), "orders"},
		{"object", []byte(`{"value":"new"}`), "orders"},
		{"array", []byte(`["new"]`), "orders"},
		{"JSON string", []byte(`"paid"`), "paid"},
		{"JSON number", []byte(`9007199254740993`), "9007199254740993"},
		{"JSON boolean", []byte(`false`), "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, key, err := binding.message(raw, packedKey, tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.key, key)
			require.Equal(t, msg.MessageId, message.MessageId)
		})
	}

	newClock := strings.ReplaceAll(string(raw), testUUID, "13814001-1dd2-11b2-8000-071353030311")
	newMessage, _, err := binding.message(json.RawMessage(newClock), packedKey, nil)
	require.NoError(t, err)
	require.NotEqual(t, msg.MessageId, newMessage.MessageId)
	newMessage, _, err = binding.message(raw, tuple.Tuple{int64(2)}.Pack(), nil)
	require.NoError(t, err)
	require.NotEqual(t, msg.MessageId, newMessage.MessageId)

	restarted := &exchangeBinding{routingKey: "updated"}
	replay, _, err := restarted.message(raw, packedKey, nil)
	require.NoError(t, err)
	require.Equal(t, msg.MessageId, replay.MessageId)
}

func TestMessageCompositeKey(t *testing.T) {
	t.Parallel()
	binding := &exchangeBinding{routingKey: "events"}
	for _, region := range []string{"us", "a<&>b", `a"b\c`} {
		packed := tuple.Tuple{region, int64(7)}.Pack()
		msg, key, err := binding.message(json.RawMessage(`{"_meta":{"uuid":"`+testUUID+`"}}`), packed, nil)
		require.NoError(t, err)
		require.Equal(t, "events", key)
		// A string tuple element followed by the integer 7, independent of the document.
		sum := sha256.Sum256([]byte("\x02" + region + "\x00\x15\x07|" + testUUID))
		require.Equal(t, hex.EncodeToString(sum[:]), msg.MessageId)
	}
}

func TestMessageErrors(t *testing.T) {
	t.Parallel()
	binding := &exchangeBinding{}
	for _, tc := range []struct{ name, doc, err string }{
		{"invalid JSON", `{`, "compacting document"},
		{"trailing JSON", `{} {}`, "compacting document"},
		{"missing UUID", `{"id":1}`, "string at /_meta/uuid"},
		{"nonstring UUID", `{"id":1,"_meta":{"uuid":12}}`, "string at /_meta/uuid"},
		{"empty UUID", `{"id":1,"_meta":{"uuid":""}}`, "string at /_meta/uuid"},
		{"long routing key", `{"id":1,"_meta":{"uuid":"` + testUUID + `"}}`, "routing key exceeds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := binding.message(json.RawMessage(tc.doc), tuple.Tuple{int64(1)}.Pack(), strings.Repeat("x", 256))
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestMessageMissingKey(t *testing.T) {
	t.Parallel()
	binding := &exchangeBinding{}
	raw := json.RawMessage(`{"_meta":{"uuid":"` + testUUID + `"}}`)
	msg, _, err := binding.message(raw, tuple.Tuple{nil}.Pack(), nil)
	require.NoError(t, err)
	sum := sha256.Sum256([]byte("\x00|" + testUUID))
	require.Equal(t, hex.EncodeToString(sum[:]), msg.MessageId)
	require.Equal(t, []byte(raw), msg.Body)
}

func TestMessageIDIgnoresBodyKeyFields(t *testing.T) {
	t.Parallel()
	// Message IDs are unchanged when omitted key fields are added to the body.
	for _, tc := range []struct {
		name   string
		value  any
		fields string
	}{
		{"missing empty string", "", ""},
		{"missing string", "store", ""},
		{"missing large integer", int64(9007199254740993), ""},
		{"missing boolean", false, ""},
		{"missing null", nil, ""},
		{"missing key", nil, ""},
		{"explicit null", nil, `,"store":null`},
		{"explicit value", "present", `,"store":"present"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := &exchangeBinding{}
			packed := tuple.Tuple{"a", tc.value}.Pack()
			raw := json.RawMessage(`{"id":"a","_meta":{"uuid":"` + testUUID + `"` + tc.fields + `}}`)
			msg, _, err := binding.message(raw, packed, nil)
			require.NoError(t, err)
			explicit, err := json.Marshal(map[string]any{"id": "a", "_meta": map[string]any{"uuid": testUUID, "store": tc.value}})
			require.NoError(t, err)
			withKey, _, err := binding.message(explicit, packed, nil)
			require.NoError(t, err)
			require.Equal(t, withKey.MessageId, msg.MessageId)
			require.Equal(t, []byte(raw), msg.Body)
		})
	}
}

func TestRoutingKeySelection(t *testing.T) {
	t.Parallel()
	for _, ptr := range []string{"/status", "/a~1b/~0key/0", "/~01", "/", "/0"} {
		for _, key := range []bool{false, true} {
			spec := &pf.MaterializationSpec_Binding{
				ResourcePath: []string{"orders"},
				Collection:   pf.CollectionSpec{Projections: []pf.Projection{{Field: "alias", Ptr: ptr}}},
			}
			if key {
				spec.FieldSelection.Keys = []string{"other", "alias"}
			} else {
				spec.FieldSelection.Values = []string{"other", "alias"}
			}
			binding, err := newExchangeBinding(spec, resource{Exchange: "orders", RoutingKeyField: ptr})
			require.NoError(t, err)
			require.Equal(t, 1, binding.routingKeyIndex)
			require.Equal(t, key, binding.routingKeyIsKey)
		}
	}
	_, err := newExchangeBinding(&pf.MaterializationSpec_Binding{}, resource{Exchange: "orders", RoutingKeyField: "/status"})
	require.ErrorContains(t, err, "must be selected")
}

func TestStoreSelectedRoutingKey(t *testing.T) {
	t.Parallel()
	for _, fromKey := range []bool{false, true} {
		spec := &pf.MaterializationSpec_Binding{
			ResourcePath: []string{"orders"},
			Collection: pf.CollectionSpec{Projections: []pf.Projection{
				{Field: "alias", Ptr: "/status"},
			}},
		}
		if fromKey {
			spec.FieldSelection.Keys = []string{"alias"}
		} else {
			spec.FieldSelection.Values = []string{"alias"}
		}
		binding, err := newExchangeBinding(spec, resource{Exchange: "orders", RoutingKeyField: "/status"})
		require.NoError(t, err)
		confirms := make(chan amqp.Confirmation, 1)
		channel := &testPublishChannel{publish: func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
			require.Equal(t, "orders", exchange)
			require.Equal(t, "selected", key)
			sum := sha256.Sum256([]byte("\x02selected\x00|" + testUUID))
			require.Equal(t, hex.EncodeToString(sum[:]), msg.MessageId)
			confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
			return nil
		}}
		tx := &transactor{channel: channel, confirms: confirms, bindings: []*exchangeBinding{binding}}
		storeDocuments(t, context.Background(), tx, []storedDocument{{
			Key: tuple.Tuple{"selected"}, Values: tuple.Tuple{"selected"},
			Doc: json.RawMessage(`{"id":"different","status":"unselected","_meta":{"uuid":"` + testUUID + `"}}`),
		}})
	}
}

func TestPublishFixture(t *testing.T) {
	t.Parallel()
	spec := integrationSpec(t, "publish")
	var bindings []*exchangeBinding
	for _, b := range spec.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		require.NoError(t, err)
		binding, err := newExchangeBinding(b, res)
		require.NoError(t, err)
		bindings = append(bindings, binding)
	}
	confirms := make(chan amqp.Confirmation, publishWindow)
	messages := make(map[string][]messageView)
	channel := &testPublishChannel{publish: func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
		messages[exchange] = append(messages[exchange], messageView{
			Queue: exchange + ".readback", Exchange: exchange, RoutingKey: key, Body: msg.Body,
			Properties: messageProperties{ContentType: msg.ContentType, DeliveryMode: msg.DeliveryMode, MessageID: msg.MessageId},
		})
		confirms <- amqp.Confirmation{Ack: true}
		return nil
	}}
	tx := &transactor{channel: channel, confirms: confirms, bindings: bindings}
	for _, transaction := range readTransactions(t) {
		storeDocuments(t, context.Background(), tx, transaction)
	}
	var got []messageView
	for _, b := range bindings {
		got = append(got, messages[b.exchange]...)
	}
	expected, err := os.ReadFile(".snapshots/TestIntegration-publish")
	require.NoError(t, err)
	actual, err := json.Marshal(got)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(actual))
}

type testStream struct {
	requests []pm.Request
}

func (s *testStream) Send(*pm.Response) error { return nil }
func (s *testStream) RecvMsg(req *pm.Request) error {
	if len(s.requests) == 0 {
		return io.EOF
	}
	*req = s.requests[0]
	s.requests = s.requests[1:]
	return nil
}

func storeIterator(ctx context.Context, count int) *m.StoreIterator {
	stream := &testStream{}
	for i := 0; i < count; i++ {
		stream.requests = append(stream.requests, pm.Request{Store: &pm.Request_Store{
			Binding:      uint32(i % 2),
			KeyPacked:    tuple.Tuple{int64(i)}.Pack(),
			ValuesPacked: tuple.Tuple{nil}.Pack(),
			DocJson:      json.RawMessage(fmt.Sprintf(`{"id":%d,"_meta":{"uuid":%q,"op":"d"}}`, i, testUUID)),
			Delete:       true,
			Exists:       false,
		}})
	}
	stream.requests = append(stream.requests, pm.Request{StartCommit: &pm.Request_StartCommit{}})
	return m.NewStoreIterator(ctx, stream, &pm.Request{Flush: &pm.Request_Flush{}})
}

type testPublishChannel struct {
	publish func(context.Context, string, string, bool, bool, amqp.Publishing) error
	closed  bool
}

func (ch *testPublishChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return ch.publish(ctx, exchange, key, mandatory, immediate, msg)
}
func (ch *testPublishChannel) IsClosed() bool { return ch.closed }

func testBindings() []*exchangeBinding {
	return []*exchangeBinding{
		{path: []string{"orders"}, exchange: "orders", routingKey: "fallback", routingKeyIndex: 0},
		{path: []string{"audit"}, exchange: "audit", routingKeyIndex: -1},
	}
}

func TestStoreBoundedConfirms(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	confirms := make(chan amqp.Confirmation, publishWindow)
	count := 0
	channel := &testPublishChannel{publish: func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
		require.False(t, mandatory)
		require.False(t, immediate)
		require.Equal(t, testBindings()[count%2].exchange, exchange)
		require.Equal(t, testBindings()[count%2].routingKey, key)
		require.Contains(t, string(msg.Body), `"op":"d"`)
		count++
		select {
		case confirms <- amqp.Confirmation{DeliveryTag: uint64(count), Ack: true}:
			return nil
		default:
			return errors.New("unconfirmed publish window exceeded")
		}
	}}
	tx := &transactor{channel: channel, confirms: confirms, bindings: testBindings()}
	for _, n := range []int{publishWindow * 2, 3} {
		startCommit, err := tx.Store(storeIterator(ctx, n))
		require.NoError(t, err)
		require.Nil(t, startCommit)
		require.Empty(t, confirms, "Store must consume every confirmation before returning")
	}
	require.Equal(t, publishWindow*2+3, count)
}

func TestStoreWaitsForConfirmation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	confirms := make(chan amqp.Confirmation, publishWindow)
	published := make(chan struct{}, 1)
	channel := &testPublishChannel{publish: func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		published <- struct{}{}
		return nil
	}}
	tx := &transactor{channel: channel, confirms: confirms, bindings: testBindings()}
	done := make(chan error, 1)
	go func() {
		_, err := tx.Store(storeIterator(ctx, 1))
		done <- err
	}()
	select {
	case <-published:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("Store returned before confirmation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestStoreCancelsBlockedPublish(t *testing.T) {
	t.Parallel()
	conn, server := net.Pipe()
	t.Cleanup(func() {
		conn.Close()
		server.Close()
	})
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))
	serve := func() error {
		if _, err := io.ReadFull(server, make([]byte, 8)); err != nil {
			return err
		}
		for _, step := range []struct {
			channel  uint16
			method   string
			requests int
		}{
			{0, "\x00\x0a\x00\x0a\x00\x09\x00\x00\x00\x00\x00\x00\x00\x05PLAIN\x00\x00\x00\x05en_US", 1}, // connection.start
			{0, "\x00\x0a\x00\x1e\x00\x00\x00\x02\x00\x00\x00\x00", 2},                                   // connection.tune
			{0, "\x00\x0a\x00\x29\x00", 1},                                                               // connection.open-ok
			{1, "\x00\x14\x00\x0b\x00\x00\x00\x00", 1},                                                   // channel.open-ok
			{1, "\x00\x55\x00\x0b", 0},                                                                   // confirm.select-ok
		} {
			frame := binary.BigEndian.AppendUint16([]byte{1}, step.channel)
			frame = binary.BigEndian.AppendUint32(frame, uint32(len(step.method)))
			frame = append(frame, step.method...)
			frame = append(frame, 0xce)
			if _, err := server.Write(frame); err != nil {
				return err
			}
			for i := 0; i < step.requests; i++ {
				header := make([]byte, 7)
				if _, err := io.ReadFull(server, header); err != nil {
					return err
				}
				if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(header[3:]))+1); err != nil {
					return err
				}
			}
		}
		// Read only one byte of the publish, leaving the rest of the write blocked.
		_, err := io.ReadFull(server, make([]byte, 1))
		return err
	}
	blocked := make(chan error, 1)
	go func() { blocked <- serve() }()
	client, err := amqp.Open(conn, amqp.Config{
		SASL: []amqp.Authentication{&amqp.PlainAuth{Username: "test", Password: "made-up-password"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { client.CloseDeadline(time.Now()) })
	channel, err := client.Channel()
	require.NoError(t, err)
	require.NoError(t, channel.Confirm(false))
	tx := &transactor{client: client, channel: channel, bindings: testBindings()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := tx.Store(storeIterator(ctx, 1))
		done <- err
	}()
	select {
	case err := <-blocked:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("publish did not reach the transport")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "publishing document")
	case <-time.After(time.Second):
		t.Fatal("Store did not return after cancellation")
	}
}

func TestStoreFailures(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"publish", "nack", "closed confirmations", "closed channel", "cancelled", "iterator"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			confirms := make(chan amqp.Confirmation, publishWindow)
			closed := make(chan *amqp.Error, 1)
			channel := &testPublishChannel{publish: func(context.Context, string, string, bool, bool, amqp.Publishing) error {
				switch failure {
				case "publish":
					return errors.New("publish failed")
				case "nack":
					confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: false}
				case "closed confirmations":
					close(confirms)
				case "closed channel":
					closed <- &amqp.Error{Code: amqp.NotFound, Reason: "exchange missing"}
				case "cancelled":
					cancel()
				}
				return nil
			}}
			tx := &transactor{channel: channel, confirms: confirms, closed: closed, bindings: testBindings()}
			it := storeIterator(ctx, 1)
			if failure == "iterator" {
				it = m.NewStoreIterator(ctx, &testStream{}, &pm.Request{Flush: &pm.Request_Flush{}})
			}
			_, err := tx.Store(it)
			switch failure {
			case "publish":
				require.ErrorContains(t, err, "publish failed")
			case "nack":
				require.ErrorContains(t, err, "nacked message 1")
			case "closed confirmations":
				require.ErrorIs(t, err, amqp.ErrClosed)
			case "closed channel":
				require.ErrorContains(t, err, "exchange missing")
			case "cancelled":
				require.ErrorIs(t, err, context.Canceled)
			case "iterator":
				require.ErrorIs(t, err, io.EOF)
			}
		})
	}
}

func TestStoreReportsCloseReason(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	confirms := make(chan amqp.Confirmation, publishWindow)
	closed := make(chan *amqp.Error, 1)
	count := 0
	channel := &testPublishChannel{publish: func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		count++
		if count == 1 {
			// The channel is marked closed before its close reason is delivered.
			go func() {
				time.Sleep(25 * time.Millisecond)
				closed <- &amqp.Error{Code: amqp.AccessRefused, Reason: "ACCESS_REFUSED - write access denied"}
			}()
			return nil
		}
		return amqp.ErrClosed
	}}
	tx := &transactor{channel: channel, confirms: confirms, closed: closed, bindings: testBindings()}
	_, err := tx.Store(storeIterator(ctx, 3))
	require.ErrorContains(t, err, "publishing document for binding [1]")
	require.ErrorContains(t, err, "ACCESS_REFUSED")
}

func TestWaitForConfirmReportsCloseReason(t *testing.T) {
	t.Parallel()
	for i := 0; i < 50; i++ {
		confirms := make(chan amqp.Confirmation)
		close(confirms)
		closed := make(chan *amqp.Error, 1)
		closed <- &amqp.Error{Code: amqp.NotFound, Reason: "NOT_FOUND - no exchange"}
		tx := &transactor{confirms: confirms, closed: closed}
		require.ErrorContains(t, tx.waitForConfirm(context.Background()), "NOT_FOUND")
	}
}
