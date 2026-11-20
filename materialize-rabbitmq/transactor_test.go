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
	binding := &exchangeBinding{
		routingKey:        "orders",
		routingKeyPointer: jsonPointer{"status"},
		keyPointers:       []jsonPointer{{"id"}},
	}
	raw := json.RawMessage(`{ "id": 1, "status": "new", "_meta": { "uuid": "` + testUUID + `", "op": "d" } }`)
	msg, key, err := binding.message(raw)
	require.NoError(t, err)
	require.Equal(t, "new", key)
	require.Equal(t, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: 2,
		MessageId:    "3c9a9a01a9b3fca0dd8f72f64c4a5dcd015edff5d34454a0d551c9ee3ed7bece",
		Body:         []byte(`{"id":1,"status":"new","_meta":{"uuid":"` + testUUID + `","op":"d"}}`),
	}, msg)

	for _, tc := range []struct {
		name   string
		fields string
		key    string
	}{
		{"string", `"status":"paid",`, "paid"},
		{"empty string", `"status":"",`, ""},
		{"escaped string", `"status":"a\"b\\c",`, "a\"b\\c"},
		{"integer", `"status":42,`, "42"},
		{"large integer", `"status":9007199254740993,`, "9007199254740993"},
		{"number", `"status":1.25,`, "1.25"},
		{"true", `"status":true,`, "true"},
		{"false", `"status":false,`, "false"},
		{"missing", "", "orders"},
		{"null", `"status":null,`, "orders"},
		{"object", `"status":{"value":"new"},`, "orders"},
		{"array", `"status":["new"],`, "orders"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := json.RawMessage(`{"id":1,` + tc.fields + `"_meta":{"uuid":"` + testUUID + `"}}`)
			message, key, err := binding.message(doc)
			require.NoError(t, err)
			require.Equal(t, tc.key, key)
			require.Equal(t, msg.MessageId, message.MessageId)
		})
	}

	newClock := strings.ReplaceAll(string(raw), testUUID, "13814001-1dd2-11b2-8000-071353030311")
	newMessage, _, err := binding.message(json.RawMessage(newClock))
	require.NoError(t, err)
	require.NotEqual(t, msg.MessageId, newMessage.MessageId)
}

func TestMessageCompositeKey(t *testing.T) {
	t.Parallel()
	binding := &exchangeBinding{keyPointers: []jsonPointer{{"region"}, {"seq"}}, routingKey: "events"}
	for _, tc := range []struct{ region, keyJSON string }{
		{`"us"`, `["us",7]`},
		{`"a<&>b"`, `["a<&>b",7]`},
		{`"a\"b\\c"`, `["a\"b\\c",7]`},
	} {
		msg, key, err := binding.message(json.RawMessage(`{"seq":7,"region":` + tc.region + `,"_meta":{"uuid":"` + testUUID + `"}}`))
		require.NoError(t, err)
		require.Equal(t, "events", key)
		sum := sha256.Sum256([]byte(tc.keyJSON + "|" + testUUID))
		require.Equal(t, hex.EncodeToString(sum[:]), msg.MessageId)
	}
}

func TestMessageErrors(t *testing.T) {
	t.Parallel()
	binding := &exchangeBinding{keyPointers: []jsonPointer{{"id"}}, routingKeyPointer: jsonPointer{"status"}}
	for _, tc := range []struct{ name, doc, err string }{
		{"invalid JSON", `{`, "compacting document"},
		{"trailing JSON", `{} {}`, "compacting document"},
		{"missing UUID", `{"id":1}`, "string at /_meta/uuid"},
		{"nonstring UUID", `{"id":1,"_meta":{"uuid":12}}`, "string at /_meta/uuid"},
		{"empty UUID", `{"id":1,"_meta":{"uuid":""}}`, "string at /_meta/uuid"},
		{"long routing key", `{"id":1,"status":"` + strings.Repeat("x", 256) + `","_meta":{"uuid":"` + testUUID + `"}}`, "routing key exceeds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := binding.message(json.RawMessage(tc.doc))
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestMessageMissingKey(t *testing.T) {
	t.Parallel()
	binding := &exchangeBinding{keyPointers: []jsonPointer{{"id"}}}
	raw := json.RawMessage(`{"_meta":{"uuid":"` + testUUID + `"}}`)
	msg, _, err := binding.message(raw)
	require.NoError(t, err)
	sum := sha256.Sum256([]byte(`[null]|` + testUUID))
	require.Equal(t, hex.EncodeToString(sum[:]), msg.MessageId)
	require.Equal(t, []byte(raw), msg.Body)
}

func TestMessageKeyDefaults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, defaultJSON, fields, keyJSON string }{
		{"empty string default", `""`, "", `["a",""]`},
		{"string default", `"store"`, "", `["a","store"]`},
		{"large integer default", `9007199254740993`, "", `["a",9007199254740993]`},
		{"boolean default", `false`, "", `["a",false]`},
		{"null default", `null`, "", `["a",null]`},
		{"no default", "", "", `["a",null]`},
		{"explicit null", `"store"`, `,"store":null`, `["a",null]`},
		{"explicit value", `"store"`, `,"store":"present"`, `["a","present"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := &exchangeBinding{}
			require.NoError(t, binding.setCollectionKey(pf.CollectionSpec{
				Key: []string{"/id", "/_meta/store"},
				Projections: []pf.Projection{
					{Field: "_meta_store", Ptr: "/_meta/store", IsPrimaryKey: true, Inference: pf.Inference{DefaultJson: json.RawMessage(tc.defaultJSON)}},
					{Field: "id", Ptr: "/id", IsPrimaryKey: true, Inference: pf.Inference{DefaultJson: json.RawMessage(`"default-id"`)}},
				},
			}))
			raw := json.RawMessage(`{"id":"a","_meta":{"uuid":"` + testUUID + `"` + tc.fields + `}}`)
			msg, _, err := binding.message(raw)
			require.NoError(t, err)
			sum := sha256.Sum256([]byte(tc.keyJSON + "|" + testUUID))
			require.Equal(t, hex.EncodeToString(sum[:]), msg.MessageId)
			require.Equal(t, []byte(raw), msg.Body)
		})
	}
}

func TestJSONPointer(t *testing.T) {
	t.Parallel()
	var doc any
	require.NoError(t, json.Unmarshal([]byte(`{"a/b":{"~key":["value"]},"~1":"literal","":false,"0":"zero"}`), &doc))
	for _, tc := range []struct {
		ptr   string
		value any
		found bool
	}{
		{"/a~1b/~0key/0", "value", true},
		{"/~01", "literal", true},
		{"/", false, true},
		{"/0", "zero", true},
		{"/a~1b/~0key/1", nil, false},
		{"/a~1b/~0key/01", nil, false},
		{"/a~1b/~0key/+0", nil, false},
		{"/a~1b/~0key/-", nil, false},
		{"/a~1b/~0key/-1", nil, false},
		{"/missing", nil, false},
		{"/0/child", nil, false},
	} {
		t.Run(tc.ptr, func(t *testing.T) {
			ptr, err := parsePointer(tc.ptr)
			require.NoError(t, err)
			value, found := ptr.get(doc)
			require.Equal(t, tc.found, found)
			require.Equal(t, tc.value, value)
		})
	}
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
		{path: []string{"orders", "fallback"}, exchange: "orders", routingKey: "fallback", routingKeyPointer: jsonPointer{"status"}, keyPointers: []jsonPointer{{"id"}}},
		{path: []string{"audit"}, exchange: "audit", keyPointers: []jsonPointer{{"id"}}},
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
