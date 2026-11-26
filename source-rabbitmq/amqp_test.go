package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// Exercise the AMQP client over an in-memory connection. Unlike the channel
// mocks, this covers channel closure during the stream probe and delivery tags
// assigned by the client, without requiring a running RabbitMQ server.
func TestAMQPBackfill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close() })
	t.Cleanup(func() { serverConn.Close() })
	require.NoError(t, serverConn.SetDeadline(time.Now().Add(10*time.Second)))
	acks := make(chan ackCall, 6)
	brokerDone := make(chan struct{})
	go func() {
		defer close(brokerDone)
		defer serverConn.Close()
		peer := amqpPeer{t: t, conn: serverConn}
		peer.handshake()
		queues := make(map[uint16]string)
		for _, queue := range []string{"orders", "events"} {
			channel, consumer := peer.consume(queue, wireTable("x-stream-offset", byte('S'), uint32(5), []byte("first")))
			if queue == "orders" {
				peer.sendMethod(channel, 20, 40, wireFields(uint16(amqp.PreconditionFailed), classicOffsetRejection, uint16(60), uint16(20)))
				closed, _ := peer.readMethod(20, 41)
				require.Equal(t, channel, closed)
				channel, consumer = peer.consume(queue, wireTable())
			}
			queues[channel] = queue
			peer.sendMethod(channel, 60, 21, wireFields(consumer))
			for i := 1; i <= 3; i++ {
				id, canary := i, fmt.Sprintf("o%d", i)
				headers := wireTable()
				if queue == "events" {
					id, canary = 100+i, fmt.Sprintf("e%d", 100+i)
					headers = wireTable("x-stream-offset", byte('l'), uint64(i-1))
				}
				body := fmt.Sprintf(`{"id":%d,"canary":%q}`, id, canary)
				if queue == "orders" && i == 3 {
					body = "plain text o3"
				}
				peer.sendMethod(channel, 60, 60, wireFields(consumer, uint64(i), byte(0), "", queue))
				// Basic content header with all captured AMQP properties.
				peer.sendFrame(2, channel, wireFields(uint16(60), uint16(0), uint64(len(body)), uint16(0xfff8),
					"application/json", "utf-8", headers, byte(2), byte(5), "correlation", "replies", "60000",
					fmt.Sprintf("m%d", id), uint64(1704067200), "order.created", "test", "sales"))
				peer.sendFrame(3, channel, []byte(body))
			}
		}
		for range 6 {
			channel, fields := peer.readMethod(60, 80)
			require.Len(t, fields, 9)
			require.Contains(t, queues, channel)
			acks <- ackCall{queue: queues[channel], tag: binary.BigEndian.Uint64(fields), multiple: fields[8] != 0}
		}
		channel, _ := peer.readMethod(10, 50)
		require.Zero(t, channel)
		peer.sendMethod(0, 10, 51, nil)
	}()

	client, err := amqp.Open(clientConn, amqp.Config{
		SASL: []amqp.Authentication{&amqp.PlainAuth{Username: "test", Password: "test"}}, Vhost: "/",
	})
	require.NoError(t, err)
	t.Cleanup(func() { client.CloseDeadline(time.Now()) })
	var bindings []*bindingInfo
	for index, queue := range []string{"orders", "events"} {
		binding := &bindingInfo{index: index, queue: queue, stateKey: queue + ".v0"}
		require.NoError(t, binding.consume(func() (consumerChannel, error) { return client.Channel() }))
		require.Equal(t, queue == "events", binding.isStream)
		bindings = append(bindings, binding)
	}

	fake := newFakeCaptureServer(ctx)
	done := make(chan error, 1)
	go func() {
		done <- runCapture(ctx, &boilerplate.PullOutput{Connector_CaptureServer: fake, NumBindings: 2}, bindings)
	}()
	require.True(t, receive(t, fake.responses).Opened.ExplicitAcknowledgements)
	seen := make(map[string]bool)
	var expectedAcks []ackCall
	for range 6 {
		captured := receive(t, fake.responses).Captured
		require.NotNil(t, captured)
		var doc struct {
			ID     int              `json:"id"`
			Canary string           `json:"canary"`
			Body   string           `json:"body"`
			Meta   documentMetadata `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(captured.DocJson, &doc))
		require.NotContains(t, seen, doc.Meta.ID)
		seen[doc.Meta.ID] = true
		require.Equal(t, doc.Meta.Queue, doc.Meta.RoutingKey)
		require.Empty(t, doc.Meta.Exchange)
		require.False(t, doc.Meta.Redelivered)
		require.Equal(t, "application/json", doc.Meta.ContentType)
		require.Equal(t, "utf-8", doc.Meta.ContentEncoding)
		require.Equal(t, "correlation", doc.Meta.CorrelationID)
		require.Equal(t, "replies", doc.Meta.ReplyTo)
		require.Equal(t, "60000", doc.Meta.Expiration)
		require.Equal(t, "order.created", doc.Meta.Type)
		require.Equal(t, "test", doc.Meta.UserID)
		require.Equal(t, "sales", doc.Meta.AppID)
		require.Equal(t, uint8(5), doc.Meta.Priority)
		require.Equal(t, uint8(2), doc.Meta.DeliveryMode)
		require.Equal(t, "2024-01-01T00:00:00Z", doc.Meta.Timestamp)
		checkpoint := receive(t, fake.responses).Checkpoint
		require.NotNil(t, checkpoint)
		require.True(t, checkpoint.State.MergePatch)
		var tag uint64
		if captured.Binding == 1 {
			require.Equal(t, "events", doc.Meta.Queue)
			require.NotNil(t, doc.Meta.Offset)
			tag = uint64(*doc.Meta.Offset + 1)
			require.Equal(t, 100+int(tag), doc.ID)
			require.Equal(t, fmt.Sprintf("e%d", doc.ID), doc.Canary)
			require.Equal(t, fmt.Sprintf("m%d", doc.ID), doc.Meta.MessageID)
			require.Equal(t, fmt.Sprintf("events:%d", *doc.Meta.Offset), doc.Meta.ID)
			require.JSONEq(t, fmt.Sprintf(`{"bindingStateV1":{"events.v0":{"nextOffset":%d}}}`, tag), string(checkpoint.State.UpdatedJson))
		} else {
			require.Zero(t, captured.Binding)
			require.Equal(t, "orders", doc.Meta.Queue)
			require.Nil(t, doc.Meta.Offset)
			require.Equal(t, doc.Meta.MessageID, doc.Meta.ID)
			_, err := fmt.Sscanf(doc.Meta.ID, "m%d", &tag)
			require.NoError(t, err)
			if tag == 3 {
				require.Equal(t, "plain text o3", doc.Body)
			} else {
				require.Equal(t, int(tag), doc.ID)
				require.Equal(t, fmt.Sprintf("o%d", tag), doc.Canary)
			}
			requireEmptyCheckpoint(t, checkpoint)
		}
		require.GreaterOrEqual(t, tag, uint64(1))
		require.LessOrEqual(t, tag, uint64(3))
		expectedAcks = append(expectedAcks, ackCall{queue: doc.Meta.Queue, tag: tag})
	}
	require.Empty(t, acks, "the broker must not receive acknowledgements before runtime commits")
	fake.requests <- &pc.Request{Acknowledge: &pc.Request_Acknowledge{Checkpoints: 6}}
	for _, expected := range expectedAcks {
		require.Equal(t, expected, receive(t, acks))
	}
	close(fake.requests)
	require.NoError(t, receive(t, done))
	require.Empty(t, fake.responses)
	require.NoError(t, client.CloseDeadline(time.Now().Add(time.Second)))
	receive(t, brokerDone)
}

type amqpPeer struct {
	t             *testing.T
	conn          net.Conn
	prefetchCount int
}

func (p amqpPeer) handshake() {
	p.handshakeAuth("PLAIN", "\x00test\x00test")
}

func (p amqpPeer) handshakeAuth(mechanism, response string) {
	p.t.Helper()
	header := make([]byte, 8)
	_, err := io.ReadFull(p.conn, header)
	require.NoError(p.t, err)
	require.Equal(p.t, []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}, header)
	p.sendMethod(0, 10, 10, wireFields(byte(0), byte(9), wireTable(), uint32(len(mechanism)), []byte(mechanism), uint32(5), []byte("en_US")))
	_, fields := p.readMethod(10, 11)
	require.GreaterOrEqual(p.t, len(fields), 4)
	propertiesSize := int(binary.BigEndian.Uint32(fields))
	require.GreaterOrEqual(p.t, len(fields), 4+propertiesSize)
	require.Equal(p.t, wireFields(mechanism, uint32(len(response)), []byte(response), ""), fields[4+propertiesSize:])
	p.sendMethod(0, 10, 30, wireFields(uint16(2047), uint32(131072), uint16(0)))
	p.readMethod(10, 31)
	p.readMethod(10, 40)
	p.sendMethod(0, 10, 41, wireFields(""))
}

func (p amqpPeer) consume(queue string, args []byte) (uint16, string) {
	p.t.Helper()
	channel, _ := p.readMethod(20, 10)
	p.sendMethod(channel, 20, 11, wireFields(uint32(0)))
	declared, fields := p.readMethod(50, 10)
	require.Equal(p.t, channel, declared)
	require.Equal(p.t, wireFields(uint16(0), queue, byte(1), wireTable()), fields, "queue declarations must be passive")
	p.sendMethod(channel, 50, 11, wireFields(queue, uint32(3), uint32(0)))
	qos, fields := p.readMethod(60, 10)
	require.Equal(p.t, channel, qos)
	prefetch := p.prefetchCount
	if prefetch == 0 {
		prefetch = defaultPrefetchCount
	}
	require.Equal(p.t, wireFields(uint32(0), uint16(prefetch), byte(0)), fields)
	p.sendMethod(channel, 60, 11, nil)
	consumed, fields := p.readMethod(60, 20)
	require.Equal(p.t, channel, consumed)
	prefix := wireFields(uint16(0), queue)
	require.GreaterOrEqual(p.t, len(fields), len(prefix)+1)
	require.Equal(p.t, prefix, fields[:len(prefix)])
	fields = fields[len(prefix):]
	consumerLen := int(fields[0])
	require.GreaterOrEqual(p.t, len(fields), 1+consumerLen)
	consumer := string(fields[1 : 1+consumerLen])
	require.Equal(p.t, wireFields(byte(0), args), fields[1+consumerLen:], "consumer must use manual acknowledgements and the requested offset")
	return channel, consumer
}

func (p amqpPeer) readMethod(class, method uint16) (uint16, []byte) {
	p.t.Helper()
	header := make([]byte, 7)
	_, err := io.ReadFull(p.conn, header)
	require.NoError(p.t, err)
	require.Equal(p.t, byte(1), header[0])
	size := binary.BigEndian.Uint32(header[3:])
	require.GreaterOrEqual(p.t, size, uint32(4))
	require.LessOrEqual(p.t, size, uint32(131072))
	payload := make([]byte, int(size)+1)
	_, err = io.ReadFull(p.conn, payload)
	require.NoError(p.t, err)
	require.Equal(p.t, byte(0xce), payload[size])
	require.Equal(p.t, class, binary.BigEndian.Uint16(payload))
	require.Equal(p.t, method, binary.BigEndian.Uint16(payload[2:]))
	return binary.BigEndian.Uint16(header[1:]), payload[4:size]
}

func (p amqpPeer) sendMethod(channel, class, method uint16, fields []byte) {
	p.t.Helper()
	p.sendFrame(1, channel, wireFields(class, method, fields))
}

func (p amqpPeer) sendFrame(kind byte, channel uint16, payload []byte) {
	p.t.Helper()
	_, err := p.conn.Write(wireFields(kind, channel, uint32(len(payload)), payload, byte(0xce)))
	require.NoError(p.t, err)
}

func wireTable(fields ...any) []byte {
	encoded := wireFields(fields...)
	return wireFields(uint32(len(encoded)), encoded)
}

func wireFields(fields ...any) []byte {
	var out []byte
	for _, field := range fields {
		switch v := field.(type) {
		case byte:
			out = append(out, v)
		case uint16:
			out = binary.BigEndian.AppendUint16(out, v)
		case uint32:
			out = binary.BigEndian.AppendUint32(out, v)
		case uint64:
			out = binary.BigEndian.AppendUint64(out, v)
		case string:
			if len(v) > 255 {
				panic("AMQP short string is too long")
			}
			out = append(out, byte(len(v)))
			out = append(out, v...)
		case []byte:
			out = append(out, v...)
		default:
			panic(fmt.Sprintf("unexpected AMQP test field %T", field))
		}
	}
	return out
}
