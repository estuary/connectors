package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	pc "github.com/estuary/flow/go/protocols/capture"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestValidateTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		delays   []time.Duration
		deadline time.Duration
		wantErr  error
	}{
		{"separate queue budgets", []time.Duration{20 * time.Second, 20 * time.Second}, time.Minute, nil},
		{"queue timeout", []time.Duration{31 * time.Second}, time.Minute, context.DeadlineExceeded},
		{"parent timeout", []time.Duration{20 * time.Second}, 5 * time.Second, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), tc.deadline)
				defer cancel()
				stopBroker := make(chan struct{})
				defer close(stopBroker)
				d := driver{connect: func(context.Context, *config) (*amqp.Connection, error) {
					clientConn, serverConn := net.Pipe()
					t.Cleanup(func() { clientConn.Close(); serverConn.Close() })
					go func() {
						defer serverConn.Close()
						peer := amqpPeer{t: t, conn: serverConn}
						peer.handshake()
						channel, _ := peer.readMethod(20, 10)
						peer.sendMethod(channel, 20, 11, wireFields(uint32(0)))
						for i, delay := range tc.delays {
							declared, fields := peer.readMethod(50, 10)
							queue := fmt.Sprintf("queue-%d", i)
							require.Equal(t, channel, declared)
							require.Equal(t, wireFields(uint16(0), queue, byte(1), wireTable()), fields)
							select {
							case <-time.After(delay):
							case <-stopBroker:
								return
							}
							peer.sendMethod(channel, 50, 11, wireFields(queue, uint32(0), uint32(0)))
						}
						peer.readMethod(10, 50)
						peer.sendMethod(0, 10, 51, nil)
					}()
					return amqp.Open(clientConn, amqp.Config{
						SASL: []amqp.Authentication{&amqp.PlainAuth{Username: "test", Password: "test"}}, Vhost: "/",
					})
				}}
				var bindings []*pc.Request_Validate_Binding
				for i := range tc.delays {
					bindings = append(bindings, &pc.Request_Validate_Binding{ResourceConfigJson: mustMarshal(t, resource{Queue: fmt.Sprintf("queue-%d", i)})})
				}
				started := time.Now()
				response, err := d.Validate(ctx, &pc.Request_Validate{ConfigJson: json.RawMessage(validConfigJSON), Bindings: bindings})
				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
					require.ErrorContains(t, err, `validating queue "queue-0"`)
					return
				}
				require.NoError(t, err)
				require.Greater(t, time.Since(started), connectTimeout, "each queue fits its own budget, but their total exceeds the connection budget")
				require.Len(t, response.Bindings, len(bindings))
				for i, binding := range response.Bindings {
					require.Equal(t, []string{fmt.Sprintf("queue-%d", i)}, binding.ResourcePath)
				}
			})
		})
	}
}

func testConfig(t *testing.T) config {
	t.Helper()
	if testing.Short() {
		t.Skipf("skipping %q in short mode: integration tests require RabbitMQ", t.Name())
	}
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	cfg := config{Host: "localhost", Port: 5673, ManagementPort: 15673, VirtualHost: "/", Username: "test", Password: "test"}
	for name, field := range map[string]*string{
		"RABBITMQ_HOST":         &cfg.Host,
		"RABBITMQ_VIRTUAL_HOST": &cfg.VirtualHost,
		"RABBITMQ_USERNAME":     &cfg.Username,
		"RABBITMQ_PASSWORD":     &cfg.Password,
	} {
		if value := os.Getenv(name); value != "" {
			*field = value
		}
	}
	for name, field := range map[string]*int{"RABBITMQ_PORT": &cfg.Port, "RABBITMQ_MANAGEMENT_PORT": &cfg.ManagementPort} {
		if value := os.Getenv(name); value != "" {
			port, err := strconv.Atoi(value)
			require.NoError(t, err)
			*field = port
		}
	}
	require.NoError(t, cfg.Validate())
	return cfg
}

func TestIntegration(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := buildClient(ctx, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	channel, err := client.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { channel.Close() })

	prefix := fmt.Sprintf("test-discover-%d-", time.Now().UnixNano())
	var bindings []*pc.Request_Validate_Binding
	for _, queueType := range []string{"classic", "quorum", "stream"} {
		name := prefix + queueType
		_, err := channel.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": queueType})
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := channel.QueueDelete(name, false, false, false)
			require.NoError(t, err)
		})
		resourceJSON, err := json.Marshal(resource{Queue: name})
		require.NoError(t, err)
		bindings = append(bindings, &pc.Request_Validate_Binding{ResourceConfigJson: resourceJSON})
	}
	temporary, err := channel.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	configJSON, err := json.Marshal(cfg)
	require.NoError(t, err)

	t.Run("discover", func(t *testing.T) {
		response, err := driver{}.Discover(ctx, &pc.Request_Discover{ConfigJson: configJSON})
		require.NoError(t, err)
		found := make(map[string]*pc.Response_Discovered_Binding)
		for _, binding := range response.Bindings {
			found[binding.RecommendedName] = binding
		}
		for _, queueType := range []string{"classic", "quorum", "stream"} {
			name := prefix + queueType
			require.Contains(t, found, name)
			require.JSONEq(t, fmt.Sprintf(`{"queue":%q,"queueType":%q}`, name, queueType), string(found[name].ResourceConfigJson))
			require.Equal(t, []string{"/_meta/id"}, found[name].Key)
		}
		require.NotContains(t, found, temporary.Name)
	})

	t.Run("validate", func(t *testing.T) {
		response, err := driver{}.Validate(ctx, &pc.Request_Validate{ConfigJson: configJSON, Bindings: bindings})
		require.NoError(t, err)
		require.Len(t, response.Bindings, 3)
		for i, queueType := range []string{"classic", "quorum", "stream"} {
			require.Equal(t, []string{prefix + queueType}, response.Bindings[i].ResourcePath)
		}
	})

	t.Run("missing queue", func(t *testing.T) {
		resourceJSON, err := json.Marshal(resource{Queue: prefix + "missing"})
		require.NoError(t, err)
		_, err = driver{}.Validate(ctx, &pc.Request_Validate{
			ConfigJson: configJSON,
			Bindings:   []*pc.Request_Validate_Binding{{ResourceConfigJson: resourceJSON}},
		})
		require.ErrorContains(t, err, "not accessible")
	})
}
